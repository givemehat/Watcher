"""
StockIQ – WebSocket Gateway
Architecture:
  Redis Pub/Sub (market:ticks) ──► asyncio listener
                                   ──► fan-out to connected WS clients
                                       filtered by their subscribed symbols

Each client sends a subscribe message specifying which symbols it wants.
The server streams only matching ticks back to that client.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio.client import PubSub

from core.config import settings
from core.redis_client import redis_pool
from models.schemas import WSSubscribeMsg, WSTickMsg, WSErrorMsg

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────────────────
#  Connection manager
# ─────────────────────────────────────────────────────────
class ConnectionManager:
    """
    Maintains the set of active WebSocket connections and their subscriptions.
    Thread-safe via asyncio (single-threaded event loop).
    """

    def __init__(self):
        # ws_id → WebSocket
        self._connections: Dict[int, WebSocket] = {}
        # ws_id → set of "EXCHANGE:SYMBOL" strings
        self._subscriptions: Dict[int, Set[str]] = {}
        self._id_counter = 0

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

    async def connect(self, ws: WebSocket) -> int:
        await ws.accept()
        cid = self._next_id()
        self._connections[cid] = ws
        self._subscriptions[cid] = set()
        logger.info("WS client %d connected (total: %d)", cid, len(self._connections))
        return cid

    def disconnect(self, cid: int):
        self._connections.pop(cid, None)
        self._subscriptions.pop(cid, None)
        logger.info("WS client %d disconnected (total: %d)", cid, len(self._connections))

    def subscribe(self, cid: int, symbols: list[str]):
        if cid in self._subscriptions:
            self._subscriptions[cid].update(s.upper() for s in symbols)

    def unsubscribe(self, cid: int, symbols: list[str]):
        if cid in self._subscriptions:
            self._subscriptions[cid].difference_update(s.upper() for s in symbols)

    async def broadcast_tick(self, tick_payload: str):
        """
        Fan-out a raw tick JSON string to all clients subscribed to that symbol.
        Parsing is done once here; only matching clients receive the message.
        """
        try:
            data = json.loads(tick_payload)
        except json.JSONDecodeError:
            return

        symbol_key = f"{data.get('exchange', '')}:{data.get('symbol', '')}".upper()

        dead_clients: list[int] = []
        for cid, ws in self._connections.items():
            subs = self._subscriptions.get(cid, set())
            # Clients subscribed to "*" receive all ticks
            if "*" in subs or symbol_key in subs:
                try:
                    await ws.send_text(tick_payload)
                except Exception:
                    dead_clients.append(cid)

        for cid in dead_clients:
            self.disconnect(cid)

    @property
    def total_connections(self) -> int:
        return len(self._connections)


manager = ConnectionManager()


# ─────────────────────────────────────────────────────────
#  Redis subscriber loop (singleton background task)
# ─────────────────────────────────────────────────────────
_redis_listener_task: asyncio.Task | None = None


async def _redis_listener():
    """
    Long-running coroutine that subscribes to Redis market channel
    and broadcasts each message to connected WS clients.
    """
    redis = redis_pool.client
    pubsub: PubSub = redis.pubsub()
    await pubsub.subscribe(settings.REDIS_MARKET_CHANNEL)
    logger.info("Redis listener subscribed to %s", settings.REDIS_MARKET_CHANNEL)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                if manager.total_connections > 0:
                    await manager.broadcast_tick(data)
    except asyncio.CancelledError:
        await pubsub.unsubscribe()
        logger.info("Redis listener cancelled cleanly")
    except Exception as exc:
        logger.error("Redis listener error: %s – restarting in 3s", exc)
        await asyncio.sleep(3)
        asyncio.create_task(_redis_listener())


def start_redis_listener():
    global _redis_listener_task
    if _redis_listener_task is None or _redis_listener_task.done():
        _redis_listener_task = asyncio.create_task(_redis_listener())


# ─────────────────────────────────────────────────────────
#  WebSocket endpoint
# ─────────────────────────────────────────────────────────
@router.websocket("/market")
async def websocket_market(ws: WebSocket):
    """
    WebSocket endpoint for real-time market ticks.

    Protocol:
      Client → Server:  {"action": "subscribe",   "symbols": ["NSE:RELIANCE", "NSE:INFY"]}
                        {"action": "unsubscribe",  "symbols": ["NSE:RELIANCE"]}
      Server → Client:  <StockTick JSON>
    """
    start_redis_listener()
    cid = await manager.connect(ws)

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = WSSubscribeMsg.model_validate_json(raw)
            except Exception:
                error = WSErrorMsg(message="Invalid message format").model_dump_json()
                await ws.send_text(error)
                continue

            if msg.action == "subscribe":
                manager.subscribe(cid, msg.symbols)
                await ws.send_text(
                    json.dumps({"type": "ack", "subscribed": msg.symbols})
                )
            elif msg.action == "unsubscribe":
                manager.unsubscribe(cid, msg.symbols)
                await ws.send_text(
                    json.dumps({"type": "ack", "unsubscribed": msg.symbols})
                )
            else:
                error = WSErrorMsg(message=f"Unknown action: {msg.action}").model_dump_json()
                await ws.send_text(error)

    except WebSocketDisconnect:
        manager.disconnect(cid)
    except Exception as exc:
        logger.error("WS client %d error: %s", cid, exc)
        manager.disconnect(cid)