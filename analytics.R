# ============================================================
#  StockIQ – R Analytics Microservice
#  Framework : plumber (REST API)
#  Packages  : rugarch (GARCH), moments (stats), DBI, RPostgres
#  Run       : Rscript entrypoint.R
# ============================================================

library(plumber)
library(rugarch)
library(moments)
library(DBI)
library(RPostgres)
library(jsonlite)

# ─────────────────────────────────────────────────────────
#  DB connection helper
# ─────────────────────────────────────────────────────────
get_db_conn <- function() {
  dbConnect(
    Postgres(),
    host     = Sys.getenv("DB_HOST",     "localhost"),
    port     = as.integer(Sys.getenv("DB_PORT", "5432")),
    dbname   = Sys.getenv("DB_NAME",     "stockiq"),
    user     = Sys.getenv("DB_USER",     "stockiq"),
    password = Sys.getenv("DB_PASSWORD", "stockiq")
  )
}

# ─────────────────────────────────────────────────────────
#  Load daily close prices for a symbol
# ─────────────────────────────────────────────────────────
load_closes <- function(symbol, exchange, n = 252) {
  con <- get_db_conn()
  on.exit(dbDisconnect(con))

  sql <- "
    SELECT close
    FROM ohlcv_1d
    WHERE symbol = $1 AND exchange = $2
    ORDER BY time_bucket DESC
    LIMIT $3
  "
  result <- dbGetQuery(con, sql, params = list(symbol, exchange, n))
  if (nrow(result) < 10) return(NULL)
  # return in chronological order
  rev(result$close)
}

# ─────────────────────────────────────────────────────────
#  Log returns
# ─────────────────────────────────────────────────────────
log_returns <- function(prices) {
  diff(log(prices))
}

# ─────────────────────────────────────────────────────────
#  GARCH(1,1) volatility clustering
# ─────────────────────────────────────────────────────────

#* @get /volatility
#* @param symbol NSE/BSE ticker symbol
#* @param exchange NSE or BSE
#* @serializer json
function(symbol, exchange) {
  tryCatch({
    prices <- load_closes(symbol, exchange)
    if (is.null(prices)) {
      stop("Insufficient data")
    }

    rets <- log_returns(prices)

    # Specify GARCH(1,1) with normal innovations
    spec <- ugarchspec(
      variance.model = list(model = "sGARCH", garchOrder = c(1, 1)),
      mean.model     = list(armaOrder = c(0, 0), include.mean = TRUE),
      distribution.model = "norm"
    )

    fit  <- ugarchfit(spec = spec, data = rets, solver = "hybrid")
    sigma_last <- tail(sigma(fit), 1)   # last conditional vol (daily)
    sigma_ann  <- sigma_last * sqrt(252)

    # Regime classification
    regime <- if (sigma_ann < 0.20) "low" else if (sigma_ann < 0.45) "medium" else "high"

    # EWM vol for comparison
    ewm_vol <- sd(tail(rets, 20)) * sqrt(252)

    list(
      symbol      = symbol,
      regime      = regime,
      garch_sigma = round(as.numeric(sigma_ann), 6),
      ewm_vol     = round(ewm_vol, 6)
    )
  }, error = function(e) {
    list(error = conditionMessage(e))
  })
}


# ─────────────────────────────────────────────────────────
#  Quantile statistics
# ─────────────────────────────────────────────────────────

#* @get /stats
#* @param symbol
#* @param exchange
#* @serializer json
function(symbol, exchange) {
  tryCatch({
    prices <- load_closes(symbol, exchange)
    if (is.null(prices)) stop("Insufficient data")

    rets <- log_returns(prices) * 100   # percentage returns

    list(
      symbol   = symbol,
      p5       = round(quantile(rets, 0.05), 4),
      p25      = round(quantile(rets, 0.25), 4),
      median   = round(median(rets), 4),
      p75      = round(quantile(rets, 0.75), 4),
      p95      = round(quantile(rets, 0.95), 4),
      skewness = round(skewness(rets), 4),
      kurtosis = round(kurtosis(rets), 4)
    )
  }, error = function(e) {
    list(error = conditionMessage(e))
  })
}


# ─────────────────────────────────────────────────────────
#  Rolling correlation between two symbols
# ─────────────────────────────────────────────────────────

#* @get /correlation
#* @param symbol_a
#* @param symbol_b
#* @param exchange
#* @param window Rolling window (default 20)
#* @serializer json
function(symbol_a, symbol_b, exchange, window = 20) {
  tryCatch({
    pa <- load_closes(symbol_a, exchange)
    pb <- load_closes(symbol_b, exchange)
    n  <- min(length(pa), length(pb))
    if (n < as.integer(window) + 5) stop("Insufficient data")

    ra <- log_returns(tail(pa, n))
    rb <- log_returns(tail(pb, n))

    w  <- as.integer(window)
    rolling_cor <- sapply((w + 1):length(ra), function(i) {
      cor(ra[(i - w):(i - 1)], rb[(i - w):(i - 1)])
    })

    list(
      symbol_a    = symbol_a,
      symbol_b    = symbol_b,
      rolling_cor = round(rolling_cor, 4),
      latest_cor  = round(tail(rolling_cor, 1), 4)
    )
  }, error = function(e) {
    list(error = conditionMessage(e))
  })
}


# ─────────────────────────────────────────────────────────
#  Health check
# ─────────────────────────────────────────────────────────

#* @get /health
#* @serializer json
function() {
  list(status = "ok", service = "r-analytics")
}