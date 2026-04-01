const express = require("express");
const app = express();

app.use(express.json());

const success = (data, sourceStatus = []) => ({
  meta: {
    request_id: `req_${Date.now()}`,
    generated_at: new Date().toISOString(),
    source_status: sourceStatus,
    warnings: []
  },
  data
});

const fail = (code, message, httpStatus = 400, sourceStatus = []) => ({
  meta: {
    request_id: `req_${Date.now()}`,
    generated_at: new Date().toISOString(),
    source_status: sourceStatus,
    warnings: []
  },
  error: {
    http_status: httpStatus,
    code,
    message,
    retryable: false
  }
});

app.post("/v1/classify-instrument", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  let instrument_type = "us_equity";
  let framework_id = "equity_core";

  if (["BABA", "TSM", "PDD", "NVO"].includes(symbol)) {
    instrument_type = "adr_equity";
    framework_id = "adr_equity_core";
  } else if (["QQQ", "SPY", "IWM", "XLF", "XLK"].includes(symbol)) {
    instrument_type = "us_etf";
    framework_id = "etf_core";
  } else if (["GLD", "GLDM", "SLV"].includes(symbol)) {
    instrument_type = "commodity_etf";
    framework_id = "commodity_etf_core";
  } else if (["BTC", "BTCUSDT", "ETH", "ETHUSDT"].includes(symbol)) {
    instrument_type = "crypto_spot";
    framework_id = "crypto_core";
  }

  return res.json(success({
    symbol,
    canonical_symbol: symbol,
    instrument_type,
    framework_id,
    confidence_score: 0.98,
    needs_user_confirmation: false
  }, [
    { provider: "mock", status: "ok", note: "mock classification used" }
  ]));
});

app.post("/v1/security-master", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const map = {
    MSFT: {
      symbol: "MSFT",
      security_name: "Microsoft Corporation",
      exchange: "NASDAQ",
      country: "United States",
      sector: "Technology",
      industry: "Software",
      trading_currency: "USD",
      reporting_currency: "USD",
      fiscal_year_end: "06-30",
      is_adr: false,
      adr_ratio: null,
      framework_id: "equity_core"
    },
    BABA: {
      symbol: "BABA",
      security_name: "Alibaba Group Holding Limited",
      exchange: "NYSE",
      country: "China",
      sector: "Consumer Discretionary",
      industry: "Internet Retail",
      trading_currency: "USD",
      reporting_currency: "CNY",
      fiscal_year_end: "03-31",
      is_adr: true,
      adr_ratio: 8.0,
      framework_id: "adr_equity_core"
    },
    QQQ: {
      symbol: "QQQ",
      security_name: "Invesco QQQ Trust",
      exchange: "NASDAQ",
      country: "United States",
      sector: "ETF",
      industry: "Large Cap Growth ETF",
      trading_currency: "USD",
      reporting_currency: "USD",
      fiscal_year_end: "12-31",
      is_adr: false,
      adr_ratio: null,
      framework_id: "etf_core"
    },
    GLD: {
      symbol: "GLD",
      security_name: "SPDR Gold Shares",
      exchange: "NYSE Arca",
      country: "United States",
      sector: "Commodity ETF",
      industry: "Gold",
      trading_currency: "USD",
      reporting_currency: "USD",
      fiscal_year_end: "12-31",
      is_adr: false,
      adr_ratio: null,
      framework_id: "commodity_etf_core"
    }
  };

  const data = map[symbol] || {
    symbol,
    security_name: `${symbol} Mock Security`,
    exchange: "NASDAQ",
    country: "United States",
    sector: "Technology",
    industry: "Software",
    trading_currency: "USD",
    reporting_currency: "USD",
    fiscal_year_end: "12-31",
    is_adr: false,
    adr_ratio: null,
    framework_id: "equity_core"
  };

  return res.json(success(data, [
    { provider: "mock", status: "ok", note: "mock security master used" }
  ]));
});

app.post("/v1/market-price-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    price_current: 452.37,
    price_timestamp: new Date().toISOString(),
    market_cap_current: 3365000000000,
    enterprise_value_current: 3290000000000,
    shares_outstanding_current: 7440000000,
    beta_snapshot: 0.91,
    ohlcv: [
      {
        ts: "2026-03-30T00:00:00Z",
        open: 449.2,
        high: 454.6,
        low: 447.9,
        close: 452.1,
        volume: 23123456
      },
      {
        ts: "2026-03-31T00:00:00Z",
        open: 452.0,
        high: 455.0,
        low: 450.1,
        close: 453.8,
        volume: 20111234
      }
    ]
  }, [
    { provider: "mock", status: "ok", note: "mock market pack used" }
  ]));
});

app.post("/v1/fundamental-actuals-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    annuals: [
      {
        fiscal_period: "FY2025",
        fiscal_year: 2025,
        period_type: "annual",
        period_end: "2025-06-30",
        filing_date: "2025-08-01",
        revenue: 260000000000,
        gross_profit: 178000000000,
        ebit: 109000000000,
        ebitda: 125000000000,
        net_income: 93000000000,
        eps_gaap: 12.5,
        eps_nongaap: 13.2,
        cfo: 118000000000,
        capex: 17000000000,
        fcff: 101000000000,
        cash: 92000000000,
        debt: 46000000000,
        net_cash: 46000000000,
        diluted_shares: 7440000000,
        gross_margin: 0.68,
        ebit_margin: 0.42,
        fcf_margin: 0.39,
        roe: 0.31,
        roic: 0.24
      }
    ],
    quarterlies: [],
    ttm: {
      fiscal_period: "TTM",
      period_type: "ttm",
      revenue: 268000000000,
      ebit: 112000000000,
      net_income: 95500000000,
      eps_gaap: 12.8,
      eps_nongaap: 13.6,
      cfo: 121000000000,
      capex: 18000000000,
      fcff: 103000000000
    }
  }, [
    { provider: "mock", status: "ok", note: "mock fundamental pack used" }
  ]));
});

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "stock-gpt-mock-api",
    endpoints: [
      "/v1/classify-instrument",
      "/v1/security-master",
      "/v1/market-price-pack",
      "/v1/fundamental-actuals-pack"
    ]
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Mock API running on port ${PORT}`);
});
