const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const ALPHAVANTAGE_API_KEY = process.env.ALPHAVANTAGE_API_KEY || "";
const FMP_API_KEY = process.env.FMP_API_KEY || "";

const success = (data, sourceStatus = [], warnings = []) => ({
  meta: {
    request_id: `req_${Date.now()}`,
    generated_at: new Date().toISOString(),
    source_status: sourceStatus,
    warnings
  },
  data
});

const fail = (code, message, httpStatus = 400, sourceStatus = [], warnings = []) => ({
  meta: {
    request_id: `req_${Date.now()}`,
    generated_at: new Date().toISOString(),
    source_status: sourceStatus,
    warnings
  },
  error: {
    http_status: httpStatus,
    code,
    message,
    retryable: false
  }
});

const isNonEmpty = (v) => v !== null && v !== undefined && v !== "";
const toNum = (v, fallback = null) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

const knownFiscalYearEnd = {
  MSFT: "06-30",
  BABA: "03-31",
  QQQ: "12-31",
  GLD: "12-31",
  AAPL: "09-30"
};

const classifyLocal = (symbol) => {
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

  return { instrument_type, framework_id };
};

async function finnhubGet(path, params = {}) {
  if (!FINNHUB_API_KEY) throw new Error("FINNHUB_API_KEY missing");
  const resp = await axios.get(`https://finnhub.io/api/v1${path}`, {
    params: { ...params, token: FINNHUB_API_KEY },
    timeout: 15000
  });
  return resp.data;
}

async function alphaGet(params = {}) {
  if (!ALPHAVANTAGE_API_KEY) throw new Error("ALPHAVANTAGE_API_KEY missing");
  const resp = await axios.get("https://www.alphavantage.co/query", {
    params: { ...params, apikey: ALPHAVANTAGE_API_KEY },
    timeout: 15000
  });
  return resp.data;
}

async function fmpGet(path, params = {}) {
  if (!FMP_API_KEY) throw new Error("FMP_API_KEY missing");
  const resp = await axios.get(`https://financialmodelingprep.com${path}`, {
    params: { ...params, apikey: FMP_API_KEY },
    timeout: 15000
  });
  return resp.data;
}

function mapFinnhubProfileToSecurityMaster(symbol, profile, fmpProfile = null) {
  const { framework_id } = classifyLocal(symbol);
  const is_adr = framework_id === "adr_equity_core";
  const adr_ratio = symbol === "BABA" ? 8.0 : null;

  return {
    symbol,
    security_name: profile?.name || fmpProfile?.companyName || symbol,
    exchange: profile?.exchange || fmpProfile?.exchangeShortName || "",
    country: profile?.country || fmpProfile?.country || "",
    sector: fmpProfile?.sector || profile?.finnhubIndustry || "",
    industry: fmpProfile?.industry || profile?.finnhubIndustry || "",
    trading_currency: profile?.currency || "USD",
    reporting_currency: profile?.currency || "USD",
    fiscal_year_end: knownFiscalYearEnd[symbol] || "12-31",
    is_adr,
    adr_ratio,
    framework_id
  };
}

function buildFinnhubOHLCV(candles) {
  if (!candles || candles.s !== "ok" || !Array.isArray(candles.t) || candles.t.length === 0) {
    return [];
  }

  const out = [];
  for (let i = 0; i < candles.t.length; i++) {
    out.push({
      ts: new Date(candles.t[i] * 1000).toISOString(),
      open: toNum(candles.o?.[i], null),
      high: toNum(candles.h?.[i], null),
      low: toNum(candles.l?.[i], null),
      close: toNum(candles.c?.[i], null),
      volume: toNum(candles.v?.[i], null)
    });
  }

  return out.filter(
    (x) =>
      x.ts &&
      x.open !== null &&
      x.high !== null &&
      x.low !== null &&
      x.close !== null &&
      x.volume !== null
  );
}

function alphaHasRateLimitOrError(payload) {
  return !!(
    payload?.Note ||
    payload?.Information ||
    payload?.["Error Message"]
  );
}

function alphaExtractGlobalQuote(payload) {
  const q = payload?.["Global Quote"];
  if (!q || typeof q !== "object" || Object.keys(q).length === 0) return null;

  const price = toNum(q["05. price"], null);
  const prevClose = toNum(q["08. previous close"], null);

  if (price === null && prevClose === null) return null;

  return {
    c: price,
    pc: prevClose,
    h: toNum(q["03. high"], null),
    l: toNum(q["04. low"], null),
    o: toNum(q["02. open"], null)
  };
}

function buildAlphaOHLCV(alphaSeries, maxPoints = 120) {
  if (!alphaSeries || typeof alphaSeries !== "object" || Object.keys(alphaSeries).length === 0) {
    return [];
  }

  const dates = Object.keys(alphaSeries).sort().slice(-maxPoints);
  return dates
    .map((d) => ({
      ts: new Date(`${d}T00:00:00Z`).toISOString(),
      open: toNum(alphaSeries[d]["1. open"], null),
      high: toNum(alphaSeries[d]["2. high"], null),
      low: toNum(alphaSeries[d]["3. low"], null),
      close: toNum(alphaSeries[d]["4. close"], null),
      volume: toNum(alphaSeries[d]["5. volume"], null)
    }))
    .filter(
      (x) =>
        x.ts &&
        x.open !== null &&
        x.high !== null &&
        x.low !== null &&
        x.close !== null &&
        x.volume !== null
    );
}

app.post("/v1/classify-instrument", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const { instrument_type, framework_id } = classifyLocal(symbol);

  return res.json(success({
    symbol,
    canonical_symbol: symbol,
    instrument_type,
    framework_id,
    confidence_score: 0.98,
    needs_user_confirmation: false
  }, [
    { provider: "local-router", status: "ok", note: "local classification used" }
  ]));
});

app.post("/v1/security-master", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const sourceStatus = [];
  const warnings = [];
  let fmpProfile = null;

  try {
    if (FMP_API_KEY) {
      const fmpData = await fmpGet(`/api/v3/profile/${symbol}`);
      if (Array.isArray(fmpData) && fmpData.length > 0) {
        fmpProfile = fmpData[0];
        sourceStatus.push({ provider: "fmp", status: "ok", note: "fmp profile augment loaded" });
      } else {
        sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp profile empty" });
      }
    } else {
      sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp api key missing" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp", status: "partial", note: `fmp profile unavailable: ${e.message}` });
  }

  try {
    const profile = await finnhubGet("/stock/profile2", { symbol });

    if (!profile || !profile.name) {
      throw new Error("finnhub profile empty");
    }

    sourceStatus.unshift({ provider: "finnhub", status: "ok", note: "primary profile loaded" });

    return res.json(success(
      mapFinnhubProfileToSecurityMaster(symbol, profile, fmpProfile),
      sourceStatus,
      warnings
    ));
  } catch (e) {
    sourceStatus.unshift({ provider: "finnhub", status: "partial", note: `primary profile unavailable: ${e.message}` });
  }

  if (fmpProfile) {
    const { framework_id } = classifyLocal(symbol);
    const is_adr = framework_id === "adr_equity_core";
    const adr_ratio = symbol === "BABA" ? 8.0 : null;

    return res.json(success({
      symbol,
      security_name: fmpProfile.companyName || symbol,
      exchange: fmpProfile.exchangeShortName || "",
      country: fmpProfile.country || "",
      sector: fmpProfile.sector || "",
      industry: fmpProfile.industry || "",
      trading_currency: fmpProfile.currency || "USD",
      reporting_currency: fmpProfile.currency || "USD",
      fiscal_year_end: knownFiscalYearEnd[symbol] || "12-31",
      is_adr,
      adr_ratio,
      framework_id
    }, sourceStatus, ["Primary source unavailable, using FMP fallback for security master."]));
  }

  return res.status(502).json(fail(
    "ALL_PROVIDERS_UNAVAILABLE",
    "Unable to load security master from Finnhub/FMP.",
    502,
    sourceStatus
  ));
});

app.post("/v1/market-price-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  const historyYears = Math.max(1, Math.min(10, Number(req.body.history_years || 1)));
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const sourceStatus = [];
  const warnings = [];

  const nowSec = Math.floor(Date.now() / 1000);
  const fromSec = nowSec - historyYears * 365 * 24 * 60 * 60;

  let fmpProfile = null;
  try {
    if (FMP_API_KEY) {
      const fmpData = await fmpGet(`/api/v3/profile/${symbol}`);
      if (Array.isArray(fmpData) && fmpData.length > 0) {
        fmpProfile = fmpData[0];
        sourceStatus.push({ provider: "fmp", status: "ok", note: "fmp profile augment loaded" });
      } else {
        sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp profile empty" });
      }
    } else {
      sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp api key missing" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp", status: "partial", note: `fmp profile unavailable: ${e.message}` });
  }

  try {
    const [quote, profile, candles] = await Promise.all([
      finnhubGet("/quote", { symbol }),
      finnhubGet("/stock/profile2", { symbol }),
      finnhubGet("/stock/candle", {
        symbol,
        resolution: "D",
        from: fromSec,
        to: nowSec
      })
    ]);

    const priceCurrent = toNum(quote?.c, null);
    const ohlcv = buildFinnhubOHLCV(candles);

    const finnhubFailureReasons = [];
    if (priceCurrent === null) finnhubFailureReasons.push("quote missing c");
    if (!candles) finnhubFailureReasons.push("candles payload missing");
    if (candles && candles.s !== "ok") finnhubFailureReasons.push(`candles status ${String(candles.s)}`);
    if (candles && Array.isArray(candles.t) && candles.t.length === 0) finnhubFailureReasons.push("candles empty");
    if (ohlcv.length === 0) finnhubFailureReasons.push("ohlcv normalized empty");

    if (priceCurrent === null || ohlcv.length === 0) {
      throw new Error(finnhubFailureReasons.join("; ") || "finnhub market pack incomplete");
    }

    const sharesOutstanding =
      isNonEmpty(fmpProfile?.sharesOutstanding)
        ? toNum(fmpProfile.sharesOutstanding, null)
        : isNonEmpty(profile?.shareOutstanding)
          ? toNum(profile.shareOutstanding, null) * 1000000
          : null;

    const marketCap =
      isNonEmpty(fmpProfile?.mktCap)
        ? toNum(fmpProfile.mktCap, null)
        : sharesOutstanding !== null
          ? sharesOutstanding * priceCurrent
          : null;

    const beta =
      isNonEmpty(fmpProfile?.beta) ? toNum(fmpProfile.beta, null) : null;

    sourceStatus.unshift({ provider: "finnhub", status: "ok", note: "primary quote/profile/candles loaded" });

    return res.json(success({
      price_current: priceCurrent,
      price_timestamp: quote?.t ? new Date(quote.t * 1000).toISOString() : new Date().toISOString(),
      market_cap_current: marketCap,
      enterprise_value_current: marketCap,
      shares_outstanding_current: sharesOutstanding,
      beta_snapshot: beta,
      ohlcv
    }, sourceStatus, warnings));
  } catch (e) {
    sourceStatus.unshift({
      provider: "finnhub",
      status: "partial",
      note: `primary market pack unavailable: ${e.message}`
    });
    warnings.push("Finnhub market pack unavailable, attempting Alpha fallback.");
  }

  try {
    if (!ALPHAVANTAGE_API_KEY) {
      throw new Error("ALPHAVANTAGE_API_KEY missing");
    }

    const [quoteRaw, dailyRaw] = await Promise.all([
      alphaGet({ function: "GLOBAL_QUOTE", symbol }),
      alphaGet({ function: "TIME_SERIES_DAILY", symbol, outputsize: "compact" })
    ]);

    if (alphaHasRateLimitOrError(quoteRaw) || alphaHasRateLimitOrError(dailyRaw)) {
      const note =
        quoteRaw?.Note ||
        quoteRaw?.Information ||
        quoteRaw?.["Error Message"] ||
        dailyRaw?.Note ||
        dailyRaw?.Information ||
        dailyRaw?.["Error Message"] ||
        "alpha returned note/error payload";

      sourceStatus.push({ provider: "alpha_vantage", status: "partial", note });

      return res.status(502).json(fail(
        "UPSTREAM_PARTIAL_DATA",
        "Alpha Vantage returned a note/error payload instead of usable market data.",
        502,
        sourceStatus,
        warnings
      ));
    }

    const quote = alphaExtractGlobalQuote(quoteRaw);
    const ohlcv = buildAlphaOHLCV(dailyRaw?.["Time Series (Daily)"], 120);

    const alphaReasons = [];
    if (!quote) alphaReasons.push("global quote missing usable fields");
    if (ohlcv.length === 0) alphaReasons.push("daily series empty");

    if (!quote || ohlcv.length === 0) {
      sourceStatus.push({
        provider: "alpha_vantage",
        status: "partial",
        note: alphaReasons.join("; ") || "alpha payload missing usable quote/daily fields"
      });

      return res.status(502).json(fail(
        "UPSTREAM_PARTIAL_DATA",
        "Alpha Vantage response did not contain usable quote/time-series data.",
        502,
        sourceStatus,
        warnings
      ));
    }

    const sharesOutstanding =
      isNonEmpty(fmpProfile?.sharesOutstanding)
        ? toNum(fmpProfile.sharesOutstanding, null)
        : null;

    const marketCap =
      isNonEmpty(fmpProfile?.mktCap)
        ? toNum(fmpProfile.mktCap, null)
        : sharesOutstanding !== null && quote.c !== null
          ? sharesOutstanding * quote.c
          : null;

    const beta =
      isNonEmpty(fmpProfile?.beta) ? toNum(fmpProfile.beta, null) : null;

    sourceStatus.push({ provider: "alpha_vantage", status: "ok", note: "alpha quote/daily fallback loaded" });

    return res.json(success({
      price_current: quote.c,
      price_timestamp: new Date().toISOString(),
      market_cap_current: marketCap,
      enterprise_value_current: marketCap,
      shares_outstanding_current: sharesOutstanding,
      beta_snapshot: beta,
      ohlcv
    }, sourceStatus, warnings));
  } catch (e) {
    if (!sourceStatus.some((s) => s.provider === "alpha_vantage")) {
      sourceStatus.push({
        provider: "alpha_vantage",
        status: "partial",
        note: `alpha fallback unavailable: ${e.message}`
      });
    }
  }

  return res.status(502).json(fail(
    "ALL_PROVIDERS_UNAVAILABLE",
    "Unable to load market pack from Finnhub/Alpha.",
    502,
    sourceStatus,
    warnings
  ));
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
    { provider: "mock", status: "ok", note: "fundamental pack still mock in phase 1" }
  ]));
});

app.post("/v1/estimates-targets-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    eps_fy0_est: 5.0,
    eps_fy1_est: 5.5,
    eps_fy2_est: 6.1,
    revenue_fy0_est: 10000000000,
    revenue_fy1_est: 11000000000,
    revenue_fy2_est: 12100000000,
    target_price_consensus: 100.0,
    target_price_low: 80.0,
    target_price_high: 120.0,
    analyst_count: 10,
    estimate_revision_direction: "flat"
  }, [
    { provider: "mock", status: "ok", note: "estimates pack still mock in phase 1" }
  ]));
});

app.post("/v1/macro-breadth-liquidity-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    macro_regime: "disinflation_with_stable_growth",
    market_state: "trend_up_but_narrowing_breadth",
    liquidity_state: "neutral_to_tightening",
    should_enter_valuation: "scenario_only",
    risk_free_rate: 0.0435,
    yield_curve_slope: 0.0021,
    usd_context: "slightly_firm",
    breadth_score: 58.4,
    breadth_health: "neutral",
    breadth_notes: [
      "Cap-weight index leadership remains concentrated",
      "Participation above 200DMA is healthy but not expanding",
      "Liquidity backdrop is not fully risk-on"
    ]
  }, [
    { provider: "mock", status: "ok", note: "macro pack still mock in phase 1" }
  ]));
});

app.post("/v1/filings-transcripts-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    filings: [],
    transcripts: [],
    guidance_notes: ["filings/transcripts pack still mock in phase 1"]
  }, [
    { provider: "mock", status: "ok", note: "filings/transcripts pack still mock in phase 1" }
  ]));
});

app.post("/v1/technical-structure-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(success({
    trend_state: "mixed",
    momentum_state: "neutral",
    structure_tags: ["insufficient_context"],
    key_dates: [],
    indicators: {
      ma20: 100,
      ma50: 98,
      ma200: 90,
      rsi14: 50,
      macd: 0,
      atr: 2
    },
    support_resistance: [
      {
        kind: "support",
        low: 97,
        high: 99,
        strength: "medium",
        basis: ["swing"],
        note: "mock support zone"
      },
      {
        kind: "resistance",
        low: 101,
        high: 103,
        strength: "medium",
        basis: ["swing"],
        note: "mock resistance zone"
      }
    ],
    valuation_channels_available: false,
    channel_notes: ["technical structure pack still mock in phase 1"]
  }, [
    { provider: "mock", status: "ok", note: "technical pack still mock in phase 1" }
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
      "/v1/fundamental-actuals-pack",
      "/v1/estimates-targets-pack",
      "/v1/macro-breadth-liquidity-pack",
      "/v1/filings-transcripts-pack",
      "/v1/technical-structure-pack"
    ]
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`API running on port ${PORT}`);
});
