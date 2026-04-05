const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const ALPHAVANTAGE_API_KEY = process.env.ALPHAVANTAGE_API_KEY || "";
const FMP_API_KEY = process.env.FMP_API_KEY || "";
const MARKETSTACK_ACCESS_KEY = process.env.MARKETSTACK_ACCESS_KEY || "";

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
    timeout: 20000
  });
  return resp.data;
}

async function fmpStableGet(path, params = {}) {
  if (!FMP_API_KEY) throw new Error("FMP_API_KEY missing");
  const resp = await axios.get(`https://financialmodelingprep.com/stable/${path}`, {
    params: { ...params, apikey: FMP_API_KEY },
    timeout: 15000
  });
  return resp.data;
}

async function marketstackGet(path = "eod", params = {}) {
  if (!MARKETSTACK_ACCESS_KEY) throw new Error("MARKETSTACK_ACCESS_KEY missing");
  const resp = await axios.get(`https://api.marketstack.com/v1/${path}`, {
    params: { ...params, access_key: MARKETSTACK_ACCESS_KEY },
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
    exchange: profile?.exchange || fmpProfile?.exchange || "",
    country: profile?.country || fmpProfile?.country || "",
    sector: fmpProfile?.sector || profile?.finnhubIndustry || "",
    industry: fmpProfile?.industry || profile?.finnhubIndustry || "",
    trading_currency: profile?.currency || fmpProfile?.currency || "USD",
    reporting_currency: profile?.currency || fmpProfile?.currency || "USD",
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

function normalizeFmpProfile(payload) {
  if (!payload) return null;
  if (Array.isArray(payload) && payload.length > 0) return payload[0];
  if (typeof payload === "object" && !Array.isArray(payload)) return payload;
  return null;
}

function buildFmpOHLCV(payload, maxPoints = 252) {
  let rows = [];

  if (Array.isArray(payload)) {
    rows = payload;
  } else if (payload?.historical && Array.isArray(payload.historical)) {
    rows = payload.historical;
  } else if (payload?.data && Array.isArray(payload.data)) {
    rows = payload.data;
  } else if (payload?.results && Array.isArray(payload.results)) {
    rows = payload.results;
  }

  return rows
    .slice(0, maxPoints)
    .map((r) => ({
      ts: r.date ? new Date(`${r.date}T00:00:00Z`).toISOString() : null,
      open: toNum(r.open, null),
      high: toNum(r.high, null),
      low: toNum(r.low, null),
      close: toNum(r.close, null),
      volume: toNum(r.volume, null)
    }))
    .filter(
      (x) =>
        x.ts &&
        x.open !== null &&
        x.high !== null &&
        x.low !== null &&
        x.close !== null &&
        x.volume !== null
    )
    .sort((a, b) => a.ts.localeCompare(b.ts));
}

function buildMarketstackOHLCV(payload, maxPoints = 252) {
  const rows = Array.isArray(payload?.data) ? payload.data : [];

  return rows
    .slice(0, maxPoints)
    .map((r) => ({
      ts: r.date ? new Date(r.date).toISOString() : null,
      open: toNum(r.open, null),
      high: toNum(r.high, null),
      low: toNum(r.low, null),
      close: toNum(r.close, null),
      volume: toNum(r.volume, null)
    }))
    .filter(
      (x) =>
        x.ts &&
        x.open !== null &&
        x.high !== null &&
        x.low !== null &&
        x.close !== null &&
        x.volume !== null
    )
    .sort((a, b) => a.ts.localeCompare(b.ts));
}

function formatDateYYYYMMDD(dateObj) {
  const y = dateObj.getUTCFullYear();
  const m = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function normalizeAlphaReports(payload, annualKey = "annualReports", quarterlyKey = "quarterlyReports") {
  return {
    annual: Array.isArray(payload?.[annualKey]) ? payload[annualKey] : [],
    quarterly: Array.isArray(payload?.[quarterlyKey]) ? payload[quarterlyKey] : []
  };
}

function normalizeAlphaShares(payload) {
  const annual =
    Array.isArray(payload?.annualSharesOutstanding)
      ? payload.annualSharesOutstanding
      : [];

  const quarterly =
    Array.isArray(payload?.quarterlySharesOutstanding)
      ? payload.quarterlySharesOutstanding
      : [];

  return { annual, quarterly };
}

function buildAlphaSharesMap(rows) {
  const m = new Map();
  for (const r of rows) {
    const key = r.fiscalDateEnding || r.date || null;
    if (!key) continue;
    const diluted = toNum(
      r.dilutedSharesOutstanding ||
      r.weightedAverageShsOutDil ||
      r.sharesOutstanding ||
      r.commonSharesOutstanding,
      null
    );
    if (diluted !== null) m.set(key, diluted);
  }
  return m;
}

function alphaChoose(...vals) {
  for (const v of vals) {
    if (v !== null && v !== undefined && v !== "") return v;
  }
  return null;
}

function buildAlphaStatementMapByDate(rows) {
  const m = new Map();
  for (const r of rows) {
    const key = r.fiscalDateEnding || r.date || null;
    if (!key) continue;
    m.set(key, r);
  }
  return m;
}

function buildAlphaUnifiedPeriods(incomeRows, balanceRows, cashRows, sharesRows, periodType) {
  const incomeMap = buildAlphaStatementMapByDate(incomeRows);
  const balanceMap = buildAlphaStatementMapByDate(balanceRows);
  const cashMap = buildAlphaStatementMapByDate(cashRows);
  const sharesMap = buildAlphaSharesMap(sharesRows);

  const keys = Array.from(new Set([
    ...incomeMap.keys(),
    ...balanceMap.keys(),
    ...cashMap.keys(),
    ...sharesMap.keys()
  ])).sort().reverse();

  return keys.map((key) => {
    const i = incomeMap.get(key) || {};
    const b = balanceMap.get(key) || {};
    const c = cashMap.get(key) || {};
    const dilutedShares = sharesMap.get(key) ?? null;

    const revenue = toNum(alphaChoose(i.totalRevenue, i.revenue), null);
    const grossProfit = toNum(alphaChoose(i.grossProfit), null);
    const ebit = toNum(alphaChoose(i.operatingIncome, i.ebit), null);
    const ebitda = toNum(alphaChoose(i.ebitda, i.EBITDA), null);
    const netIncome = toNum(alphaChoose(i.netIncome), null);
    const epsGaap = toNum(alphaChoose(i.reportedEPS, i.dilutedEPS, i.eps), null);
    const epsNonGaap = epsGaap;

    const cfo = toNum(alphaChoose(
      c.operatingCashflow,
      c.operatingCashFlow
    ), null);

    const capexRaw = toNum(alphaChoose(
      c.capitalExpenditures,
      c.capitalExpenditure
    ), null);

    const capex = capexRaw === null ? null : Math.abs(capexRaw);
    const fcff = (cfo !== null && capex !== null) ? (cfo - capex) : null;

    const cash = toNum(alphaChoose(
      b.cashAndCashEquivalentsAtCarryingValue,
      b.cashAndShortTermInvestments,
      b.cashAndCashEquivalents
    ), null);

    const debt = toNum(alphaChoose(
      b.shortLongTermDebtTotal,
      b.longTermDebtNoncurrent,
      b.currentDebtAndCapitalLeaseObligation
    ), null);

    const netCash = (cash !== null && debt !== null) ? (cash - debt) : null;

    const grossMargin = revenue !== null && grossProfit !== null && revenue !== 0 ? grossProfit / revenue : null;
    const ebitMargin = revenue !== null && ebit !== null && revenue !== 0 ? ebit / revenue : null;
    const fcfMargin = revenue !== null && fcff !== null && revenue !== 0 ? fcff / revenue : null;

    const fiscalYear = toNum((key || "").slice(0, 4), null);

    return {
      fiscal_period: periodType === "annual"
        ? `FY${fiscalYear ?? ""}`
        : alphaChoose(i.fiscalDateEnding, key),
      fiscal_year: fiscalYear,
      period_type: periodType,
      period_end: key,
      filing_date: null,
      revenue,
      gross_profit: grossProfit,
      ebit,
      ebitda,
      net_income: netIncome,
      eps_gaap: epsGaap,
      eps_nongaap: epsNonGaap,
      cfo,
      capex,
      fcff,
      cash,
      debt,
      net_cash: netCash,
      diluted_shares: dilutedShares,
      gross_margin: grossMargin,
      ebit_margin: ebitMargin,
      fcf_margin: fcfMargin,
      roe: null,
      roic: null
    };
  });
}

function buildAlphaTTM(incomeQuarterly, balanceQuarterly, cashQuarterly, sharesQuarterly) {
  const iq = [...incomeQuarterly].slice(0, 4);
  const cq = [...cashQuarterly].slice(0, 4);
  const b0 = balanceQuarterly[0] || {};
  const sq = sharesQuarterly[0] || {};

  const sum = (rows, fieldNames) => {
    const vals = rows.map((r) => toNum(alphaChoose(...fieldNames.map((f) => r[f])), null)).filter((v) => v !== null);
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  };

  const revenue = sum(iq, ["totalRevenue", "revenue"]);
  const grossProfit = sum(iq, ["grossProfit"]);
  const ebit = sum(iq, ["operatingIncome", "ebit"]);
  const ebitda = sum(iq, ["ebitda", "EBITDA"]);
  const netIncome = sum(iq, ["netIncome"]);
  const cfo = sum(cq, ["operatingCashflow", "operatingCashFlow"]);
  const capexAbs = (() => {
    const vals = cq
      .map((r) => toNum(alphaChoose(r.capitalExpenditures, r.capitalExpenditure), null))
      .filter((v) => v !== null)
      .map((v) => Math.abs(v));
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  })();

  const fcff = (cfo !== null && capexAbs !== null) ? (cfo - capexAbs) : null;

  const cash = toNum(alphaChoose(
    b0.cashAndCashEquivalentsAtCarryingValue,
    b0.cashAndShortTermInvestments,
    b0.cashAndCashEquivalents
  ), null);

  const debt = toNum(alphaChoose(
    b0.shortLongTermDebtTotal,
    b0.longTermDebtNoncurrent,
    b0.currentDebtAndCapitalLeaseObligation
  ), null);

  const dilutedShares = toNum(alphaChoose(
    sq.dilutedSharesOutstanding,
    sq.sharesOutstanding,
    sq.commonSharesOutstanding
  ), null);

  return {
    fiscal_period: "TTM",
    fiscal_year: null,
    period_type: "ttm",
    period_end: alphaChoose(iq[0]?.fiscalDateEnding, cq[0]?.fiscalDateEnding, b0?.fiscalDateEnding, null),
    filing_date: null,
    revenue,
    gross_profit: grossProfit,
    ebit,
    ebitda,
    net_income: netIncome,
    eps_gaap: dilutedShares && dilutedShares !== 0 && netIncome !== null ? netIncome / dilutedShares : null,
    eps_nongaap: dilutedShares && dilutedShares !== 0 && netIncome !== null ? netIncome / dilutedShares : null,
    cfo,
    capex: capexAbs,
    fcff,
    cash,
    debt,
    net_cash: (cash !== null && debt !== null) ? (cash - debt) : null,
    diluted_shares: dilutedShares,
    gross_margin: revenue !== null && grossProfit !== null && revenue !== 0 ? grossProfit / revenue : null,
    ebit_margin: revenue !== null && ebit !== null && revenue !== 0 ? ebit / revenue : null,
    fcf_margin: revenue !== null && fcff !== null && revenue !== 0 ? fcff / revenue : null,
    roe: null,
    roic: null
  };
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
    const fmpData = await fmpStableGet("profile", { symbol });
    fmpProfile = normalizeFmpProfile(fmpData);

    if (fmpProfile) {
      sourceStatus.push({ provider: "fmp", status: "ok", note: "fmp stable profile loaded" });
    } else {
      sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp stable profile empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp", status: "partial", note: `fmp stable profile unavailable: ${e.message}` });
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
      exchange: fmpProfile.exchange || "",
      country: fmpProfile.country || "",
      sector: fmpProfile.sector || "",
      industry: fmpProfile.industry || "",
      trading_currency: fmpProfile.currency || "USD",
      reporting_currency: fmpProfile.currency || "USD",
      fiscal_year_end: knownFiscalYearEnd[symbol] || "12-31",
      is_adr,
      adr_ratio,
      framework_id
    }, sourceStatus, ["Primary source unavailable, using FMP stable fallback for security master."]));
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

  let finnhubQuote = null;
  let fmpProfile = null;
  let finnhubOHLCV = [];
  let fmpOHLCV = [];
  let marketstackOHLCV = [];

  try {
    const [quote] = await Promise.all([
      finnhubGet("/quote", { symbol }),
      finnhubGet("/stock/profile2", { symbol })
    ]);

    const priceCurrent = toNum(quote?.c, null);
    if (priceCurrent === null) {
      throw new Error("finnhub quote missing c");
    }

    finnhubQuote = quote;
    sourceStatus.push({ provider: "finnhub", status: "ok", note: "finnhub quote/profile loaded" });
  } catch (e) {
    sourceStatus.push({ provider: "finnhub", status: "partial", note: `finnhub quote/profile unavailable: ${e.message}` });
  }

  try {
    const candles = await finnhubGet("/stock/candle", {
      symbol,
      resolution: "D",
      from: fromSec,
      to: nowSec
    });

    finnhubOHLCV = buildFinnhubOHLCV(candles);

    if (finnhubOHLCV.length > 0) {
      sourceStatus.push({ provider: "finnhub_candles", status: "ok", note: "finnhub candles loaded" });
    } else {
      sourceStatus.push({
        provider: "finnhub_candles",
        status: "partial",
        note: `finnhub candles unavailable: status=${String(candles?.s)}`
      });
    }
  } catch (e) {
    sourceStatus.push({ provider: "finnhub_candles", status: "partial", note: `finnhub candles unavailable: ${e.message}` });
  }

  try {
    const [profileData, historicalData] = await Promise.all([
      fmpStableGet("profile", { symbol }),
      fmpStableGet("historical-price-eod/full", { symbol })
    ]);

    fmpProfile = normalizeFmpProfile(profileData);
    fmpOHLCV = buildFmpOHLCV(historicalData, Math.min(252 * historyYears, 2520));

    if (fmpProfile) {
      sourceStatus.push({ provider: "fmp", status: "ok", note: "fmp stable profile loaded" });
    } else {
      sourceStatus.push({ provider: "fmp", status: "partial", note: "fmp stable profile empty" });
    }

    if (fmpOHLCV.length > 0) {
      sourceStatus.push({ provider: "fmp_history", status: "ok", note: "fmp historical eod loaded" });
    } else {
      sourceStatus.push({ provider: "fmp_history", status: "partial", note: "fmp historical eod empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp", status: "partial", note: `fmp stable fallback unavailable: ${e.message}` });
  }

  try {
    if (!MARKETSTACK_ACCESS_KEY) {
      throw new Error("MARKETSTACK_ACCESS_KEY missing");
    }

    const toDate = new Date();
    const fromDate = new Date();
    fromDate.setUTCDate(fromDate.getUTCDate() - historyYears * 365);

    const msData = await marketstackGet("eod", {
      symbols: symbol,
      date_from: formatDateYYYYMMDD(fromDate),
      date_to: formatDateYYYYMMDD(toDate),
      limit: Math.min(1000, 260 * historyYears)
    });

    marketstackOHLCV = buildMarketstackOHLCV(msData, Math.min(252 * historyYears, 2520));

    if (marketstackOHLCV.length > 0) {
      sourceStatus.push({ provider: "marketstack", status: "ok", note: "marketstack eod history loaded" });
    } else {
      sourceStatus.push({ provider: "marketstack", status: "partial", note: "marketstack eod history empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "marketstack", status: "partial", note: `marketstack fallback unavailable: ${e.message}` });
  }

  let priceCurrent = toNum(finnhubQuote?.c, null);
  if (priceCurrent === null && fmpProfile) {
    priceCurrent = toNum(fmpProfile.price, null);
    if (priceCurrent !== null) {
      warnings.push("Using FMP profile price fallback because Finnhub quote was unavailable.");
    }
  }

  let ohlcv = [];
  if (finnhubOHLCV.length > 0) {
    ohlcv = finnhubOHLCV;
  } else if (fmpOHLCV.length > 0) {
    ohlcv = fmpOHLCV;
  } else if (marketstackOHLCV.length > 0) {
    ohlcv = marketstackOHLCV;
    warnings.push("Using Marketstack EOD fallback for OHLCV.");
  }

  if (ohlcv.length === 0 && ALPHAVANTAGE_API_KEY) {
    try {
      const dailyRaw = await alphaGet({
        function: "TIME_SERIES_DAILY_ADJUSTED",
        symbol,
        outputsize: "compact"
      });

      if (alphaHasRateLimitOrError(dailyRaw)) {
        const note =
          dailyRaw?.Note ||
          dailyRaw?.Information ||
          dailyRaw?.["Error Message"] ||
          "alpha returned note/error payload";
        sourceStatus.push({ provider: "alpha_vantage", status: "partial", note });
      } else {
        const alphaOHLCV = buildAlphaOHLCV(
          dailyRaw?.["Time Series (Daily)"] || dailyRaw?.["Time Series (Daily Adjusted)"],
          120
        );
        if (alphaOHLCV.length > 0) {
          ohlcv = alphaOHLCV;
          sourceStatus.push({ provider: "alpha_vantage", status: "ok", note: "alpha daily fallback loaded" });
          warnings.push("Using Alpha Vantage fallback for OHLCV.");
        } else {
          sourceStatus.push({ provider: "alpha_vantage", status: "partial", note: "alpha daily payload empty" });
        }
      }
    } catch (e) {
      sourceStatus.push({ provider: "alpha_vantage", status: "partial", note: `alpha fallback unavailable: ${e.message}` });
    }
  }

  if (priceCurrent === null || ohlcv.length === 0) {
    return res.status(502).json(fail(
      "ALL_PROVIDERS_UNAVAILABLE",
      "Unable to assemble a usable market price pack from Finnhub/FMP/Marketstack/Alpha.",
      502,
      sourceStatus,
      warnings
    ));
  }

  const marketCap =
    toNum(fmpProfile?.marketCap, null) ??
    (toNum(fmpProfile?.sharesOutstanding, null) !== null ? toNum(fmpProfile.sharesOutstanding, null) * priceCurrent : null);

  const sharesOutstanding =
    toNum(fmpProfile?.sharesOutstanding, null) ??
    (marketCap !== null && priceCurrent > 0 ? marketCap / priceCurrent : null);

  const beta =
    toNum(fmpProfile?.beta, null);

  return res.json(success({
    price_current: priceCurrent,
    price_timestamp: finnhubQuote?.t ? new Date(finnhubQuote.t * 1000).toISOString() : new Date().toISOString(),
    market_cap_current: marketCap,
    enterprise_value_current: marketCap,
    shares_outstanding_current: sharesOutstanding,
    beta_snapshot: beta,
    ohlcv
  }, sourceStatus, warnings));
});

app.post("/v1/fundamental-actuals-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  const annualYears = Math.max(1, Math.min(10, Number(req.body.annual_years || 10)));
  const quarterlyPeriods = Math.max(1, Math.min(12, Number(req.body.quarterly_periods || 12)));
  const includeTtm = req.body.include_ttm !== false;

  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const sourceStatus = [];
  const warnings = [];

  try {
    const [incomeRaw, balanceRaw, cashRaw, sharesRaw] = await Promise.all([
      alphaGet({ function: "INCOME_STATEMENT", symbol }),
      alphaGet({ function: "BALANCE_SHEET", symbol }),
      alphaGet({ function: "CASH_FLOW", symbol }),
      alphaGet({ function: "SHARES_OUTSTANDING", symbol })
    ]);

    if (
      alphaHasRateLimitOrError(incomeRaw) ||
      alphaHasRateLimitOrError(balanceRaw) ||
      alphaHasRateLimitOrError(cashRaw) ||
      alphaHasRateLimitOrError(sharesRaw)
    ) {
      const note =
        incomeRaw?.Note || incomeRaw?.Information || incomeRaw?.["Error Message"] ||
        balanceRaw?.Note || balanceRaw?.Information || balanceRaw?.["Error Message"] ||
        cashRaw?.Note || cashRaw?.Information || cashRaw?.["Error Message"] ||
        sharesRaw?.Note || sharesRaw?.Information || sharesRaw?.["Error Message"] ||
        "alpha returned note/error payload";
      sourceStatus.push({ provider: "alpha_vantage", status: "partial", note });
      return res.status(502).json(fail(
        "UPSTREAM_PARTIAL_DATA",
        "Alpha Vantage returned a note/error payload instead of usable fundamental actuals.",
        502,
        sourceStatus,
        warnings
      ));
    }

    const income = normalizeAlphaReports(incomeRaw);
    const balance = normalizeAlphaReports(balanceRaw);
    const cash = normalizeAlphaReports(cashRaw);
    const shares = normalizeAlphaShares(sharesRaw);

    const annuals = buildAlphaUnifiedPeriods(
      income.annual.slice(0, annualYears),
      balance.annual.slice(0, annualYears),
      cash.annual.slice(0, annualYears),
      shares.annual.slice(0, annualYears),
      "annual"
    );

    const quarterlies = buildAlphaUnifiedPeriods(
      income.quarterly.slice(0, quarterlyPeriods),
      balance.quarterly.slice(0, quarterlyPeriods),
      cash.quarterly.slice(0, quarterlyPeriods),
      shares.quarterly.slice(0, quarterlyPeriods),
      "quarterly"
    );

    const ttm = includeTtm
      ? buildAlphaTTM(
          income.quarterly.slice(0, 4),
          balance.quarterly.slice(0, 1),
          cash.quarterly.slice(0, 4),
          shares.quarterly.slice(0, 1)
        )
      : {
          fiscal_period: "TTM",
          fiscal_year: null,
          period_type: "ttm",
          period_end: null,
          filing_date: null,
          revenue: null,
          gross_profit: null,
          ebit: null,
          ebitda: null,
          net_income: null,
          eps_gaap: null,
          eps_nongaap: null,
          cfo: null,
          capex: null,
          fcff: null,
          cash: null,
          debt: null,
          net_cash: null,
          diluted_shares: null,
          gross_margin: null,
          ebit_margin: null,
          fcf_margin: null,
          roe: null,
          roic: null
        };

    sourceStatus.push({ provider: "alpha_vantage", status: "ok", note: "alpha income/balance/cashflow/shares loaded" });

    return res.json(success({
      annuals,
      quarterlies,
      ttm
    }, sourceStatus, warnings));
  } catch (e) {
    sourceStatus.push({ provider: "alpha_vantage", status: "partial", note: `alpha fundamental actuals unavailable: ${e.message}` });
    return res.status(502).json(fail(
      "ALL_PROVIDERS_UNAVAILABLE",
      "Unable to assemble usable fundamental actuals from Alpha Vantage endpoints.",
      502,
      sourceStatus,
      warnings
    ));
  }
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
    ],
    live_phase: {
      security_master: true,
      market_price_pack: true,
      fundamental_actuals_pack: true,
      estimates_targets_pack: false,
      macro_breadth_liquidity_pack: false,
      filings_transcripts_pack: false,
      technical_structure_pack: false
    }
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`API running on port ${PORT}`);
});
