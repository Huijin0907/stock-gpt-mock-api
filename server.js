const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const ALPHAVANTAGE_API_KEY = process.env.ALPHAVANTAGE_API_KEY || "";
const FMP_API_KEY = process.env.FMP_API_KEY || "";
const MARKETSTACK_ACCESS_KEY = process.env.MARKETSTACK_ACCESS_KEY || "";
const SEC_USER_AGENT =
  process.env.SEC_USER_AGENT ||
  "stock-gpt-mock-api/1.0 (+https://stock-gpt-mock-api.onrender.com)";

const ACTUALS_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const SEC_CACHE_TTL_MS = 6 * 60 * 60 * 1000;

const actualsCache = new Map();
const actualsInflight = new Map();
const secTickerMapCache = new Map();
const secCompanyFactsCache = new Map();

const knownFiscalYearEnd = {
  MSFT: "06-30",
  BABA: "03-31",
  QQQ: "12-31",
  GLD: "12-31",
  AAPL: "09-30"
};

const SEC_ANNUAL_FORMS = new Set([
  "10-K",
  "10-K/A",
  "20-F",
  "20-F/A",
  "40-F",
  "40-F/A"
]);

const SEC_QUARTERLY_FORMS = new Set([
  "10-Q",
  "10-Q/A",
  "6-K",
  "6-K/A"
]);

const SEC_METRIC_TAGS = {
  revenue: [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "Revenues",
    "RevenueFromContractWithCustomerIncludingAssessedTax"
  ],
  gross_profit: ["GrossProfit"],
  ebit: ["OperatingIncomeLoss"],
  net_income: ["NetIncomeLoss", "ProfitLoss"],
  eps_gaap: ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"],
  cfo: [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
  ],
  capex: [
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PropertyPlantAndEquipmentAdditions"
  ],
  cash: [
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    "CashCashEquivalentsAndShortTermInvestments"
  ],
  debt_total: [
    "LongTermDebtAndCapitalLeaseObligations",
    "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
    "DebtAndFinanceLeaseObligations",
    "DebtInstrumentFaceAmount"
  ],
  debt_current: [
    "LongTermDebtCurrent",
    "LongTermDebtAndCapitalLeaseObligationsCurrent",
    "ShortTermBorrowings"
  ],
  debt_noncurrent: [
    "LongTermDebtNoncurrent",
    "LongTermDebtAndCapitalLeaseObligationsNoncurrent"
  ],
  diluted_shares_duration: ["WeightedAverageNumberOfDilutedSharesOutstanding"],
  common_shares_outstanding: ["CommonStockSharesOutstanding"]
};

const success = (data, sourceStatus = [], warnings = []) => ({
  meta: {
    request_id: `req_${Date.now()}`,
    generated_at: new Date().toISOString(),
    source_status: sourceStatus,
    warnings
  },
  data
});

const fail = (
  code,
  message,
  httpStatus = 400,
  sourceStatus = [],
  warnings = [],
  retryable = false
) => ({
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
    retryable
  }
});

const toNum = (v, fallback = null) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

const alphaChoose = (...vals) => {
  for (const v of vals) {
    if (v !== null && v !== undefined && v !== "") return v;
  }
  return null;
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
  const resp = await axios.get(
    `https://financialmodelingprep.com/stable/${path}`,
    {
      params: { ...params, apikey: FMP_API_KEY },
      timeout: 15000
    }
  );
  return resp.data;
}

async function marketstackGet(path = "eod", params = {}) {
  if (!MARKETSTACK_ACCESS_KEY)
    throw new Error("MARKETSTACK_ACCESS_KEY missing");
  const resp = await axios.get(`https://api.marketstack.com/v1/${path}`, {
    params: { ...params, access_key: MARKETSTACK_ACCESS_KEY },
    timeout: 15000
  });
  return resp.data;
}

async function secGet(url) {
  const resp = await axios.get(url, {
    headers: {
      "User-Agent": SEC_USER_AGENT,
      Accept: "application/json, text/plain, */*",
      Referer: "https://www.sec.gov/",
      "Accept-Encoding": "gzip, deflate"
    },
    timeout: 20000
  });
  return resp.data;
}

function normalizeFmpProfile(payload) {
  if (!payload) return null;
  if (Array.isArray(payload) && payload.length > 0) return payload[0];
  if (typeof payload === "object" && !Array.isArray(payload)) return payload;
  return null;
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
  if (
    !candles ||
    candles.s !== "ok" ||
    !Array.isArray(candles.t) ||
    candles.t.length === 0
  ) {
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

function buildFmpOHLCV(payload, maxPoints = 252) {
  let rows = [];
  if (Array.isArray(payload)) rows = payload;
  else if (payload?.historical && Array.isArray(payload.historical))
    rows = payload.historical;
  else if (payload?.data && Array.isArray(payload.data)) rows = payload.data;
  else if (payload?.results && Array.isArray(payload.results))
    rows = payload.results;

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

function buildAlphaOHLCV(alphaSeries, maxPoints = 120) {
  if (
    !alphaSeries ||
    typeof alphaSeries !== "object" ||
    Object.keys(alphaSeries).length === 0
  ) {
    return [];
  }

  return Object.keys(alphaSeries)
    .sort()
    .slice(-maxPoints)
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

function formatDateYYYYMMDD(dateObj) {
  const y = dateObj.getUTCFullYear();
  const m = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function alphaExtractProblem(payload) {
  if (!payload || typeof payload !== "object") return null;
  const raw =
    payload?.Note || payload?.Information || payload?.["Error Message"] || null;
  if (!raw) return null;

  const message = String(raw);
  const lower = message.toLowerCase();

  if (payload?.["Error Message"]) {
    return {
      code: "ALPHAVANTAGE_UPSTREAM_ERROR",
      message,
      httpStatus: 502,
      retryable: false
    };
  }
  if (lower.includes("per minute")) {
    return {
      code: "ALPHAVANTAGE_RATE_LIMIT_PER_MINUTE",
      message,
      httpStatus: 429,
      retryable: true
    };
  }
  if (
    lower.includes("25 requests per day") ||
    lower.includes("25 calls per day") ||
    lower.includes("per day")
  ) {
    return {
      code: "ALPHAVANTAGE_RATE_LIMIT_PER_DAY",
      message,
      httpStatus: 429,
      retryable: false
    };
  }
  if (lower.includes("premium")) {
    return {
      code: "ALPHAVANTAGE_PREMIUM_OR_PLAN_LIMIT",
      message,
      httpStatus: 429,
      retryable: false
    };
  }
  return {
    code: "ALPHAVANTAGE_NOTE_OR_INFORMATION",
    message,
    httpStatus: 502,
    retryable: false
  };
}

function alphaHasRateLimitOrError(payload) {
  return !!alphaExtractProblem(payload);
}

function alphaProblemToSourceStatus(provider, problem) {
  return {
    provider,
    status: "partial",
    note: problem?.message || "alpha returned note/error payload"
  };
}

function normalizeAlphaReports(payload) {
  return {
    annual: Array.isArray(payload?.annualReports) ? payload.annualReports : [],
    quarterly: Array.isArray(payload?.quarterlyReports)
      ? payload.quarterlyReports
      : []
  };
}

function normalizeAlphaShares(payload) {
  return {
    annual: Array.isArray(payload?.annualSharesOutstanding)
      ? payload.annualSharesOutstanding
      : [],
    quarterly: Array.isArray(payload?.quarterlySharesOutstanding)
      ? payload.quarterlySharesOutstanding
      : []
  };
}

function buildAlphaStatementMapByDate(rows) {
  const m = new Map();
  for (const r of rows) {
    const key = r.fiscalDateEnding || r.date || null;
    if (key) m.set(key, r);
  }
  return m;
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

function buildAlphaUnifiedPeriods(
  incomeRows,
  balanceRows,
  cashRows,
  sharesRows,
  periodType
) {
  const incomeMap = buildAlphaStatementMapByDate(incomeRows);
  const balanceMap = buildAlphaStatementMapByDate(balanceRows);
  const cashMap = buildAlphaStatementMapByDate(cashRows);
  const sharesMap = buildAlphaSharesMap(sharesRows);

  const keys = Array.from(
    new Set([
      ...incomeMap.keys(),
      ...balanceMap.keys(),
      ...cashMap.keys(),
      ...sharesMap.keys()
    ])
  )
    .sort()
    .reverse();

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
    const epsGaap = toNum(
      alphaChoose(i.reportedEPS, i.dilutedEPS, i.eps),
      null
    );

    const cfo = toNum(alphaChoose(c.operatingCashflow, c.operatingCashFlow), null);
    const capexRaw = toNum(
      alphaChoose(c.capitalExpenditures, c.capitalExpenditure),
      null
    );
    const capex = capexRaw === null ? null : Math.abs(capexRaw);
    const fcff = cfo !== null && capex !== null ? cfo - capex : null;

    const cash = toNum(
      alphaChoose(
        b.cashAndCashEquivalentsAtCarryingValue,
        b.cashAndShortTermInvestments,
        b.cashAndCashEquivalents
      ),
      null
    );

    const debt = toNum(
      alphaChoose(
        b.shortLongTermDebtTotal,
        b.longTermDebtNoncurrent,
        b.currentDebtAndCapitalLeaseObligation
      ),
      null
    );

    const fiscalYear = toNum((key || "").slice(0, 4), null);

    return {
      fiscal_period:
        periodType === "annual"
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
      eps_nongaap: epsGaap,
      cfo,
      capex,
      fcff,
      cash,
      debt,
      net_cash: cash !== null && debt !== null ? cash - debt : null,
      diluted_shares: dilutedShares,
      gross_margin:
        revenue !== null && grossProfit !== null && revenue !== 0
          ? grossProfit / revenue
          : null,
      ebit_margin:
        revenue !== null && ebit !== null && revenue !== 0
          ? ebit / revenue
          : null,
      fcf_margin:
        revenue !== null && fcff !== null && revenue !== 0
          ? fcff / revenue
          : null,
      roe: null,
      roic: null
    };
  });
}

function buildEmptyTtm() {
  return {
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
}

function buildAlphaTTM(
  incomeQuarterly,
  balanceQuarterly,
  cashQuarterly,
  sharesQuarterly = []
) {
  const iq = [...incomeQuarterly].slice(0, 4);
  const cq = [...cashQuarterly].slice(0, 4);
  const b0 = balanceQuarterly[0] || {};
  const sq = sharesQuarterly[0] || {};

  const sum = (rows, fieldNames) => {
    const vals = rows
      .map((r) =>
        toNum(alphaChoose(...fieldNames.map((f) => r[f])), null)
      )
      .filter((v) => v !== null);
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
      .map((r) =>
        toNum(alphaChoose(r.capitalExpenditures, r.capitalExpenditure), null)
      )
      .filter((v) => v !== null)
      .map((v) => Math.abs(v));
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  })();

  const quarterlyEpsSum = (() => {
    const vals = iq
      .map((r) =>
        toNum(alphaChoose(r.reportedEPS, r.dilutedEPS, r.eps), null)
      )
      .filter((v) => v !== null);
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  })();

  const cash = toNum(
    alphaChoose(
      b0.cashAndCashEquivalentsAtCarryingValue,
      b0.cashAndShortTermInvestments,
      b0.cashAndCashEquivalents
    ),
    null
  );

  const debt = toNum(
    alphaChoose(
      b0.shortLongTermDebtTotal,
      b0.longTermDebtNoncurrent,
      b0.currentDebtAndCapitalLeaseObligation
    ),
    null
  );

  const dilutedShares = toNum(
    alphaChoose(
      sq.dilutedSharesOutstanding,
      sq.sharesOutstanding,
      sq.commonSharesOutstanding
    ),
    null
  );

  const epsFromNetIncome =
    dilutedShares && dilutedShares !== 0 && netIncome !== null
      ? netIncome / dilutedShares
      : null;
  const epsTtm = epsFromNetIncome ?? quarterlyEpsSum;
  const fcff = cfo !== null && capexAbs !== null ? cfo - capexAbs : null;

  return {
    fiscal_period: "TTM",
    fiscal_year: null,
    period_type: "ttm",
    period_end: alphaChoose(
      iq[0]?.fiscalDateEnding,
      cq[0]?.fiscalDateEnding,
      b0?.fiscalDateEnding,
      null
    ),
    filing_date: null,
    revenue,
    gross_profit: grossProfit,
    ebit,
    ebitda,
    net_income: netIncome,
    eps_gaap: epsTtm,
    eps_nongaap: epsTtm,
    cfo,
    capex: capexAbs,
    fcff,
    cash,
    debt,
    net_cash: cash !== null && debt !== null ? cash - debt : null,
    diluted_shares: dilutedShares,
    gross_margin:
      revenue !== null && grossProfit !== null && revenue !== 0
        ? grossProfit / revenue
        : null,
    ebit_margin:
      revenue !== null && ebit !== null && revenue !== 0
        ? ebit / revenue
        : null,
    fcf_margin:
      revenue !== null && fcff !== null && revenue !== 0
        ? fcff / revenue
        : null,
    roe: null,
    roic: null
  };
}

function cleanupCache(cacheMap) {
  const now = Date.now();
  for (const [key, entry] of cacheMap.entries()) {
    if (!entry || entry.expiresAt <= now) {
      cacheMap.delete(key);
    }
  }
}

function getCacheEntry(cacheMap, cacheKey) {
  cleanupCache(cacheMap);
  const entry = cacheMap.get(cacheKey);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    cacheMap.delete(cacheKey);
    return null;
  }
  return entry.value;
}

function setCacheEntry(cacheMap, cacheKey, value, ttlMs) {
  cacheMap.set(cacheKey, { expiresAt: Date.now() + ttlMs, value });
}

function buildActualsCacheKey(
  symbol,
  annualYears,
  quarterlyPeriods,
  includeTtm
) {
  return `${symbol}__a${annualYears}__q${quarterlyPeriods}__ttm${
    includeTtm ? 1 : 0
  }`;
}

function secPadCik(cik) {
  return String(cik || "").replace(/\D/g, "").padStart(10, "0");
}

async function getSecTickerMap() {
  const cached = getCacheEntry(secTickerMapCache, "company_tickers");
  if (cached) return cached;

  const raw = await secGet("https://www.sec.gov/files/company_tickers.json");
  const out = new Map();

  if (raw && typeof raw === "object") {
    for (const item of Object.values(raw)) {
      const ticker = String(item?.ticker || "").toUpperCase().trim();
      const cik = secPadCik(item?.cik_str);
      if (ticker && cik) {
        out.set(ticker, { ticker, cik, title: item?.title || ticker });
      }
    }
  }

  setCacheEntry(secTickerMapCache, "company_tickers", out, SEC_CACHE_TTL_MS);
  return out;
}

async function resolveSecTicker(symbol) {
  const tickerMap = await getSecTickerMap();
  return tickerMap.get(String(symbol || "").toUpperCase().trim()) || null;
}

async function getSecCompanyFacts(cik) {
  const cacheKey = `companyfacts_${cik}`;
  const cached = getCacheEntry(secCompanyFactsCache, cacheKey);
  if (cached) return cached;

  const raw = await secGet(
    `https://data.sec.gov/api/xbrl/companyfacts/CIK${cik}.json`
  );
  setCacheEntry(secCompanyFactsCache, cacheKey, raw, SEC_CACHE_TTL_MS);
  return raw;
}

function secParseDateMs(v) {
  const t = Date.parse(v || "");
  return Number.isFinite(t) ? t : 0;
}

function secDurationDaysFromRow(row) {
  const s = secParseDateMs(row?.start);
  const e = secParseDateMs(row?.end);
  if (!s || !e || e < s) return null;
  return Math.round((e - s) / 86400000);
}

function secFactRowsByTag(companyFacts, tag, unitsPriority = []) {
  const unitsObj = companyFacts?.facts?.["us-gaap"]?.[tag]?.units || null;
  if (!unitsObj || typeof unitsObj !== "object") return [];

  const selectedUnits =
    unitsPriority.length > 0
      ? unitsPriority.filter((u) => Array.isArray(unitsObj[u]))
      : Object.keys(unitsObj);

  const rows = [];
  for (const unit of selectedUnits) {
    for (const row of unitsObj[unit] || []) {
      rows.push({
        tag,
        unit,
        val: toNum(row?.val, null),
        start: row?.start || null,
        end: row?.end || null,
        fy: row?.fy || null,
        fp: row?.fp || null,
        form: row?.form || null,
        filed: row?.filed || null,
        frame: row?.frame || null,
        durationDays: secDurationDaysFromRow(row)
      });
    }
  }

  return rows.filter((r) => r.val !== null && r.end);
}

function secIsAnnualRow(row) {
  return (
    row?.fp === "FY" ||
    SEC_ANNUAL_FORMS.has(row?.form) ||
    (row?.durationDays !== null && row.durationDays >= 300)
  );
}

function secIsSingleQuarterRow(row) {
  if (!row || row.durationDays === null) return false;
  return row.durationDays >= 70 && row.durationDays <= 110;
}

function secIsNineMonthRow(row) {
  if (!row || row.durationDays === null) return false;
  return row.durationDays >= 240 && row.durationDays <= 300;
}

function secSortRowsForPreference(rows = []) {
  return [...rows].sort((a, b) => {
    const filedDiff = secParseDateMs(b.filed) - secParseDateMs(a.filed);
    if (filedDiff !== 0) return filedDiff;

    const aQuarterFp = /^Q[1-4]$/.test(String(a.fp || "")) ? 1 : 0;
    const bQuarterFp = /^Q[1-4]$/.test(String(b.fp || "")) ? 1 : 0;
    if (bQuarterFp !== aQuarterFp) return bQuarterFp - aQuarterFp;

    const aFrame = a.frame ? 1 : 0;
    const bFrame = b.frame ? 1 : 0;
    return bFrame - aFrame;
  });
}

function secGetRowsForTags(
  companyFacts,
  tags,
  unitsPriority = ["USD", "USD/shares", "shares", "pure"]
) {
  let rows = [];
  for (const tag of tags) {
    rows = rows.concat(secFactRowsByTag(companyFacts, tag, unitsPriority));
  }
  return rows;
}

function secFindBestInstantValue(companyFacts, tags, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForTags(companyFacts, tags, [
      "USD",
      "shares",
      "USD/shares",
      "pure"
    ]).filter((row) => row.end === periodEnd)
  );
  return candidates[0]?.val ?? null;
}

function secFindBestAnnualFlowRow(companyFacts, tags, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForTags(companyFacts, tags, ["USD", "USD/shares", "pure"]).filter(
      (row) => row.end === periodEnd && secIsAnnualRow(row)
    )
  );
  return candidates[0] || null;
}

function secFindBestQuarterFlowRow(companyFacts, tags, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForTags(companyFacts, tags, ["USD", "USD/shares", "pure"]).filter(
      (row) => row.end === periodEnd && secIsSingleQuarterRow(row)
    )
  );
  return candidates[0] || null;
}

function secFindBestNineMonthRow(companyFacts, tags, fiscalYear, annualEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForTags(companyFacts, tags, ["USD", "USD/shares", "pure"]).filter(
      (row) => {
        if (!secIsNineMonthRow(row)) return false;
        if (annualEnd && secParseDateMs(row.end) >= secParseDateMs(annualEnd))
          return false;
        if (
          fiscalYear !== null &&
          fiscalYear !== undefined &&
          row.fy !== null &&
          row.fy !== undefined
        ) {
          return Number(row.fy) === Number(fiscalYear);
        }
        return true;
      }
    )
  );

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => {
    const endDiff = secParseDateMs(b.end) - secParseDateMs(a.end);
    if (endDiff !== 0) return endDiff;
    return secParseDateMs(b.filed) - secParseDateMs(a.filed);
  });
  return candidates[0];
}

function secFindQuarterFlowValue(companyFacts, tags, periodEnd) {
  const directQuarter = secFindBestQuarterFlowRow(companyFacts, tags, periodEnd);
  if (directQuarter) return directQuarter.val;

  const annualRow = secFindBestAnnualFlowRow(companyFacts, tags, periodEnd);
  if (!annualRow) return null;

  const nineMonthRow = secFindBestNineMonthRow(
    companyFacts,
    tags,
    annualRow.fy,
    periodEnd
  );
  if (!nineMonthRow) return null;

  return annualRow.val - nineMonthRow.val;
}

function secHasMeaningfulQuarter(period) {
  return [
    period?.revenue,
    period?.gross_profit,
    period?.ebit,
    period?.net_income,
    period?.cfo,
    period?.capex,
    period?.eps_gaap
  ].some((v) => v !== null && v !== undefined);
}

function secCollectAnnualEnds(companyFacts) {
  const rows = secGetRowsForTags(companyFacts, SEC_METRIC_TAGS.revenue, ["USD"]);
  const ends = new Set();
  for (const row of rows) {
    if (secIsAnnualRow(row)) ends.add(row.end);
  }
  return Array.from(ends).sort().reverse();
}

function secCollectQuarterCandidateEnds(companyFacts) {
  const directQuarterRows = secGetRowsForTags(
    companyFacts,
    SEC_METRIC_TAGS.revenue,
    ["USD"]
  ).filter((row) => secIsSingleQuarterRow(row));

  const annualRows = secGetRowsForTags(
    companyFacts,
    SEC_METRIC_TAGS.revenue,
    ["USD"]
  ).filter((row) => secIsAnnualRow(row));

  const ends = new Set();

  for (const row of directQuarterRows) ends.add(row.end);
  for (const row of annualRows) ends.add(row.end);

  return Array.from(ends).sort().reverse();
}

function secBuildAnnualPeriods(companyFacts, limit) {
  const periodEnds = secCollectAnnualEnds(companyFacts).slice(0, limit);

  return periodEnds.map((periodEnd) => {
    const revenue =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.revenue,
        periodEnd
      )?.val ?? null;
    const grossProfit =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.gross_profit,
        periodEnd
      )?.val ?? null;
    const ebit =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.ebit,
        periodEnd
      )?.val ?? null;
    const netIncome =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.net_income,
        periodEnd
      )?.val ?? null;
    const epsGaap =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.eps_gaap,
        periodEnd
      )?.val ?? null;
    const cfo =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.cfo,
        periodEnd
      )?.val ?? null;

    const capexRaw =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.capex,
        periodEnd
      )?.val ?? null;
    const capex = capexRaw === null ? null : Math.abs(capexRaw);
    const fcff = cfo !== null && capex !== null ? cfo - capex : null;

    const cash = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.cash,
      periodEnd
    );
    const debtTotal = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_total,
      periodEnd
    );
    const debtCurrent = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_current,
      periodEnd
    );
    const debtNoncurrent = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_noncurrent,
      periodEnd
    );
    const debt =
      debtTotal ??
      (debtCurrent !== null && debtNoncurrent !== null
        ? debtCurrent + debtNoncurrent
        : debtCurrent ?? debtNoncurrent ?? null);

    const dilutedShares =
      secFindBestInstantValue(
        companyFacts,
        SEC_METRIC_TAGS.common_shares_outstanding,
        periodEnd
      ) ??
      (secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.diluted_shares_duration,
        periodEnd
      )?.val ?? null);

    const annualMeta =
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.revenue,
        periodEnd
      ) ||
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.net_income,
        periodEnd
      );
    const fiscalYear = toNum(
      annualMeta?.fy ?? (periodEnd || "").slice(0, 4),
      null
    );

    return {
      fiscal_period: `FY${fiscalYear ?? ""}`,
      fiscal_year: fiscalYear,
      period_type: "annual",
      period_end: periodEnd,
      filing_date: annualMeta?.filed || null,
      revenue,
      gross_profit: grossProfit,
      ebit,
      ebitda: null,
      net_income: netIncome,
      eps_gaap: epsGaap,
      eps_nongaap: epsGaap,
      cfo,
      capex,
      fcff,
      cash,
      debt,
      net_cash: cash !== null && debt !== null ? cash - debt : null,
      diluted_shares: dilutedShares,
      gross_margin:
        revenue !== null && grossProfit !== null && revenue !== 0
          ? grossProfit / revenue
          : null,
      ebit_margin:
        revenue !== null && ebit !== null && revenue !== 0
          ? ebit / revenue
          : null,
      fcf_margin:
        revenue !== null && fcff !== null && revenue !== 0
          ? fcff / revenue
          : null,
      roe: null,
      roic: null
    };
  });
}

function secBuildQuarterlyPeriods(companyFacts, limit) {
  const periodEnds = secCollectQuarterCandidateEnds(companyFacts);
  const out = [];

  for (const periodEnd of periodEnds) {
    const revenue = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.revenue,
      periodEnd
    );
    const grossProfit = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.gross_profit,
      periodEnd
    );
    const ebit = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.ebit,
      periodEnd
    );
    const netIncome = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.net_income,
      periodEnd
    );
    const epsGaap = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.eps_gaap,
      periodEnd
    );
    const cfo = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.cfo,
      periodEnd
    );

    const capexRaw = secFindQuarterFlowValue(
      companyFacts,
      SEC_METRIC_TAGS.capex,
      periodEnd
    );
    const capex = capexRaw === null ? null : Math.abs(capexRaw);
    const fcff = cfo !== null && capex !== null ? cfo - capex : null;

    const cash = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.cash,
      periodEnd
    );
    const debtTotal = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_total,
      periodEnd
    );
    const debtCurrent = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_current,
      periodEnd
    );
    const debtNoncurrent = secFindBestInstantValue(
      companyFacts,
      SEC_METRIC_TAGS.debt_noncurrent,
      periodEnd
    );
    const debt =
      debtTotal ??
      (debtCurrent !== null && debtNoncurrent !== null
        ? debtCurrent + debtNoncurrent
        : debtCurrent ?? debtNoncurrent ?? null);

    const dilutedShares =
      secFindBestInstantValue(
        companyFacts,
        SEC_METRIC_TAGS.common_shares_outstanding,
        periodEnd
      ) ??
      (secFindBestQuarterFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.diluted_shares_duration,
        periodEnd
      )?.val ?? null);

    const quarterMeta =
      secFindBestQuarterFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.revenue,
        periodEnd
      ) ||
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.revenue,
        periodEnd
      ) ||
      secFindBestQuarterFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.net_income,
        periodEnd
      ) ||
      secFindBestAnnualFlowRow(
        companyFacts,
        SEC_METRIC_TAGS.net_income,
        periodEnd
      );

    const period = {
      fiscal_period: periodEnd,
      fiscal_year: toNum(
        quarterMeta?.fy ?? (periodEnd || "").slice(0, 4),
        null
      ),
      period_type: "quarterly",
      period_end: periodEnd,
      filing_date: quarterMeta?.filed || null,
      revenue,
      gross_profit: grossProfit,
      ebit,
      ebitda: null,
      net_income: netIncome,
      eps_gaap: epsGaap,
      eps_nongaap: epsGaap,
      cfo,
      capex,
      fcff,
      cash,
      debt,
      net_cash: cash !== null && debt !== null ? cash - debt : null,
      diluted_shares: dilutedShares,
      gross_margin:
        revenue !== null && grossProfit !== null && revenue !== 0
          ? grossProfit / revenue
          : null,
      ebit_margin:
        revenue !== null && ebit !== null && revenue !== 0
          ? ebit / revenue
          : null,
      fcf_margin:
        revenue !== null && fcff !== null && revenue !== 0
          ? fcff / revenue
          : null,
      roe: null,
      roic: null
    };

    if (secHasMeaningfulQuarter(period)) {
      out.push(period);
    }

    if (out.length >= limit) break;
  }

  return out;
}

function secBuildTTMFromQuarterlies(quarterlies) {
  const q = [...quarterlies].slice(0, 4);
  if (q.length === 0) return buildEmptyTtm();

  const sumField = (field) => {
    const vals = q
      .map((row) => toNum(row?.[field], null))
      .filter((v) => v !== null);
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  };

  const revenue = sumField("revenue");
  const grossProfit = sumField("gross_profit");
  const ebit = sumField("ebit");
  const netIncome = sumField("net_income");
  const cfo = sumField("cfo");
  const capex = sumField("capex");
  const fcff = cfo !== null && capex !== null ? cfo - capex : null;
  const epsQuarterlySum = sumField("eps_gaap");

  const latest = q[0] || {};
  const dilutedShares = latest?.diluted_shares ?? null;
  const epsFromNetIncome =
    dilutedShares && dilutedShares !== 0 && netIncome !== null
      ? netIncome / dilutedShares
      : null;
  const epsTtm = epsFromNetIncome ?? epsQuarterlySum;

  return {
    fiscal_period: "TTM",
    fiscal_year: null,
    period_type: "ttm",
    period_end: latest?.period_end || null,
    filing_date: latest?.filing_date || null,
    revenue,
    gross_profit: grossProfit,
    ebit,
    ebitda: null,
    net_income: netIncome,
    eps_gaap: epsTtm,
    eps_nongaap: epsTtm,
    cfo,
    capex,
    fcff,
    cash: latest?.cash ?? null,
    debt: latest?.debt ?? null,
    net_cash:
      latest?.cash !== null &&
      latest?.cash !== undefined &&
      latest?.debt !== null &&
      latest?.debt !== undefined
        ? latest.cash - latest.debt
        : null,
    diluted_shares: dilutedShares,
    gross_margin:
      revenue !== null && grossProfit !== null && revenue !== 0
        ? grossProfit / revenue
        : null,
    ebit_margin:
      revenue !== null && ebit !== null && revenue !== 0
        ? ebit / revenue
        : null,
    fcf_margin:
      revenue !== null && fcff !== null && revenue !== 0
        ? fcff / revenue
        : null,
    roe: null,
    roic: null
  };
}

async function fetchAlphaFundamentalActuals(
  symbol,
  annualYears,
  quarterlyPeriods,
  includeTtm
) {
  const sourceStatus = [];
  const warnings = [];

  const [incomeRaw, balanceRaw, cashRaw] = await Promise.all([
    alphaGet({ function: "INCOME_STATEMENT", symbol }),
    alphaGet({ function: "BALANCE_SHEET", symbol }),
    alphaGet({ function: "CASH_FLOW", symbol })
  ]);

  const coreProblems = [
    alphaExtractProblem(incomeRaw),
    alphaExtractProblem(balanceRaw),
    alphaExtractProblem(cashRaw)
  ].filter(Boolean);

  if (coreProblems.length > 0) {
    for (const problem of coreProblems) {
      sourceStatus.push(alphaProblemToSourceStatus("alpha_vantage_core", problem));
    }
    const primary = coreProblems[0];
    const err = new Error(
      "Alpha Vantage returned a note/error payload instead of usable core fundamental statements."
    );
    err.isKnownUpstream = true;
    err.httpStatus = primary.httpStatus;
    err.code = primary.code;
    err.retryable = primary.retryable;
    err.sourceStatus = sourceStatus;
    err.warnings = warnings;
    throw err;
  }

  const income = normalizeAlphaReports(incomeRaw);
  const balance = normalizeAlphaReports(balanceRaw);
  const cash = normalizeAlphaReports(cashRaw);

  let annuals = buildAlphaUnifiedPeriods(
    income.annual.slice(0, annualYears),
    balance.annual.slice(0, annualYears),
    cash.annual.slice(0, annualYears),
    [],
    "annual"
  );

  let quarterlies = buildAlphaUnifiedPeriods(
    income.quarterly.slice(0, quarterlyPeriods),
    balance.quarterly.slice(0, quarterlyPeriods),
    cash.quarterly.slice(0, quarterlyPeriods),
    [],
    "quarterly"
  );

  let ttm = includeTtm
    ? buildAlphaTTM(
        income.quarterly.slice(0, 4),
        balance.quarterly.slice(0, 1),
        cash.quarterly.slice(0, 4),
        []
      )
    : buildEmptyTtm();

  if (annuals.length === 0 && quarterlies.length === 0) {
    const err = new Error(
      "Alpha Vantage returned empty statement arrays for the requested symbol."
    );
    err.isKnownUpstream = true;
    err.httpStatus = 502;
    err.code = "ALPHAVANTAGE_EMPTY_STATEMENTS";
    err.retryable = true;
    err.sourceStatus = [
      {
        provider: "alpha_vantage_core",
        status: "partial",
        note: "alpha income/balance/cashflow payload was valid but contained no statement rows"
      }
    ];
    err.warnings = warnings;
    throw err;
  }

  sourceStatus.push({
    provider: "alpha_vantage_core",
    status: "ok",
    note: "alpha income/balance/cashflow loaded"
  });

  const needShares =
    annuals.some((row) => row?.diluted_shares == null) ||
    quarterlies.some((row) => row?.diluted_shares == null) ||
    (includeTtm && ttm?.diluted_shares == null);

  if (needShares) {
    try {
      const sharesRaw = await alphaGet({
        function: "SHARES_OUTSTANDING",
        symbol
      });
      const sharesProblem = alphaExtractProblem(sharesRaw);

      if (sharesProblem) {
        sourceStatus.push(
          alphaProblemToSourceStatus("alpha_vantage_shares", sharesProblem)
        );
        warnings.push(
          "Alpha shares endpoint returned note/error payload; continuing without diluted_shares enrichment where unavailable."
        );
      } else {
        const shares = normalizeAlphaShares(sharesRaw);
        annuals = buildAlphaUnifiedPeriods(
          income.annual.slice(0, annualYears),
          balance.annual.slice(0, annualYears),
          cash.annual.slice(0, annualYears),
          shares.annual.slice(0, annualYears),
          "annual"
        );
        quarterlies = buildAlphaUnifiedPeriods(
          income.quarterly.slice(0, quarterlyPeriods),
          balance.quarterly.slice(0, quarterlyPeriods),
          cash.quarterly.slice(0, quarterlyPeriods),
          shares.quarterly.slice(0, quarterlyPeriods),
          "quarterly"
        );
        ttm = includeTtm
          ? buildAlphaTTM(
              income.quarterly.slice(0, 4),
              balance.quarterly.slice(0, 1),
              cash.quarterly.slice(0, 4),
              shares.quarterly.slice(0, 1)
            )
          : buildEmptyTtm();
        sourceStatus.push({
          provider: "alpha_vantage_shares",
          status: "ok",
          note: "alpha shares outstanding loaded"
        });
      }
    } catch (e) {
      sourceStatus.push({
        provider: "alpha_vantage_shares",
        status: "partial",
        note: `alpha shares outstanding unavailable: ${e.message}`
      });
      warnings.push(
        "Alpha shares endpoint failed; continuing without diluted_shares enrichment where unavailable."
      );
    }
  } else {
    sourceStatus.push({
      provider: "alpha_vantage_shares",
      status: "ok",
      note: "shares endpoint skipped because core statement payload was already sufficient"
    });
  }

  return { data: { annuals, quarterlies, ttm }, sourceStatus, warnings };
}

async function fetchSecFundamentalActuals(
  symbol,
  annualYears,
  quarterlyPeriods,
  includeTtm
) {
  const sourceStatus = [];
  const warnings = [];

  const tickerInfo = await resolveSecTicker(symbol);
  if (!tickerInfo) {
    const err = new Error(`SEC ticker mapping not found for ${symbol}.`);
    err.isKnownUpstream = true;
    err.httpStatus = 404;
    err.code = "SEC_TICKER_NOT_FOUND";
    err.retryable = false;
    err.sourceStatus = [
      {
        provider: "sec_tickers",
        status: "partial",
        note: `SEC ticker mapping not found for ${symbol}`
      }
    ];
    err.warnings = warnings;
    throw err;
  }

  sourceStatus.push({
    provider: "sec_tickers",
    status: "ok",
    note: `resolved ${symbol} to CIK ${tickerInfo.cik}`
  });

  let companyFacts;
  try {
    companyFacts = await getSecCompanyFacts(tickerInfo.cik);
    sourceStatus.push({
      provider: "sec_companyfacts",
      status: "ok",
      note: "SEC companyfacts loaded"
    });
  } catch (e) {
    const err = new Error("SEC companyfacts unavailable for the requested symbol.");
    err.isKnownUpstream = true;
    err.httpStatus = 502;
    err.code = "SEC_COMPANYFACTS_UNAVAILABLE";
    err.retryable = true;
    err.sourceStatus = [
      ...sourceStatus,
      {
        provider: "sec_companyfacts",
        status: "partial",
        note: `SEC companyfacts unavailable: ${e.message}`
      }
    ];
    err.warnings = warnings;
    throw err;
  }

  const annuals = secBuildAnnualPeriods(companyFacts, annualYears);
  const quarterlies = secBuildQuarterlyPeriods(companyFacts, quarterlyPeriods);
  const ttm = includeTtm
    ? secBuildTTMFromQuarterlies(quarterlies)
    : buildEmptyTtm();

  if (annuals.length === 0 && quarterlies.length === 0) {
    const err = new Error(
      "SEC companyfacts did not yield usable annual or quarterly periods."
    );
    err.isKnownUpstream = true;
    err.httpStatus = 502;
    err.code = "SEC_EMPTY_FACTS";
    err.retryable = true;
    err.sourceStatus = [
      ...sourceStatus,
      {
        provider: "sec_companyfacts",
        status: "partial",
        note: "SEC companyfacts loaded but no usable annual/quarterly periods were extracted"
      }
    ];
    err.warnings = warnings;
    throw err;
  }

  warnings.push(
    "SEC quarterly normalization uses pure single-quarter extraction and derives Q4 from FY minus 9M when needed."
  );

  return { data: { annuals, quarterlies, ttm }, sourceStatus, warnings };
}

function buildUnsupportedActualsPayload(instrumentType) {
  return {
    data: { annuals: [], quarterlies: [], ttm: buildEmptyTtm() },
    sourceStatus: [
      {
        provider: "local-router",
        status: "ok",
        note: `fundamental actuals not applicable for instrument_type=${instrumentType}`
      }
    ],
    warnings: [
      `fundamental_actuals_pack is not applicable to instrument_type=${instrumentType}; use framework-specific data instead.`
    ]
  };
}

app.post("/v1/classify-instrument", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const { instrument_type, framework_id } = classifyLocal(symbol);
  return res.json(
    success(
      {
        symbol,
        canonical_symbol: symbol,
        instrument_type,
        framework_id,
        confidence_score: 0.98,
        needs_user_confirmation: false
      },
      [{ provider: "local-router", status: "ok", note: "local classification used" }]
    )
  );
});

app.post("/v1/security-master", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const sourceStatus = [];
  const warnings = [];
  let fmpProfile = null;

  try {
    const fmpData = await fmpStableGet("profile", { symbol });
    fmpProfile = normalizeFmpProfile(fmpData);
    sourceStatus.push({
      provider: "fmp",
      status: fmpProfile ? "ok" : "partial",
      note: fmpProfile ? "fmp stable profile loaded" : "fmp stable profile empty"
    });
  } catch (e) {
    sourceStatus.push({
      provider: "fmp",
      status: "partial",
      note: `fmp stable profile unavailable: ${e.message}`
    });
  }

  try {
    const profile = await finnhubGet("/stock/profile2", { symbol });
    if (!profile || !profile.name) throw new Error("finnhub profile empty");

    sourceStatus.unshift({
      provider: "finnhub",
      status: "ok",
      note: "primary profile loaded"
    });
    return res.json(
      success(mapFinnhubProfileToSecurityMaster(symbol, profile, fmpProfile), sourceStatus, warnings)
    );
  } catch (e) {
    sourceStatus.unshift({
      provider: "finnhub",
      status: "partial",
      note: `primary profile unavailable: ${e.message}`
    });
  }

  if (fmpProfile) {
    const { framework_id } = classifyLocal(symbol);
    return res.json(
      success(
        {
          symbol,
          security_name: fmpProfile.companyName || symbol,
          exchange: fmpProfile.exchange || "",
          country: fmpProfile.country || "",
          sector: fmpProfile.sector || "",
          industry: fmpProfile.industry || "",
          trading_currency: fmpProfile.currency || "USD",
          reporting_currency: fmpProfile.currency || "USD",
          fiscal_year_end: knownFiscalYearEnd[symbol] || "12-31",
          is_adr: framework_id === "adr_equity_core",
          adr_ratio: symbol === "BABA" ? 8.0 : null,
          framework_id
        },
        sourceStatus,
        ["Primary source unavailable, using FMP stable fallback for security master."]
      )
    );
  }

  return res
    .status(502)
    .json(
      fail(
        "ALL_PROVIDERS_UNAVAILABLE",
        "Unable to load security master from Finnhub/FMP.",
        502,
        sourceStatus
      )
    );
});

app.post("/v1/market-price-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  const historyYears = Math.max(
    1,
    Math.min(10, Number(req.body.history_years || 1))
  );

  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
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

    if (toNum(quote?.c, null) === null) {
      throw new Error("finnhub quote missing c");
    }

    finnhubQuote = quote;
    sourceStatus.push({
      provider: "finnhub",
      status: "ok",
      note: "finnhub quote/profile loaded"
    });
  } catch (e) {
    sourceStatus.push({
      provider: "finnhub",
      status: "partial",
      note: `finnhub quote/profile unavailable: ${e.message}`
    });
  }

  try {
    const candles = await finnhubGet("/stock/candle", {
      symbol,
      resolution: "D",
      from: fromSec,
      to: nowSec
    });

    finnhubOHLCV = buildFinnhubOHLCV(candles);

    sourceStatus.push({
      provider: "finnhub_candles",
      status: finnhubOHLCV.length > 0 ? "ok" : "partial",
      note:
        finnhubOHLCV.length > 0
          ? "finnhub candles loaded"
          : `finnhub candles unavailable: status=${String(candles?.s)}`
    });
  } catch (e) {
    sourceStatus.push({
      provider: "finnhub_candles",
      status: "partial",
      note: `finnhub candles unavailable: ${e.message}`
    });
  }

  try {
    const [profileData, historicalData] = await Promise.all([
      fmpStableGet("profile", { symbol }),
      fmpStableGet("historical-price-eod/full", { symbol })
    ]);

    fmpProfile = normalizeFmpProfile(profileData);
    fmpOHLCV = buildFmpOHLCV(
      historicalData,
      Math.min(252 * historyYears, 2520)
    );

    sourceStatus.push({
      provider: "fmp",
      status: fmpProfile ? "ok" : "partial",
      note: fmpProfile ? "fmp stable profile loaded" : "fmp stable profile empty"
    });

    sourceStatus.push({
      provider: "fmp_history",
      status: fmpOHLCV.length > 0 ? "ok" : "partial",
      note: fmpOHLCV.length > 0 ? "fmp historical eod loaded" : "fmp historical eod empty"
    });
  } catch (e) {
    sourceStatus.push({
      provider: "fmp",
      status: "partial",
      note: `fmp stable fallback unavailable: ${e.message}`
    });
  }

  try {
    const toDate = new Date();
    const fromDate = new Date();
    fromDate.setUTCDate(fromDate.getUTCDate() - historyYears * 365);

    const msData = await marketstackGet("eod", {
      symbols: symbol,
      date_from: formatDateYYYYMMDD(fromDate),
      date_to: formatDateYYYYMMDD(toDate),
      limit: Math.min(1000, 260 * historyYears)
    });

    marketstackOHLCV = buildMarketstackOHLCV(
      msData,
      Math.min(252 * historyYears, 2520)
    );

    sourceStatus.push({
      provider: "marketstack",
      status: marketstackOHLCV.length > 0 ? "ok" : "partial",
      note:
        marketstackOHLCV.length > 0
          ? "marketstack eod history loaded"
          : "marketstack eod history empty"
    });
  } catch (e) {
    sourceStatus.push({
      provider: "marketstack",
      status: "partial",
      note: `marketstack fallback unavailable: ${e.message}`
    });
  }

  let priceCurrent = toNum(finnhubQuote?.c, null);
  if (priceCurrent === null && fmpProfile) {
    priceCurrent = toNum(fmpProfile.price, null);
    if (priceCurrent !== null) {
      warnings.push(
        "Using FMP profile price fallback because Finnhub quote was unavailable."
      );
    }
  }

  let ohlcv = [];
  if (finnhubOHLCV.length > 0) ohlcv = finnhubOHLCV;
  else if (fmpOHLCV.length > 0) ohlcv = fmpOHLCV;
  else if (marketstackOHLCV.length > 0) {
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

      const problem = alphaExtractProblem(dailyRaw);

      if (problem) {
        sourceStatus.push(alphaProblemToSourceStatus("alpha_vantage", problem));
      } else {
        const alphaOHLCV = buildAlphaOHLCV(
          dailyRaw?.["Time Series (Daily)"] ||
            dailyRaw?.["Time Series (Daily Adjusted)"],
          120
        );
        if (alphaOHLCV.length > 0) {
          ohlcv = alphaOHLCV;
          sourceStatus.push({
            provider: "alpha_vantage",
            status: "ok",
            note: "alpha daily fallback loaded"
          });
          warnings.push("Using Alpha Vantage fallback for OHLCV.");
        } else {
          sourceStatus.push({
            provider: "alpha_vantage",
            status: "partial",
            note: "alpha daily payload empty"
          });
        }
      }
    } catch (e) {
      sourceStatus.push({
        provider: "alpha_vantage",
        status: "partial",
        note: `alpha fallback unavailable: ${e.message}`
      });
    }
  }

  if (priceCurrent === null || ohlcv.length === 0) {
    return res
      .status(502)
      .json(
        fail(
          "ALL_PROVIDERS_UNAVAILABLE",
          "Unable to assemble a usable market price pack from Finnhub/FMP/Marketstack/Alpha.",
          502,
          sourceStatus,
          warnings
        )
      );
  }

  const marketCap =
    toNum(fmpProfile?.marketCap, null) ??
    (toNum(fmpProfile?.sharesOutstanding, null) !== null
      ? toNum(fmpProfile.sharesOutstanding, null) * priceCurrent
      : null);

  const sharesOutstanding =
    toNum(fmpProfile?.sharesOutstanding, null) ??
    (marketCap !== null && priceCurrent > 0 ? marketCap / priceCurrent : null);

  return res.json(
    success(
      {
        price_current: priceCurrent,
        price_timestamp: finnhubQuote?.t
          ? new Date(finnhubQuote.t * 1000).toISOString()
          : new Date().toISOString(),
        market_cap_current: marketCap,
        enterprise_value_current: marketCap,
        shares_outstanding_current: sharesOutstanding,
        beta_snapshot: toNum(fmpProfile?.beta, null),
        ohlcv
      },
      sourceStatus,
      warnings
    )
  );
});

app.post("/v1/fundamental-actuals-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  const annualYears = Math.max(
    1,
    Math.min(10, Number(req.body.annual_years || 10))
  );
  const quarterlyPeriods = Math.max(
    1,
    Math.min(12, Number(req.body.quarterly_periods || 12))
  );
  const includeTtm = req.body.include_ttm !== false;

  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const { instrument_type } = classifyLocal(symbol);
  if (!["us_equity", "adr_equity"].includes(instrument_type)) {
    const unsupported = buildUnsupportedActualsPayload(instrument_type);
    return res.json(
      success(unsupported.data, unsupported.sourceStatus, unsupported.warnings)
    );
  }

  const cacheKey = buildActualsCacheKey(
    symbol,
    annualYears,
    quarterlyPeriods,
    includeTtm
  );
  const cached = getCacheEntry(actualsCache, cacheKey);
  if (cached) {
    return res.json(
      success(
        cached.data,
        [{ provider: "cache", status: "ok", note: "fundamental actuals cache hit" }, ...cached.sourceStatus],
        cached.warnings
      )
    );
  }

  try {
    let sharedPromise = actualsInflight.get(cacheKey);
    const hadInflight = !!sharedPromise;

    if (!sharedPromise) {
      sharedPromise = (async () => {
        try {
          return await fetchSecFundamentalActuals(
            symbol,
            annualYears,
            quarterlyPeriods,
            includeTtm
          );
        } catch (secErr) {
          const secSourceStatus = secErr?.sourceStatus || [];
          const secWarnings = secErr?.warnings || [];
          const secMessage = secErr?.message || "SEC actuals unavailable.";

          try {
            const alphaResult = await fetchAlphaFundamentalActuals(
              symbol,
              annualYears,
              quarterlyPeriods,
              includeTtm
            );
            return {
              data: alphaResult.data,
              sourceStatus: [
                ...secSourceStatus,
                {
                  provider: "dispatcher",
                  status: "partial",
                  note: `SEC primary failed; falling back to Alpha Vantage. reason=${secMessage}`
                },
                ...alphaResult.sourceStatus
              ],
              warnings: [
                ...secWarnings,
                "SEC primary actuals failed; using Alpha Vantage fallback.",
                ...(alphaResult.warnings || [])
              ]
            };
          } catch (alphaErr) {
            const combined = new Error(
              "Unable to assemble usable fundamental actuals from SEC/Alpha providers."
            );
            combined.isKnownUpstream = true;
            combined.httpStatus =
              alphaErr?.httpStatus || secErr?.httpStatus || 502;
            combined.code =
              alphaErr?.code || secErr?.code || "ALL_PROVIDERS_UNAVAILABLE";
            combined.retryable = !!(alphaErr?.retryable || secErr?.retryable);
            combined.sourceStatus = [
              ...secSourceStatus,
              {
                provider: "dispatcher",
                status: "partial",
                note: `SEC primary failed; attempting Alpha Vantage fallback. reason=${secMessage}`
              },
              ...(alphaErr?.sourceStatus || [])
            ];
            combined.warnings = [...secWarnings, ...(alphaErr?.warnings || [])];
            throw combined;
          }
        }
      })();
      actualsInflight.set(cacheKey, sharedPromise);
    }

    const result = await sharedPromise;
    setCacheEntry(actualsCache, cacheKey, result, ACTUALS_CACHE_TTL_MS);

    const sourceStatus = hadInflight
      ? [
          {
            provider: "inflight",
            status: "ok",
            note: "reused in-flight fundamental actuals request"
          },
          ...result.sourceStatus
        ]
      : result.sourceStatus;

    return res.json(success(result.data, sourceStatus, result.warnings));
  } catch (e) {
    if (e?.isKnownUpstream) {
      return res
        .status(e.httpStatus || 502)
        .json(
          fail(
            e.code || "ALL_PROVIDERS_UNAVAILABLE",
            e.message ||
              "Unable to assemble usable fundamental actuals from SEC/Alpha providers.",
            e.httpStatus || 502,
            e.sourceStatus || [],
            e.warnings || [],
            !!e.retryable
          )
        );
    }

    return res
      .status(502)
      .json(
        fail(
          "ALL_PROVIDERS_UNAVAILABLE",
          "Unable to assemble usable fundamental actuals from SEC/Alpha providers.",
          502,
          [
            {
              provider: "dispatcher",
              status: "partial",
              note: `unexpected actuals dispatcher failure: ${e.message}`
            }
          ],
          [],
          false
        )
      );
  } finally {
    actualsInflight.delete(cacheKey);
  }
});

app.post("/v1/estimates-targets-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(
    success(
      {
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
      },
      [{ provider: "mock", status: "ok", note: "estimates pack still mock in phase 1" }]
    )
  );
});

app.post("/v1/macro-breadth-liquidity-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(
    success(
      {
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
      },
      [{ provider: "mock", status: "ok", note: "macro pack still mock in phase 1" }]
    )
  );
});

app.post("/v1/filings-transcripts-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(
    success(
      {
        filings: [],
        transcripts: [],
        guidance_notes: ["filings/transcripts pack still mock in phase 1"]
      },
      [{ provider: "mock", status: "ok", note: "filings/transcripts pack still mock in phase 1" }]
    )
  );
});

app.post("/v1/technical-structure-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  return res.json(
    success(
      {
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
      },
      [{ provider: "mock", status: "ok", note: "technical pack still mock in phase 1" }]
    )
  );
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
