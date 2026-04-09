const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json());

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "";
const ALPHAVANTAGE_API_KEY = process.env.ALPHAVANTAGE_API_KEY || "";
const FMP_API_KEY = process.env.FMP_API_KEY || "";
const MARKETSTACK_ACCESS_KEY = process.env.MARKETSTACK_ACCESS_KEY || "";
const FRED_API_KEY = process.env.FRED_API_KEY || "";
const TRADINGECONOMICS_API_KEY =
  process.env.TRADINGECONOMICS_API_KEY || "guest:guest";
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

const SEC_QUARTERLY_FORMS = new Set(["10-Q", "10-Q/A", "6-K", "6-K/A"]);
const SEC_TAXONOMY_PRIORITY = ["us-gaap", "ifrs-full", "srt"];

const SEC_METRIC_TAGS = {
  revenue: {
    "us-gaap": [
      "RevenueFromContractWithCustomerExcludingAssessedTax",
      "SalesRevenueNet",
      "Revenues",
      "RevenueFromContractWithCustomerIncludingAssessedTax"
    ],
    "ifrs-full": ["Revenue"],
    srt: []
  },
  gross_profit: {
    "us-gaap": ["GrossProfit"],
    "ifrs-full": ["GrossProfit"],
    srt: []
  },
  ebit: {
    "us-gaap": ["OperatingIncomeLoss"],
    "ifrs-full": ["ProfitLossFromOperatingActivities", "OperatingProfitLoss"],
    srt: []
  },
  net_income: {
    "us-gaap": ["NetIncomeLoss", "ProfitLoss"],
    "ifrs-full": ["ProfitLoss"],
    srt: []
  },
  eps_gaap: {
    "us-gaap": ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted"],
    "ifrs-full": [
      "DilutedEarningsLossPerShare",
      "BasicAndDilutedEarningsLossPerShare"
    ],
    srt: []
  },
  cfo: {
    "us-gaap": [
      "NetCashProvidedByUsedInOperatingActivities",
      "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"
    ],
    "ifrs-full": ["CashFlowsFromUsedInOperatingActivities"],
    srt: []
  },
  capex: {
    "us-gaap": [
      "PaymentsToAcquirePropertyPlantAndEquipment",
      "PropertyPlantAndEquipmentAdditions"
    ],
    "ifrs-full": [
      "PurchaseOfPropertyPlantAndEquipment",
      "PropertyPlantAndEquipmentAdditions"
    ],
    srt: []
  },
  cash: {
    "us-gaap": [
      "CashAndCashEquivalentsAtCarryingValue",
      "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
      "CashCashEquivalentsAndShortTermInvestments"
    ],
    "ifrs-full": [
      "CashAndCashEquivalents",
      "CashAndCashEquivalentsIfDifferentFromStatementOfFinancialPosition"
    ],
    srt: []
  },
  debt_total: {
    "us-gaap": [
      "LongTermDebtAndCapitalLeaseObligations",
      "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities",
      "DebtAndFinanceLeaseObligations",
      "DebtInstrumentFaceAmount"
    ],
    "ifrs-full": ["Borrowings"],
    srt: []
  },
  debt_current: {
    "us-gaap": [
      "LongTermDebtCurrent",
      "LongTermDebtAndCapitalLeaseObligationsCurrent",
      "ShortTermBorrowings"
    ],
    "ifrs-full": ["CurrentBorrowings"],
    srt: []
  },
  debt_noncurrent: {
    "us-gaap": [
      "LongTermDebtNoncurrent",
      "LongTermDebtAndCapitalLeaseObligationsNoncurrent"
    ],
    "ifrs-full": ["NoncurrentBorrowings"],
    srt: []
  },
  diluted_shares_duration: {
    "us-gaap": ["WeightedAverageNumberOfDilutedSharesOutstanding"],
    "ifrs-full": [
      "WeightedAverageNumberOfSharesOutstandingDiluted",
      "WeightedAverageNumberOfOrdinarySharesOutstandingDiluted"
    ],
    srt: []
  },
  common_shares_outstanding: {
    "us-gaap": ["CommonStockSharesOutstanding"],
    "ifrs-full": ["NumberOfSharesOutstanding"],
    srt: []
  }
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
    }))    .filter(
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
      .map((r) => toNum(alphaChoose(...fieldNames.map((f) => r[f])), null))
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
  })();  const quarterlyEpsSum = (() => {
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

function buildActualsCacheKey(symbol, annualYears, quarterlyPeriods, includeTtm) {
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

function secTaxonomyRank(taxonomy) {
  const idx = SEC_TAXONOMY_PRIORITY.indexOf(taxonomy);
  return idx === -1 ? 999 : idx;
}

function secFactRowsByTag(companyFacts, taxonomy, tag, unitsPriority = []) {
  const unitsObj = companyFacts?.facts?.[taxonomy]?.[tag]?.units || null;
  if (!unitsObj || typeof unitsObj !== "object") return [];

  const selectedUnits =
    unitsPriority.length > 0
      ? unitsPriority.filter((u) => Array.isArray(unitsObj[u]))
      : Object.keys(unitsObj);

  const rows = [];
  for (const unit of selectedUnits) {
    for (const row of unitsObj[unit] || []) {
      rows.push({
        taxonomy,
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

function secGetRowsForMetric(
  companyFacts,
  metricKey,
  unitsPriority = ["USD", "USD/shares", "shares", "pure"]
) {
  const metric = SEC_METRIC_TAGS[metricKey] || {};
  let rows = [];

  for (const taxonomy of SEC_TAXONOMY_PRIORITY) {
    const tags = Array.isArray(metric[taxonomy]) ? metric[taxonomy] : [];
    for (const tag of tags) {
      rows = rows.concat(secFactRowsByTag(companyFacts, taxonomy, tag, unitsPriority));
    }
  }

  return rows;
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
  if (/^Q[1-4]$/.test(String(row.fp || ""))) {
    return row.durationDays >= 70 && row.durationDays <= 110;
  }
  return (
    row.durationDays >= 70 &&
    row.durationDays <= 110 &&
    (SEC_QUARTERLY_FORMS.has(row.form) || !SEC_ANNUAL_FORMS.has(row.form))
  );
}

function secIsNineMonthRow(row) {
  if (!row || row.durationDays === null) return false;
  return row.durationDays >= 240 && row.durationDays <= 300;
}

function secSortRowsForPreference(rows = []) {
  return [...rows].sort((a, b) => {
    const taxonomyDiff =
      secTaxonomyRank(a.taxonomy) - secTaxonomyRank(b.taxonomy);
    if (taxonomyDiff !== 0) return taxonomyDiff;

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

function secFindBestInstantValue(companyFacts, metricKey, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForMetric(companyFacts, metricKey, [
      "USD",
      "shares",
      "USD/shares",
      "pure"
    ]).filter((row) => row.end === periodEnd)
  );
  return candidates[0]?.val ?? null;
}

function secFindBestAnnualFlowRow(companyFacts, metricKey, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForMetric(companyFacts, metricKey, [
      "USD",
      "USD/shares",
      "pure"
    ]).filter((row) => row.end === periodEnd && secIsAnnualRow(row))
  );
  return candidates[0] || null;
}

function secFindBestQuarterFlowRow(companyFacts, metricKey, periodEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForMetric(companyFacts, metricKey, [
      "USD",
      "USD/shares",
      "pure"
    ]).filter((row) => row.end === periodEnd && secIsSingleQuarterRow(row))
  );
  return candidates[0] || null;
}function secFindBestNineMonthRow(companyFacts, metricKey, fiscalYear, annualEnd) {
  const candidates = secSortRowsForPreference(
    secGetRowsForMetric(companyFacts, metricKey, [
      "USD",
      "USD/shares",
      "pure"
    ]).filter((row) => {
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
    })
  );

  if (candidates.length === 0) return null;
  candidates.sort((a, b) => {
    const endDiff = secParseDateMs(b.end) - secParseDateMs(a.end);
    if (endDiff !== 0) return endDiff;
    return secParseDateMs(b.filed) - secParseDateMs(a.filed);
  });
  return candidates[0];
}

function secFindQuarterFlowValue(companyFacts, metricKey, periodEnd) {
  const directQuarter = secFindBestQuarterFlowRow(companyFacts, metricKey, periodEnd);
  if (directQuarter) return directQuarter.val;

  const annualRow = secFindBestAnnualFlowRow(companyFacts, metricKey, periodEnd);
  if (!annualRow) return null;

  const nineMonthRow = secFindBestNineMonthRow(
    companyFacts,
    metricKey,
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

function secBuildAnnualPeriods(companyFacts, annualYears) {
  const revenueRows = secGetRowsForMetric(companyFacts, "revenue", ["USD"]);
  const annualRevenueRows = secSortRowsForPreference(
    revenueRows.filter(secIsAnnualRow)
  );

  const uniqueAnnualEnds = [];
  const seen = new Set();
  for (const row of annualRevenueRows) {
    if (!seen.has(row.end)) {
      uniqueAnnualEnds.push(row);
      seen.add(row.end);
    }
  }

  const selected = uniqueAnnualEnds.slice(0, annualYears);

  return selected.map((row) => {
    const periodEnd = row.end;
    const revenue = row.val;
    const grossProfit = secFindBestAnnualFlowRow(companyFacts, "gross_profit", periodEnd)?.val ?? null;
    const ebit = secFindBestAnnualFlowRow(companyFacts, "ebit", periodEnd)?.val ?? null;
    const netIncome = secFindBestAnnualFlowRow(companyFacts, "net_income", periodEnd)?.val ?? null;
    const epsGaap = secFindBestAnnualFlowRow(companyFacts, "eps_gaap", periodEnd)?.val ?? null;
    const cfo = secFindBestAnnualFlowRow(companyFacts, "cfo", periodEnd)?.val ?? null;
    const capexVal = secFindBestAnnualFlowRow(companyFacts, "capex", periodEnd)?.val ?? null;
    const capex = capexVal === null ? null : Math.abs(capexVal);
    const fcff = cfo !== null && capex !== null ? cfo - capex : null;

    const cash = secFindBestInstantValue(companyFacts, "cash", periodEnd);
    const debtTotal = secFindBestInstantValue(companyFacts, "debt_total", periodEnd);
    const debtCurrent = secFindBestInstantValue(companyFacts, "debt_current", periodEnd);
    const debtNoncurrent = secFindBestInstantValue(companyFacts, "debt_noncurrent", periodEnd);
    const debt =
      debtTotal ??
      (debtCurrent !== null || debtNoncurrent !== null
        ? (debtCurrent || 0) + (debtNoncurrent || 0)
        : null);

    const dilutedShares =
      secFindBestAnnualFlowRow(companyFacts, "diluted_shares_duration", periodEnd)?.val ??
      secFindBestInstantValue(companyFacts, "common_shares_outstanding", periodEnd);

    const fiscalYear = toNum(row.fy, null);

    return {
      fiscal_period: `FY${fiscalYear ?? ""}`,
      fiscal_year: fiscalYear,
      period_type: "annual",
      period_end: periodEnd,
      filing_date: row.filed || null,
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

function secBuildQuarterlyPeriods(companyFacts, quarterlyPeriods) {
  const revenueRows = secGetRowsForMetric(companyFacts, "revenue", ["USD"]);
  const quarterRevenueRows = secSortRowsForPreference(
    revenueRows.filter(secIsSingleQuarterRow)
  );

  const uniqueQuarterEnds = [];
  const seen = new Set();
  for (const row of quarterRevenueRows) {
    if (!seen.has(row.end)) {
      uniqueQuarterEnds.push(row);
      seen.add(row.end);
    }
  }

  const selected = uniqueQuarterEnds.slice(0, quarterlyPeriods);

  return selected
    .map((row) => {
      const periodEnd = row.end;
      const revenue = row.val;
      const grossProfit = secFindQuarterFlowValue(companyFacts, "gross_profit", periodEnd);
      const ebit = secFindQuarterFlowValue(companyFacts, "ebit", periodEnd);
      const netIncome = secFindQuarterFlowValue(companyFacts, "net_income", periodEnd);
      const epsGaap = secFindQuarterFlowValue(companyFacts, "eps_gaap", periodEnd);
      const cfo = secFindQuarterFlowValue(companyFacts, "cfo", periodEnd);
      const capexVal = secFindQuarterFlowValue(companyFacts, "capex", periodEnd);
      const capex = capexVal === null ? null : Math.abs(capexVal);
      const fcff = cfo !== null && capex !== null ? cfo - capex : null;

      const cash = secFindBestInstantValue(companyFacts, "cash", periodEnd);
      const debtTotal = secFindBestInstantValue(companyFacts, "debt_total", periodEnd);
      const debtCurrent = secFindBestInstantValue(companyFacts, "debt_current", periodEnd);
      const debtNoncurrent = secFindBestInstantValue(companyFacts, "debt_noncurrent", periodEnd);
      const debt =
        debtTotal ??
        (debtCurrent !== null || debtNoncurrent !== null
          ? (debtCurrent || 0) + (debtNoncurrent || 0)
          : null);

      const dilutedShares =
        secFindBestQuarterFlowRow(companyFacts, "diluted_shares_duration", periodEnd)?.val ??
        secFindBestInstantValue(companyFacts, "common_shares_outstanding", periodEnd);

      return {
        fiscal_period: row.fp || null,
        fiscal_year: toNum(row.fy, null),
        period_type: "quarterly",
        period_end: periodEnd,
        filing_date: row.filed || null,
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
    })
    .filter(secHasMeaningfulQuarter);
}

function secBuildTtmFromQuarterlies(quarterlies) {
  const q = [...quarterlies].slice(0, 4);
  if (q.length < 4) {
    return buildEmptyTtm();
  }

  const sumMaybe = (field) => {
    const vals = q.map((x) => x[field]).filter((v) => v !== null && v !== undefined);
    return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) : null;
  };

  const revenue = sumMaybe("revenue");
  const grossProfit = sumMaybe("gross_profit");
  const ebit = sumMaybe("ebit");
  const netIncome = sumMaybe("net_income");
  const cfo = sumMaybe("cfo");
  const capex = sumMaybe("capex");
  const fcff = cfo !== null && capex !== null ? cfo - capex : null;

  const latest = q[0];
  const dilutedShares = latest?.diluted_shares ?? null;
  const epsGaap =
    dilutedShares && dilutedShares !== 0 && netIncome !== null
      ? netIncome / dilutedShares
      : sumMaybe("eps_gaap");

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
    eps_gaap: epsGaap,
    eps_nongaap: epsGaap,
    cfo,
    capex,
    fcff,
    cash: latest?.cash ?? null,
    debt: latest?.debt ?? null,
    net_cash:
      latest?.cash !== null &&
      latest?.cash !== undefined &&
      latest?.debt !== null &&
      latest?.debt !== undefined  let finnhubProfile = null;
  let fmpProfile = null;
  const warnings = [];

  try {
    finnhubProfile = await finnhubGet("/stock/profile2", { symbol });
    sourceStatus.push({ provider: "finnhub_profile", status: "ok", note: "finnhub profile2 loaded" });
  } catch (e) {
    sourceStatus.push({ provider: "finnhub_profile", status: "partial", note: `finnhub profile2 unavailable: ${e.message}` });
  }

  try {
    const fmpData = await fmpStableGet("profile", { symbol });
    fmpProfile = normalizeFmpProfile(fmpData);
    if (fmpProfile) {
      sourceStatus.push({ provider: "fmp_profile", status: "ok", note: "fmp stable profile loaded" });
    } else {
      sourceStatus.push({ provider: "fmp_profile", status: "partial", note: "fmp stable profile empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp_profile", status: "partial", note: `fmp stable profile unavailable: ${e.message}` });
  }

  if (!finnhubProfile && !fmpProfile) {
    return res.status(502).json(fail(
      "ALL_PROVIDERS_UNAVAILABLE",
      "Unable to assemble a usable security master from Finnhub/FMP.",
      502,
      sourceStatus
    ));
  }

  const master = mapFinnhubProfileToSecurityMaster(symbol, finnhubProfile || {}, fmpProfile);

  if (!master.sector && !master.industry) {
    warnings.push("Sector/industry classification is sparse; relying on whichever provider filled profile fields.");
  }

  return res.json(success(master, sourceStatus, warnings));
});

app.post("/v1/market-price-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  const historyYears = Math.max(1, Math.min(10, Number(req.body.history_years || 3)));
  if (!symbol) {
    return res.status(400).json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const sourceStatus = [];
  const warnings = [];

  let finnhubQuote = null;
  try {
    finnhubQuote = await finnhubGet("/quote", { symbol });
    sourceStatus.push({ provider: "finnhub_quote", status: "ok", note: "finnhub quote loaded" });
  } catch (e) {
    sourceStatus.push({ provider: "finnhub_quote", status: "partial", note: `finnhub quote unavailable: ${e.message}` });
  }

  let finnhubOHLCV = [];
  try {
    const toDate = new Date();
    const fromDate = new Date();
    fromDate.setUTCDate(fromDate.getUTCDate() - historyYears * 365);

    const candles = await finnhubGet("/stock/candle", {
      symbol,
      resolution: "D",
      from: Math.floor(fromDate.getTime() / 1000),
      to: Math.floor(toDate.getTime() / 1000)
    });
    finnhubOHLCV = buildFinnhubOHLCV(candles);
    if (finnhubOHLCV.length > 0) {
      sourceStatus.push({ provider: "finnhub_candles", status: "ok", note: "finnhub candles loaded" });
    } else {
      sourceStatus.push({ provider: "finnhub_candles", status: "partial", note: "finnhub candles empty or not ok" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "finnhub_candles", status: "partial", note: `finnhub candles unavailable: ${e.message}` });
  }

  let fmpProfile = null;
  try {
    const fmpData = await fmpStableGet("profile", { symbol });
    fmpProfile = normalizeFmpProfile(fmpData);
    if (fmpProfile) {
      sourceStatus.push({ provider: "fmp_profile", status: "ok", note: "fmp stable profile loaded" });
    } else {
      sourceStatus.push({ provider: "fmp_profile", status: "partial", note: "fmp stable profile empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp_profile", status: "partial", note: `fmp stable profile unavailable: ${e.message}` });
  }

  let fmpOHLCV = [];
  try {
    const fmpHist = await fmpStableGet("historical-price-eod/full", {
      symbol,
      limit: Math.min(252 * historyYears, 2520)
    });
    fmpOHLCV = buildFmpOHLCV(fmpHist, Math.min(252 * historyYears, 2520));
    if (fmpOHLCV.length > 0) {
      sourceStatus.push({ provider: "fmp_eod", status: "ok", note: "fmp stable historical eod loaded" });
    } else {
      sourceStatus.push({ provider: "fmp_eod", status: "partial", note: "fmp stable historical eod empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "fmp_eod", status: "partial", note: `fmp stable historical eod unavailable: ${e.message}` });
  }

  let marketstackOHLCV = [];
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

  const cacheKey = buildActualsCacheKey(symbol, annualYears, quarterlyPeriods, includeTtm);
  const cached = getActualsCacheEntry(cacheKey);

  if (cached) {
    return res.json(success(
      cached.data,
      [{ provider: "cache", status: "ok", note: "fundamental actuals cache hit" }, ...cached.sourceStatus],
      cached.warnings
    ));
  }

  try {
    let sharedPromise = actualsInflight.get(cacheKey);
    const hadInflight = !!sharedPromise;

    if (!sharedPromise) {
      sharedPromise = fetchAlphaFundamentalActuals(symbol, annualYears, quarterlyPeriods, includeTtm);
      actualsInflight.set(cacheKey, sharedPromise);
    }

    const result = await sharedPromise;
    setActualsCacheEntry(cacheKey, result);    const sourceStatus = hadInflight
      ? [{ provider: "inflight", status: "ok", note: "reused in-flight fundamental actuals request" }, ...result.sourceStatus]
      : result.sourceStatus;

    return res.json(success(result.data, sourceStatus, result.warnings));
  } catch (e) {
    if (e?.isKnownUpstream) {
      return res.status(e.httpStatus || 502).json(fail(
        e.code || "UPSTREAM_PARTIAL_DATA",
        e.message || "Unable to assemble usable fundamental actuals from Alpha Vantage endpoints.",
        e.httpStatus || 502,
        e.sourceStatus || [],
        e.warnings || [],
        !!e.retryable
      ));
    }

    const sourceStatus = [
      { provider: "alpha_vantage_core", status: "partial", note: `alpha fundamental actuals unavailable: ${e.message}` }
    ];

    return res.status(502).json(fail(
      "ALL_PROVIDERS_UNAVAILABLE",
      "Unable to assemble usable fundamental actuals from Alpha Vantage endpoints.",
      502,
      sourceStatus,
      [],
      false
    ));
  } finally {
    actualsInflight.delete(cacheKey);
  }
});

function normalizeArrayPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.earningsCalendar)) return payload.earningsCalendar;
  if (Array.isArray(payload?.calendar)) return payload.calendar;
  return [];
}

function parseIsoDate(value) {
  if (!value) return null;
  const t = Date.parse(value);
  return Number.isFinite(t) ? new Date(t) : null;
}

function inferCurrentFiscalYear(symbol, asOf = new Date()) {
  const fye = knownFiscalYearEnd[symbol] || "12-31";
  const [mm, dd] = String(fye).split("-").map((x) => Number(x));
  const year = asOf.getUTCFullYear();
  const month = asOf.getUTCMonth() + 1;
  const day = asOf.getUTCDate();
  if (month > mm || (month === mm && day > dd)) return year + 1;
  return year;
}

function parseEstimateYear(raw) {
  if (raw === null || raw === undefined) return null;
  const s = String(raw).trim();
  const m = s.match(/(20\d{2})/);
  return m ? Number(m[1]) : null;
}

function pickFirstNum(obj, keys) {
  for (const key of keys) {
    const v = toNum(obj?.[key], null);
    if (v !== null) return v;
  }
  return null;
}

async function fredGet(path, params = {}) {
  if (!FRED_API_KEY) throw new Error("FRED_API_KEY missing");
  const resp = await axios.get(`https://api.stlouisfed.org/fred/${path}`, {
    params: { ...params, api_key: FRED_API_KEY, file_type: "json" },
    timeout: 15000
  });
  return resp.data;
}

async function tradingEconomicsGet(path, params = {}) {
  const resp = await axios.get(`https://api.tradingeconomics.com${path}`, {
    params: { ...params, c: TRADINGECONOMICS_API_KEY, f: "json" },
    timeout: 15000
  });
  return resp.data;
}

async function treasuryXmlGet(url) {
  const resp = await axios.get(url, {
    headers: {
      "User-Agent": SEC_USER_AGENT,
      Accept: "application/xml, text/xml;q=0.9, */*;q=0.8"
    },
    timeout: 15000
  });
  return String(resp.data || "");
}

function extractLatestXmlTagValue(xml, tagName) {
  const re = new RegExp(`<${tagName}>([^<]+)</${tagName}>`, "g");
  const matches = [...xml.matchAll(re)];
  if (!matches.length) return null;
  const raw = matches[matches.length - 1][1];
  return toNum(raw, null);
}

async function fredLatestObservation(seriesId) {
  const payload = await fredGet("series/observations", {
    series_id: seriesId,
    sort_order: "desc",
    limit: 12
  });
  const rows = Array.isArray(payload?.observations) ? payload.observations : [];
  for (const row of rows) {
    const v = toNum(row?.value, null);
    if (v !== null) return { date: row.date || null, value: v };
  }
  return { date: null, value: null };
}

async function fredYoy(seriesId) {
  const payload = await fredGet("series/observations", {
    series_id: seriesId,
    sort_order: "desc",
    limit: 24
  });
  const rows = Array.isArray(payload?.observations) ? payload.observations : [];
  const clean = rows
    .map((r) => ({ date: r?.date || null, value: toNum(r?.value, null) }))
    .filter((r) => r.value !== null);
  if (clean.length < 13) return { date: clean[0]?.date || null, value: null };
  return {
    date: clean[0].date,
    value: clean[12].value !== 0 ? clean[0].value / clean[12].value - 1 : null
  };
}

async function fredDiffLatest(seriesId) {
  const payload = await fredGet("series/observations", {
    series_id: seriesId,
    sort_order: "desc",
    limit: 3
  });
  const rows = Array.isArray(payload?.observations) ? payload.observations : [];
  const clean = rows
    .map((r) => ({ date: r?.date || null, value: toNum(r?.value, null) }))
    .filter((r) => r.value !== null);
  if (clean.length < 2) return { date: clean[0]?.date || null, value: null };
  return { date: clean[0].date, value: clean[0].value - clean[1].value };
}

async function tryFinnhub(path, params, providerName, sourceStatus, warnings) {
  try {
    const data = await finnhubGet(path, params);
    sourceStatus.push({ provider: providerName, status: "ok", note: `${providerName} loaded` });
    return data;
  } catch (e) {
    sourceStatus.push({
      provider: providerName,
      status: "partial",
      note: `${providerName} unavailable: ${e.message}`
    });
    warnings.push(`${providerName} unavailable: ${e.message}`);
    return null;
  }
}

function normalizeRecommendationTrendRows(payload) {
  return normalizeArrayPayload(payload)
    .map((r) => ({
      period: r?.period || null,
      strongBuy: toNum(r?.strongBuy, null),
      buy: toNum(r?.buy, null),
      hold: toNum(r?.hold, null),
      sell: toNum(r?.sell, null),
      strongSell: toNum(r?.strongSell, null)
    }))
    .filter((r) => r.period);
}

function normalizeFinnhubEarningsCalendar(payload, symbol) {
  const rows = normalizeArrayPayload(payload);
  const filtered = rows
    .filter((r) => String(r?.symbol || "").toUpperCase() === symbol)
    .sort((a, b) => secParseDateMs(a?.date) - secParseDateMs(b?.date));
  return filtered[0] || null;
}

function normalizeEstimateRows(payload, valueKeys = []) {
  return normalizeArrayPayload(payload)
    .map((r) => ({
      year: parseEstimateYear(r?.year ?? r?.period ?? r?.date ?? r?.fiscalYear),
      period: r?.period || r?.date || r?.year || null,
      avg: pickFirstNum(r, valueKeys),
      high: pickFirstNum(r, ["high", "estimateHigh", "epsHigh", "revenueHigh"]),
      low: pickFirstNum(r, ["low", "estimateLow", "epsLow", "revenueLow"]),
      analysts: pickFirstNum(r, ["numberAnalysts", "analystCount", "analysts"]),
      raw: r
    }))
    .filter((r) => r.year !== null);
}

function mapAnnualEstimateRowsToFY(rows, currentFY) {
  const byYear = new Map();
  for (const row of rows) {
    if (!byYear.has(row.year)) byYear.set(row.year, row);
  }
  return {
    fy0: byYear.get(currentFY) || null,
    fy1: byYear.get(currentFY + 1) || null,
    fy2: byYear.get(currentFY + 2) || null
  };
}

function buildEstimateCoverage({ recommendation, earningsCalendar, epsMap, revenueMap, priceTarget }) {
  const hasRecommendation = !!recommendation;
  const hasCalendar = !!earningsCalendar;
  const hasAnnualEstimates =
    !!(epsMap?.fy0?.avg || epsMap?.fy1?.avg || revenueMap?.fy0?.avg || revenueMap?.fy1?.avg);
  const hasPriceTarget =
    !!(
      priceTarget &&
      [
        toNum(priceTarget?.target_mean, null),
        toNum(priceTarget?.target_high, null),
        toNum(priceTarget?.target_low, null),
        toNum(priceTarget?.target_median, null)
      ].some((v) => v !== null)
    );

  if (hasRecommendation && hasCalendar && hasAnnualEstimates && hasPriceTarget) {
    return "full";
  }
  if (hasRecommendation || hasCalendar || hasAnnualEstimates || hasPriceTarget) {
    return "partial";
  }
  return "unavailable";
}

async function buildEstimatesTargetsPack(symbol) {
  const sourceStatus = [];
  const warnings = [];
  const currentFY = inferCurrentFiscalYear(symbol);

  const recommendationPayload = await tryFinnhub(
    "/stock/recommendation",
    { symbol },
    "finnhub_recommendation_trends",
    sourceStatus,
    warnings
  );
  const earningsCalendarPayload = await tryFinnhub(
    "/calendar/earnings",
    {
      symbol,
      from: formatDateYYYYMMDD(new Date()),
      to: formatDateYYYYMMDD(new Date(Date.now() + 120 * 86400000))
    },
    "finnhub_earnings_calendar",
    sourceStatus,
    warnings
  );
  const epsEstimatePayload = await tryFinnhub(
    "/stock/eps-estimate",
    { symbol, freq: "annual" },
    "finnhub_eps_estimate",
    sourceStatus,
    warnings
  );
  const revenueEstimatePayload = await tryFinnhub(
    "/stock/revenue-estimate",
    { symbol, freq: "annual" },
    "finnhub_revenue_estimate",
    sourceStatus,
    warnings
  );
  const priceTargetPayload = await tryFinnhub(
    "/stock/price-target",
    { symbol },
    "finnhub_price_target",
    sourceStatus,
    warnings
  );

  const recommendationRows = normalizeRecommendationTrendRows(recommendationPayload);
  const latestRecommendation = recommendationRows.sort(
    (a, b) => secParseDateMs(b.period) - secParseDateMs(a.period)
  )[0] || null;  const earningsCalendar = normalizeFinnhubEarningsCalendar(earningsCalendarPayload, symbol);

  const epsRows = normalizeEstimateRows(epsEstimatePayload, [
    "avg",
    "estimate",
    "epsAvg",
    "epsMean",
    "value"
  ]);
  const revenueRows = normalizeEstimateRows(revenueEstimatePayload, [
    "avg",
    "estimate",
    "revenueAvg",
    "revenueMean",
    "value"
  ]);
  const epsMap = mapAnnualEstimateRowsToFY(epsRows, currentFY);
  const revenueMap = mapAnnualEstimateRowsToFY(revenueRows, currentFY);

  const canonicalPriceTarget =
    priceTargetPayload && typeof priceTargetPayload === "object"
      ? {
          target_mean: toNum(priceTargetPayload?.targetMean, null),
          target_high: toNum(priceTargetPayload?.targetHigh, null),
          target_low: toNum(priceTargetPayload?.targetLow, null),
          target_median: toNum(priceTargetPayload?.targetMedian, null),
          last_updated: priceTargetPayload?.lastUpdated || null,
          provider: "finnhub_price_target"
        }
      : null;

  const coverageAssessment = buildEstimateCoverage({
    recommendation: latestRecommendation,
    earningsCalendar,
    epsMap,
    revenueMap,
    priceTarget: canonicalPriceTarget
  });

  if (coverageAssessment === "unavailable") {
    warnings.push("No usable structured estimates data was returned; all estimate fields remain null.");
  }

  const analystCount =
    epsMap?.fy1?.analysts ??
    epsMap?.fy0?.analysts ??
    revenueMap?.fy1?.analysts ??
    revenueMap?.fy0?.analysts ??
    null;

  const out = {
    coverage_assessment: coverageAssessment,
    current_fiscal_year: currentFY,
    canonical_estimates: {
      recommendation_trends: latestRecommendation
        ? {
            period: latestRecommendation.period,
            strong_buy: latestRecommendation.strongBuy,
            buy: latestRecommendation.buy,
            hold: latestRecommendation.hold,
            sell: latestRecommendation.sell,
            strong_sell: latestRecommendation.strongSell,
            provider: "finnhub_recommendation_trends"
          }
        : null,
      earnings_calendar: earningsCalendar
        ? {
            date: earningsCalendar?.date || null,
            hour: earningsCalendar?.hour || null,
            eps_estimate: toNum(
              earningsCalendar?.epsEstimate ?? earningsCalendar?.epsActualEstimate,
              null
            ),
            revenue_estimate: toNum(
              earningsCalendar?.revenueEstimate ?? earningsCalendar?.revenueActualEstimate,
              null
            ),
            provider: "finnhub_earnings_calendar",
            estimate_basis: "non_gaap_or_unknown"
          }
        : null,
      eps_revenue_estimates: {
        eps_fy0_est: epsMap?.fy0?.avg ?? null,
        eps_fy1_est: epsMap?.fy1?.avg ?? null,
        eps_fy2_est: epsMap?.fy2?.avg ?? null,
        revenue_fy0_est: revenueMap?.fy0?.avg ?? null,
        revenue_fy1_est: revenueMap?.fy1?.avg ?? null,
        revenue_fy2_est: revenueMap?.fy2?.avg ?? null,
        eps_basis: epsRows.length > 0 ? "non_gaap_or_unknown" : "unknown",
        revenue_basis: revenueRows.length > 0 ? "reported_currency_or_unknown" : "unknown",
        provider:
          epsRows.length > 0 || revenueRows.length > 0
            ? "finnhub_eps_revenue_estimates"
            : null
      },
      analyst_price_targets:
        canonicalPriceTarget &&
        [
          canonicalPriceTarget.target_mean,
          canonicalPriceTarget.target_high,
          canonicalPriceTarget.target_low,
          canonicalPriceTarget.target_median
        ].some((v) => v !== null)
          ? canonicalPriceTarget
          : null
    },
    reference_estimates: {
      provider: null,
      usage: "reference_only_not_canonical",
      notes: []
    },
    event_context: {
      next_earnings_date: earningsCalendar?.date || null,
      next_earnings_hour: earningsCalendar?.hour || null,
      provider: earningsCalendar ? "finnhub_earnings_calendar" : null
    },

    eps_fy0_est: epsMap?.fy0?.avg ?? null,
    eps_fy1_est: epsMap?.fy1?.avg ?? null,
    eps_fy2_est: epsMap?.fy2?.avg ?? null,
    revenue_fy0_est: revenueMap?.fy0?.avg ?? null,
    revenue_fy1_est: revenueMap?.fy1?.avg ?? null,
    revenue_fy2_est: revenueMap?.fy2?.avg ?? null,
    target_price_consensus: canonicalPriceTarget?.target_mean ?? null,
    target_price_low: canonicalPriceTarget?.target_low ?? null,
    target_price_high: canonicalPriceTarget?.target_high ?? null,
    analyst_count: analystCount,
    estimate_revision_direction: null,
    next_earnings_date: earningsCalendar?.date || null
  };

  return { data: out, sourceStatus, warnings };
}

async function getQuoteWithFallback(symbol) {
  const sourceStatus = [];
  let data = null;

  try {
    const q = await finnhubGet("/quote", { symbol });
    if (q && toNum(q.c, null) !== null) {
      data = {
        provider: "finnhub_quote",
        symbol,
        price: toNum(q.c, null),
        change: toNum(q.d, null),
        change_pct: toNum(q.dp, null),
        ts: q.t ? new Date(q.t * 1000).toISOString() : null
      };
      sourceStatus.push({ provider: "finnhub_quote", status: "ok", note: "quote loaded" });
      return { data, sourceStatus };
    }
    sourceStatus.push({ provider: "finnhub_quote", status: "partial", note: "empty quote response" });
  } catch (e) {
    sourceStatus.push({ provider: "finnhub_quote", status: "partial", note: `quote unavailable: ${e.message}` });
  }

  try {
    const alpha = await alphaGet({ function: "GLOBAL_QUOTE", symbol });
    const q = alpha?.["Global Quote"] || {};
    const price = toNum(q["05. price"], null);
    if (price !== null) {
      data = {
        provider: "alpha_global_quote",
        symbol,
        price,
        change: toNum(q["09. change"], null),
        change_pct: toNum(String(q["10. change percent"] || "").replace("%", ""), null),
        ts: null
      };
      sourceStatus.push({ provider: "alpha_global_quote", status: "ok", note: "global quote loaded" });
      return { data, sourceStatus };
    }
    sourceStatus.push({ provider: "alpha_global_quote", status: "partial", note: "alpha global quote empty" });
  } catch (e) {
    sourceStatus.push({ provider: "alpha_global_quote", status: "partial", note: `alpha global quote unavailable: ${e.message}` });
  }

  return { data: null, sourceStatus };
}

async function getProxyQuoteMap(symbols = []) {
  const results = await Promise.all(symbols.map((s) => getQuoteWithFallback(s)));
  const map = {};
  const sourceStatus = [];
  for (let i = 0; i < symbols.length; i++) {
    map[symbols[i]] = results[i].data;
    sourceStatus.push(...results[i].sourceStatus);
  }
  return { map, sourceStatus };
}

function selectTopBottomByChange(quotes = {}) {
  const rows = Object.entries(quotes)
    .map(([symbol, q]) => ({
      symbol,
      change_pct: toNum(q?.change_pct, null)
    }))
    .filter((r) => r.change_pct !== null)
    .sort((a, b) => b.change_pct - a.change_pct);

  return {
    leaders: rows.slice(0, 2),
    laggards: rows.slice(-2).reverse()
  };
}

function classifyMarketState({ spy, qqq, iwm, vix }) {
  const pos = [spy, qqq, iwm].filter((x) => x !== null && x > 0).length;
  if (vix !== null && vix >= 25 && pos <= 1) return "risk_off_high_vol";
  if (pos === 3 && (vix === null || vix < 20)) return "trend_up_broadening";
  if (pos >= 2) return "trend_up_but_selective";
  if (pos === 1) return "mixed";
  return "risk_off";
}

function classifyBreadthHealth({ leaders, laggards, spy, iwm, qqq }) {
  const leaderPos = leaders.filter((x) => x.change_pct !== null && x.change_pct > 0).length;
  const laggardNeg = laggards.filter((x) => x.change_pct !== null && x.change_pct < 0).length;
  if (spy !== null && qqq !== null && iwm !== null && spy > 0 && qqq > 0 && iwm > 0 && leaderPos >= 2) {
    return "healthy";
  }
  if (spy !== null && qqq !== null && iwm !== null && qqq > 0 && iwm < 0) {
    return "narrow";
  }
  if (laggardNeg >= 2) return "weak";
  return "neutral";
}

function classifyLiquidityState({ us10y, real10y, vix, breadthHealth }) {
  if ((us10y !== null && us10y >= 4.5) || (real10y !== null && real10y >= 2.0) || (vix !== null && vix >= 25)) {
    return "tight";
  }
  if (breadthHealth === "healthy" && (vix === null || vix < 20)) return "supportive";
  return "neutral";
}

function classifyMacroRegime({ cpiYoy, unemployment, payrolls }) {
  if (cpiYoy !== null && cpiYoy > 0.035 && unemployment !== null && unemployment < 0.045) {
    return "inflationary_with_resilient_growth";
  }
  if (cpiYoy !== null && cpiYoy < 0.03 && payrolls !== null && payrolls > 0) {
    return "disinflation_with_stable_growth";
  }
  return "mixed";
}

function shouldEnterValuation({ instrumentType = "us_equity", liquidityState, macroRiskLevel }) {
  if (!["us_equity", "adr_equity"].includes(instrumentType)) return "background_only";
  if (macroRiskLevel === "high") return "scenario_only";
  if (liquidityState === "tight") return "scenario_only";
  return "direct";
}

async function buildMacroBreadthLiquidityPack(symbol) {
  const sourceStatus = [];
  const warnings = [];
  const { instrument_type } = classifyLocal(symbol);

  const fredSeries = {
    cpi_yoy: null,
    core_cpi_yoy: null,
    pce_yoy: null,
    core_pce_yoy: null,
    unemployment_rate: null,
    payrolls_last_change: null,
    retail_sales_yoy: null,
    ism_manufacturing: null,
    dxy_broad: null,
    wti: null,
    brent: null,
    gold: null,
    silver: null,
    copper: null,
    natgas: null,
    vix: null,
    sp500: null,
    nasdaq: null
  };  if (FRED_API_KEY) {
    try {
      const [
        cpiYoy,
        coreCpiYoy,
        pceYoy,
        corePceYoy,
        unrate,
        payrollDiff,
        retailYoy,
        ismMfg,
        dxyBroad,
        wti,
        brent,
        gold,
        silver,
        copper,
        natgas,
        vix,
        sp500,
        nasdaq
      ] = await Promise.all([
        fredYoy("CPIAUCSL"),
        fredYoy("CPILFESL"),
        fredYoy("PCEPI"),
        fredYoy("PCEPILFE"),
        fredLatestObservation("UNRATE"),
        fredDiffLatest("PAYEMS"),
        fredYoy("RSAFS"),
        fredLatestObservation("NAPM"),
        fredLatestObservation("DTWEXBGS"),
        fredLatestObservation("DCOILWTICO"),
        fredLatestObservation("DCOILBRENTEU"),
        fredLatestObservation("GOLDAMGBD228NLBM"),
        fredLatestObservation("SLVPRUSD"),
        fredLatestObservation("PCOPPUSDM"),
        fredLatestObservation("DHHNGSP"),
        fredLatestObservation("VIXCLS"),
        fredLatestObservation("SP500"),
        fredLatestObservation("NASDAQCOM")
      ]);

      fredSeries.cpi_yoy = cpiYoy.value;
      fredSeries.core_cpi_yoy = coreCpiYoy.value;
      fredSeries.pce_yoy = pceYoy.value;
      fredSeries.core_pce_yoy = corePceYoy.value;
      fredSeries.unemployment_rate = unrate.value;
      fredSeries.payrolls_last_change = payrollDiff.value;
      fredSeries.retail_sales_yoy = retailYoy.value;
      fredSeries.ism_manufacturing = ismMfg.value;
      fredSeries.dxy_broad = dxyBroad.value;
      fredSeries.wti = wti.value;
      fredSeries.brent = brent.value;
      fredSeries.gold = gold.value;
      fredSeries.silver = silver.value;
      fredSeries.copper = copper.value;
      fredSeries.natgas = natgas.value;
      fredSeries.vix = vix.value;
      fredSeries.sp500 = sp500.value;
      fredSeries.nasdaq = nasdaq.value;

      sourceStatus.push({ provider: "fred", status: "ok", note: "macro series loaded" });
    } catch (e) {
      sourceStatus.push({ provider: "fred", status: "partial", note: `fred macro series unavailable: ${e.message}` });
      warnings.push(`fred macro series unavailable: ${e.message}`);
    }
  } else {
    sourceStatus.push({ provider: "fred", status: "partial", note: "FRED_API_KEY missing" });
    warnings.push("FRED_API_KEY missing; macro series coverage reduced.");
  }

  let treasuryRates = { us2y: null, us10y: null, us30y: null, real10y: null };
  try {
    const nominalXml = await treasuryXmlGet(
      "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve"
    );
    const realXml = await treasuryXmlGet(
      "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_real_yield_curve"
    );
    treasuryRates.us2y = extractLatestXmlTagValue(nominalXml, "BC_2YEAR");
    treasuryRates.us10y = extractLatestXmlTagValue(nominalXml, "BC_10YEAR");
    treasuryRates.us30y = extractLatestXmlTagValue(nominalXml, "BC_30YEAR");
    treasuryRates.real10y = extractLatestXmlTagValue(realXml, "TC_10YEAR");
    if (treasuryRates.us2y !== null || treasuryRates.us10y !== null || treasuryRates.real10y !== null) {
      sourceStatus.push({ provider: "treasury", status: "ok", note: "treasury yield curves loaded" });
    } else {
      sourceStatus.push({ provider: "treasury", status: "partial", note: "treasury xml parsed but values empty" });
    }
  } catch (e) {
    sourceStatus.push({ provider: "treasury", status: "partial", note: `treasury rates unavailable: ${e.message}` });
    warnings.push(`treasury rates unavailable: ${e.message}`);
  }

  const proxies = [
    "SPY", "QQQ", "DIA", "IWM",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLP", "XLY", "XLB", "XLU", "XLRE", "XLC",
    "VUG", "VTV"
  ];
  const { map: proxyQuotes, sourceStatus: proxySourceStatus } = await getProxyQuoteMap(proxies);
  sourceStatus.push(...proxySourceStatus);

  const sectors = {
    Technology: proxyQuotes.XLK,
    Financials: proxyQuotes.XLF,
    Energy: proxyQuotes.XLE,
    Healthcare: proxyQuotes.XLV,
    Industrials: proxyQuotes.XLI,
    Staples: proxyQuotes.XLP,
    Discretionary: proxyQuotes.XLY,
    Materials: proxyQuotes.XLB,
    Utilities: proxyQuotes.XLU,
    RealEstate: proxyQuotes.XLRE,
    Communication: proxyQuotes.XLC
  };

  const sectorRank = selectTopBottomByChange(
    Object.fromEntries(
      Object.entries(sectors).map(([k, v]) => [k, { change_pct: toNum(v?.change_pct, null) }])
    )
  );

  let calendarEvents = [];
  try {
    const rawCalendar = await tradingEconomicsGet("/calendar");
    calendarEvents = normalizeArrayPayload(rawCalendar)
      .slice(0, 300)
      .map((r) => ({
        date: r?.Date || r?.date || null,
        country: r?.Country || r?.country || null,
        event: r?.Event || r?.event || null,
        importance: r?.Importance || r?.importance || r?.Category || null,
        actual: r?.Actual ?? null,
        forecast: r?.Forecast ?? null,
        previous: r?.Previous ?? null
      }))
      .filter((r) => r.date && r.event);
    sourceStatus.push({ provider: "tradingeconomics_calendar", status: "ok", note: "calendar snapshot loaded" });
  } catch (e) {
    sourceStatus.push({ provider: "tradingeconomics_calendar", status: "partial", note: `calendar unavailable: ${e.message}` });
    warnings.push(`calendar unavailable: ${e.message}`);
  }

  const nextByRegex = (regex) =>
    calendarEvents
      .filter((r) => regex.test(String(r.event || "")) && secParseDateMs(r.date) >= Date.now())
      .sort((a, b) => secParseDateMs(a.date) - secParseDateMs(b.date))[0] || null;

  const nextCpi = nextByRegex(/\bCPI\b|Consumer Price/i);
  const nextNfp = nextByRegex(/Non Farm Payroll|Nonfarm Payroll|Payroll/i);
  const nextFomc = nextByRegex(/FOMC|Federal Reserve|Interest Rate Decision/i);
  const nextPce = nextByRegex(/\bPCE\b|Personal Consumption Expenditure/i);
  const nextRetail = nextByRegex(/Retail Sales/i);

  const spyChg = toNum(proxyQuotes.SPY?.change_pct, null);
  const qqqChg = toNum(proxyQuotes.QQQ?.change_pct, null);
  const iwmChg = toNum(proxyQuotes.IWM?.change_pct, null);

  const marketState = classifyMarketState({
    spy: spyChg,
    qqq: qqqChg,
    iwm: iwmChg,
    vix: fredSeries.vix
  });
  const breadthHealth = classifyBreadthHealth({
    leaders: sectorRank.leaders,
    laggards: sectorRank.laggards,
    spy: spyChg,
    iwm: iwmChg,
    qqq: qqqChg
  });
  const liquidityState = classifyLiquidityState({
    us10y: treasuryRates.us10y,
    real10y: treasuryRates.real10y,
    vix: fredSeries.vix,
    breadthHealth
  });
  const macroRegime = classifyMacroRegime({
    cpiYoy: fredSeries.cpi_yoy,
    unemployment: fredSeries.unemployment_rate,
    payrolls: fredSeries.payrolls_last_change
  });

  const calendarRiskLevel =
    [nextCpi, nextNfp, nextFomc].filter(Boolean).length >= 2
      ? "high"
      : [nextCpi, nextNfp, nextFomc].filter(Boolean).length === 1
      ? "medium"
      : "low";

  const shouldValuation = shouldEnterValuation({
    instrumentType: instrument_type,
    liquidityState,
    macroRiskLevel: calendarRiskLevel
  });

  const out = {
    as_of_time: new Date().toISOString(),
    rates: {
      us2y: treasuryRates.us2y,
      us10y: treasuryRates.us10y,
      us30y: treasuryRates.us30y,
      real10y: treasuryRates.real10y,
      curve_2s10s:
        treasuryRates.us10y !== null && treasuryRates.us2y !== null
          ? treasuryRates.us10y - treasuryRates.us2y
          : null,
      rate_regime:
        treasuryRates.us10y !== null
          ? treasuryRates.us10y >= 4.5
            ? "high_rate"
            : treasuryRates.us10y >= 3.5
            ? "restrictive_but_normalized"
            : "accommodative"
          : null,
      valuation_pressure:
        treasuryRates.us10y !== null || treasuryRates.real10y !== null
          ? liquidityState === "tight"
            ? "high"
            : liquidityState === "supportive"
            ? "low"
            : "medium"
          : null
    },
    macro_calendar: {
      next_cpi: nextCpi,
      next_nfp: nextNfp,
      next_fomc: nextFomc,
      next_pce: nextPce,
      next_retail_sales: nextRetail,
      calendar_risk_level: calendarRiskLevel
    },
    macro_series: {
      cpi_yoy: fredSeries.cpi_yoy,
      core_cpi_yoy: fredSeries.core_cpi_yoy,
      pce_yoy: fredSeries.pce_yoy,
      core_pce_yoy: fredSeries.core_pce_yoy,
      unemployment_rate: fredSeries.unemployment_rate,
      payrolls_last_change: fredSeries.payrolls_last_change,
      retail_sales_yoy: fredSeries.retail_sales_yoy,
      ism_manufacturing: fredSeries.ism_manufacturing,
      macro_growth_inflation_mix:
        fredSeries.cpi_yoy !== null && fredSeries.unemployment_rate !== null
          ? fredSeries.cpi_yoy > 0.03 && fredSeries.unemployment_rate < 0.045
            ? "hot_growth_hot_inflation"
            : fredSeries.cpi_yoy < 0.03 && fredSeries.unemployment_rate < 0.05
            ? "soft_landing"
            : "mixed"
          : null
    },
    market_environment: {
      spx_proxy: proxyQuotes.SPY,
      ndx_proxy: proxyQuotes.QQQ,
      dji_proxy: proxyQuotes.DIA,
      rut_proxy: proxyQuotes.IWM,
      vix: fredSeries.vix,
      top_sector_1: sectorRank.leaders[0] || null,
      top_sector_2: sectorRank.leaders[1] || null,
      weakest_sector_1: sectorRank.laggards[0] || null,
      weakest_sector_2: sectorRank.laggards[1] || null,
      growth_vs_value:
        toNum(proxyQuotes.VUG?.change_pct, null) !== null &&
        toNum(proxyQuotes.VTV?.change_pct, null) !== null
          ? proxyQuotes.VUG.change_pct > proxyQuotes.VTV.change_pct
            ? "growth_outperforming"
            : "value_outperforming"
          : null,
      large_vs_small:
        spyChg !== null && iwmChg !== null
          ? spyChg > iwmChg
            ? "large_outperforming"
            : "small_outperforming"
          : null,
      market_state: marketState
    },
    breadth: {
      breadth_health: breadthHealth,
      sector_leadership: {
        leaders: sectorRank.leaders,
        laggards: sectorRank.laggards
      },
      leadership_concentration:
        qqqChg !== null && iwmChg !== null
          ? qqqChg > iwmChg
            ? "mega_cap_concentrated"
            : "broadening"
          : null,
      risk_appetite:
        marketState === "trend_up_broadening"
          ? "risk_on"
          : marketState === "risk_off_high_vol" || marketState === "risk_off"
          ? "risk_off"
          : "mixed"
    },    liquidity: {
      liquidity_state: liquidityState,
      credit_conditions_note:
        treasuryRates.us10y !== null && treasuryRates.us10y >= 4.5
          ? "long-end yields remain restrictive"
          : "no acute rate stress signal",
      funding_stress_note:
        fredSeries.vix !== null && fredSeries.vix >= 25
          ? "volatility elevated; monitor funding and positioning"
          : "no acute volatility stress signal"
    },
    commodities_fx: {
      dxy_broad: fredSeries.dxy_broad,
      wti: fredSeries.wti,
      brent: fredSeries.brent,
      gold: fredSeries.gold,
      silver: fredSeries.silver,
      copper: fredSeries.copper,
      natgas: fredSeries.natgas,
      usd_regime:
        fredSeries.dxy_broad !== null
          ? fredSeries.dxy_broad >= 125
            ? "usd_strong"
            : "usd_neutral"
          : null,
      commodity_regime:
        fredSeries.wti !== null && fredSeries.gold !== null
          ? fredSeries.wti > 85
            ? "energy_tight"
            : fredSeries.gold > 2200
            ? "defensive_precious_metals_bid"
            : "mixed"
          : null
    },
    final_labels: {
      macro_regime: macroRegime,
      market_state: marketState,
      liquidity_state: liquidityState,
      breadth_health: breadthHealth,
      should_enter_valuation: shouldValuation
    },

    macro_regime: macroRegime,
    market_state: marketState,
    liquidity_state: liquidityState,
    should_enter_valuation: shouldValuation,
    risk_free_rate:
      treasuryRates.us10y !== null ? treasuryRates.us10y / 100 : null,
    yield_curve_slope:
      treasuryRates.us10y !== null && treasuryRates.us2y !== null
        ? (treasuryRates.us10y - treasuryRates.us2y) / 100
        : null,
    usd_context:
      fredSeries.dxy_broad !== null
        ? fredSeries.dxy_broad >= 125
          ? "firm"
          : "neutral"
        : null,
    breadth_score:
      breadthHealth === "healthy" ? 75 :
      breadthHealth === "narrow" ? 45 :
      breadthHealth === "weak" ? 35 :
      breadthHealth === "neutral" ? 55 : null,
    breadth_health: breadthHealth,
    breadth_notes: [
      sectorRank.leaders[0]
        ? `Leading sector proxy: ${sectorRank.leaders[0].symbol} (${sectorRank.leaders[0].change_pct}%).`
        : "No sector leadership signal available.",
      sectorRank.laggards[0]
        ? `Weakest sector proxy: ${sectorRank.laggards[0].symbol} (${sectorRank.laggards[0].change_pct}%).`
        : "No sector laggard signal available.",
      calendarRiskLevel === "high"
        ? "High macro event density ahead."
        : "No clustered macro risk window detected."
    ]
  };

  return { data: out, sourceStatus, warnings };
}

app.post("/v1/estimates-targets-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  try {
    const result = await buildEstimatesTargetsPack(symbol);

    if (result.data.coverage_assessment === "unavailable") {
      return res
        .status(502)
        .json(
          fail(
            "ALL_PROVIDERS_UNAVAILABLE",
            "Unable to assemble usable structured estimates/targets data.",
            502,
            result.sourceStatus,
            result.warnings,
            false
          )
        );
    }

    return res.json(success(result.data, result.sourceStatus, result.warnings));
  } catch (e) {
    return res
      .status(502)
      .json(
        fail(
          "ALL_PROVIDERS_UNAVAILABLE",
          `Unable to assemble usable structured estimates/targets data: ${e.message}`,
          502,
          [
            {
              provider: "estimates_dispatcher",
              status: "partial",
              note: `unexpected estimates failure: ${e.message}`
            }
          ],
          [],
          false
        )
      );
  }
});

app.post("/v1/macro-breadth-liquidity-pack", async (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  try {
    const result = await buildMacroBreadthLiquidityPack(symbol);
    return res.json(success(result.data, result.sourceStatus, result.warnings));
  } catch (e) {
    return res
      .status(502)
      .json(
        fail(
          "ALL_PROVIDERS_UNAVAILABLE",
          `Unable to assemble macro/breadth/liquidity pack: ${e.message}`,
          502,
          [
            {
              provider: "macro_dispatcher",
              status: "partial",
              note: `unexpected macro pack failure: ${e.message}`
            }
          ],
          [],
          false
        )
      );
  }
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
      estimates_targets_pack: true,
      macro_breadth_liquidity_pack: true,
      filings_transcripts_pack: false,
      technical_structure_pack: false
    }
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`API running on port ${PORT}`);
});
