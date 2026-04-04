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

app.post("/v1/estimates-targets-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const map = {
    MSFT: {
      eps_fy0_est: 13.2,
      eps_fy1_est: 14.4,
      eps_fy2_est: 15.8,
      revenue_fy0_est: 276000000000,
      revenue_fy1_est: 292000000000,
      revenue_fy2_est: 309000000000,
      target_price_consensus: 495.0,
      target_price_low: 430.0,
      target_price_high: 560.0,
      analyst_count: 42,
      estimate_revision_direction: "up"
    },
    BABA: {
      eps_fy0_est: 9.1,
      eps_fy1_est: 10.0,
      eps_fy2_est: 11.2,
      revenue_fy0_est: 980000000000,
      revenue_fy1_est: 1050000000000,
      revenue_fy2_est: 1125000000000,
      target_price_consensus: 108.0,
      target_price_low: 86.0,
      target_price_high: 132.0,
      analyst_count: 31,
      estimate_revision_direction: "mixed"
    },
    QQQ: {
      eps_fy0_est: null,
      eps_fy1_est: null,
      eps_fy2_est: null,
      revenue_fy0_est: null,
      revenue_fy1_est: null,
      revenue_fy2_est: null,
      target_price_consensus: null,
      target_price_low: null,
      target_price_high: null,
      analyst_count: 0,
      estimate_revision_direction: "unavailable"
    },
    GLD: {
      eps_fy0_est: null,
      eps_fy1_est: null,
      eps_fy2_est: null,
      revenue_fy0_est: null,
      revenue_fy1_est: null,
      revenue_fy2_est: null,
      target_price_consensus: null,
      target_price_low: null,
      target_price_high: null,
      analyst_count: 0,
      estimate_revision_direction: "unavailable"
    }
  };

  const data = map[symbol] || {
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
  };

  return res.json(
    success(data, [
      { provider: "mock", status: "ok", note: "mock estimates and target price used" }
    ])
  );
});

app.post("/v1/macro-breadth-liquidity-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const defaultData = {
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
  };

  const map = {
    MSFT: {
      ...defaultData,
      should_enter_valuation: "scenario_only"
    },
    BABA: {
      ...defaultData,
      usd_context: "firm_usd_is_headwind_for_adr_translation",
      should_enter_valuation: "direct"
    },
    QQQ: {
      ...defaultData,
      market_state: "trend_up_with_narrow_leadership",
      should_enter_valuation: "direct",
      breadth_notes: [
        "Nasdaq leadership remains concentrated in large-cap growth",
        "Equal-weight participation is weaker than cap-weight performance",
        "Broadening is improving but not yet decisive"
      ]
    },
    GLD: {
      macro_regime: "real_rate_sensitive_gold_environment",
      market_state: "range_bound_with_macro_bids",
      liquidity_state: "neutral",
      should_enter_valuation: "direct",
      risk_free_rate: 0.0435,
      yield_curve_slope: 0.0021,
      usd_context: "firm_usd_caps_gold_upside",
      breadth_score: 50.0,
      breadth_health: "neutral",
      breadth_notes: [
        "Gold-related products are more sensitive to real rates and USD than equity breadth",
        "Risk-off demand exists but is not dominant",
        "Macro impulse matters more than stock-style participation"
      ]
    }
  };

  const data = map[symbol] || defaultData;

  return res.json(
    success(data, [
      { provider: "mock", status: "ok", note: "mock macro breadth liquidity pack used" }
    ])
  );
});

app.post("/v1/filings-transcripts-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const map = {
    MSFT: {
      filings: [
        {
          form_type: "10-K",
          filing_date: "2025-08-01",
          period_end: "2025-06-30",
          source_url: "https://www.sec.gov/",
          title: "Annual report"
        },
        {
          form_type: "10-Q",
          filing_date: "2025-10-28",
          period_end: "2025-09-30",
          source_url: "https://www.sec.gov/",
          title: "Quarterly report"
        }
      ],
      transcripts: [
        {
          event_date: "2026-01-28",
          fiscal_label: "Q2 FY2026",
          source_url: "https://example.com/transcript/msft-q2fy26",
          provider_label: "mock"
        }
      ],
      guidance_notes: [
        "Management commentary emphasizes Azure and AI monetization.",
        "Capital spending remains elevated but tied to AI infrastructure expansion."
      ]
    },
    BABA: {
      filings: [
        {
          form_type: "20-F",
          filing_date: "2025-07-18",
          period_end: "2025-03-31",
          source_url: "https://www.sec.gov/",
          title: "Annual report"
        },
        {
          form_type: "6-K",
          filing_date: "2026-02-07",
          period_end: "2025-12-31",
          source_url: "https://www.sec.gov/",
          title: "Other report"
        }
      ],
      transcripts: [
        {
          event_date: "2026-02-07",
          fiscal_label: "FY2026 Q3",
          source_url: "https://example.com/transcript/baba-fy26q3",
          provider_label: "mock"
        }
      ],
      guidance_notes: [
        "ADR / FX effects should be separated from underlying operating growth.",
        "Foreign private issuer cadence follows 20-F / 6-K regime."
      ]
    },
    QQQ: {
      filings: [
        {
          form_type: "Prospectus/ETF disclosure",
          filing_date: "2025-12-01",
          period_end: null,
          source_url: "https://www.invesco.com/",
          title: "Product disclosure / prospectus update"
        }
      ],
      transcripts: [],
      guidance_notes: [
        "ETF framework: do not treat disclosures as company management earnings guidance.",
        "Use holdings, structure, fees, market breadth and macro sensitivity instead."
      ]
    },
    GLD: {
      filings: [
        {
          form_type: "Trust/ETF disclosure",
          filing_date: "2025-12-01",
          period_end: null,
          source_url: "https://www.ssga.com/",
          title: "Product disclosure"
        }
      ],
      transcripts: [],
      guidance_notes: [
        "Commodity ETF framework: focus on structure, fees, real rates, USD and underlying commodity."
      ]
    }
  };

  const data = map[symbol] || {
    filings: [
      {
        form_type: "10-K",
        filing_date: "2025-08-01",
        period_end: "2025-06-30",
        source_url: "https://www.sec.gov/",
        title: "Annual report"
      }
    ],
    transcripts: [],
    guidance_notes: [
      "Mock filing pack used. Replace with live SEC / transcript data later."
    ]
  };

  return res.json(
    success(data, [
      { provider: "mock", status: "ok", note: "mock filings and transcripts used" }
    ])
  );
});

app.post("/v1/technical-structure-pack", (req, res) => {
  const symbol = (req.body.symbol || "").toUpperCase().trim();
  if (!symbol) {
    return res
      .status(400)
      .json(fail("MISSING_REQUIRED_FIELD", "symbol is required.", 400));
  }

  const map = {
    MSFT: {
      trend_state: "daily_uptrend_weekly_consolidation",
      momentum_state: "moderately_positive",
      structure_tags: [
        "near_prior_breakout_zone",
        "weekly_consolidation",
        "no_confirmed_exhaustion"
      ],
      key_dates: ["2026-03-12", "2026-03-26"],
      indicators: {
        ma20: 447.5,
        ma50: 438.9,
        ma200: 401.2,
        rsi14: 59.4,
        macd: 3.12,
        atr: 7.65
      },
      support_resistance: [
        {
          kind: "support",
          low: 444.0,
          high: 447.0,
          strength: "strong",
          basis: ["moving_average", "volume_node"],
          note: "20DMA and dense traded range overlap"
        },
        {
          kind: "support",
          low: 436.0,
          high: 439.0,
          strength: "medium",
          basis: ["moving_average", "swing"],
          note: "50DMA region and prior swing support"
        },
        {
          kind: "support",
          low: 425.0,
          high: 429.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "lower valuation band area"
        },
        {
          kind: "resistance",
          low: 455.0,
          high: 458.0,
          strength: "strong",
          basis: ["swing", "valuation_channel"],
          note: "recent local high and upper valuation band overlap"
        },
        {
          kind: "resistance",
          low: 465.0,
          high: 469.0,
          strength: "medium",
          basis: ["swing"],
          note: "higher swing zone"
        },
        {
          kind: "resistance",
          low: 478.0,
          high: 482.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "extended valuation channel area"
        }
      ],
      valuation_channels_available: true,
      channel_notes: [
        "PE band plotted from trailing distribution",
        "PEG band uses model-imputed growth snapshots"
      ]
    },
    BABA: {
      trend_state: "daily_rebound_with_weekly_base_building",
      momentum_state: "neutral_to_positive",
      structure_tags: [
        "base_building",
        "adr_translation_sensitive",
        "no_confirmed_breakout"
      ],
      key_dates: ["2026-03-18", "2026-03-29"],
      indicators: {
        ma20: 98.4,
        ma50: 95.1,
        ma200: 86.7,
        rsi14: 54.3,
        macd: 1.05,
        atr: 3.48
      },
      support_resistance: [
        {
          kind: "support",
          low: 96.0,
          high: 98.0,
          strength: "strong",
          basis: ["moving_average", "volume_node"],
          note: "20DMA and high participation zone"
        },
        {
          kind: "support",
          low: 92.0,
          high: 94.0,
          strength: "medium",
          basis: ["swing"],
          note: "recent swing floor"
        },
        {
          kind: "support",
          low: 87.0,
          high: 89.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "lower valuation support area"
        },
        {
          kind: "resistance",
          low: 102.0,
          high: 104.0,
          strength: "strong",
          basis: ["swing", "valuation_channel"],
          note: "near-term breakout ceiling"
        },
        {
          kind: "resistance",
          low: 108.0,
          high: 111.0,
          strength: "medium",
          basis: ["swing"],
          note: "upper swing cluster"
        },
        {
          kind: "resistance",
          low: 118.0,
          high: 121.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "extended valuation zone"
        }
      ],
      valuation_channels_available: true,
      channel_notes: [
        "PE band uses ADR-adjusted EPS assumptions",
        "PEG band is model-imputed and low-confidence"
      ]
    },
    QQQ: {
      trend_state: "weekly_uptrend_daily_consolidation",
      momentum_state: "moderately_positive",
      structure_tags: [
        "narrow_leadership",
        "daily_consolidation",
        "trend_intact"
      ],
      key_dates: ["2026-03-14", "2026-03-27"],
      indicators: {
        ma20: 518.6,
        ma50: 507.9,
        ma200: 472.4,
        rsi14: 57.1,
        macd: 4.88,
        atr: 8.35
      },
      support_resistance: [
        {
          kind: "support",
          low: 515.0,
          high: 519.0,
          strength: "strong",
          basis: ["moving_average", "volume_node"],
          note: "20DMA and dense traded zone"
        },
        {
          kind: "support",
          low: 505.0,
          high: 509.0,
          strength: "medium",
          basis: ["moving_average", "swing"],
          note: "50DMA region"
        },
        {
          kind: "support",
          low: 492.0,
          high: 496.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "lower ETF valuation band proxy"
        },
        {
          kind: "resistance",
          low: 526.0,
          high: 530.0,
          strength: "strong",
          basis: ["swing", "valuation_channel"],
          note: "recent local high"
        },
        {
          kind: "resistance",
          low: 538.0,
          high: 542.0,
          strength: "medium",
          basis: ["swing"],
          note: "upper breakout zone"
        },
        {
          kind: "resistance",
          low: 552.0,
          high: 557.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "extended move area"
        }
      ],
      valuation_channels_available: true,
      channel_notes: [
        "ETF valuation channel is a proxy layer, not company DCF",
        "No chip_peak or options_wall included in v1"
      ]
    },
    GLD: {
      trend_state: "range_bound_with_macro_support",
      momentum_state: "neutral",
      structure_tags: [
        "commodity_macro_sensitive",
        "range_trade",
        "no_clean_breakout"
      ],
      key_dates: ["2026-03-11", "2026-03-28"],
      indicators: {
        ma20: 244.3,
        ma50: 241.8,
        ma200: 228.9,
        rsi14: 51.2,
        macd: 0.74,
        atr: 3.16
      },
      support_resistance: [
        {
          kind: "support",
          low: 242.0,
          high: 244.0,
          strength: "strong",
          basis: ["moving_average", "volume_node"],
          note: "20DMA zone"
        },
        {
          kind: "support",
          low: 238.0,
          high: 240.0,
          strength: "medium",
          basis: ["swing"],
          note: "recent swing support"
        },
        {
          kind: "support",
          low: 233.0,
          high: 235.0,
          strength: "weak",
          basis: ["swing"],
          note: "deeper support area"
        },
        {
          kind: "resistance",
          low: 247.0,
          high: 249.0,
          strength: "strong",
          basis: ["swing"],
          note: "range ceiling"
        },
        {
          kind: "resistance",
          low: 252.0,
          high: 254.0,
          strength: "medium",
          basis: ["swing"],
          note: "breakout extension level"
        },
        {
          kind: "resistance",
          low: 259.0,
          high: 262.0,
          strength: "weak",
          basis: ["valuation_channel"],
          note: "extended commodity valuation proxy"
        }
      ],
      valuation_channels_available: true,
      channel_notes: [
        "Commodity ETF channel is macro/price proxy-based",
        "No chip_peak or options_wall included in v1"
      ]
    }
  };

  const data = map[symbol] || {
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
    channel_notes: ["Mock technical pack used"]
  };

  return res.json(
    success(data, [
      { provider: "mock", status: "ok", note: "mock technical structure pack used" }
    ])
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
    ]
  });
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Mock API running on port ${PORT}`);
});
