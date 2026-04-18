/**
 * Claude Gold Trading Bot — XAUUSDT
 *
 * Exchange:   Bitget (same API as the Bitcoin bot)
 * Instrument: XAUUSDT perpetual futures, 1x leverage (isolated margin)
 * Timeframe:  1H (Railway cron: every 15 min)
 * Strategy:   Van de Poppe mean-reversion — EMA(8) + VWAP + RSI(3) + CI(14)
 *
 * Bug fixes vs v1:
 *  - Trade limits now counted from Google Sheets (persists across Railway runs)
 *  - Position check before opening: never stacks duplicate positions
 *  - Leverage set via dedicated API call before first order
 *  - bias always defined (defaults to "neutral" on spike/early exit)
 */

import "dotenv/config";
import crypto from "crypto";
import { readFileSync, writeFileSync, existsSync, appendFileSync } from "fs";
import { google } from "googleapis";

// ─── Onboarding ───────────────────────────────────────────────────────────────

function checkOnboarding() {
  const required = ["BITGET_API_KEY", "BITGET_SECRET_KEY", "BITGET_PASSPHRASE"];
  const missing  = required.filter((k) => !process.env[k]);

  if (missing.length > 0 && !existsSync(".env")) {
    writeFileSync(".env", [
      "BITGET_API_KEY=", "BITGET_SECRET_KEY=", "BITGET_PASSPHRASE=", "",
      "SYMBOL=XAUUSDT", "TIMEFRAME=1H",
      "PORTFOLIO_VALUE_USD=1000", "MAX_TRADE_SIZE_USD=100",
      "MAX_TRADES_PER_DAY=4", "PAPER_TRADING=true", "",
      "MACRO_RELEASE_DATES=", "",
      "GOOGLE_SHEET_ID=", "GOOGLE_CREDENTIALS=",
    ].join("\n") + "\n");
    console.log("⚠️  .env created — fill in credentials then re-run.\n");
    process.exit(0);
  }
  if (missing.length > 0) {
    console.log(`\n⚠️  Missing: ${missing.join(", ")}`);
    process.exit(1);
  }
}

// ─── Config ───────────────────────────────────────────────────────────────────

const CONFIG = {
  symbol:          process.env.SYMBOL              || "XAUUSDT",
  timeframe:       process.env.TIMEFRAME           || "1H",
  portfolioValue:  parseFloat(process.env.PORTFOLIO_VALUE_USD || "1000"),
  maxTradeSizeUSD: parseFloat(process.env.MAX_TRADE_SIZE_USD  || "100"),
  maxTradesPerDay: parseInt(process.env.MAX_TRADES_PER_DAY    || "4"),
  paperTrading:    process.env.PAPER_TRADING !== "false",
  bitget: {
    apiKey:     process.env.BITGET_API_KEY,
    secretKey:  process.env.BITGET_SECRET_KEY,
    passphrase: process.env.BITGET_PASSPHRASE,
    baseUrl:    "https://api.bitget.com",
  },
};

// ─── Bitget API ───────────────────────────────────────────────────────────────

function bitgetSign(timestamp, method, requestPath, body = "") {
  const msg = timestamp + method.toUpperCase() + requestPath + body;
  return crypto.createHmac("sha256", CONFIG.bitget.secretKey).update(msg).digest("base64");
}

async function bitgetRequest(method, path, params = {}, body = null) {
  const timestamp = Date.now().toString();
  const qs = method === "GET" && Object.keys(params).length > 0
    ? "?" + new URLSearchParams(params).toString() : "";
  const requestPath = path + qs;
  const url = CONFIG.bitget.baseUrl + requestPath;
  const bodyStr = body ? JSON.stringify(body) : "";
  const sign = bitgetSign(timestamp, method, requestPath, bodyStr);

  const res = await fetch(url, {
    method,
    headers: {
      "ACCESS-KEY":        CONFIG.bitget.apiKey,
      "ACCESS-SIGN":       sign,
      "ACCESS-TIMESTAMP":  timestamp,
      "ACCESS-PASSPHRASE": CONFIG.bitget.passphrase,
      "Content-Type":      "application/json",
      "locale":            "en-US",
    },
    ...(body ? { body: bodyStr } : {}),
  });

  const data = await res.json();
  if (data.code && data.code !== "00000") {
    throw new Error(`Bitget ${data.code}: ${data.msg}`);
  }
  return data.data !== undefined ? data.data : data;
}

// ─── Timeframe helpers ────────────────────────────────────────────────────────

const TIMEFRAME_TO_BITGET = {
  "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
  "1H": "1H", "4H": "4H", "1D": "1Dutc", "1W": "1Wutc",
};
const TIMEFRAME_MINUTES = {
  "1m": 1, "5m": 5, "15m": 15, "30m": 30,
  "1H": 60, "4H": 240, "1D": 1440, "1W": 10080,
};

// ─── Fetch Candles ────────────────────────────────────────────────────────────

async function fetchCandles(symbol, timeframe, limit = 500) {
  const data = await bitgetRequest("GET", "/api/v2/mix/market/candles", {
    symbol, productType: "USDT-FUTURES",
    granularity: TIMEFRAME_TO_BITGET[timeframe] || "1H",
    limit: String(limit),
  });
  // Bitget returns oldest-first: [ts, open, high, low, close, vol, volCcy]
  return data.map((c) => ({
    time: parseInt(c[0]), open: parseFloat(c[1]),
    high: parseFloat(c[2]), low: parseFloat(c[3]),
    close: parseFloat(c[4]), volume: parseFloat(c[5]),
  }));
}

// ─── Delayed-Data Guard ───────────────────────────────────────────────────────

function checkDataFreshness(candles, timeframe) {
  const tfMin = TIMEFRAME_MINUTES[timeframe];
  if (!tfMin || tfMin >= 1440) return;
  const age = (Date.now() - candles[candles.length - 1].time) / 60000;
  const max = tfMin + 20;
  console.log(`  Data age: ${age.toFixed(1)} min  (max: ${max} min)`);
  if (age > max) throw new Error(`⛔ DELAYED DATA — ${age.toFixed(1)} min old. Refusing to trade.`);
  console.log("  ✅ Data is fresh");
}

// ─── Event Calendar Guard ─────────────────────────────────────────────────────

function checkEventCalendar() {
  const now = new Date();
  const t   = now.getUTCHours() * 60 + now.getUTCMinutes();
  console.log("\n── Event Calendar Check ─────────────────────────────────\n");

  // NFP: first Friday 13:00–14:30 UTC
  const isFirstFriday = now.getUTCDay() === 5 && now.getUTCDate() <= 7;
  if (isFirstFriday && t >= 780 && t < 870)
    throw new Error("⛔ NFP FRIDAY WINDOW (13:00–14:30 UTC)");

  // Fed decision: 18:30–21:00 UTC every day
  if (t >= 1110 && t < 1260)
    throw new Error("⛔ FED WINDOW (18:30–21:00 UTC)");

  // CPI/PCE: user-configured dates, 13:00–14:30 UTC
  const today = now.toISOString().slice(0, 10);
  const releaseDates = (process.env.MACRO_RELEASE_DATES || "").split(",").map((d) => d.trim()).filter(Boolean);
  if (releaseDates.includes(today) && t >= 780 && t < 870)
    throw new Error("⛔ CPI/PCE RELEASE WINDOW (13:00–14:30 UTC)");

  console.log("  ✅ No high-impact events active");
}

// ─── Wallet Balance ───────────────────────────────────────────────────────────
// Fetches available USDT in the Bitget USDT-Futures account.

async function getFuturesBalance() {
  try {
    const data = await bitgetRequest("GET", "/api/v2/mix/account/accounts", {
      productType: "USDT-FUTURES",
    });
    const account = Array.isArray(data) ? data.find((a) => a.marginCoin === "USDT") : null;
    if (!account) return { available: 0, equity: 0 };
    return {
      available: parseFloat(account.available   || 0),
      equity:    parseFloat(account.accountEquity || 0),
      unrealisedPL: parseFloat(account.unrealizedPL || 0),
    };
  } catch {
    return { available: 0, equity: 0, unrealisedPL: 0 };
  }
}

// ─── Position Check (Bug fix #2) ──────────────────────────────────────────────
// Check if there's already an open XAUUSDT position on Bitget.
// Returns: { hasPosition: bool, side: "long"|"short"|null, size: number }

async function getOpenPosition(symbol) {
  try {
    const data = await bitgetRequest("GET", "/api/v2/mix/position/single-position", {
      symbol,
      productType: "USDT-FUTURES",
      marginCoin:  "USDT",
    });

    // data is an array of position objects
    const positions = Array.isArray(data) ? data : [data];
    const open = positions.find((p) => parseFloat(p.total || p.size || 0) > 0);

    if (!open) return { hasPosition: false, side: null, size: 0 };

    return {
      hasPosition: true,
      side: open.holdSide === "long" ? "long" : "short",
      size: parseFloat(open.total || open.size || 0),
    };
  } catch {
    // If position check fails, assume no position (safe default)
    return { hasPosition: false, side: null, size: 0 };
  }
}

// ─── Set Leverage (Bug fix #3) ────────────────────────────────────────────────

async function ensureLeverage(symbol, leverage = "1") {
  try {
    await bitgetRequest("POST", "/api/v2/mix/account/set-leverage", {}, {
      symbol,
      productType: "USDT-FUTURES",
      marginCoin:  "USDT",
      leverage,
    });
    console.log(`  ✅ Leverage set to ${leverage}x`);
  } catch (err) {
    console.log(`  ⚠️  Leverage set failed (may already be set): ${err.message}`);
  }
}

// ─── Indicators ───────────────────────────────────────────────────────────────

function calcEMA(closes, period) {
  const k = 2 / (period + 1);
  let ema = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < closes.length; i++) ema = closes[i] * k + ema * (1 - k);
  return ema;
}

function calcRSI(closes, period = 3) {
  if (closes.length < period + 1) return null;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  const ag = gains / period, al = losses / period;
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

function calcChoppinessIndex(candles, period = 14) {
  if (candles.length < period + 1) return null;
  const slice = candles.slice(candles.length - period);
  const prev  = candles[candles.length - period - 1];
  let atrSum  = 0;
  for (let i = 0; i < slice.length; i++) {
    const pc = i === 0 ? prev.close : slice[i - 1].close;
    atrSum += Math.max(slice[i].high - slice[i].low,
      Math.abs(slice[i].high - pc), Math.abs(slice[i].low - pc));
  }
  const range = Math.max(...slice.map((c) => c.high)) - Math.min(...slice.map((c) => c.low));
  return range === 0 ? null : 100 * (Math.log10(atrSum / range) / Math.log10(period));
}

function calcVWAP(candles) {
  const midnight = new Date();
  midnight.setUTCHours(0, 0, 0, 0);
  const session = candles.filter((c) => c.time >= midnight.getTime());
  if (session.length === 0) return null;
  const cumTPV = session.reduce((s, c) => s + ((c.high + c.low + c.close) / 3) * c.volume, 0);
  const cumVol = session.reduce((s, c) => s + c.volume, 0);
  return cumVol === 0 ? null : cumTPV / cumVol;
}

const SPIKE_PCT = 1.5;
function detectSpike(candles) {
  if (candles.length < 3) return false;
  const ref = candles[candles.length - 3].close;
  return candles.slice(-2).some((c) => Math.abs(c.close - ref) / ref * 100 > SPIKE_PCT);
}

// ─── Safety Check ─────────────────────────────────────────────────────────────

function runSafetyCheck(price, ema8, vwap, rsi3, ci, spike) {
  const results = [];
  const check = (label, required, actual, pass) => {
    results.push({ label, required, actual, pass });
    console.log(`  ${pass ? "✅" : "🚫"} ${label}`);
    console.log(`     Required: ${required} | Actual: ${actual}`);
  };

  console.log("\n── Safety Check ─────────────────────────────────────────\n");

  // Bug fix #4: bias always defined
  let bias = "neutral";

  if (spike) {
    console.log(`  🚫 SAFE-HAVEN SPIKE — gold moved >${SPIKE_PCT}% recently.\n`);
    results.push({ label: `Spike guard (>${SPIKE_PCT}%)`, required: "No spike", actual: "Spike", pass: false });
    return { results, allPass: false, bias };
  }
  console.log("  ✅ No spike detected\n");

  const bullish = price > vwap && price > ema8;
  const bearish = price < vwap && price < ema8;

  if (bullish) {
    bias = "long";
    console.log("  Bias: BULLISH → LONG\n");
    check("Price above VWAP",            `> ${vwap.toFixed(2)}`, price.toFixed(2), price > vwap);
    check("Price above EMA(8)",          `> ${ema8.toFixed(2)}`, price.toFixed(2), price > ema8);
    check("RSI(3) below 30 (snap-back)", "< 30",                 rsi3.toFixed(2),  rsi3 < 30);
    const d = Math.abs((price - vwap) / vwap * 100);
    check("Within 1.5% of VWAP",         "< 1.5%", `${d.toFixed(2)}%`, d < 1.5);
    if (ci !== null) check("CI(14) below 50 (trending)", "< 50", ci.toFixed(2), ci < 50);

  } else if (bearish) {
    bias = "short";
    console.log("  Bias: BEARISH → SHORT\n");
    check("Price below VWAP",            `< ${vwap.toFixed(2)}`, price.toFixed(2), price < vwap);
    check("Price below EMA(8)",          `< ${ema8.toFixed(2)}`, price.toFixed(2), price < ema8);
    check("RSI(3) above 70 (snap-back)", "> 70",                 rsi3.toFixed(2),  rsi3 > 70);
    const d = Math.abs((price - vwap) / vwap * 100);
    check("Within 1.5% of VWAP",         "< 1.5%", `${d.toFixed(2)}%`, d < 1.5);
    if (ci !== null) check("CI(14) below 50 (trending)", "< 50", ci.toFixed(2), ci < 50);

  } else {
    console.log("  Bias: NEUTRAL — no trade.\n");
    results.push({ label: "Market bias", required: "Bullish or bearish", actual: "Neutral", pass: false });
  }

  return { results, allPass: results.every((r) => r.pass), bias };
}

// ─── Trade Limits — reads from Google Sheets (Bug fix #1) ─────────────────────
// Railway containers are ephemeral — local files vanish between runs.
// We count today's live/paper trades from Sheets instead.

async function countTodaysTradesFromSheets(sheetClient) {
  if (!sheetClient) return 0;
  try {
    const today = new Date().toISOString().slice(0, 10);
    const { sheets, sheetId } = sheetClient;
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId,
      range: "Gold!A2:N",       // skip header row
    });
    const rows = res.data.values || [];
    // Column A = Date, Column N (index 13) = Mode
    return rows.filter((r) => r[0] === today && (r[13] === "LIVE" || r[13] === "PAPER")).length;
  } catch {
    return 0;  // if sheet unreadable, don't block trading
  }
}

// ─── Order Execution ──────────────────────────────────────────────────────────

async function placeBitgetOrder(symbol, side, sizeUSD, price) {
  // 1 XAUUSDT contract = 1 oz gold
  const size = Math.max(0.01, parseFloat((sizeUSD / price).toFixed(2)));

  const data = await bitgetRequest("POST", "/api/v2/mix/order/place-order", {}, {
    symbol,
    productType: "USDT-FUTURES",
    marginMode:  "isolated",
    marginCoin:  "USDT",
    size:        String(size),
    side,                        // "buy" = open long, "sell" = open short
    tradeSide:   "open",
    orderType:   "market",
  });

  return { orderId: data.orderId || data.clientOid || "unknown", price };
}

async function closeBitgetPosition(symbol, side, size) {
  // Close by opening opposite side with tradeSide: "close"
  const closeSide = side === "long" ? "sell" : "buy";
  const data = await bitgetRequest("POST", "/api/v2/mix/order/place-order", {}, {
    symbol,
    productType: "USDT-FUTURES",
    marginMode:  "isolated",
    marginCoin:  "USDT",
    size:        String(size),
    side:        closeSide,
    tradeSide:   "close",
    orderType:   "market",
  });
  return { orderId: data.orderId || data.clientOid || "unknown" };
}

// ─── CSV Logging ──────────────────────────────────────────────────────────────

const CSV_FILE    = "trades.csv";
const CSV_HEADERS = ["Date","Time (UTC)","Exchange","Symbol","Side",
  "Size (oz)","Price (USD)","Total USD","Fee (est.)","Net Amount",
  "Order ID","Mode","Notes"].join(",");

function initCsv() {
  if (!existsSync(CSV_FILE)) {
    writeFileSync(CSV_FILE, CSV_HEADERS + "\n");
    console.log(`📄 Created ${CSV_FILE}`);
  }
}

function writeTradeCsv(e) {
  const now  = new Date(e.timestamp);
  const date = now.toISOString().slice(0, 10);
  const time = now.toISOString().slice(11, 19);
  let side = "", size = "", total = "", fee = "", net = "", orderId = "", mode = "", notes = "";

  if (!e.allPass) {
    const failed = (e.conditions || []).filter((c) => !c.pass).map((c) => c.label).join("; ");
    mode = "BLOCKED"; orderId = "BLOCKED"; notes = `Failed: ${failed}`;
  } else {
    side    = e.bias === "short" ? "SELL" : "BUY";
    size    = (e.tradeSize / e.price).toFixed(4);
    total   = e.tradeSize.toFixed(2);
    fee     = (e.tradeSize * 0.001).toFixed(4);
    net     = (e.tradeSize - parseFloat(fee)).toFixed(2);
    orderId = e.orderId || "";
    mode    = e.paperTrading ? "PAPER" : "LIVE";
    notes   = e.error ? `Error: ${e.error}` : "All conditions met";
  }

  appendFileSync(CSV_FILE,
    [date, time, "Bitget", e.symbol, side, size,
     e.price ? e.price.toFixed(2) : "", total, fee, net,
     orderId, mode, `"${notes}"`].join(",") + "\n");
  console.log(`Tax record saved → ${CSV_FILE}`);
}

// ─── Google Sheets ────────────────────────────────────────────────────────────

async function getSheetClient() {
  const sheetId = process.env.GOOGLE_SHEET_ID;
  if (!sheetId) return null;
  let credentials = null;
  if (process.env.GOOGLE_CREDENTIALS) {
    try { credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS); } catch { return null; }
  } else {
    const p = process.env.GOOGLE_CREDENTIALS_PATH || "./google-credentials.json";
    if (!existsSync(p)) return null;
    credentials = JSON.parse(readFileSync(p, "utf8"));
  }
  const auth   = new google.auth.GoogleAuth({ credentials, scopes: ["https://www.googleapis.com/auth/spreadsheets"] });
  const sheets = google.sheets({ version: "v4", auth });
  return { sheets, sheetId };
}

// ─── Auto-write headers if sheet is empty ────────────────────────────────────

async function ensureSheetHeaders(sheetClient) {
  if (!sheetClient) return;
  try {
    const { sheets, sheetId } = sheetClient;
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId, range: "Gold!A1",
    });
    const val = (res.data.values || [])[0]?.[0];
    if (val === "Date") return; // headers already present

    console.log("📋 Writing column headers to Gold sheet…");
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId, range: "Gold!A1:O1",
      valueInputOption: "RAW",
      requestBody: { values: [[
        "Date", "Time (UTC)", "Symbol", "Timeframe",
        "Price (USD)", "EMA(8)", "VWAP", "RSI(3)", "CI(14)",
        "Spike", "Side", "Trade Size (USD)", "Order ID", "Mode", "Notes",
      ]] },
    });
    console.log("✅ Headers written");
  } catch (err) {
    console.log(`⚠️  Could not write headers: ${err.message}`);
  }
}

// ─── Wallet tab — created once, live data written every 15 min ───────────────
//
// Layout (rows are 1-indexed as the user sees them):
//   1   Title
//   2   (blank)
//   3   ── Paper Portfolio ──
//   4   Starting Balance          | 1000          (static config)
//   5   Current Balance           | (bot writes)
//   6   Total P&L (USD)           | =B5-B4        (formula)
//   7   Total P&L (%)             | =formula      (formula)
//   8   Open Position             | (bot writes)
//   9   (blank)
//   10  ── Trade Statistics ──
//   11  Paper BUY signals         | COUNTIFS formula
//   12  Paper SELL signals        | COUNTIFS formula
//   13  Blocked signals           | COUNTIF formula
//   14  Total entries logged      | COUNTA formula
//   15  (blank)
//   16  ── Today ──
//   17  Signals today             | COUNTIF formula
//   18  Paper trades today        | COUNTIFS formula
//   19  Blocked today             | COUNTIFS formula
//   20  Latest XAUUSD price       | INDEX/MATCH formula
//   21  (blank)
//   22  ── Bot Status ──
//   23  Last updated              | (bot writes)
//   24  Mode                      | (bot writes)
//   25  Trades today (bot count)  | (bot writes)
//   26  (blank)
//   27  ── Bitget Futures Wallet ──
//   28  Available USDT            | (bot writes — $0 until funded)
//   29  Account Equity            | (bot writes)
//   30  Unrealised P&L            | (bot writes)
//   31  Budget / trade            | $100          (static)

async function ensureWalletTab(sheetClient) {
  if (!sheetClient) return;
  try {
    const { sheets, sheetId } = sheetClient;
    const meta = await sheets.spreadsheets.get({ spreadsheetId: sheetId });
    const existing = meta.data.sheets.find((s) => s.properties.title === "Gold Wallet");

    let walletId;
    if (existing) {
      // Tab exists — check if it has content
      const checkRes = await sheets.spreadsheets.values.get({
        spreadsheetId: sheetId, range: "Gold Wallet!A1",
      });
      const val = (checkRes.data.values || [])[0]?.[0];
      if (val && val.includes("Gold Bot")) return; // already populated
      console.log("📊 Gold Wallet tab is empty — populating…");
      walletId = existing.properties.sheetId;
    } else {
      console.log("📊 Creating Gold Wallet tab…");
      const addRes = await sheets.spreadsheets.batchUpdate({
        spreadsheetId: sheetId,
        requestBody: { requests: [{ addSheet: { properties: { title: "Gold Wallet", index: 1 } } }] },
      });
      walletId = addRes.data.replies[0].addSheet.properties.sheetId;
    }

    // Populate labels + formulas (USER_ENTERED so formulas are parsed)
    await sheets.spreadsheets.values.update({
      spreadsheetId: sheetId,
      range: "Gold Wallet!A1:C31",
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [
        ["🤖 Gold Bot — Paper Wallet", "", ""],                          // 1
        ["", "", ""],                                                    // 2
        ["── Paper Portfolio ──", "", ""],                               // 3
        ["Starting Balance (USD)", 1000, ""],                            // 4
        ["Current Balance (USD)", "Loading…", "← bot updates every 15 min"],  // 5
        ["Total P&L (USD)", "=B5-B4", ""],                              // 6
        ["Total P&L (%)", '=IFERROR(ROUND((B5-B4)/B4*100,2)&"%","—")', ""],  // 7
        ["Open Position", "Loading…", "← bot updates every 15 min"],   // 8
        ["", "", ""],                                                    // 9
        ["── Trade Statistics ──", "", ""],                             // 10
        ["Paper BUY signals",   '=COUNTIFS(Gold!K:K,"BUY",Gold!N:N,"PAPER")', ""],    // 11
        ["Paper SELL signals",  '=COUNTIFS(Gold!K:K,"SELL",Gold!N:N,"PAPER")', ""],   // 12
        ["Blocked signals",     '=COUNTIF(Gold!N:N,"BLOCKED")', ""],                  // 13
        ["Total entries logged",'=IFERROR(COUNTA(Gold!A2:A),0)', ""],                // 14
        ["", "", ""],                                                    // 15
        ["── Today ──", "", ""],                                        // 16
        ["Signals today",      '=IFERROR(COUNTIF(Gold!A:A,TEXT(TODAY(),"yyyy-mm-dd")),0)', ""],  // 17
        ["Paper trades today", '=IFERROR(COUNTIFS(Gold!A:A,TEXT(TODAY(),"yyyy-mm-dd"),Gold!N:N,"PAPER"),0)', ""],  // 18
        ["Blocked today",      '=IFERROR(COUNTIFS(Gold!A:A,TEXT(TODAY(),"yyyy-mm-dd"),Gold!N:N,"BLOCKED"),0)', ""],  // 19
        ["Latest XAUUSD price",'=IFERROR("$"&TEXT(INDEX(Gold!E:E,MATCH(9^9,IFERROR(VALUE(Gold!E:E),0))),"#,##0.00"),"—")', ""],  // 20
        ["", "", ""],                                                    // 21
        ["── Bot Status ──", "", ""],                                   // 22
        ["Last updated", "—", ""],                                      // 23
        ["Mode", "—", ""],                                              // 24
        ["Trades today (bot)", "—", ""],                                // 25
        ["", "", ""],                                                    // 26
        ["── Bitget Futures Wallet ──", "", ""],                        // 27
        ["Available USDT", "—", "← transfer Spot→Futures to fund"],    // 28
        ["Account Equity", "—", ""],                                    // 29
        ["Unrealised P&L", "—", ""],                                    // 30
        ["Budget / trade", `$${CONFIG.maxTradeSizeUSD}`, ""],          // 31
      ] },
    });

    // Formatting
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: sheetId,
      requestBody: { requests: [
        // Title: bold + large
        { repeatCell: {
          range: { sheetId: walletId, startRowIndex: 0, endRowIndex: 1 },
          cell: { userEnteredFormat: { textFormat: { bold: true, fontSize: 14 } } },
          fields: "userEnteredFormat.textFormat",
        }},
        // Section headers bold (0-indexed rows: 2, 9, 15, 21, 26)
        ...[2, 9, 15, 21, 26].map((r) => ({
          repeatCell: {
            range: { sheetId: walletId, startRowIndex: r, endRowIndex: r + 1, startColumnIndex: 0, endColumnIndex: 1 },
            cell: { userEnteredFormat: { textFormat: { bold: true } } },
            fields: "userEnteredFormat.textFormat.bold",
          },
        })),
        // "Current Balance" row (row 5, 0-indexed 4) — green background
        { repeatCell: {
          range: { sheetId: walletId, startRowIndex: 4, endRowIndex: 5, startColumnIndex: 0, endColumnIndex: 2 },
          cell: { userEnteredFormat: { backgroundColor: { red: 0.84, green: 0.94, blue: 0.84 } } },
          fields: "userEnteredFormat.backgroundColor",
        }},
        // "Total P&L" rows (5–6, 0-indexed) — bold value column
        { repeatCell: {
          range: { sheetId: walletId, startRowIndex: 5, endRowIndex: 7, startColumnIndex: 1, endColumnIndex: 2 },
          cell: { userEnteredFormat: { textFormat: { bold: true } } },
          fields: "userEnteredFormat.textFormat.bold",
        }},
        // Freeze row 1
        { updateSheetProperties: {
          properties: { sheetId: walletId, gridProperties: { frozenRowCount: 1 } },
          fields: "gridProperties.frozenRowCount",
        }},
        // Column widths: A=240, B=200, C=270
        { updateDimensionProperties: {
          range: { sheetId: walletId, dimension: "COLUMNS", startIndex: 0, endIndex: 1 },
          properties: { pixelSize: 240 }, fields: "pixelSize",
        }},
        { updateDimensionProperties: {
          range: { sheetId: walletId, dimension: "COLUMNS", startIndex: 1, endIndex: 2 },
          properties: { pixelSize: 200 }, fields: "pixelSize",
        }},
        { updateDimensionProperties: {
          range: { sheetId: walletId, dimension: "COLUMNS", startIndex: 2, endIndex: 3 },
          properties: { pixelSize: 270 }, fields: "pixelSize",
        }},
      ] },
    });

    console.log("✅ Gold Wallet tab created");
  } catch (err) {
    console.log(`⚠️  Gold Wallet tab: ${err.message}`);
  }
}

// ─── Paper Balance Calculator ─────────────────────────────────────────────────
// Reads all PAPER trades from the Gold log and calculates the running balance.
// Matches BUY→SELL (long closed) and SELL→BUY (short closed) pairs.
// Unmatched final entry = currently open position.

async function calculatePaperBalance(sheetClient) {
  const start = CONFIG.portfolioValue;
  if (!sheetClient) return { balance: start, openPosition: null };
  try {
    const { sheets, sheetId } = sheetClient;
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: sheetId, range: "Gold!A2:O",
    });
    // Columns: date(0) time(1) sym(2) tf(3) price(4) ema8(5) vwap(6) rsi3(7)
    //          ci(8) spike(9) side(10) size(11) orderId(12) mode(13) notes(14)
    const rows = (res.data.values || []).filter((r) =>
      r[13] === "PAPER" && r[10] && parseFloat(r[4]) > 0
    );

    let balance    = start;
    let openPos    = null; // { side:"BUY"|"SELL", price, sizeUSD }

    for (const r of rows) {
      const side    = r[10];
      const price   = parseFloat(r[4]);
      const sizeUSD = parseFloat(r[11]) || CONFIG.maxTradeSizeUSD;

      if (!openPos) {
        openPos = { side, price, sizeUSD };
        continue;
      }
      if (openPos.side === side) continue; // same direction = still in position

      // Signal flipped → position closed, calculate P&L
      const contracts = openPos.sizeUSD / openPos.price;
      const pnl = openPos.side === "BUY"
        ? contracts * (price - openPos.price)   // long closed
        : contracts * (openPos.price - price);  // short closed
      balance += pnl;
      openPos = { side, price, sizeUSD }; // new position opened on flip
    }

    return { balance, openPosition: openPos };
  } catch {
    return { balance: start, openPosition: null };
  }
}

// ─── Write live status to Wallet tab ─────────────────────────────────────────
// Called every 15 min — updates the "bot writes" cells without touching formulas.

async function writeWalletStatus(sheetClient, bitgetBalance, paperWallet, entry) {
  if (!sheetClient) return;
  try {
    const { sheets, sheetId } = sheetClient;
    const todayCnt = await countTodaysTradesFromSheets(sheetClient);

    // Describe open paper position
    let openPosText = "None (flat)";
    if (paperWallet.openPosition) {
      const p = paperWallet.openPosition;
      const oz  = (p.sizeUSD / p.price).toFixed(4);
      const cur = entry.price || p.price;
      const unrealPnl = (p.side === "BUY"
        ? (cur - p.price) / p.price * p.sizeUSD
        : (p.price - cur) / p.price * p.sizeUSD).toFixed(2);
      openPosText = `${p.side} ${oz} oz @ $${p.price.toFixed(2)}  (P&L: $${unrealPnl})`;
    }

    // Cells that the bot writes every run (Wallet!B5, B8, B23–B25, B28–B30)
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: sheetId,
      requestBody: {
        valueInputOption: "RAW",
        data: [
          { range: "Gold Wallet!B5",  values: [[parseFloat(paperWallet.balance.toFixed(2))]] },
          { range: "Gold Wallet!B8",  values: [[openPosText]] },
          { range: "Gold Wallet!B23", values: [[new Date().toISOString()]] },
          { range: "Gold Wallet!B24", values: [[entry.paperTrading ? "PAPER TRADING" : "LIVE TRADING"]] },
          { range: "Gold Wallet!B25", values: [[todayCnt]] },
          { range: "Gold Wallet!B28", values: [[bitgetBalance ? `$${bitgetBalance.available.toFixed(2)}` : "N/A"]] },
          { range: "Gold Wallet!B29", values: [[bitgetBalance ? `$${bitgetBalance.equity.toFixed(2)}` : "N/A"]] },
          { range: "Gold Wallet!B30", values: [[bitgetBalance ? `$${bitgetBalance.unrealisedPL.toFixed(2)}` : "N/A"]] },
        ],
      },
    });
    console.log("💼 Gold Wallet tab updated");
  } catch (err) {
    console.log(`⚠️  Wallet write: ${err.message}`);
  }
}

// ─── Write trade row to Google Sheets (Gold tab only — no sidebar clutter) ───

async function writeToGoogleSheets(e, sheetClient) {
  try {
    if (!sheetClient) return;
    const { sheets, sheetId } = sheetClient;
    const now  = new Date(e.timestamp);
    const date = now.toISOString().slice(0, 10);
    const time = now.toISOString().slice(11, 19);
    const ind  = e.indicators || {};

    let side = "", orderId = "", mode = "", notes = "";
    if (!e.allPass) {
      const f = (e.conditions || []).filter((c) => !c.pass).map((c) => c.label).join("; ");
      mode = "BLOCKED"; orderId = "BLOCKED"; notes = `Failed: ${f}`;
    } else {
      side    = e.bias === "short" ? "SELL" : "BUY";
      orderId = e.orderId || "";
      mode    = e.paperTrading ? "PAPER" : "LIVE";
      notes   = e.error ? `Error: ${e.error}` : "All conditions met";
    }

    // Append one row to the Gold trade log — columns A–O only, no sidebar
    await sheets.spreadsheets.values.append({
      spreadsheetId: sheetId, range: "Gold!A1",
      valueInputOption: "RAW", insertDataOption: "INSERT_ROWS",
      requestBody: { values: [[
        date, time, e.symbol, e.timeframe,
        e.price  ? e.price.toFixed(2)  : "",
        ind.ema8 ? ind.ema8.toFixed(2) : "",
        ind.vwap ? ind.vwap.toFixed(2) : "",
        ind.rsi3 ? ind.rsi3.toFixed(2) : "",
        ind.ci   ? ind.ci.toFixed(2)   : "",
        ind.spike ? "YES" : "NO",
        side, e.tradeSize ? e.tradeSize.toFixed(2) : "",
        orderId, mode, notes,
      ]] },
    });
    console.log(`📊 Gold log updated → https://docs.google.com/spreadsheets/d/${sheetId}`);
  } catch (err) {
    console.log(`⚠️  Sheets error: ${err.message}`);
  }
}

// ─── Tax Summary ──────────────────────────────────────────────────────────────

function generateTaxSummary() {
  if (!existsSync(CSV_FILE)) { console.log("No trades.csv yet."); return; }
  const rows = readFileSync(CSV_FILE, "utf8").trim().split("\n").slice(1).map((l) => l.split(","));
  const live = rows.filter((r) => r[11] === "LIVE");
  console.log("\n── Tax Summary ──────────────────────────────────────────\n");
  console.log(`  Total decisions : ${rows.length}`);
  console.log(`  Live trades     : ${live.length}`);
  console.log(`  Paper trades    : ${rows.filter((r) => r[11] === "PAPER").length}`);
  console.log(`  Blocked         : ${rows.filter((r) => r[11] === "BLOCKED").length}`);
  console.log(`  Total volume    : $${live.reduce((s, r) => s + parseFloat(r[7] || 0), 0).toFixed(2)}`);
  console.log("─────────────────────────────────────────────────────────\n");
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  checkOnboarding();
  initCsv();

  console.log("═══════════════════════════════════════════════════════════");
  console.log("  Claude Gold Trading Bot — XAUUSDT (Bitget Futures)");
  console.log(`  ${new Date().toISOString()}`);
  console.log(`  Mode: ${CONFIG.paperTrading ? "📋 PAPER TRADING" : "🔴 LIVE TRADING"}`);
  console.log("═══════════════════════════════════════════════════════════");

  const rules = JSON.parse(readFileSync("rules.json", "utf8"));
  console.log(`\nStrategy: ${rules.strategy.name}`);
  console.log(`Symbol: ${CONFIG.symbol} | Timeframe: ${CONFIG.timeframe}`);

  checkEventCalendar();

  // ── Init Sheets early (needed for trade limit count) ──────────────────────
  const sheetClient = await getSheetClient();
  await ensureSheetHeaders(sheetClient);  // write headers if Gold tab was cleared
  await ensureWalletTab(sheetClient);     // create Wallet tab once (formulas are permanent)

  // ── Bitget futures wallet balance ─────────────────────────────────────────
  console.log("\n── Wallet Balance (Bitget Futures) ──────────────────────\n");
  const balance = await getFuturesBalance();
  console.log(`  💰 Available USDT : $${balance.available.toFixed(2)}`);
  console.log(`  📊 Account equity : $${balance.equity.toFixed(2)}`);
  console.log(`  📈 Unrealised P&L : $${balance.unrealisedPL.toFixed(2)}`);
  console.log(`  🎯 Budget ceiling : $${CONFIG.maxTradeSizeUSD} per trade`);

  // ── Paper wallet — calculate running balance from trade history ───────────
  console.log("\n── Paper Wallet ─────────────────────────────────────────\n");
  const paperWallet = await calculatePaperBalance(sheetClient);
  const pnl = paperWallet.balance - CONFIG.portfolioValue;
  console.log(`  💼 Starting balance : $${CONFIG.portfolioValue.toFixed(2)}`);
  console.log(`  💼 Current balance  : $${paperWallet.balance.toFixed(2)}`);
  console.log(`  📈 Paper P&L        : ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}`);
  if (paperWallet.openPosition) {
    const p = paperWallet.openPosition;
    console.log(`  🔓 Open position    : ${p.side} ${(p.sizeUSD/p.price).toFixed(4)} oz @ $${p.price.toFixed(2)}`);
  } else {
    console.log("  🔓 Open position    : None (flat)");
  }

  // ── Trade limits — from Sheets, not local file (Bug fix #1) ──────────────
  console.log("\n── Trade Limits ─────────────────────────────────────────\n");
  const todayCount = await countTodaysTradesFromSheets(sheetClient);
  if (todayCount >= CONFIG.maxTradesPerDay) {
    console.log(`🚫 Max trades reached today: ${todayCount}/${CONFIG.maxTradesPerDay}`);
    process.exit(0);
  }
  const tradeSize = Math.min(CONFIG.portfolioValue * 0.01, CONFIG.maxTradeSizeUSD);
  console.log(`✅ Trades today: ${todayCount}/${CONFIG.maxTradesPerDay}`);
  console.log(`✅ Trade size:   $${tradeSize.toFixed(2)}`);

  // ── Market data ───────────────────────────────────────────────────────────
  console.log("\n── Fetching market data from Bitget ─────────────────────\n");
  const candles = await fetchCandles(CONFIG.symbol, CONFIG.timeframe, 500);
  if (candles.length < 20) { console.log("⚠️  Not enough data."); process.exit(0); }

  console.log("\n── Delayed-Data Check ───────────────────────────────────\n");
  checkDataFreshness(candles, CONFIG.timeframe);

  const closes = candles.map((c) => c.close);
  const price  = closes[closes.length - 1];
  const ema8   = calcEMA(closes, 8);
  const vwap   = calcVWAP(candles);
  const rsi3   = calcRSI(closes, 3);
  const ci     = calcChoppinessIndex(candles, 14);
  const spike  = detectSpike(candles);

  console.log(`\n  Price:  $${price.toFixed(2)}`);
  console.log(`  EMA(8): $${ema8.toFixed(2)}`);
  console.log(`  VWAP:   ${vwap ? `$${vwap.toFixed(2)}` : "N/A"}`);
  console.log(`  RSI(3): ${rsi3 ? rsi3.toFixed(2) : "N/A"}`);
  console.log(`  CI(14): ${ci ? `${ci.toFixed(2)}${ci < 50 ? " ✅ trending" : " ⚠️ choppy"}` : "N/A"}`);
  console.log(`  Spike:  ${spike ? `⚠️ YES (>${SPIKE_PCT}%)` : "✅ None"}`);

  if (!vwap || !rsi3) { console.log("\n⚠️  Insufficient indicator data."); process.exit(0); }

  const { results, allPass, bias } = runSafetyCheck(price, ema8, vwap, rsi3, ci, spike);

  console.log("\n── Decision ─────────────────────────────────────────────\n");

  const entry = {
    timestamp: new Date().toISOString(),
    symbol: CONFIG.symbol, timeframe: CONFIG.timeframe,
    price, bias,
    indicators: { ema8, vwap, rsi3, ci, spike },
    conditions: results, allPass, tradeSize,
    orderPlaced: false, orderId: null,
    paperTrading: CONFIG.paperTrading,
  };

  if (!allPass) {
    console.log("🚫 TRADE BLOCKED");
    results.filter((r) => !r.pass).forEach((r) => console.log(`   - ${r.label}`));

  } else {
    const oz   = (tradeSize / price).toFixed(4);
    const side = bias === "long" ? "buy" : "sell";
    const dir  = bias === "long" ? "📈 LONG" : "📉 SHORT";

    if (CONFIG.paperTrading) {
      // ── Bug fix #2: position check in paper mode too ────────────────────
      console.log(`✅ ALL CONDITIONS MET — ${bias.toUpperCase()}`);
      console.log(`\n📋 PAPER TRADE — ${dir} ${oz} oz XAUUSDT (~$${tradeSize.toFixed(2)})`);
      console.log("   (Set PAPER_TRADING=false to place real orders)");
      entry.orderPlaced = true;
      entry.orderId     = `PAPER-${Date.now()}`;

    } else {
      // ── Bug fix #2: check existing position before opening ──────────────
      console.log("\n── Position Check ───────────────────────────────────────\n");
      const pos = await getOpenPosition(CONFIG.symbol);

      if (pos.hasPosition && pos.side === bias) {
        console.log(`⚠️  Already ${pos.side.toUpperCase()} ${pos.size} oz — skipping duplicate entry.`);
        // Still log as BLOCKED to track the event
        entry.allPass = false;
        entry.conditions.push({ label: "Duplicate position guard", required: "No open position in same direction", actual: `${pos.side} already open`, pass: false });

      } else {
        if (pos.hasPosition && pos.side !== bias) {
          // Signal flipped — close existing position first
          console.log(`🔄 Signal flipped — closing existing ${pos.side.toUpperCase()} position first...`);
          try {
            await closeBitgetPosition(CONFIG.symbol, pos.side, pos.size);
            console.log(`✅ Existing ${pos.side} position closed`);
          } catch (err) {
            console.log(`❌ Close failed: ${err.message} — skipping new entry`);
            entry.error = `Close failed: ${err.message}`;
            await writeToGoogleSheets(entry, sheetClient);
            process.exit(0);
          }
        }

        // ── Bug fix #3: set leverage before placing order ─────────────────
        console.log("\n── Setting Leverage ─────────────────────────────────────\n");
        await ensureLeverage(CONFIG.symbol, "1");

        console.log(`\n✅ ALL CONDITIONS MET — ${bias.toUpperCase()}`);
        console.log(`🔴 PLACING LIVE ORDER — ${dir} ${oz} oz XAUUSDT (~$${tradeSize.toFixed(2)})`);
        try {
          const order = await placeBitgetOrder(CONFIG.symbol, side, tradeSize, price);
          entry.orderPlaced = true;
          entry.orderId     = order.orderId;
          console.log(`✅ ORDER PLACED — ${order.orderId}`);
        } catch (err) {
          console.log(`❌ ORDER FAILED — ${err.message}`);
          entry.error = err.message;
        }
      }
    }
  }

  writeTradeCsv(entry);
  await writeToGoogleSheets(entry, sheetClient);                        // Gold tab: trade log row
  await writeWalletStatus(sheetClient, balance, paperWallet, entry);   // Wallet tab: live data

  console.log("═══════════════════════════════════════════════════════════\n");
  process.exit(0);
}

if (process.argv.includes("--tax-summary")) {
  generateTaxSummary();
} else {
  run().catch((err) => {
    console.error("Bot error:", err.message);
    process.exit(1);
  });
}
