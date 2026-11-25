// Pump.fun Replay Sprint — paper trading bot (agent-fun/replay/v2).
//
// Reads the harness stream on stdin: one JSON line at a time, a `hello`
// first, then one `batch` per slot window, then `done`. Answers every batch
// with exactly one `reply` line carrying this batch's orders. Paper money
// only: no network, no clock, no randomness — every decision is a pure
// function of the replayed events, so two runs agree byte for byte.
//
// Strategy: early-momentum breakout with strict exits.
//   * Watch every mint's bonding-curve price (virtual quote reserves over
//     virtual token reserves, kept as a scaled BigInt so comparisons are
//     exact). A mint becomes a candidate once it has traded enough and its
//     price has bounced at least 8% off its recent low while recent trades
//     lean buy-side — momentum with confirmation, not a blind chase.
//   * Buy a fixed 0.20 SOL slice (clamped under the harness cap of 100 bps
//     of the mint's real quote reserves, and never below 0.01 SOL), at most
//     5 open positions at once, so one bad coin cannot sink the book.
//   * Exit on take-profit (+25% from entry), stop-loss (-15%), a 12%
//     trailing stop from the peak since entry, a bonding-curve completion,
//     or a 2000-batch maximum hold. Every exit sells the whole position.
//   * Positions and cash are read from the harness `account` snapshot in
//     each batch, never from local bookkeeping, so a rejected or failed
//     fill can never desync the bot from the real paper account.
//
// Pricing source matters: the harness `marks` in a batch only cover mints
// the bot already holds or has an order pending for, so a bot that prices
// entries from marks alone can never buy its first coin. Every price and
// reserve used here comes from the batch's own `events` instead
// (TradeEvent / CreateEvent carry the post-trade reserves); marks are only
// folded in as a supplement for mints already held.

import * as readline from "node:readline";

// ---- tunables (all in fixed units; nothing here reads the wall clock) ----
const SCALE = 1000000000000n; // price scale: lamports per token * 1e12
const BUY_SIZE_LAMPORTS = 200000000n; // 0.20 SOL per position
const MIN_BUY_LAMPORTS = 10000000n; // 0.01 SOL, the round-trip bar
const MAX_OPEN_POSITIONS = 5;
const MIN_TRADES_BEFORE_BUY = 8;
const PRICE_WINDOW = 48; // recent price observations kept per mint
const BOUNCE_NUM = 108n; // buy when price >= 108% of the recent low
const BOUNCE_DEN = 100n;
const MAX_CHASE_NUM = 3n; // never chase a mint past 3x its first price
const MAX_CHASE_DEN = 1n;
const TAKE_PROFIT_NUM = 125n; // sell at +25% over entry
const STOP_LOSS_NUM = 85n; // sell at -15% under entry
const TRAILING_NUM = 88n; // sell 12% below the peak since entry
const TRAILING_DEN = 100n;
const MAX_HOLD_BATCHES = 2000;
const BUY_SLIPPAGE_BPS = 1500; // wide enough to survive the 2-slot latency
const SELL_SLIPPAGE_BPS = 10000; // exits must never be refused on slippage
const RECENT_TRADES = 20;

// ---- per-mint market state ----------------------------------------------
interface MintState {
  prices: bigint[]; // recent scaled prices, oldest first
  firstPrice: bigint | null;
  buyTrades: number; // buys among the last RECENT_TRADES trades
  sellTrades: number;
  tradeQueue: boolean[]; // last RECENT_TRADES trade sides, oldest first
  lastSeenSeq: number;
  completed: boolean;
  latestPrice: bigint | null; // freshest price, from events (or marks)
  latestRealQuote: bigint; // freshest real quote reserves, from events (or marks)
}

interface Held {
  entryPrice: bigint;
  peakPrice: bigint;
  entrySeq: number;
}

const mints = new Map<string, MintState>();
const held = new Map<string, Held>();
// Mints with a buy in flight (sent, fill not yet reported): the harness
// resolves orders 2 slots later, so without this the same mint would be
// bought again on the next batch before the first buy lands.
const pendingBuys = new Set<string>();
// Mints already bought (or whose buy was rejected): bought once, never again.
const decided = new Set<string>();

let capitalLamports = 10000000000n; // 10 SOL, replaced by hello
let maxOrderBpsOfRealReserve = 100;
let maxOrdersPerBatch = 16;
let cashLamports = 0n;
let seq = 0;

function mintState(mint: string): MintState {
  let st = mints.get(mint);
  if (st === undefined) {
    st = {
      prices: [],
      firstPrice: null,
      buyTrades: 0,
      sellTrades: 0,
      tradeQueue: [],
      lastSeenSeq: seq,
      completed: false,
      latestPrice: null,
      latestRealQuote: 0n,
    };
    mints.set(mint, st);
  }
  return st;
}

// Scaled price of one token: virtual quote reserves / virtual token reserves.
function scaledPrice(virtualQuote: string, virtualToken: string): bigint | null {
  const q = BigInt(virtualQuote);
  const t = BigInt(virtualToken);
  if (t === 0n || q === 0n) return null;
  return (q * SCALE) / t;
}

function observePrice(st: MintState, price: bigint): void {
  if (st.firstPrice === null) st.firstPrice = price;
  st.latestPrice = price;
  st.prices.push(price);
  if (st.prices.length > PRICE_WINDOW) st.prices.shift();
  st.lastSeenSeq = seq;
}

function observeTrade(st: MintState, isBuy: boolean): void {
  st.tradeQueue.push(isBuy);
  if (isBuy) st.buyTrades += 1;
  else st.sellTrades += 1;
  if (st.tradeQueue.length > RECENT_TRADES) {
    const old = st.tradeQueue.shift();
    if (old === true) st.buyTrades -= 1;
    else if (old === false) st.sellTrades -= 1;
  }
}

function recentLow(st: MintState): bigint | null {
  let low: bigint | null = null;
  for (const p of st.prices) {
    if (low === null || p < low) low = p;
  }
  return low;
}

// ---- order helpers --------------------------------------------------------
interface Order {
  side: "buy" | "sell";
  client_id: string;
  mint: string;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

let orderCounter = 0;
function nextClientId(kind: string): string {
  orderCounter += 1;
  return `${kind}${seq}o${orderCounter}`;
}

// ---- batch handling -------------------------------------------------------
function handleBatch(msg: any): void {
  seq = msg.seq as number;
  const orders: Order[] = [];
  const completedNow = new Set<string>();

  // 1. Fold the batch's events into per-mint state. Events are the only
  // pricing source that covers mints the bot does not hold yet.
  for (const ev of msg.events as any[]) {
    if (ev.event === "TradeEvent") {
      const st = mintState(ev.mint as string);
      const p = scaledPrice(ev.virtual_quote_reserves, ev.virtual_token_reserves);
      if (p !== null) observePrice(st, p);
      if (ev.real_quote_reserves !== undefined && ev.real_quote_reserves !== null) {
        st.latestRealQuote = BigInt(ev.real_quote_reserves);
      }
      observeTrade(st, ev.is_buy === true);
    } else if (ev.event === "CreateEvent") {
      const st = mintState(ev.mint as string);
      const p = scaledPrice(ev.virtual_quote_reserves, ev.virtual_token_reserves);
      if (p !== null) observePrice(st, p);
      if (ev.real_quote_reserves !== undefined && ev.real_quote_reserves !== null) {
        st.latestRealQuote = BigInt(ev.real_quote_reserves);
      }
    } else if (ev.event === "CompleteEvent") {
      const st = mintState(ev.mint as string);
      st.completed = true;
      completedNow.add(ev.mint as string);
    }
  }

  // 2. Marks supplement the event state for mints already held or pending
  // (the only mints marks ever cover). They never drive entries.
  for (const m of msg.marks as any[]) {
    const st = mintState(m.mint as string);
    const p = scaledPrice(m.virtual_quote_reserves, m.virtual_token_reserves);
    if (p !== null) observePrice(st, p);
    st.latestRealQuote = BigInt(m.real_quote_reserves);
  }

  // 3. Reconcile fills: a newly filled buy anchors entry/peak prices, a
  // rejected buy retires the mint for good, a filled sell closes it.
  for (const f of msg.fills as any[]) {
    const mint = f.mint as string;
    if (f.side === "buy" && f.status === "filled") {
      pendingBuys.delete(mint);
      decided.add(mint);
      if (!held.has(mint)) {
        const p = mints.get(mint)?.latestPrice ?? null;
        if (p !== null) held.set(mint, { entryPrice: p, peakPrice: p, entrySeq: seq });
      }
    } else if (f.side === "buy" && f.status === "rejected") {
      // A rejected buy is never retried: the mint is decided, like the
      // baseline's once-and-done rule, so slots are not burned re-trying it.
      pendingBuys.delete(mint);
      decided.add(mint);
    }
    if (f.side === "sell" && f.status === "filled") {
      // A full sell closes the position; the account snapshot below is
      // authoritative, but drop our exit state at once so we never sell air.
      const stillHeld = (msg.account.positions as any[]).some(
        (pos: any) => pos.mint === mint && BigInt(pos.token_amount) > 0n,
      );
      if (!stillHeld) held.delete(mint);
    }
  }

  // 4. Authoritative account snapshot: cash and open positions.
  cashLamports = BigInt(msg.account.cash_lamports as string);
  const positions = new Map<string, bigint>();
  for (const pos of msg.account.positions as any[]) {
    const amount = BigInt(pos.token_amount as string);
    if (amount > 0n) positions.set(pos.mint as string, amount);
  }

  // Drop exit state for positions the harness no longer shows.
  for (const mint of [...held.keys()]) {
    if (!positions.has(mint)) held.delete(mint);
  }
  // Anchor any held mint we missed (e.g. a buy filled before we saw it).
  for (const mint of positions.keys()) {
    if (!held.has(mint)) {
      const p = mints.get(mint)?.latestPrice ?? null;
      if (p !== null) held.set(mint, { entryPrice: p, peakPrice: p, entrySeq: seq });
    }
  }

  // 5. Exits first: take-profit, stop-loss, trailing stop, completion, age.
  for (const [mint, tokens] of positions) {
    const h = held.get(mint);
    const st = mints.get(mint);
    const price = st?.latestPrice ?? null;
    if (h === undefined || price === null || price === undefined) continue;
    if (price > h.peakPrice) h.peakPrice = price;
    const done =
      completedNow.has(mint) ||
      (st !== undefined && st.completed) ||
      price * 100n >= h.entryPrice * TAKE_PROFIT_NUM ||
      price * 100n <= h.entryPrice * STOP_LOSS_NUM ||
      price * TRAILING_DEN <= h.peakPrice * TRAILING_NUM ||
      seq - h.entrySeq >= MAX_HOLD_BATCHES;
    if (done && orders.length < maxOrdersPerBatch) {
      orders.push({
        side: "sell",
        client_id: nextClientId("s"),
        mint,
        token_amount: tokens.toString(),
        max_slippage_bps: SELL_SLIPPAGE_BPS,
      });
    }
  }

  // 6. Entries: momentum bounce with buy-side confirmation, priced from
  // the batch's events. One buy per mint ever; buys in flight count
  // against the open-position cap until their fill is reported.
  const sellsPlanned = orders.length;
  const freeSlots = MAX_OPEN_POSITIONS - positions.size - pendingBuys.size + sellsPlanned;
  if (freeSlots > 0 && cashLamports >= MIN_BUY_LAMPORTS) {
    let buys = 0;
    for (const [mint, st] of mints) {
      if (buys >= freeSlots) break;
      if (orders.length >= maxOrdersPerBatch) break;
      if (positions.has(mint)) continue;
      if (held.has(mint)) continue;
      if (pendingBuys.has(mint)) continue;
      if (decided.has(mint)) continue;
      if (st.completed) continue;
      if (st.tradeQueue.length < MIN_TRADES_BEFORE_BUY) continue;
      if (st.buyTrades <= st.sellTrades) continue;
      const price = st.latestPrice;
      if (price === null || st.firstPrice === null) continue;
      const low = recentLow(st);
      if (low === null) continue;
      // Bounce: price at least 8% above the recent low.
      if (price * BOUNCE_DEN < low * BOUNCE_NUM) continue;
      // No chasing: price at most 3x where the mint first printed.
      if (price * MAX_CHASE_DEN > st.firstPrice * MAX_CHASE_NUM) continue;
      // Size: 0.20 SOL, clamped under the real-reserve cap, floor 0.01 SOL.
      // The cap is the harness rule: max_order_bps_of_real_reserve of the
      // mint's real quote reserves.
      const cap = (st.latestRealQuote * BigInt(maxOrderBpsOfRealReserve)) / 10000n;
      let size = BUY_SIZE_LAMPORTS;
      if (cap < size) size = cap;
      if (size < MIN_BUY_LAMPORTS) continue;
      if (size > cashLamports) continue;
      orders.push({
        side: "buy",
        client_id: nextClientId("b"),
        mint,
        quote_lamports: size.toString(),
        max_slippage_bps: BUY_SLIPPAGE_BPS,
      });
      buys += 1;
      pendingBuys.add(mint);
      cashLamports -= size;
    }
  }

  // 7. One reply per batch, echoing its seq.
  process.stdout.write(JSON.stringify({ type: "reply", seq, orders }) + "\n");
}

// ---- main loop ------------------------------------------------------------
async function main(): Promise<void> {
  const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of lines) {
    const text = line.trim();
    if (text === "") continue;
    let msg: any;
    try {
      msg = JSON.parse(text);
    } catch {
      continue;
    }
    if (msg.type === "hello") {
      capitalLamports = BigInt(msg.capital.lamports as string);
      cashLamports = capitalLamports;
      maxOrderBpsOfRealReserve = msg.caps.max_order_bps_of_real_reserve as number;
      maxOrdersPerBatch = msg.caps.max_orders_per_batch as number;
      continue;
    }
    if (msg.type === "batch") {
      handleBatch(msg);
      continue;
    }
    if (msg.type === "done") {
      break;
    }
  }
}

main().catch(() => {
  process.exit(1);
});
