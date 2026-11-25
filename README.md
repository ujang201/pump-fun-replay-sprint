[![make.fun · u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/ujang201/pump-fun-replay-sprint/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/leaderboard)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/ujang201/pump-fun-replay-sprint/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

# Pump.fun Replay Sprint — Paper Bot

A deterministic paper-trading bot for the make.fun Pump.fun Replay Sprint
(`u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122`). It trades 10 SOL
of paper money on a 24h replay of pump.fun bonding-curve activity. Nothing
is traded for real: no network, no wallet, no transactions — the bot only
reads replayed events and emits paper orders.

## Strategy

Early-momentum breakout with strict exits:

- **Price** is the bonding curve's virtual quote reserves over virtual token
  reserves, kept as a scaled `BigInt` so every comparison is exact.
- **Entry:** a mint becomes a candidate after at least 8 trades, once its
  price has bounced ≥ 8% off its recent low (last 48 observations) while
  recent trades lean buy-side, and while it is still under 3× its first
  printed price (no chasing moons). Buys are 0.20 SOL slices, clamped
  under the harness cap of 100 bps of the mint's real quote reserves
  (floor 0.01 SOL, the round-trip bar), at most 5 open positions at once.
  One buy per mint ever; a buy in flight counts against the cap until its
  fill is reported (orders resolve 2 slots later), and a rejected buy
  retires the mint for good.
- **Pricing source:** prices and reserves come from each batch's own
  `events` (TradeEvent / CreateEvent carry the post-trade reserves).
  The harness `marks` only cover mints already held or pending, so they
  can never price a first entry — they are folded in only as a
  supplement for held mints. Buys go out at 1500 bps slippage so the
  2-slot latency does not get them refused; sells always at 10000 bps so
  an exit is never refused on slippage.
- **Exit:** +25% take-profit, −15% stop-loss, a 12% trailing stop from the
  peak since entry, bonding-curve completion, or a 2000-batch maximum hold.
  Every exit sells the whole position.
- **Accounting:** cash and positions are read from the harness `account`
  snapshot in every batch, never from local bookkeeping, so a rejected or
  failed fill can never desync the bot.

## Protocol

The bot speaks `agent-fun/replay/v2` over stdio: newline-delimited JSON in
(`hello`, then one `batch` per slot window, then `done`), one `reply` line
out per batch echoing the batch `seq` with that batch's `orders`. Fully
deterministic — no clock, no randomness — so two runs produce byte-identical
action logs.

## Run

```sh
node main.ts
```

Requires Node 22.18+ (Node 22.6–22.17: `node --experimental-strip-types main.ts`).
The harness drives it; there is nothing to configure.

## Grade locally

From the unpacked kit directory:

```sh
node cli/makefun.mjs grade-local --kit . --bot <path-to-this-repo> --driver process --commit <sha>
```

---

Built for [make.fun](https://make.fun/r/wfjv7kgb) — free to enter, paper money only.
