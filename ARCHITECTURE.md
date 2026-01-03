# Aerodrome Tax & Analytics – Architecture & Refactor Plan

## Purpose

This repository analyzes on-chain activity related to **Aerodrome Finance** on Base in order to:
1. Index protocol-specific contracts and events (LPs, gauges, locks, etc.)
2. Classify wallet transactions (swap, LP add/remove, vote, lock, claim)
3. Assign USD values at the time of each transaction
4. Determine tax implications (FIFO/LIFO, income vs capital gains)

The repo is currently functional but **structurally messy**.  
This document is the single source of truth for refactoring, architecture, and execution order.

---

## High-Level Architecture

The codebase consists of **two distinct applications**:

### App 1 – Protocol Indexing & Metadata
> “What exists on Aerodrome?”

Responsible for:
- Discovering Aerodrome pools/contracts
- Extracting protocol-level metadata
- Writing **canonical CSV/JSONL datasets** for reuse

**Output-focused. No tax logic here.**

---

### App 2 – Wallet Transactions & Tax Engine
> “What did *I* do, and what does the IRS think about it?”

Responsible for:
- Pulling wallet transactions
- Classifying actions
- Pricing assets in USD at timestamp
- Applying tax accounting logic (FIFO/LIFO)

**Consumes outputs from App 1.**

---

## Target Repository Structure


---

## App 1 – Aerodrome Indexing (Protocol Layer)

### Goals
- Deterministically discover **all relevant Aerodrome contracts**
- Normalize outputs into machine-readable datasets
- Never care about a specific wallet

### Responsibilities

- Pool discovery via `PoolCreated` events
- Gauge identification
- Lock / vote contract indexing
- Token pair resolution
- Human-readable naming

### Canonical Outputs

| File | Description |
|----|----|
| `pools.csv` | Pool address, token0, token1, type |
| `pools.jsonl` | Same as CSV + raw metadata |
| `gauges.csv` | Gauge → pool mapping |
| `locks.csv` | Lock contract metadata |

These outputs **must not change format casually**.

---

## App 2 – Wallet Transactions & Tax Engine

### Pipeline Stages

1. **Fetch**
   - Pull all wallet txs via Etherscan V2
   - Include logs & receipts

2. **Classify**
   - Swap
   - LP add/remove
   - Gauge stake/unstake
   - Lock create/increase/withdraw
   - Claim (fees vs rewards)

3. **Price**
   - Token → USD at timestamp
   - Stablecoin shortcuts
   - Fallback logic for unknown tokens

4. **Tax Compute**
   - FIFO
   - LIFO
   - Income vs capital gain tagging

Each stage produces a **new dataset**, not in-place mutation.

---

## Transaction Classification Model

Each transaction resolves to **exactly one primary action**:

```text
SWAP
LP_ADD
LP_REMOVE
GAUGE_STAKE
GAUGE_UNSTAKE
LOCK_CREATE
LOCK_INCREASE
LOCK_WITHDRAW
CLAIM_FEES
CLAIM_REWARDS
TRANSFER
UNKNOWN
