# rpchub

> Unified RPC proxy backed by chainlist: set chain IDs in env, get one stable JSON-RPC endpoint per chain.

```
POST http://localhost:9563/1          # Ethereum, by chain id
POST http://localhost:9563/ethereum   # same chain, by slug
POST http://localhost:9563/56         # BNB Smart Chain
POST http://localhost:9563/bnb        # short names work too
POST http://localhost:9563/solana     # exception: served from a static list
```

## Why

Public RPCs from chainlist are individually unreliable — some are dead, some rate-limited, some lagging behind the chain. rpchub puts all of them behind a single endpoint per chain:

- **Source:** `https://chainlist.org/rpcs.json` is fetched at boot, refreshed every `REFRESH_INTERVAL`, and cached on disk so the service still starts when chainlist is unreachable. URLs are aggressively sanitized: `${API_KEY}` placeholders, `wss://` entries, garbage records and invisible characters are all dropped.
- **Health:** every endpoint is probed periodically (EVM: `eth_blockNumber`, Solana: `getSlot`). Chain identity is verified on first contact (`eth_chainId` / `getGenesisHash`) — an endpoint answering for a different chain is excluded permanently. Endpoints more than `MAX_BLOCK_LAG` behind the pool median are pulled out of rotation (stale-data guard).
- **Selection & failover:** power-of-two-choices among healthy endpoints (two random candidates, the lower-latency one wins). Timeouts, connection errors, 429/5xx and non-JSON bodies fail over to the next endpoint (`MAX_RETRIES` attempts). JSON-RPC-level errors are the upstream's own answer and pass through verbatim. An endpoint that keeps failing enters an exponential cooldown.
- **Solana exception:** chainlist is EVM-only, so `/solana` is served from a built-in public list plus `SOLANA_RPCS` (mainnet-beta only; the genesis hash is verified).

## Quick start

```sh
CHAIN_IDS=1,56 SOLANA_ENABLED=true go run ./cmd/rpchub

curl -X POST localhost:9563/1 \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'

curl -X POST localhost:9563/solana \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}'
```

## Configuration

See [.env.example](.env.example) for every knob. The ones that matter most:

| Env | Default | What it does |
|---|---|---|
| `CHAIN_IDS` | — | Enabled EVM chains (e.g. `1,56,137`). The only required setting. |
| `SOLANA_ENABLED` / `SOLANA_RPCS` | `false` | Enables the `/solana` path; setting `SOLANA_RPCS` turns it on automatically. |
| `EXTRA_RPCS_<id>` / `EXTRA_RPCS_SOLANA` | — | Your own nodes; they go to the front of the list at the highest priority. |
| `ALIASES` | — | Extra path tokens: `bsc:56` → `POST /bsc`. |
| `MAX_BLOCK_LAG` | `10` | Endpoints this many blocks behind the median are benched (×20 slots on Solana). |
| `FILTER_TRACKING` | `false` | `true`: only use RPCs marked `tracking: none` in chainlist. |

## Endpoints

| Endpoint | Description |
|---|---|
| `POST /{chain}` | JSON-RPC proxy. `{chain}` = chain id, slug, short name or alias. Batch requests supported. |
| `GET /chains` | Enabled chains, their tokens, healthy/total endpoint counts and the reference height. |
| `GET /health` | 200 when every chain has ≥1 healthy endpoint; `warming` during boot warm-up; 503 otherwise. |
| `GET /{chain}/health` | Per-endpoint status/latency/height (URL paths are redacted so API keys cannot leak). |

## Deployment (Docker / Dokploy)

```sh
docker compose up -d --build
```

On Dokploy: add the repo as a Dockerfile application, set the env vars in the panel, and point the healthcheck at `/health`. A volume is defined for `CACHE_DIR` (`/data`), so restarts come up from cache even when chainlist is unreachable.

## Known limits (v1)

- No WebSocket/subscription proxying (`wss://` entries are filtered out anyway).
- No client auth, client rate-limiting or response caching.
- Deep `eth_getLogs` / `trace_*` calls may fail on some public RPCs; failover compensates partially, but there is no archive-node guarantee.
