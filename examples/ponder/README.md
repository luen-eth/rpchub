# Independent Ponder workers

This example indexes every Ethereum or BNB block and WETH/WBNB Transfer events with Ponder 0.17.12. Each process has one chain, a separate database/schema and a separate HTTP port. It keeps Ponder's standard ordering and reorg protection.

Install dependencies with `pnpm install --frozen-lockfile` in this directory. Set `RPCHUB_URL` to your deployed service. Choose the first block to index for each chain and export `ETH_START_BLOCK` and `BNB_START_BLOCK` as decimal integers. No default start block is supplied so starting this example cannot accidentally request a genesis backfill.

For production, export `DATABASE_URL` with your PostgreSQL connection string, then run in two terminals:

```sh
PONDER_CHAIN_ID=1 PONDER_START_BLOCK="$ETH_START_BLOCK" pnpm start --schema rpchub_ethereum --port 42069
PONDER_CHAIN_ID=56 PONDER_START_BLOCK="$BNB_START_BLOCK" pnpm start --schema rpchub_bnb --port 42070
```

Keep these schema names, start blocks, code and database stable across restarts. Do not run two workers against the same schema. Local runs without `DATABASE_URL` use separate `.ponder/pglite-1` and `.ponder/pglite-56` directories; preserve those directories across local restarts. PGlite here is for local testing.

`GET /status` on each port reports the latest stored block, its chain timestamp and lag. It returns 503 until there is data or when lag exceeds 60 seconds (Ethereum) / 30 seconds (BNB). `GET /ready` alone does not establish freshness.

One chain per process prevents Ethereum's slower safe checkpoint from rolling back BNB's handler history. It cannot remove the chain's own unfinalized recovery window or reorg replay. Handlers only write deterministic primary keys to Ponder's reversible database; do not send irreversible external effects directly from live block handlers. Use a finalized outbox for such effects. A shared multi-chain application is still appropriate if your actual business logic requires cross-chain ordering.
