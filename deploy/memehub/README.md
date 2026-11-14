# Memehub archive transport profile

This profile serves Base (8453) and BNB Smart Chain (56) through explicitly configured RPC providers. It does not run a blockchain node, store chain history, or establish provider independence.

## Closed provider configuration

`CHAINLIST_FILE=/etc/rpchub/memehub-chains.json` selects the bundled metadata file. Its public RPC lists are empty. `EXTRA_RPCS_56` and `EXTRA_RPCS_8453` are therefore the only possible upstreams. Local-file mode never downloads Chainlist, loads a remote-source cache, or starts the remote refresh loop. An invalid/missing file or a chain with no usable explicit endpoint stops startup.

Use one endpoint per chain in each witness instance. Do not add a primary provider as a fallback in an independently verified secondary instance. A proxy to Alchemy and a direct Alchemy URL are still the same source. Different API keys, hostnames, or proxy deployments do not establish independent infrastructure.

The supplied private deployment profile initially uses the already available Alchemy endpoints. It is a single-provider transport profile and **must not enable BNB SAFU's independent-source gate**. A second provider requires separate verification and deployment configuration.

## Access and limits

The Compose file creates two persistent services and one isolated setup service:

| Service | Access | Purpose |
| --- | --- | --- |
| `rpchub:9563` | Private Compose backend network; no host port | Explicit-provider RPC proxy |
| `gateway:8080` | Dokploy edge network; no host port | HTTP Basic authentication and request limits |
| `auth-init` | No network, no host mounts or Docker socket | Initialize this application's password-hash volume, then exit |

Attach only the gateway to the `rpc.luen.casa` HTTPS domain in Dokploy. Do not route a public domain to port 9563, expose that port, or attach unrelated containers to the backend network. Preserve existing Memehub services and the legacy RPChub app.

The gateway requires a password-hash file for RPC and diagnostic routes, strips client Authorization/Cookie headers before proxying, hides upstream/CORS headers, and disables access logging. Its `/health` endpoint reports gateway liveness only and explicitly says archive readiness is not checked. Diagnostic routes remain authenticated.

Limits are aggregate across gateway clients: eight HTTP requests per second, burst sixteen, eight concurrent requests, and a 256 KiB request body. `delay=4` permits the first four excess requests immediately, then paces the remaining queue at the configured rate; requests beyond the bounded queue or connection limit receive HTTP 429. A full burst adds about 1.5 seconds of intentional gateway pacing, separate from provider latency. The core admits at most thirty batch items, matching Memehub's current viem client, and rejects an oversized or mixed disallowed batch before forwarding any item. These limits do not increase an upstream plan's allowance; one HTTP batch can contain thirty RPC calls. Both chains share the gateway budget because their Alchemy account also shares its compute-unit allowance. The private core port does not apply the gateway's HTTP rate limit.

The readiness regression replays HTTP/RPC flows captured from the actual Memehub configuration flow and installed viem client: five creator routes, Base SAFU enabled, and BNB SAFU disabled. One cold configuration makes fifteen HTTP requests containing twenty-three RPC calls; a warm configuration makes nine HTTP requests containing seventeen RPC calls. The 3.2 HTTP requests/second calculation for two cold-plus-warm polling cycles every fifteen seconds is a fast-source baseline before queueing, not a sustained-capacity guarantee. Readiness expires five seconds after its work starts: a queued configuration can finish after that deadline, making its follow-up cold again. Two independent cold-plus-cold cycles then consume four HTTP requests/second before any other traffic. Neither baseline establishes arbitrary application capacity, and same-process coalescing must not be assumed across independent server processes.

The deployment gate includes a continuous fifty-four-HTTP-request wave: cold configuration, immediate warm configuration, then cold flows from two independent workers, without a drain interval between stages. It also checks repeated fifteen-second polls, individual request deadlines, and overload rejection. The previous two-request rate and burst four reproduce readiness failures even though paced archive and identity probes pass. The configured gateway must pass the readiness workload before switching application consumers. Its gateway-policy label changes when this policy changes so Compose recreates Nginx; verify the mounted limits and new gateway start time after deployment.

The dynamic cache test selects each follow-up flow from the actual elapsed time and repeats two independent workers over two fifteen-second cycles. With 500 ms of synthetic latency on each readiness HTTP request, the four-request policy expired every follow-up cache; six requests/second also crossed the five-second expiry once. Eight requests/second was the smallest tested practical candidate to keep all cold flows below five seconds and all immediate follow-ups warm. This is a finite local replay against a deterministic upstream, not a full Next.js test or a measurement of real-provider/indexer latency. Actual application configuration, capabilities and creator routes must still pass through the deployed gateway before consumer rollout.

`READ_ONLY=true` requires `WS_ENABLED=false`. Only allowlisted read methods with explicit non-null JSON-RPC IDs are accepted. Writes, unknown methods and notifications are rejected. The ordinary and archive routes are both covered. Defaults remain compatible for existing RPChub installations that do not opt into these settings.

## Deployment inputs

Copy `.env.example` to a private file **outside the repository** and replace the placeholder endpoints. Use mode 0600 for that environment file and the raw gateway login credentials. Keep their parent directory mode 0700. Never paste rendered Compose output, credentials or upstream URLs into a public report.

`RPCHUB_GATEWAY_HTPASSWD` contains one `username:SHA512crypt-hash` line. Generate the hash privately with `openssl passwd -6` or an equivalent password-hashing tool; never place the raw password in this variable. Single-quote the value in a Compose `.env` file so its dollar signs remain literal. The raw password remains in a separate mode 0600 file. Do not commit either value. Provision TLS through Dokploy before supplying Basic credentials over the public domain.

The network-disabled `auth-init` service validates the hash format and initializes the app-scoped `gateway-auth` named volume. It runs with only the CHOWN capability and no host directory or Docker socket access. The volume directory is mode 0700 and its hash file is mode 0644, both owned by the pinned Nginx worker UID/GID 101. The gateway mounts the volume read-only at `/etc/rpchub-auth`, outside the runtime tmpfs, and starts only after initialization succeeds. This avoids Compose file-secret ownership limitations and does not require SSH, host chmod, or a new infrastructure credential. Changing the hash and redeploying reruns initialization with an atomic file replacement. Do not share this named volume with another app.

Memehub's installed viem transport supports Basic-auth URL userinfo and removes it from the actual fetch URL. Authenticated consumer URLs must stay in server-only private configuration; never put them in browser-visible environment variables, public configuration responses or logs. Keep transaction-broadcasting workers on their separately configured write-capable providers: this profile is read-only.

```sh
docker compose --env-file /private/rpchub/profile.env \
  -f docker-compose.memehub.yml config --quiet
docker compose --env-file /private/rpchub/profile.env \
  -f docker-compose.memehub.yml up -d --build
```

Set `RPCHUB_EDGE_NETWORK` to the existing Dokploy edge network. The gateway image is digest-pinned. Build the RPChub image from the reviewed commit, then pin `RPCHUB_IMAGE` to the resulting immutable image for deployment. Nginx re-resolves the core's Docker DNS name every five seconds, so core container recreation does not require restarting the gateway. Requests can briefly fail while the core restarts or its new address is resolved; callers must retain normal retry handling.

An existing single-container Dockerfile application cannot represent this profile directly. Use a separate Compose deployment. Do not replace an unrelated application's configuration to make this profile fit.

## RPC paths and archive acceptance

- `POST /8453/archive` and `POST /56/archive`: archive-probed providers only.
- `POST /8453/indexer` and `POST /56/indexer`: sticky, capability-checked read routes; these are not archive-only routes.
- `POST /8453` and `POST /56`: ordinary read-only proxy routes.
- Authenticated `GET /chains`, `GET /{chain}/health`, `GET /{chain}/indexer/health`: diagnostics.

The stock archive probe only asks for the zero address's balance at block 1. Its one-hour positive verdict is not proof that a provider can execute every historical contract call or return complete logs. Ordinary `/health` also permits a 90-second warm-up and does not establish archive readiness.

Before treating a provider as a SAFU witness, compare the actual finalized block hash and the required historical contract calls, code/storage, balances and logs against a separately operated source. Record provider ownership and infrastructure provenance, the exact block, method and result, and the test date. Keep the feature disabled if independence or historical correctness is unverified. No deployment profile setting bypasses that requirement.

Alchemy Free's observed `eth_getLogs` window is ten blocks. RPChub does not split larger ranges. Keep the application's log chunk size at ten or below for that plan; a successful one-block indexer probe does not certify larger ranges.

## Local verification

```sh
go test -race ./...
go vet ./...
docker build -t rpchub:memehub-archive .
python3 deploy/memehub/test_profile.py \
  --readiness-delay-ms 500 --require-warm-followups
```

The profile smoke test uses a local mock upstream and dummy credentials. It creates its own temporary Docker project/network and only publishes its gateway to loopback. It does not use production endpoints, alter existing services, send transactions, or activate Memehub features.
