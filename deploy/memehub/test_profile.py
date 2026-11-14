#!/usr/bin/env python3
"""Smoke-test the production Compose profile against isolated local RPC fixtures.

Requires Docker Compose, the built RPChub image, Go, and OpenSSL. The test never
uses real RPC providers, existing application containers, or real credentials.
Only its unique Compose project and edge network are removed on completion.
"""

import argparse
import base64
import concurrent.futures
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]


def run(command, *, env=None, check=True, input=None):
    result = subprocess.run(command, cwd=ROOT, env=env, input=input, text=True,
                            capture_output=True, timeout=180)
    if check and result.returncode:
        raise RuntimeError(f"Command failed: {command[0]} {command[1]}\n{result.stderr[-4000:]}")
    return result


def rpc(identifier, method="eth_chainId", params=None):
    return {"jsonrpc": "2.0", "id": identifier, "method": method, "params": params or []}


def gateway_limits(config):
    return {
        "httpRequestsPerSecond": int(re.search(r"rate=(\d+)r/s;", config).group(1)),
        "burst": int(re.search(r"limit_req zone=rpc_budget burst=(\d+)", config).group(1)),
        "connections": int(re.search(r"limit_conn rpc_connections (\d+);", config).group(1)),
        "delayAfter": int(re.search(r"delay=(\d+)", config).group(1)) if re.search(r"delay=(\d+)", config) else None,
    }


def with_gateway_limits(config, rate, burst, connections, delay=None):
    config = re.sub(r"rate=\d+r/s;", f"rate={rate}r/s;", config)
    config = re.sub(r"limit_req zone=rpc_budget burst=\d+", f"limit_req zone=rpc_budget burst={burst}", config)
    config = re.sub(r"(limit_req zone=rpc_budget burst=\d+) (?:nodelay|delay=\d+);",
                    lambda match: match.group(1) + (f" delay={delay};" if delay is not None else " nodelay;"), config)
    return re.sub(r"limit_conn rpc_connections \d+;", f"limit_conn rpc_connections {connections};", config)


def valid_readiness_result(request, result, chain):
    if request["method"] == "eth_chainId":
        return result == hex(chain)
    if request["method"] == "eth_getCode":
        return result == "0x6000600055"
    if request["method"] == "eth_getBlockByNumber":
        return (isinstance(result, dict) and result.get("number") == "0x64" and
                result.get("hash") == "0x" + format(100, "064x") and
                abs(time.time() - int(result.get("timestamp", "0x0"), 16)) <= 120)
    selector = request["params"][0]["data"][:10]
    if selector == "0x2ae9c600":
        return result == "0x" + format(32, "064x") + format(10, "064x") + b"memehub-v1".hex().ljust(64, "0")
    if selector == "0x3d069542":
        return result == "0x" + format(4_500_000_000_000_000_000, "064x")
    return selector in ("0x2e5a5b86", "0xfa847836") and result == "0x" + format(1, "064x")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="rpchub:memehub-archive")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--candidate-rate", type=int, help="Temporary measurement override; never changes the production config")
    parser.add_argument("--candidate-burst", type=int)
    parser.add_argument("--candidate-connections", type=int)
    parser.add_argument("--candidate-delay", type=int, help="Number of immediate excess requests before Nginx paces the queue")
    parser.add_argument("--cache-regression-only", action="store_true", help="Run only startup checks and the two-worker dynamic readiness-cache regression")
    parser.add_argument("--readiness-delay-ms", type=int, default=0, help="Synthetic local fixture response latency; never applied to health probes")
    parser.add_argument("--require-warm-followups", action="store_true", help="Require cold config below the 5s TTL and warm immediate followups")
    args = parser.parse_args()
    candidate = (args.candidate_rate, args.candidate_burst, args.candidate_connections)
    if any(value is not None for value in candidate) and not all(value is not None and value > 0 for value in candidate):
        parser.error("Candidate rate, burst and connection limits must all be positive integers")
    if args.candidate_delay is not None and (not args.candidate_rate or not 0 <= args.candidate_delay <= args.candidate_burst):
        parser.error("Candidate delay requires candidate limits and must be between zero and the burst limit")
    if not 0 <= args.readiness_delay_ms <= 5000:
        parser.error("Synthetic readiness delay must be between zero and 5000ms")
    image_id = run(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"] ).stdout.strip()
    image_arch = run(["docker", "image", "inspect", args.image, "--format", "{{.Architecture}}"] ).stdout.strip()
    project = "rpchub-smoke-" + uuid.uuid4().hex[:12]
    edge = project + "-edge"
    checks = []
    created_network = False
    started_project = False
    blocker_id = None
    compose = None
    env = None
    with tempfile.TemporaryDirectory(prefix=project + "-") as directory:
        temp = Path(directory)
        os.chmod(temp, 0o700)
        password = "local-test-" + uuid.uuid4().hex
        user = "smoke"
        password_hash = run(["openssl", "passwd", "-6", "-stdin"], input=password + "\n").stdout.strip()
        password_entry = user + ":" + password_hash
        fixture = temp / "upstream"
        build_env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=image_arch)
        run(["go", "build", "-trimpath", "-o", str(fixture),
             "./deploy/memehub/testdata/upstream.go"], env=build_env)
        os.chmod(fixture, 0o555)
        env = dict(os.environ)
        env.update({
            "RPCHUB_IMAGE": image_id,
            "RPCHUB_EDGE_NETWORK": edge,
            "RPCHUB_GATEWAY_HTPASSWD": password_entry,
            "BNB_RPC_URL": "http://fixture:9570/56/dummy-bnb-upstream-key",
            "BASE_RPC_URL": "http://fixture:9570/8453/dummy-base-upstream-key",
        })
        envfile = temp / "test.env"
        envfile.write_text("# All test overrides are passed in the subprocess environment.\n")
        production_config = (ROOT / "deploy/memehub/nginx.conf").read_text()
        selected_config = with_gateway_limits(production_config, *candidate, args.candidate_delay) if args.candidate_rate else production_config
        selected_limits = gateway_limits(selected_config)
        candidate_config = temp / "nginx-candidate.conf"
        candidate_config.write_text(selected_config)
        legacy_config = temp / "nginx-legacy.conf"
        legacy_config.write_text(with_gateway_limits(production_config, 2, 4, 4))
        override = temp / "override.json"
        override_data = {"services": {
            "rpchub": {
                "depends_on": ["fixture"],
                "environment": {"PROBE_INTERVAL": "2s", "REQUEST_TIMEOUT": "2s"},
            },
            "gateway": {
                "ports": [{"target": 8080, "published": "0", "host_ip": "127.0.0.1"}],
                "volumes": [str(candidate_config) + ":/etc/nginx/nginx.conf:ro"],
            },
            "fixture": {
                "image": image_id,
                "entrypoint": ["/fixture"],
                "environment": {"FIXTURE_READINESS_DELAY_MS": str(args.readiness_delay_ms)},
                "volumes": [str(fixture) + ":/fixture:ro"],
                "networks": ["backend"],
                "read_only": True,
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
                "healthcheck": {"disable": True},
            },
        }}
        override.write_text(json.dumps(override_data))
        compose = ["docker", "compose", "-p", project, "--env-file", str(envfile),
                   "-f", str(ROOT / "docker-compose.memehub.yml"), "-f", str(override)]
        try:
            run(["docker", "network", "create", "--label", "rpchub.smoke=" + project, edge], env=env)
            created_network = True
            # Pull only the profile's digest-pinned public gateway image if absent.
            rendered = json.loads(run(compose + ["config", "--format", "json"], env=env).stdout)
            assert rendered["services"]["rpchub"]["environment"]["MAX_BATCH_ITEMS"] == "30", "Expected the Memehub thirty-call batch limit"
            legacy_service = copy.deepcopy(rendered["services"]["gateway"])
            for mount in legacy_service["volumes"]:
                if mount["target"] == "/etc/nginx/nginx.conf":
                    mount["source"] = str(legacy_config)
            override_data["services"]["legacy-gateway"] = legacy_service
            override.write_text(json.dumps(override_data))
            gateway_image = rendered["services"]["gateway"]["image"]
            if run(["docker", "image", "inspect", gateway_image], env=env, check=False).returncode:
                run(["docker", "pull", gateway_image], env=env)
            existing = run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + project], env=env).stdout.strip()
            assert not existing, "Refusing to reuse an existing Compose project"
            started_project = True
            run(compose + ["up", "-d", "--no-build", "--pull", "never"], env=env)
            binding = run(compose + ["port", "gateway", "8080"], env=env).stdout.strip()
            assert binding.startswith("127.0.0.1:"), "Gateway must bind only to loopback"
            base = "http://" + binding
            legacy_binding = run(compose + ["port", "legacy-gateway", "8080"], env=env).stdout.strip()
            assert legacy_binding.startswith("127.0.0.1:"), "Legacy test gateway must bind only to loopback"
            legacy_base = "http://" + legacy_binding
            auth = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()
            last_request = 0.0

            def http(path, payload=None, authorization=auth, pace=True, base_url=None, timeout=12):
                nonlocal last_request
                if pace:
                    time.sleep(max(0, 0.55 - (time.monotonic() - last_request)))
                    last_request = time.monotonic()
                body = payload if isinstance(payload, bytes) else None if payload is None else json.dumps(payload).encode()
                headers = {"Content-Type": "application/json", "Cookie": "smoke-cookie=must-not-leak"}
                if authorization:
                    headers["Authorization"] = authorization
                request = urllib.request.Request((base_url or base) + path, data=body, headers=headers)
                try:
                    with urllib.request.urlopen(request, timeout=timeout) as response:
                        return response.status, dict(response.headers), response.read().decode()
                except urllib.error.HTTPError as error:
                    return error.code, dict(error.headers), error.read().decode()

            def stats():
                return json.loads(run(compose + ["exec", "-T", "fixture", "wget", "-qO-",
                                                 "http://127.0.0.1:9570/__stats"], env=env).stdout)

            def check(name, condition):
                assert condition, name
                checks.append(name)

            readiness_fixture = json.loads((ROOT / "deploy/memehub/testdata/readiness-trace.json").read_text())
            readiness_runs = {item["name"]: item for item in readiness_fixture["runs"]}

            def replay_readiness(trace_name, label, base_url):
                """Replay recorded HTTP boundaries, preserving sequential calls per chain.

                Two chain workers remain concurrent, as in the real config() call.
                Each round waits viem's 10ms batch window after the prior reply;
                no artificial rate pacing, retry, or budget drain is introduced.
                """
                recorded = readiness_runs[trace_name]
                started = time.monotonic()

                def chain_flow(chain_name):
                    chain = {"base": 8453, "bnb": 56}[chain_name]
                    outcomes = []
                    pinned_block = None
                    entries = [entry for entry in recorded["trace"] if entry["chain"] == chain_name]
                    for index, entry in enumerate(entries):
                        time.sleep(readiness_fixture["viem"]["waitMs"] / 1000)
                        payload = [rpc(f"smoke-readiness-{label}-{chain}-{index}-{i}", request["method"], request.get("params"))
                                   for i, request in enumerate(entry["requests"])]
                        before = time.monotonic()
                        code, _, body = http(f"/{chain}/archive", payload, pace=False, base_url=base_url, timeout=10)
                        elapsed = (time.monotonic() - before) * 1000
                        valid = False
                        if code == 200:
                            responses = json.loads(body)
                            by_id = {response.get("id"): response for response in responses} if isinstance(responses, list) else {}
                            valid = isinstance(responses, list) and len(responses) == len(by_id) == len(payload) and all(
                                by_id.get(sent["id"], {}).get("jsonrpc") == "2.0" and
                                "error" not in by_id.get(sent["id"], {}) and
                                valid_readiness_result(request, by_id.get(sent["id"], {}).get("result"), chain)
                                for request, sent in zip(entry["requests"], payload))
                            for request, sent in zip(entry["requests"], payload):
                                if request["method"] == "eth_getBlockByNumber" and valid:
                                    result = by_id[sent["id"]]["result"]
                                    current = (result["number"], result["hash"])
                                    if request["params"][0] == "latest":
                                        pinned_block = current
                                    else:
                                        valid = pinned_block == current and request["params"][0] == current[0]
                        outcomes.append({"chain": chain_name, "status": code, "valid": valid,
                                         "batchSize": len(payload), "durationMs": round(elapsed, 2),
                                         "startedMs": round((before - started) * 1000, 2)})
                    return outcomes

                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as workers:
                    outcomes = [item for chain in workers.map(chain_flow, ("base", "bnb")) for item in chain]
                return {"name": label, "trace": trace_name, "httpRequests": len(outcomes),
                        "rpcCalls": sum(item["batchSize"] for item in outcomes),
                        "durationMs": round((time.monotonic() - started) * 1000, 2), "calls": outcomes}

            def readiness_sequence(label, base_url):
                started = time.monotonic()
                flows = [replay_readiness("cold", label + "-cold", base_url),
                         replay_readiness("immediate_warm", label + "-warm", base_url)]
                # Separate replayers deliberately do not share viem's same-process
                # batching scheduler or readiness cache. Each starts cold.
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as workers:
                    flows.extend(workers.map(lambda index: replay_readiness("cold", label + f"-independent-{index}", base_url), range(2)))
                return {"name": label, "durationMs": round((time.monotonic() - started) * 1000, 2),
                        "httpRequests": sum(flow["httpRequests"] for flow in flows), "flows": flows}

            def assert_readiness(name, flows):
                check(name + " preserves all five routes, native pairs and Base-only SAFU", all(
                    call["status"] == 200 and call["valid"] for flow in flows for call in flow["calls"]))
                check(name + " respects each 10s RPC timeout and 20s config timeout", all(
                    flow["durationMs"] < 20000 and all(call["durationMs"] < 10000 for call in flow["calls"]) for flow in flows))

            def dynamic_cache_regression():
                started = time.monotonic()
                cache_seconds = readiness_fixture["readinessCacheMs"] / 1000

                def worker(worker_id):
                    cold_started = None
                    flows = []
                    for cycle in range(2):
                        # Match the consumer cadence; do not drain the gateway's
                        # bucket before a cycle or between the immediate calls.
                        time.sleep(max(0, started + cycle * 15 - time.monotonic()))
                        for config_index in range(2):
                            invoked = time.monotonic()
                            age = None if cold_started is None else invoked - cold_started
                            trace_name = "cold" if age is None or age >= cache_seconds else "immediate_warm"
                            if trace_name == "cold":
                                cold_started = invoked
                            flow = replay_readiness(trace_name, f"dynamic-{worker_id}-{cycle}-{config_index}", base)
                            flow.update({"workerId": worker_id, "cycle": cycle, "configIndex": config_index,
                                         "invokedMs": round((invoked - started) * 1000, 2),
                                         "cacheAgeMs": None if age is None else round(age * 1000, 2),
                                         "readinessExpiresMs": round((cold_started + cache_seconds - started) * 1000, 2)})
                            flows.append(flow)
                    return flows

                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as workers:
                    flows = [flow for result in workers.map(worker, range(2)) for flow in result]
                assert_readiness("two-worker dynamic-cache config and immediate followup", flows)
                check("dynamic cache selects cold or warm from the actual invocation time", all(
                    (flow["trace"] == "cold") == (flow["cacheAgeMs"] is None or flow["cacheAgeMs"] >= 5000)
                    for flow in flows))
                expiry_cases = []
                for worker_id in range(2):
                    for cycle in range(2):
                        pair = [flow for flow in flows if flow["workerId"] == worker_id and flow["cycle"] == cycle]
                        if pair[0]["durationMs"] >= 5000:
                            expiry_cases.append(pair[1])
                check("any followup after 5s cache expiry uses a cold readiness trace",
                      all(flow["trace"] == "cold" for flow in expiry_cases))
                if args.require_warm_followups:
                    check("cold configs finish before cache expiry and immediate followups stay warm", all(
                        flow["trace"] == "cold" and flow["durationMs"] < 5000 if flow["configIndex"] == 0
                        else flow["trace"] == "immediate_warm" for flow in flows))
                expected_http = sum(flow["httpRequests"] for flow in flows)
                admissions = [item for item in stats()["readinessRequests"]
                              if item["ids"][0].startswith("smoke-readiness-dynamic-")]
                check("dynamic-cache requests reach the upstream exactly once without rejection", len(admissions) == expected_http)
                times = sorted(item["atMs"] for item in admissions)
                return {"workers": 2, "cyclesPerWorker": 2, "configsPerCycle": 2,
                        "cycleCadenceMs": 15000, "readinessCacheMs": 5000,
                        "durationMs": round((time.monotonic() - started) * 1000, 2),
                        "httpRequests": expected_http, "rpcCalls": sum(flow["rpcCalls"] for flow in flows),
                        "httpRejections": sum(call["status"] != 200 for flow in flows for call in flow["calls"]),
                        "expiredCacheFollowups": len(expiry_cases), "syntheticReadinessLatencyMs": args.readiness_delay_ms,
                        "timeouts": 0, "upstreamAdmissionSpanMs": times[-1] - times[0],
                        "admissionOffsetsMs": [value - times[0] for value in times], "flows": flows,
                        "scope": "Recorded Base/BNB RPC flows with independent worker caches and explicitly configured synthetic local latency; no full Next.js or measured real-provider latency."}

            def await_ready():
                deadline = time.monotonic() + 60
                last_reply = "No response"
                while True:
                    try:
                        code, _, body = http("/chains")
                        last_reply = f"HTTP {code}: {body[:2000]}"
                        chains = json.loads(body) if code == 200 else []
                        if len(chains) == 2 and all(c["healthy"] == 1 and c["archive_healthy"] == 1 for c in chains):
                            return chains
                    except (OSError, ValueError) as error:
                        last_reply = str(error)
                    if time.monotonic() >= deadline:
                        raise AssertionError("Profile did not become archive-ready against the local fixtures: " + last_reply)
                    time.sleep(0.5)

            def auth_attributes():
                return run(compose + ["exec", "-T", "--user", "101:101", "gateway", "stat", "-c", "%u:%g:%a",
                                      "/etc/rpchub-auth", "/etc/rpchub-auth/users.htpasswd"], env=env).stdout.splitlines()

            check("fresh authentication volume has the exact restricted ownership and modes",
                  auth_attributes() == ["101:101:700", "101:101:644"])
            stored_digest = run(compose + ["exec", "-T", "--user", "101:101", "gateway", "sha256sum",
                                           "/etc/rpchub-auth/users.htpasswd"], env=env).stdout.split()[0]
            check("gateway worker can read the exact initialized password-hash entry",
                  stored_digest == hashlib.sha256((password_entry + "\n").encode()).hexdigest())
            chains = await_ready()
            if args.cache_regression_only:
                dynamic_cache = dynamic_cache_regression()
                return args, {"status": "passed", "suite": "dynamic-readiness-cache", "image": image_id,
                              "gatewayImage": gateway_image, "gatewayLimits": selected_limits,
                              "temporaryCandidateOverride": bool(args.candidate_rate), "checksPassed": len(checks),
                              "syntheticReadinessLatencyMs": args.readiness_delay_ms,
                              "checks": checks, "realProvidersUsed": False, "dynamicCacheReadiness": dynamic_cache,
                              "hostBinding": "127.0.0.1 only", "cleanup": "unique test project and network only"}
            check("only Base and BNB enabled with one explicit upstream each",
                  {c["chain_id"] for c in chains} == {56, 8453} and
                  all(c["total"] == 1 and c["ws_total"] == 0 for c in chains))
            core_id = run(compose + ["ps", "-q", "rpchub"], env=env).stdout.strip()
            core = json.loads(run(["docker", "inspect", core_id], env=env).stdout)[0]
            check("core has no host-published ports", not any(core["NetworkSettings"]["Ports"].values()))
            gateway_id = run(compose + ["ps", "-q", "gateway"], env=env).stdout.strip()
            gateway = json.loads(run(["docker", "inspect", gateway_id], env=env).stdout)[0]
            auth_mounts = [mount for mount in gateway["Mounts"] if mount["Destination"] == "/etc/rpchub-auth"]
            check("gateway uses a read-only named authentication volume", len(auth_mounts) == 1 and
                  auth_mounts[0]["Type"] == "volume" and not auth_mounts[0]["RW"])
            init_id = run(compose + ["ps", "-aq", "auth-init"], env=env).stdout.strip()
            initializer = json.loads(run(["docker", "inspect", init_id], env=env).stdout)[0]
            check("authentication initializer has no network and only CHOWN capability",
                  initializer["HostConfig"]["NetworkMode"] == "none" and
                  {cap.removeprefix("CAP_") for cap in initializer["HostConfig"]["CapDrop"]} == {"ALL"} and
                  {cap.removeprefix("CAP_") for cap in initializer["HostConfig"]["CapAdd"]} == {"CHOWN"})
            rerun = run(compose + ["run", "--rm", "--no-deps", "-T", "auth-init"], env=env)
            check("authentication initializer rerun does not log its credentials",
                  password not in rerun.stdout + rerun.stderr and password_hash not in rerun.stdout + rerun.stderr)
            code, _, body = http("/56", rpc("smoke-auth-rerun"))
            check("authentication initializer rerun is idempotent", code == 200 and
                  json.loads(body)["result"] == "0x38" and auth_attributes() == ["101:101:700", "101:101:644"])
            for label, invalid_entry in (("missing hash", user + ":invalid"),
                                         ("extra line", password_entry + "\n" + password_entry),
                                         ("invalid username", "../smoke:" + password_hash)):
                invalid_env = dict(env, RPCHUB_GATEWAY_HTPASSWD=invalid_entry)
                result = run(compose + ["run", "--rm", "--no-deps", "-T", "auth-init"], env=invalid_env, check=False)
                check("authentication initializer rejects " + label, result.returncode != 0 and
                      password_hash not in result.stdout + result.stderr)
            code, _, body = http("/56", rpc("smoke-auth-after-rejection"))
            check("rejected initialization preserves existing authentication", code == 200 and
                  json.loads(body)["result"] == "0x38" and auth_attributes() == ["101:101:700", "101:101:644"])

            code, _, body = http("/health", authorization=None)
            check("public health is gateway liveness only", code == 200 and json.loads(body)["archiveReadiness"] == "not_checked")
            code, _, _ = http("/56", rpc("smoke-unauthorized"), authorization=None)
            check("missing authentication rejected", code == 401)
            code, _, _ = http("/56", rpc("smoke-wrong-password"), authorization="Basic " + base64.b64encode(b"smoke:wrong").decode())
            check("wrong authentication rejected", code == 401)
            code, _, _ = http("/chains", authorization=None)
            check("operational metadata also requires authentication", code == 401)

            for chain in (56, 8453):
                for suffix in ("", "/archive", "/indexer"):
                    code, headers, body = http(f"/{chain}{suffix}", rpc(f"smoke-valid-{chain}-{suffix}"))
                    check(f"authenticated {chain}{suffix or '/general'} read works", code == 200 and json.loads(body)["result"] == hex(chain))
                    lower_headers = {key.lower(): value for key, value in headers.items()}
                    check(f"{chain}{suffix or '/general'} headers hide upstream identity and CORS",
                          "x-rpchub-upstream" not in lower_headers and "access-control-allow-origin" not in lower_headers)
                code, _, body = http(f"/{chain}/health")
                check(f"{chain} health hides upstream credentials", code == 200 and "dummy-" not in body and
                      json.loads(body)["endpoints"][0]["url"] == "http://fixture:9570/…")

            rejected_ids = []
            code, _, body = http("/56/archive", [rpc(f"smoke-boundary-{i}") for i in range(30)])
            check("a thirty-call batch is accepted at the configured boundary", code == 200 and len(json.loads(body)) == 30)
            for suffix in ("", "/archive"):
                label = suffix.replace("/", "") or "general"
                bad_cases = [
                    ("writes", rpc(f"smoke-write-{label}", "eth_sendRawTransaction", ["0x00"])),
                    ("unknown methods", rpc(f"smoke-unknown-method-{label}", "admin_peers")),
                    ("mixed read/write batches", [rpc(f"smoke-mixed-safe-{label}"), rpc(f"smoke-mixed-write-{label}", "eth_sendRawTransaction", ["0x00"])]),
                    ("null IDs", rpc(None)),
                    ("notifications", {"jsonrpc": "2.0", "method": "eth_chainId", "params": []}),
                    ("batches over thirty items", [rpc(f"smoke-batch-{label}-{i}") for i in range(31)]),
                    ("mixed-case method keys", dict(rpc(f"smoke-case-{label}", "eth_sendRawTransaction"), METHOD="eth_chainId")),
                    ("duplicate method keys", ('{"jsonrpc":"2.0","id":"smoke-duplicate-' + label + '","method":"eth_sendRawTransaction","method":"eth_chainId","params":[]}').encode()),
                    ("non-string methods", dict(rpc(f"smoke-method-type-{label}"), method=123)),
                    ("object IDs", rpc({"unexpected": "value"})),
                    ("non-2.0 JSON-RPC versions", dict(rpc(f"smoke-version-{label}"), jsonrpc="1.0")),
                ]
                for case, payload in bad_cases:
                    decoded = json.loads(payload) if isinstance(payload, bytes) else payload
                    for item in decoded if isinstance(decoded, list) else [decoded]:
                        if isinstance(item.get("id"), str):
                            rejected_ids.append(item["id"])
                    code, _, _ = http("/56" + suffix, payload)
                    check(f"{label} rejects {case}", code == 400)
                for malformed in (b'{"jsonrpc":"2.0",', b'[]', b'null'):
                    code, _, _ = http("/56" + suffix, malformed)
                    check(f"{label} rejects malformed envelope {malformed.decode()}", code == 400)
            code, _, _ = http("/not-a-route", rpc("smoke-unknown-route"))
            check("unknown route rejected", code == 404)
            rejected_ids += ["smoke-unauthorized", "smoke-wrong-password", "smoke-unknown-route"]
            code, _, _ = http("/56", b" " * (256 * 1024 + 1))
            check("gateway body limit enforced", code == 413)
            observed = stats()
            check("rejected requests never reached an upstream", all(identifier not in observed["ids"] for identifier in rejected_ids))
            check("null IDs and notifications never reached an upstream", observed["missingOrNullIds"] == 0)
            check("gateway never forwards caller credentials or cookies", observed["authorizationLeaked"] == 0 and observed["cookieLeaked"] == 0)

            check("recorded consumer fixture includes the expected cold and warm HTTP costs",
                  readiness_runs["cold"]["httpRequests"] == 15 and readiness_runs["immediate_warm"]["httpRequests"] == 9)
            legacy_readiness = readiness_sequence("legacy", legacy_base)
            check("legacy 2r/s burst4 rejects the real sequential readiness shape", any(
                  call["status"] == 429 for call in legacy_readiness["flows"][0]["calls"]))
            cadence_start = time.monotonic()
            candidate_readiness = readiness_sequence("candidate", base)
            check("continuous cold, warm and two independent cold flows issue 54 HTTP requests",
                  candidate_readiness["httpRequests"] == 54)
            check("first cold config completes within the 5s cache lifetime required by the warm replay",
                  candidate_readiness["flows"][0]["durationMs"] < readiness_fixture["readinessCacheMs"])
            assert_readiness("continuous consumer workload", candidate_readiness["flows"])
            admissions = [item for item in stats()["readinessRequests"]
                          if item["ids"][0].startswith("smoke-readiness-candidate-")]
            check("all 54 candidate HTTP requests reach the fixture exactly once", len(admissions) == 54)
            admission_times = sorted(item["atMs"] for item in admissions)
            admission_span = admission_times[-1] - admission_times[0]
            if selected_limits["delayAfter"] is not None:
                minimum_span = ((54 - selected_limits["delayAfter"] - 1) / selected_limits["httpRequestsPerSecond"] - 0.5) * 1000
                check("readiness upstream admission span remains within the configured queue budget", admission_span >= minimum_span)
            polls = []
            for index in (1, 2):
                time.sleep(max(0, cadence_start + index * 15 - time.monotonic()))
                polls.append(replay_readiness("cold", f"poll-{index}", base))
            assert_readiness("repeated 15-second config polling", polls)
            dynamic_cache = dynamic_cache_regression()

            # Keep the old address occupied so the core must receive a different IP.
            # Only this test's backend network and containers are involved.
            backend, address = next(iter(core["NetworkSettings"]["Networks"].items()))
            previous_ip = address["IPAddress"]
            run(compose + ["rm", "--stop", "--force", "rpchub"], env=env)
            blocker_id = run(["docker", "create", "--name", project + "-old-ip", "--network", backend,
                              "--ip", previous_ip, "--label", "rpchub.smoke=" + project,
                              "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                              "--entrypoint", "sleep", image_id, "300"], env=env).stdout.strip()
            run(["docker", "start", blocker_id], env=env)
            run(compose + ["up", "-d", "--no-deps", "--no-build", "--pull", "never", "rpchub"], env=env)
            replacement_id = run(compose + ["ps", "-q", "rpchub"], env=env).stdout.strip()
            replacement = json.loads(run(["docker", "inspect", replacement_id], env=env).stdout)[0]
            check("recreated core receives a different private address",
                  replacement["NetworkSettings"]["Networks"][backend]["IPAddress"] != previous_ip)
            await_ready()
            code, _, body = http("/56/archive", rpc("smoke-recreated-core"))
            check("unchanged gateway resolves and reaches the recreated core", code == 200 and
                  json.loads(body)["result"] == "0x38" and
                  run(compose + ["ps", "-q", "gateway"], env=env).stdout.strip() == gateway_id)
            run(["docker", "rm", "--force", blocker_id], env=env)
            blocker_id = None

            time.sleep(3)
            overload_count = max(64, selected_limits["burst"] * 3, selected_limits["connections"] * 4)
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                burst = list(executor.map(lambda i: http("/56", rpc(f"smoke-rate-{i}"), pace=False)[0], range(overload_count)))
            check("concurrent burst is rate limited", 429 in burst and all(code in (200, 429) for code in burst))
            overload_admissions = sorted(item["atMs"] for item in stats()["rateRequests"])
            check("only accepted overload requests reach the fixture", len(overload_admissions) == burst.count(200))
            overload_span = overload_admissions[-1] - overload_admissions[0]
            if selected_limits["delayAfter"] is not None:
                minimum_span = ((len(overload_admissions) - selected_limits["delayAfter"] - 1) /
                                selected_limits["httpRequestsPerSecond"] - 0.1) * 1000
                check("zero-latency overload confirms queued gateway pacing", len(overload_admissions) > selected_limits["delayAfter"] + 1 and
                      overload_span >= minimum_span)
            time.sleep(selected_limits["burst"] / selected_limits["httpRequestsPerSecond"] + 1)
            run(compose + ["stop", "fixture"], env=env)
            for chain in (56, 8453):
                code, _, body = http(f"/{chain}/archive", rpc(f"smoke-offline-{chain}"))
                check(f"{chain} fails closed after its only source stops", code in (502, 503) and "error" in json.loads(body))
                check(f"{chain} outage response hides its upstream URL", "fixture" not in body and "dummy-" not in body)
                code, _, body = http(f"/{chain}/health")
                document = json.loads(body)
                check(f"{chain} never discovers a fallback provider", code == 200 and document["total"] == 1 and "dummy-" not in body)
            logs = run(compose + ["logs", "--no-color"], env=env).stdout
            check("service logs contain no authentication or provider credentials", all(secret not in logs for secret in
                  (password, password_hash, password_entry, auth, "dummy-bnb-upstream-key", "dummy-base-upstream-key")))
            report = {"status": "passed", "image": image_id, "gatewayImage": gateway_image,
                      "checksPassed": len(checks), "checks": checks, "realProvidersUsed": False,
                      "maxCallsPerHttpRequest": 30, "rateLimitUnit": "HTTP requests, not individual batch calls",
                      "gatewayLimits": selected_limits, "temporaryCandidateOverride": bool(args.candidate_rate),
                      "syntheticReadinessLatencyMs": args.readiness_delay_ms,
                      "dynamicCacheReadiness": dynamic_cache,
                      "overload": {"requests": overload_count, "accepted": burst.count(200), "rejected": burst.count(429),
                                   "syntheticLatencyMs": 0, "upstreamAdmissionSpanMs": overload_span},
                      "readinessReplay": {"fixtureSource": readiness_fixture["source"],
                                          "legacy": legacy_readiness, "candidate": candidate_readiness,
                                          "upstreamAdmissionSpanMs": admission_span, "polls": polls,
                                          "limitations": [
                                              "Replays captured RPC boundaries; does not invoke the complete Next.js handler.",
                                              "The deterministic local upstream excludes real provider and indexer-status latency.",
                                              "Only Base and BNB readiness are exercised; unrelated chains and background traffic are excluded.",
                                              "The legacy replay continues after errors to measure rejection; the real client may fail earlier.",
                                          ]},
                      "hostBinding": "127.0.0.1 only", "cleanup": "unique test project and network only"}
        except Exception:
            if compose and started_project:
                logs = run(compose + ["logs", "--tail", "30"], env=env, check=False)
                print(logs.stdout[-6000:])
                print(logs.stderr[-2000:])
            raise
        finally:
            if blocker_id:
                run(["docker", "rm", "--force", blocker_id], env=env)
            if compose and started_project:
                run(compose + ["down", "--volumes", "--remove-orphans"], env=env)
            if created_network:
                run(["docker", "network", "rm", edge], env=env)
    return args, report


if __name__ == "__main__":
    arguments, result = main()
    if arguments.report:
        arguments.report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
