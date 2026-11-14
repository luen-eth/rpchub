#!/usr/bin/env python3
"""Smoke-test the production Compose profile against isolated local RPC fixtures.

Requires Docker Compose, the built RPChub image, Go, and OpenSSL. The test never
uses real RPC providers, existing application containers, or real credentials.
Only its unique Compose project and edge network are removed on completion.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="rpchub:memehub-archive")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
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
        override = temp / "override.json"
        override.write_text(json.dumps({"services": {
            "rpchub": {
                "depends_on": ["fixture"],
                "environment": {"PROBE_INTERVAL": "2s", "REQUEST_TIMEOUT": "2s"},
            },
            "gateway": {"ports": [{"target": 8080, "published": "0", "host_ip": "127.0.0.1"}]},
            "fixture": {
                "image": image_id,
                "entrypoint": ["/fixture"],
                "volumes": [str(fixture) + ":/fixture:ro"],
                "networks": ["backend"],
                "read_only": True,
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
                "healthcheck": {"disable": True},
            },
        }}))
        compose = ["docker", "compose", "-p", project, "--env-file", str(envfile),
                   "-f", str(ROOT / "docker-compose.memehub.yml"), "-f", str(override)]
        try:
            run(["docker", "network", "create", "--label", "rpchub.smoke=" + project, edge], env=env)
            created_network = True
            # Pull only the profile's digest-pinned public gateway image if absent.
            rendered = json.loads(run(compose + ["config", "--format", "json"], env=env).stdout)
            assert rendered["services"]["rpchub"]["environment"]["MAX_BATCH_ITEMS"] == "30", "Expected the Memehub thirty-call batch limit"
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
            auth = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()
            last_request = 0.0

            def http(path, payload=None, authorization=auth, pace=True):
                nonlocal last_request
                if pace:
                    time.sleep(max(0, 0.55 - (time.monotonic() - last_request)))
                    last_request = time.monotonic()
                body = payload if isinstance(payload, bytes) else None if payload is None else json.dumps(payload).encode()
                headers = {"Content-Type": "application/json", "Cookie": "smoke-cookie=must-not-leak"}
                if authorization:
                    headers["Authorization"] = authorization
                request = urllib.request.Request(base + path, data=body, headers=headers)
                try:
                    with urllib.request.urlopen(request, timeout=12) as response:
                        return response.status, dict(response.headers), response.read().decode()
                except urllib.error.HTTPError as error:
                    return error.code, dict(error.headers), error.read().decode()

            def stats():
                return json.loads(run(compose + ["exec", "-T", "fixture", "wget", "-qO-",
                                                 "http://127.0.0.1:9570/__stats"], env=env).stdout)

            def check(name, condition):
                assert condition, name
                checks.append(name)

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
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                burst = list(executor.map(lambda i: http("/56", rpc(f"smoke-rate-{i}"), pace=False)[0], range(12)))
            check("concurrent burst is rate limited", 429 in burst and all(code in (200, 429) for code in burst))
            time.sleep(3)
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
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
