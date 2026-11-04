"""Check RX01 gateway privacy and immutable frontend assets without publishing."""
import ast
import hashlib
import json
from pathlib import Path
import urllib.request
import urllib.error

home = Path.home()
run = home / "binance_exec_trade06/run/restore_audit_20261030"
tree = ast.parse((home / "binance_exec_trade06/run/rx01_activation_ops.py").read_text())
nodes = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)) or (isinstance(n, ast.FunctionDef) and n.name in {"db_env", "sql", "session"})]
namespace = {"MANAGER": home / "crypto_cta_manager"}
for n in tree.body:
    if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ORIGIN" for t in n.targets):
        namespace["ORIGIN"] = ast.literal_eval(n.value)
exec(compile(ast.Module(body=nodes, type_ignores=[]), "private_session_helper", "exec"), namespace)
paths = ["/cta/binance-cta-perp-rx01/", "/cta/binance-cta-perp-rx01/config/", "/cta/binance-cta-perp-rx01/signals/"]
result = {}
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
for user in ["anonymous", "prc", "lgq"]:
    codes = {}
    def check(api):
        for path in paths:
            try:
                answer = api(path)
                code = answer["status"]
            except RuntimeError as e:
                if "HTTP 403" in str(e): code = 403
                elif "HTTP 401" in str(e): code = 401
                else: raise
            codes[path] = code
            assert code == 200 if user == "lgq" else code in [401, 403]
    if user == "anonymous":
        def api(path):
            try:
                with opener.open(namespace["ORIGIN"] + path, timeout=15) as r:
                    return {"status": r.status}
            except urllib.error.HTTPError as e:
                raise RuntimeError("HTTP " + str(e.code)) from None
        check(api)
    else:
        with namespace["session"](user) as api: check(api)
    result[user] = codes
for path in ["/cta/binance-cta-spread-rx01/", "/cta/binance-cta-spread-rx01/config/"]:
    try:
        opener.open(namespace["ORIGIN"] + path, timeout=15)
        raise AssertionError("retired Spread URL is still live")
    except urllib.error.HTTPError as e:
        assert e.code == 410
manifest = json.loads((home / "deploy-staging/rx01-web-20261030/manifest.json").read_text())
for name, digest in manifest["frontend_files"].items():
    with opener.open(namespace["ORIGIN"] + "/" + name, timeout=15) as r:
        assert r.status == 200 and hashlib.sha256(r.read()).hexdigest() == digest
result.update(frontend_assets_verified=len(manifest["frontend_files"]), old_spread_status=410, browser_rendered=False)
(run / "web_verified.json").write_text(json.dumps(result, indent=2)+"\n")
print(json.dumps(result))
