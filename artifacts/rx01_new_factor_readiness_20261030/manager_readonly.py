"""Read current RX03 Manager reference through a short-lived lgq auth session."""
import ast
import json
from pathlib import Path

home = Path.home()
helper = home / "binance_exec_trade06/run/rx01_activation_ops.py"
tree = ast.parse(helper.read_text())
imports = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {"db_env", "sql", "session"}]
namespace = {"MANAGER": home / "crypto_cta_manager"}
for node in tree.body:
    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "ORIGIN" for t in node.targets):
        namespace["ORIGIN"] = ast.literal_eval(node.value)
exec(compile(ast.Module(body=imports + functions, type_ignores=[]), str(helper), "exec"), namespace)
with namespace["session"]("lgq") as api:
    result = {}
    for path in ("/manager/api/catalog/accounts/binance_exec_trade07", "/manager/api/catalog/virtual-accounts"):
        data = api(path)
        if isinstance(data, dict):
            print("endpoint", path, "keys", list(data))
            for key in ("mode", "virtual_id", "follows", "configuration"):
                if key in data:
                    print(key, data[key])
            for binding in data.get("bindings", [])[:1]:
                print("reference_binding", {k: binding.get(k) for k in ("binding_name", "position_strategy_name", "order_strategy_name", "shares")})
        elif isinstance(data, list):
            print("virtuals", [{k: v.get(k) for k in ("virtual_id", "name")} for v in data])
        result[path] = data
    output = home / "binance_exec_trade06/run/restore_audit_20261030/manager_reference.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    output.chmod(0o600)
