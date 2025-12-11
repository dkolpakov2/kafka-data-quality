import yaml
import re
from datetime import datetime
from typing import Tuple, Dict, Any

SAFE_GLOBALS = {"re": re, "datetime": datetime}

class RuleEngine:
    def __init__(self, rules_path: str):
        with open(rules_path) as f:
            self.doc = yaml.safe_load(f)
        self.rules = self.doc.get("rules", [])

    def _safe_eval(self, expr: str, local_vars: dict) -> bool:
        # IMPORTANT: restrict builtins; provide only SAFE_GLOBALS + locals
        return eval(expr, {"__builtins__": {} , **SAFE_GLOBALS}, local_vars)

    def apply(self, msg: Dict[str, Any]) -> Tuple[bool, list]:
        """
        Apply rules to msg.
        Returns: (is_valid: bool, failures: list of {id, severity, message, route})
        """
        failures = []
        # local context accessible by conditions
        ctx = {"msg": msg, "now": datetime.utcnow()}
        for rule in self.rules:
            # preprocess step
            for prep in rule.get("preprocess", []) if rule.get("preprocess") else []:
                # each preprocess item has name & expression
                name = prep["name"]
                expr = prep["expression"]
                try:
                    ctx[name] = self._safe_eval(expr, {"msg": msg, "now": ctx["now"]})
                except Exception as e:
                    failures.append({"id": rule["id"], "severity": "fail", "message": f"preprocess error: {e}", "route": rule.get("route","invalid")})
                    if rule.get("severity","fail") == "fail":
                        return False, failures

            try:
                cond = rule["condition"]
                ok = self._safe_eval(cond, ctx)
                if not ok:
                    failures.append({"id": rule["id"], "severity": rule.get("severity","fail"), "message": rule.get("message",""), "route": rule.get("route","invalid")})
            except Exception as e:
                failures.append({"id": rule["id"], "severity": "fail", "message": f"condition error: {e}", "route": rule.get("route","invalid")})
                if rule.get("severity","fail") == "fail":
                    return False, failures

        # message is valid if no failing rule with severity 'fail'
        is_valid = not any(f["severity"]=="fail" for f in failures)
        return is_valid, failures
