from __future__ import annotations
import json, re

SEP = "; "

def deterministic_serialize_list(x):
    if x is None:
        return ""
    if isinstance(x, list):
        vals = x
    elif isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                vals = json.loads(s)
                if not isinstance(vals, list):
                    vals = [s]
            except Exception:
                vals = [s]
        else:
            parts = [p.strip() for p in re.split(r"[;,|]", s) if p.strip()]
            vals = parts
    else:
        vals = [str(x)]
    vals = [str(v).strip() for v in vals if str(v).strip() != ""]
    vals = sorted(set(vals), key=lambda z: (z.lower(), z))
    return SEP.join(vals)

def deterministic_json(obj) -> str:
    try:
        return json.dumps(obj, sort_keys=True, ensure_ascii=False)
    except Exception:
        return ""

def normalize_doi(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r'^https?://(dx\.)?doi\.org/', '', s, flags=re.I)
    s = s.strip().strip('/')
    s = s.lower()
    if ' ' in s or '/' not in s:
        return ""
    return s
