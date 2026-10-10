"""Create a local Airflow Snowflake connection from .env (no extra packages)."""

import json
from pathlib import Path

root = Path(__file__).resolve().parent
env = {}
for line in (root / ".env").read_text(encoding="utf-8").splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
        continue
    key, sep, value = line.partition("=")
    if not sep:
        raise ValueError(f"Invalid .env line: {line!r}")
    env[key.strip()] = value.strip().strip('"').strip("'")

required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_ROLE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE"]
missing = [key for key in required if not env.get(key)]
if missing:
    raise SystemExit("Fill in .env first: " + ", ".join(missing))

settings = {"airflow": {"connections": [{
    "conn_id": "snowflake_default",
    "conn_type": "snowflake",
    "conn_login": env["SNOWFLAKE_USER"],
    "conn_password": env["SNOWFLAKE_PASSWORD"],
    "conn_schema": "BRONZE",
    "conn_extra": {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "warehouse": env["SNOWFLAKE_WAREHOUSE"],
        "database": env["SNOWFLAKE_DATABASE"],
        "role": env["SNOWFLAKE_ROLE"],
    },
}]}}
(root / "airflow_settings.yaml").write_text(
    json.dumps(settings, indent=2) + "\n", encoding="utf-8"
)
print("Created local airflow_settings.yaml from .env")
