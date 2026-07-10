from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_env_rel = (os.environ.get("UNION_AGENT_ENV_FILE") or ".env").strip() or ".env"
_root = _PROJECT_ROOT.resolve()
_candidate = (_PROJECT_ROOT / Path(_env_rel)).resolve()
try:
    _candidate.relative_to(_root)
    _ENV_FILE = _candidate
except ValueError:
    _ENV_FILE = _root / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    debug: bool = False
    log_level: str = "INFO"
    log_format: str = "json"

    database_url: str | None = None

    kingdee_url: str | None = None
    kingdee_acct_id: str | None = None
    kingdee_test_acct_id: str | None = None
    kingdee_username: str | None = None
    kingdee_password: str | None = None

    wecom_corp_id: str | None = None
    wecom_corp_secret: str | None = None
    wecom_agent_id: int | None = None
    wecom_proxy_url: str | None = None
    wecom_proxy_secret: str | None = None

    sf_module_enabled: bool = True
    sf_base_url: str = "https://portal-gateway.sit.sf-express.com:55443/scc-portal-api-service/omsPortalService/sendRequest"
    sf_company_code: str = "YLCS"
    sf_warehouse_code: str = "755DCG"
    sf_warehouse: str = "CK043"
    sf_access_code: str = ""
    sf_checkword: str = ""
    sf_special_str: str = ""

    jwt_expire_hours: int = Field(default=72, ge=1, le=8760)


settings = Settings()
