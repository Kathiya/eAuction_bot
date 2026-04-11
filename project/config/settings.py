from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Pipe-separated search URLs when LISTING_SEARCH_URLS is unset; empty env value = legacy single template.
EAUCTIONS_DEFAULT_LISTING_SEARCH_URLS = (
    "https://www.eauctionsindia.com/search?keyword=&category=residential&state=gujarat&city=ahmedabad&area=&bank=&from=&to=&min_price=&max_price=|"
    "https://www.eauctionsindia.com/search?keyword=&category=vehicle-auctions&state=gujarat&city=&area=&bank=&from=&to=&min_price=&max_price="
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    source_site: str = Field(
        default="eauctionsindia",
        description="Logical source id for cache envelope (set to match portal)",
    )

    listing_portal: Literal["auto", "eauctionsindia"] = Field(
        default="auto",
        validation_alias="LISTING_PORTAL",
        description="eAuctions India only (Baanknet removed)",
    )

    @field_validator("listing_portal", mode="before")
    @classmethod
    def _listing_portal_legacy(cls, v: object) -> object:
        if v is None:
            return "auto"
        s = str(v).strip().lower()
        if s == "baanknet":
            return "eauctionsindia"
        return v

    listing_cache_path: str = Field(
        default="./data/listings_cache.json",
        validation_alias="LISTING_CACHE_PATH",
    )
    filters_json_path: str = Field(
        default="./config/filters.json",
        validation_alias="FILTERS_JSON_PATH",
    )

    scraper_backend: Literal["http"] = Field(
        default="http",
        validation_alias="SCRAPER_BACKEND",
        description="HTTP-only (Playwright removed)",
    )

    @field_validator("scraper_backend", mode="before")
    @classmethod
    def _scraper_backend_legacy(cls, v: object) -> object:
        if v is None:
            return "http"
        s = str(v).strip().lower()
        if s in ("auto", "playwright"):
            return "http"
        return v

    listing_search_urls: str = Field(
        default=EAUCTIONS_DEFAULT_LISTING_SEARCH_URLS,
        validation_alias="LISTING_SEARCH_URLS",
        description="Pipe | separated eAuctions search URLs; blank env uses built-in Gujarat defaults",
    )

    @field_validator("listing_search_urls", mode="before")
    @classmethod
    def _empty_listing_search_urls_use_defaults(cls, v: object) -> object:
        """GitHub Actions sets missing secrets to ''; that must not enable unfiltered /search?page=1."""
        if v is None:
            return EAUCTIONS_DEFAULT_LISTING_SEARCH_URLS
        if str(v).strip() == "":
            return EAUCTIONS_DEFAULT_LISTING_SEARCH_URLS
        return v

    listing_page_url: str = Field(
        default="https://www.eauctionsindia.com/search?page=1",
        validation_alias="LISTING_PAGE_URL",
    )
    listing_page_url_template: str | None = Field(
        default="https://www.eauctionsindia.com/search?page={page}",
        validation_alias="LISTING_PAGE_URL_TEMPLATE",
        description="Used only when LISTING_SEARCH_URLS is empty",
    )

    http_listing_api_url: str | None = Field(
        default=None,
        validation_alias="HTTP_LISTING_API_URL",
        description="Optional internal JSON API base; if set, HTTP client tries API before HTML",
    )
    http_user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        validation_alias="HTTP_USER_AGENT",
    )
    http_verify_ssl: bool = Field(
        default=True,
        validation_alias="HTTP_VERIFY_SSL",
        description="Set false only if TLS verification fails (e.g. corporate MITM)",
    )

    request_timeout_s: float = Field(default=30.0, validation_alias="REQUEST_TIMEOUT_S")
    max_retries: int = Field(default=3, validation_alias="MAX_RETRIES")
    rate_limit_delay_s: float = Field(default=1.0, validation_alias="RATE_LIMIT_DELAY_S")
    max_pages_per_run: int = Field(
        default=5,
        validation_alias="MAX_PAGES_PER_RUN",
        description="Max page number per search URL (LISTING_SEARCH_URLS) or per template",
    )

    telegram_bot_token: str | None = Field(default=None, validation_alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_ids: str = Field(
        default="",
        validation_alias="TELEGRAM_CHAT_ID",
        description="Comma-separated chat IDs",
    )

    poll_interval_seconds: int = Field(default=900, validation_alias="POLL_INTERVAL_SECONDS")

    notify_on_first_run: bool = Field(default=False, validation_alias="NOTIFY_ON_FIRST_RUN")
    notify_on_content_change: bool = Field(
        default=False,
        validation_alias="NOTIFY_ON_CONTENT_CHANGE",
        description="If true, notify when content_hash changes for an existing stable_id",
    )

    telegram_send_full_digest_each_run: bool = Field(
        default=False,
        validation_alias="TELEGRAM_FULL_DIGEST_EACH_RUN",
        description="After each successful run, send all scraped listings (chunked) plus search URLs for audit",
    )

    log_json: bool = Field(default=True, validation_alias="LOG_JSON")

    api_host: str = Field(default="127.0.0.1", validation_alias="API_HOST")
    api_port: int = Field(default=8080, validation_alias="API_PORT")

    @field_validator("telegram_chat_ids", mode="before")
    @classmethod
    def strip_chat_ids(cls, v: object) -> str:
        if v is None:
            return ""
        return str(v).strip()

    def parsed_telegram_chat_ids(self) -> list[str]:
        if not self.telegram_chat_ids.strip():
            return []
        return [x.strip() for x in self.telegram_chat_ids.split(",") if x.strip()]

    def parsed_listing_search_urls(self) -> list[str]:
        raw = (self.listing_search_urls or "").strip()
        if not raw:
            return []
        return [x.strip() for x in raw.split("|") if x.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
