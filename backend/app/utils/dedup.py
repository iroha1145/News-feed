import hashlib
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_TRACKING_KEYS = {
    "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "igshid",
    "vero_conv", "vero_id", "mkt_tok", "oly_anon_id", "oly_enc_id",
    "ref_src", "ref_url", "spm", "yclid",
}


def normalize_title(title: str) -> str:
    """Normalize a news title for deduplication."""
    title = title.lower().strip()
    # Remove extra whitespace
    title = re.sub(r"[^\w\s]", " ", title, flags=re.UNICODE)
    title = re.sub(r"\s+", " ", title)
    # Remove common trailing punctuation
    return title.strip()


def normalize_url(url: str) -> str:
    """Canonicalize a URL and remove common analytics parameters."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return url.strip()
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        return url.strip()

    host = parts.hostname.lower()
    port = parts.port
    if port and not ((parts.scheme == "http" and port == 80) or (parts.scheme == "https" and port == 443)):
        host = f"{host}:{port}"

    query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_KEYS:
            continue
        query.append((key, value))
    query.sort()
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    return urlunsplit((parts.scheme.lower(), host, path, urlencode(query, doseq=True), ""))


def publication_bucket(published_at: object) -> str:
    """Map a publication timestamp to a UTC calendar-day bucket."""
    if not published_at:
        return "unknown"
    text = str(published_at).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).date().isoformat()
    except ValueError:
        match = re.match(r"\d{4}-\d{2}-\d{2}", text)
        return match.group(0) if match else "unknown"


def compute_content_hash(title: str, url: str = "", published_at: object = None) -> str:
    """
    Compute a deduplication hash from the normalized title.
    Falls back to URL if title is empty.
    """
    normalized = normalize_title(title)
    if normalized:
        bucket = publication_bucket(published_at)
        if bucket != "unknown":
            normalized = f"title:{normalized}|day:{bucket}"
        elif url:
            normalized = f"title:{normalized}|url:{normalize_url(url)}"
        else:
            normalized = f"title:{normalized}|day:unknown"
    elif url:
        normalized = f"url:{normalize_url(url)}"

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def compute_legacy_content_hash(title: str, url: str = "") -> str:
    """Reproduce the pre-2026 hash so upgrades do not reinsert old headlines."""
    normalized = re.sub(r"\s+", " ", title.lower().strip()).rstrip(".,!?;:")
    if not normalized and url:
        normalized = url.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def similar_titles(left: str, right: str, threshold: float = 0.92) -> bool:
    if left == right:
        return True
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    left_numbers = set(re.findall(r"\b\d+(?:\.\d+)?%?\b", left))
    right_numbers = set(re.findall(r"\b\d+(?:\.\d+)?%?\b", right))
    if left_numbers != right_numbers:
        return False
    union = left_tokens | right_tokens
    token_similarity = len(left_tokens & right_tokens) / len(union) if union else 0
    if token_similarity >= threshold:
        return True
    return token_similarity >= 0.75 and SequenceMatcher(
        None,
        left,
        right,
        autojunk=False,
    ).ratio() >= threshold


def deduplicate_batch(items: Iterable[dict], *, threshold: float = 0.92) -> tuple[list[dict], int]:
    """Remove exact URL and near-title duplicates within the same publication day."""
    unique: list[dict] = []
    seen_urls: set[str] = set()
    titles_by_day: dict[str, list[str]] = {}
    duplicates = 0

    for original in items:
        item = dict(original)
        item["url"] = normalize_url(str(item.get("url") or ""))
        normalized_title = normalize_title(str(item.get("title") or ""))
        day = publication_bucket(item.get("published_at"))

        if item["url"] and item["url"] in seen_urls:
            duplicates += 1
            continue
        day_titles = titles_by_day.setdefault(day, [])
        if normalized_title and any(similar_titles(normalized_title, prior, threshold) for prior in day_titles):
            duplicates += 1
            continue

        if item["url"]:
            seen_urls.add(item["url"])
        if normalized_title:
            day_titles.append(normalized_title)
        unique.append(item)

    return unique, duplicates
