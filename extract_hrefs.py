import asyncio
import csv
import json
import os
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from traceback import format_exc

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from database_client import DatabaseClient, DatabaseConfig

DOWNLOAD_DIR = Path.cwd() / "downloads"
VIEW_TIMEOUT = 15_000
NAV_TIMEOUT = 60_000
PROPERTY_PORTAL_URLS = [
    # AUTO_SEARCH_V1:HENNEPIN
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTExNCIsInNldGtleSI6IjgyOCIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:DAKOTA
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTExNiIsInNldGtleSI6IjQ0MCIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:RAMSEY
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTEyMiIsInNldGtleSI6IjkzMyIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:WASHINGTON
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTEyMCIsInNldGtleSI6IjgzMCIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:ANOKA
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTEyMSIsInNldGtleSI6IjI1MSIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:SCOTT
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTExOCIsInNldGtleSI6IjEwMSIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0",
    # AUTO_SEARCH_V1:CARVER
    "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTEyNCIsInNldGtleSI6IjY2IiwiZW1haWwiOiJ0d2FnbmVyNTVAZ21haWwuY29tIiwicmVzb3VyY2VpZCI6MCwiYWdlbnRpZCI6MTg0NDcyLCJpc2RlbHRhIjpmYWxzZSwiVmlld01vZGUiOiIxIn0=&SMS=0",
]
LOT_LOOKUP_LIMIT = int(os.environ.get("LOT_LOOKUP_LIMIT", "25"))
LOT_MATCH_THRESHOLD = float(os.environ.get("LOT_MATCH_THRESHOLD", "0.65"))
LOT_MATCH_MODE = os.environ.get("LOT_MATCH_MODE", "improved").lower()
DB_CLIENT = DatabaseClient(DatabaseConfig())


def log(message: str):
    print(f"[workflow] {message}")


DIRECTION_TOKENS = {
    "n",
    "s",
    "e",
    "w",
    "ne",
    "nw",
    "se",
    "sw",
    "north",
    "south",
    "east",
    "west",
    "northeast",
    "northwest",
    "southeast",
    "southwest",
}
STREET_TYPE_TOKENS = {
    "st",
    "street",
    "ave",
    "avenue",
    "rd",
    "road",
    "dr",
    "drive",
    "ln",
    "lane",
    "blvd",
    "boulevard",
    "cir",
    "circle",
    "ct",
    "court",
    "pl",
    "place",
    "ter",
    "terrace",
    "trl",
    "trail",
    "pkwy",
    "parkway",
    "way",
}
UNIT_TOKENS = {"apt", "unit", "suite", "ste"}


def normalize_address(address: str | None, city: str | None) -> str:
    address = (address or "").strip()
    city = (city or "").strip()
    if address and city:
        return f"{address}, {city}"
    return address or city


def tokenize(value: str | None) -> list[str]:
    if not value:
        return []
    cleaned = re.sub(r"[^\w\s#]", " ", value.lower())
    return [token for token in cleaned.split() if token]


def strip_unit_tokens(tokens: list[str]) -> list[str]:
    for index, token in enumerate(tokens):
        if token in UNIT_TOKENS or token.startswith("#"):
            return tokens[:index]
    return tokens


def remove_unit_tokens(tokens: list[str]) -> list[str]:
    return [token for token in tokens if token not in UNIT_TOKENS and not token.startswith("#")]


def split_street_components(address: str | None) -> tuple[str | None, str | None]:
    if not address:
        return None, None
    parts = tokenize(address)
    if not parts:
        return None, None
    first = parts[0].rstrip()
    if first.isdigit():
        remainder = strip_unit_tokens(parts[1:])
        return first, " ".join(remainder).strip() or None
    remainder = strip_unit_tokens(parts)
    return None, " ".join(remainder).strip() or None


def extract_street_tokens(address: str | None) -> list[str]:
    if not address:
        return []
    _, street_name = split_street_components(address)
    tokens = tokenize(street_name)
    return tokens


def significant_street_tokens(tokens: list[str]) -> list[str]:
    return [
        token
        for token in tokens
        if token not in DIRECTION_TOKENS and token not in STREET_TYPE_TOKENS
    ]


def extract_postal_code(record: dict[str, Any]) -> str | None:
    for key in ("Zip", "ZIP", "Zip Code", "PostalCode", "Postal Code", "Postal"):
        value = record.get(key)
        if not value:
            continue
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        if len(digits) >= 5:
            return digits[:5]
    address = record.get("Address")
    city = record.get("City")
    combined = normalize_address(address, city)
    if combined:
        return extract_zip_from_text(combined)
    return None


def city_variants(city: str | None) -> list[str]:
    tokens = tokenize(city)
    if not tokens:
        return []
    variants = {" ".join(tokens)}
    if tokens[0] == "st":
        variants.add("saint " + " ".join(tokens[1:]))
    if tokens[0] == "saint":
        variants.add("st " + " ".join(tokens[1:]))
    return list(variants)


def normalize_for_match(text: str) -> str:
    tokens = remove_unit_tokens(tokenize(text))
    return " ".join(tokens)


def extract_house_number_from_text(text: str | None) -> str | None:
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    return stripped.split()[0]


def extract_zip_from_text(text: str | None) -> str | None:
    if not text:
        return None
    stripped = text.strip()
    if not stripped:
        return None
    last_token = stripped.split()[-1]
    digits = "".join(ch for ch in last_token if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    return None


def strip_trailing_state_tokens(tokens: list[str]) -> list[str]:
    if tokens and tokens[-1].isalpha() and len(tokens[-1]) == 2:
        return tokens[:-1]
    return tokens


def build_detail_tokens_from_text(
    text: str | None,
    house_number: str | None,
    postal_code: str | None,
) -> list[str]:
    tokens = remove_unit_tokens(tokenize(text))
    if house_number and tokens and tokens[0] == house_number.lower():
        tokens = tokens[1:]
    if postal_code and tokens and tokens[-1] == postal_code:
        tokens = tokens[:-1]
    tokens = strip_trailing_state_tokens(tokens)
    return tokens


def build_db_house_number(row: dict[str, Any], formatted: str | None) -> str | None:
    anumber = row.get("anumber")
    if anumber is not None:
        prefix = (row.get("anumberpre") or "").strip()
        suffix = (row.get("anumbersuf") or "").strip()
        base = f"{anumber}"
        return f"{prefix}{base}{suffix}" if prefix or suffix else base
    if formatted:
        return extract_house_number_from_text(formatted)
    return None


def make_like_fragment(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    lowered = value.lower().strip()
    if not lowered:
        return None
    return "formatted_address ILIKE %s", f"%{lowered}%"


def make_number_fragment(value: str | None) -> tuple[str, str] | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if not digits:
        return None
    return "formatted_address ILIKE %s", f"%{digits}%"


def query_lot_candidates(fragments: list[tuple[str, str]]):
    if not fragments:
        return []
    where_clause = " AND ".join(fragment for fragment, _ in fragments)
    sql = f"SELECT * FROM lots WHERE {where_clause} LIMIT %s"
    params = [value for _, value in fragments]
    params.append(LOT_LOOKUP_LIMIT)
    try:
        return DB_CLIENT.query(sql, params)
    except Exception as exc:  # pragma: no cover - defensive logging during runtime
        log(f"lot_lookup: query failed - {exc}")
        return []


def fetch_lot_candidates(
    address: str | None,
    city: str | None,
    postal_code: str | None,
) -> list[dict[str, Any]]:
    street_number, street_name = split_street_components(address)
    street_tokens = extract_street_tokens(address)
    significant_tokens = significant_street_tokens(street_tokens)
    street_query = " ".join(significant_tokens) or street_name

    city_fragments = [make_like_fragment(value) for value in city_variants(city)]
    city_fragments = [fragment for fragment in city_fragments if fragment]
    street_name_fragment = make_like_fragment(street_query)
    number_fragment = make_number_fragment(street_number)
    postal_fragment = make_number_fragment(postal_code)

    strategies: list[list[tuple[str, str]]] = []
    for city_fragment in city_fragments or [None]:
        combined = [
            fragment
            for fragment in (city_fragment, street_name_fragment, number_fragment, postal_fragment)
            if fragment
        ]
        if combined:
            strategies.append(combined)

        if postal_fragment and street_name_fragment and number_fragment:
            strategies.append([postal_fragment, street_name_fragment, number_fragment])
        if postal_fragment and number_fragment:
            strategies.append([postal_fragment, number_fragment])
        if city_fragment and street_name_fragment and number_fragment:
            strategies.append([city_fragment, street_name_fragment, number_fragment])
        if city_fragment and street_name_fragment:
            strategies.append([city_fragment, street_name_fragment])
        if street_name_fragment and number_fragment:
            strategies.append([street_name_fragment, number_fragment])
        if city_fragment and number_fragment:
            strategies.append([city_fragment, number_fragment])
        if postal_fragment and street_name_fragment:
            strategies.append([postal_fragment, street_name_fragment])

    if not number_fragment and postal_fragment:
        strategies.append([postal_fragment])
    if not number_fragment and street_name_fragment:
        strategies.append([street_name_fragment])
    if not number_fragment and city_fragments:
        strategies.extend([[fragment] for fragment in city_fragments if fragment])

    seen: set[tuple[str, ...]] = set()
    for fragments in strategies:
        key = tuple(fragment for fragment, _ in fragments)
        if key in seen:
            continue
        seen.add(key)
        rows = query_lot_candidates(fragments)
        if rows:
            return rows
    return []


def fetch_number_zip_candidates(
    house_number: str | None,
    postal_code: str | None,
) -> list[dict[str, Any]]:
    fragments: list[tuple[str, str | int]] = []
    if postal_code:
        fragments.append(("zip = %s", postal_code))
    if house_number:
        if house_number.isdigit():
            fragments.append(("anumber = %s", int(house_number)))
        else:
            fragments.append(("formatted_address ILIKE %s", f"{house_number} %"))
    if not fragments:
        return []
    return list(query_lot_candidates(fragments))


def number_zip_lot_lookup(record: dict[str, Any]) -> dict[str, Any] | None:
    address = record.get("Address")
    city = record.get("City")
    postal_code = extract_postal_code(record)
    combined_text = " ".join(part for part in (address, city, postal_code) if part)
    if not combined_text:
        return None

    house_number = extract_house_number_from_text(address or combined_text)
    zip_code = extract_zip_from_text(combined_text) or postal_code
    if not house_number or not zip_code:
        return None

    candidates = fetch_number_zip_candidates(house_number, zip_code)
    if not candidates:
        return None

    target_detail = " ".join(
        build_detail_tokens_from_text(combined_text, house_number, zip_code)
    )
    best_row: dict[str, Any] | None = None
    best_score = 0.0
    for row in candidates:
        formatted = row.get("formatted_address")
        if not formatted:
            continue
        db_house = build_db_house_number(row, formatted)
        db_zip = (row.get("zip") or "").strip() or extract_zip_from_text(formatted)
        if not db_house or not db_zip:
            continue
        if db_house.lower() != house_number.lower():
            continue
        if db_zip != zip_code:
            continue

        candidate_detail = " ".join(
            build_detail_tokens_from_text(formatted, db_house, db_zip)
        )
        score = SequenceMatcher(None, candidate_detail, target_detail).ratio()
        if score > best_score:
            best_row = row
            best_score = score

    if not best_row or best_score < LOT_MATCH_THRESHOLD:
        return None

    match = dict(best_row)
    match["match_score"] = round(best_score, 4)
    return match


def lot_lookup(
    record: dict[str, Any],
    mode: str | None = None,
) -> dict[str, Any] | None:
    resolved_mode = (mode or LOT_MATCH_MODE).lower()
    if resolved_mode in {"number_zip", "number-zip", "zip"}:
        return number_zip_lot_lookup(record)
    address = record.get("Address")
    city = record.get("City")
    postal_code = extract_postal_code(record)
    target = normalize_address(address, city)
    if not target:
        return None

    candidates = fetch_lot_candidates(address, city, postal_code)
    if not candidates:
        return None

    target_number, _ = split_street_components(address)
    target_street_tokens = significant_street_tokens(extract_street_tokens(address))
    target_numeric_tokens = [
        token for token in target_street_tokens if any(ch.isdigit() for ch in token)
    ]
    normalized_target = normalize_for_match(target)

    best_row: dict[str, Any] | None = None
    best_score = 0.0
    for row in candidates:
        formatted = row.get("formatted_address")
        if not formatted:
            continue
        formatted_tokens = set(remove_unit_tokens(tokenize(formatted)))
        if target_number and target_number not in formatted_tokens:
            continue
        if target_numeric_tokens and not any(
            token in formatted_tokens for token in target_numeric_tokens
        ):
            continue
        if target_street_tokens and not any(
            token in formatted_tokens for token in target_street_tokens
        ):
            continue

        normalized_formatted = normalize_for_match(formatted)
        score = SequenceMatcher(None, normalized_formatted, normalized_target).ratio()
        if score > best_score:
            best_row = row
            best_score = score

    if not best_row or best_score < LOT_MATCH_THRESHOLD:
        return None

    match = dict(best_row)
    match["match_score"] = round(best_score, 4)
    return match


def make_step_tracker(summary: dict):
    def track(step: str, status: str, detail: str | None = None):
        entry = {"step": step, "status": status}
        if detail:
            entry["detail"] = detail
        summary["steps"].append(entry)

    return track


async def click_close_icon(page):
    log("click_close_icon: Waiting for close icon to become visible.")
    locator = page.locator("svg path[d^='M13.73']")
    await locator.first.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    try:
        log("click_close_icon: Close icon visible, attempting to click directly.")
        await locator.first.click()
    except Exception:
        log(
            "click_close_icon: Direct click failed, attempting fallback DOM event dispatch."
        )
        handle = await locator.first.element_handle()
        if not handle:
            raise RuntimeError(
                "Unable to locate the close SVG icon for interaction"
            ) from None
        await handle.evaluate(
            """
            node => {
                const target = node.ownerSVGElement || node;
                target.dispatchEvent(new MouseEvent('click', { bubbles: true }));
            }
            """
        )


async def click_view_as_list(page):
    log("click_view_as_list: Switching the UI to list view.")
    locator = page.locator("div.radio-mock[data-tooltip='View as List']")
    await locator.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    await locator.click()


async def export_to_csv(page, download_dir: Path):
    log(f"export_to_csv: Preparing to export CSV into {download_dir}.")
    button = page.get_by_role("button", name="Export to CSV")
    await button.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    download_dir.mkdir(parents=True, exist_ok=True)
    async with page.expect_download() as download_info:
        log("export_to_csv: Export button ready, clicking to trigger download.")
        await button.click()
    download = await download_info.value
    target_path = download_dir / download.suggested_filename
    await download.save_as(target_path)
    return target_path


def resolve_urls() -> list[str]:
    log("resolve_urls: Starting URL resolution workflow.")
    env_urls = os.environ.get("URLS_JSON")
    if env_urls:
        log("resolve_urls: Found URLS_JSON environment variable, parsing JSON payload.")
        try:
            data = json.loads(env_urls)
        except Exception as exc:  # pragma: no cover - defensive logging in prod usage
            log(f"resolve_urls: Failed to parse URLS_JSON - {exc}")
            raise
        if not isinstance(data, list):
            raise TypeError("resolve_urls: URLS_JSON must encode a JSON list of strings")
        for index, value in enumerate(data):
            if not isinstance(value, str):
                raise TypeError(
                    f"resolve_urls: Entry at index {index} is not a string: {value!r}"
                )
        log(f"resolve_urls: Using {len(data)} URL(s) from environment override.")
        return data

    log(
        "resolve_urls: No environment override detected, using hardcoded property portal URLs."
    )
    log(
        f"resolve_urls: Hardcoded URL inventory ready ({len(PROPERTY_PORTAL_URLS)} entries)."
    )
    return PROPERTY_PORTAL_URLS.copy()


async def run_workflow(url: str):
    summary: dict = {
        "status": "pending",
        "download_path": None,
        "steps": [],
        "traceback": None,
        "url": url,
    }
    log(f"run_workflow: Starting workflow for URL: {url}")
    track = make_step_tracker(summary)
    download_dir = DOWNLOAD_DIR
    download_dir.mkdir(parents=True, exist_ok=True)
    track("init", "ok", f"Download dir: {download_dir}")
    log(f"Using download directory {download_dir}")

    try:
        log("run_workflow: Launching Playwright and Chromium browser.")
        async with async_playwright() as playwright:
            log("run_workflow: Playwright context acquired.")
            browser = await playwright.chromium.launch(headless=True)
            log("run_workflow: Chromium browser launched (headless=True).")
            context = await browser.new_context(accept_downloads=True)
            log("run_workflow: Browser context created; downloads enabled.")
            page = await context.new_page()
            log("run_workflow: New page opened, beginning scripted interactions.")

            try:
                track("navigate", "started", "Navigating to property portal")
                log(f"Navigating to property portal: {url}")
                await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT)
                log("run_workflow: Navigation completed, network idle.")
                track("navigate", "ok")

                track("close_overlay", "started", "Closing intro overlay")
                log("Closing intro overlay...")
                await click_close_icon(page)
                log("run_workflow: Intro overlay closed.")
                track("close_overlay", "ok")

                track("view_mode", "started", "Switching to list view")
                log("Switching to list view...")
                await click_view_as_list(page)
                log("run_workflow: List view confirmed.")
                track("view_mode", "ok")

                track("export", "started", "Exporting to CSV")
                log("Triggering Export to CSV...")
                csv_path = await export_to_csv(page, download_dir)
                summary["download_path"] = str(csv_path)
                log(f"CSV saved to: {csv_path}")
                track("export", "ok")
                summary["status"] = "success"

            finally:
                await context.close()
                await browser.close()

    except PlaywrightTimeoutError as exc:
        summary["status"] = "failed"
        summary["error"] = f"Timeout while interacting with page: {exc}"
        summary["traceback"] = format_exc()
        track("timeout", "error", summary["error"])
        log(summary["error"])

    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        summary["traceback"] = format_exc()
        track("fatal", "error", summary["error"])
        log(f"Unhandled exception: {exc}")

    return summary


def iter_combined_csv_rows(csv_paths: list[Path]):
    if not csv_paths:
        log("iter_combined_csv_rows: No CSV files provided; skipping merge.")
        return

    log(
        f"iter_combined_csv_rows: Combining rows from {len(csv_paths)} CSV file(s) without writing an intermediate file."
    )
    header_reference: list[str] | None = None

    for csv_path in csv_paths:
        log(f"iter_combined_csv_rows: Reading source CSV {csv_path}.")
        with csv_path.open("r", newline="", encoding="utf-8") as source_file:
            reader = csv.DictReader(source_file)
            header = reader.fieldnames
            if not header:
                log(f"iter_combined_csv_rows: CSV {csv_path} is empty; skipping.")
                continue

            if header_reference is None:
                header_reference = header
                log("iter_combined_csv_rows: Header initialized for combined data stream.")
            elif header != header_reference:
                raise ValueError(
                    f"iter_combined_csv_rows: Header mismatch detected in {csv_path.name}"
                )

            for row in reader:
                yield dict(row)


async def process_all_urls(urls: list[str]):
    log(f"process_all_urls: Starting processing for {len(urls)} URL(s).")
    results = []
    for index, url in enumerate(urls, start=1):
        log(f"process_all_urls: Beginning workflow {index}/{len(urls)}.")
        print(f"Processing: {url}")
        results.append(await run_workflow(url))
        log(f"process_all_urls: Completed workflow {index}/{len(urls)}.")
    log("process_all_urls: All workflows completed.")
    return results


if __name__ == "__main__":
    log("main: Workflow runner starting up.")
    url_list = resolve_urls()
    log(f"main: URL list ready with {len(url_list)} entries.")
    result = asyncio.run(process_all_urls(url_list))
    log("main: Workflow runner finished execution.")
    csv_paths = [
        Path(item["download_path"])
        for item in result
        if item.get("download_path")
    ]
    record_count = 0
    for record_count, obj in enumerate(iter_combined_csv_rows(csv_paths), start=1):
        payload = dict(obj)
        log(obj)
        # match = lot_lookup(payload)
        # payload["lot_match"] = match
        # log(f"json_record_{record_count}: {json.dumps(payload, default=str)}")

    if record_count == 0:
        log("main: No rows found across downloaded CSVs; nothing to log.")
    else:
        log(
            f"main: Finished logging {record_count} JSON record(s) aggregated across downloaded CSVs."
        )
