import asyncio
import json
import os
from pathlib import Path
from traceback import format_exc

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

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


def log(message: str):
    print(f"[workflow] {message}")


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
    # Return value for external callers (if needed)
    output = [{"json": item} for item in result]
