import asyncio
from pathlib import Path
from traceback import format_exc

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

URL = "https://portal.onehome.com/en-US/properties?token=eyJPU04iOiJOU1RBUiIsInR5cGUiOiIxIiwiY29udGFjdGlkIjo3OTMzNzI0LCJzZXRpZCI6IjgxNTExNCIsInNldGtleSI6IjgyOCIsImVtYWlsIjoidHdhZ25lcjU1QGdtYWlsLmNvbSIsInJlc291cmNlaWQiOjAsImFnZW50aWQiOjE4NDQ3MiwiaXNkZWx0YSI6ZmFsc2UsIlZpZXdNb2RlIjoiMSJ9&SMS=0"
DOWNLOAD_DIR = Path.cwd() / "downloads"
VIEW_TIMEOUT = 15_000
NAV_TIMEOUT = 60_000


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
    locator = page.locator("svg path[d^='M13.73']")
    await locator.first.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    try:
        await locator.first.click()
    except Exception:
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
    locator = page.locator("div.radio-mock[data-tooltip='View as List']")
    await locator.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    await locator.click()


async def export_to_csv(page, download_dir: Path):
    button = page.get_by_role("button", name="Export to CSV")
    await button.wait_for(state="visible", timeout=VIEW_TIMEOUT)
    download_dir.mkdir(parents=True, exist_ok=True)
    async with page.expect_download() as download_info:
        await button.click()
    download = await download_info.value
    target_path = download_dir / download.suggested_filename
    await download.save_as(target_path)
    return target_path


async def run_workflow():
    summary: dict = {
        "status": "pending",
        "download_path": None,
        "steps": [],
        "traceback": None,
    }
    track = make_step_tracker(summary)
    download_dir = DOWNLOAD_DIR
    download_dir.mkdir(parents=True, exist_ok=True)
    track("init", "ok", f"Download dir: {download_dir}")
    log(f"Using download directory {download_dir}")

    try:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()

            try:
                track("navigate", "started", "Navigating to property portal")
                log("Navigating to property portal...")
                await page.goto(URL, wait_until="networkidle", timeout=NAV_TIMEOUT)
                track("navigate", "ok")

                track("close_overlay", "started", "Closing intro overlay")
                log("Closing intro overlay...")
                await click_close_icon(page)
                track("close_overlay", "ok")

                track("view_mode", "started", "Switching to list view")
                log("Switching to list view...")
                await click_view_as_list(page)
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


if __name__ == "__main__":
    result = asyncio.run(run_workflow())
    # Return value for external callers (if needed)
    output = [{"json": result}]

