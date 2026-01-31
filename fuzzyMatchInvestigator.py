"""Run fuzzy lot lookups for a fixed batch of addresses with verbose logging.

Usage:
    python fuzzyMatchInvestigator.py

The script processes the first 50 workflow addresses five at a time, enforces a
10-second PostgreSQL statement timeout for every lookup, logs the outcome after
each address, and writes the aggregated results to MatchingSummary.csv.
"""

from __future__ import annotations

import csv
from pathlib import Path

from psycopg2.errors import QueryCanceled

import extract_hrefs


DB_CLIENT = extract_hrefs.DB_CLIENT
LOT_LOOKUP_LIMIT = extract_hrefs.LOT_LOOKUP_LIMIT


def query_lot_candidates_with_timeout(fragments):
    """Run the candidate query with a 10s statement timeout."""

    if not fragments:
        return []

    where_clause = " AND ".join(fragment for fragment, _ in fragments)
    sql = f"SELECT * FROM lots WHERE {where_clause} LIMIT %s"
    params = [value for _, value in fragments]
    params.append(LOT_LOOKUP_LIMIT)

    with DB_CLIENT.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout TO 10000")
            try:
                cur.execute(sql, params)
            except QueryCanceled as exc:
                raise SystemExit("lot candidate query canceled after 10 seconds") from exc
            if cur.description:
                return cur.fetchall()
            return []


# Monkey-patch extract_hrefs to enforce the timeout for this run only.
extract_hrefs.query_lot_candidates = query_lot_candidates_with_timeout
lot_lookup = extract_hrefs.lot_lookup

ADDRESSES_ADJ = [
    # TODO: FILL THIS IN WITH ALL
]

ADDRESSES: list[dict[str, str]] = [
    {"Address": "15181 116th Avenue N", "City": "Dayton"},
    {"Address": "15044 116th Avenue N", "City": "Dayton"},
    {"Address": "11649 Harbor Circle N", "City": "Dayton"},
    {"Address": "11664 Harbor Lane N", "City": "Dayton"},
    {"Address": "11532 Polaris Lane N", "City": "Dayton"},
    {"Address": "11675 Harbor Lane N", "City": "Dayton"},
    {"Address": "15177 116th Avenue N", "City": "Dayton"},
    {"Address": "11621 Minnesota Lane N", "City": "Dayton"},
    {"Address": "4687 Starflower Lane", "City": "Minnetrista"},
    {"Address": "7127 Morgan Avenue S", "City": "Richfield"},
    {"Address": "11657 Minnesota Lane N", "City": "Dayton"},
    {"Address": "11645 Harbor Circle N", "City": "Dayton"},
    {"Address": "2628 Ulysses Street NE", "City": "Minneapolis"},
    {"Address": "16105 37th Avenue N", "City": "Plymouth"},
    {"Address": "2130 Douglas Drive N", "City": "Golden Valley"},
    {"Address": "912 W 66th Street", "City": "Richfield"},
    {"Address": "14980 144th Avenue N", "City": "Dayton"},
    {"Address": "24235 Williams Road", "City": "Rogers"},
    {"Address": "5001 Kelsey Terrace", "City": "Edina"},
    {"Address": "10568 Harbor Lane N", "City": "Maple Grove"},
    {"Address": "2225 Old Post Road", "City": "Independence"},
    {"Address": "24225 Williams Road", "City": "Rogers"},
    {"Address": "2900 11th Avenue S #413", "City": "Minneapolis"},
    {"Address": "4114 Xenwood Avenue S", "City": "St Louis Park"},
    {"Address": "5616 Mahoney Avenue", "City": "Minnetonka"},
    {"Address": "2714 Girard Avenue N", "City": "Minneapolis"},
    {"Address": "4905 Dupont Avenue N", "City": "Minneapolis"},
    {"Address": "2770 Gale Road", "City": "Wayzata"},
    {"Address": "13354 73rd Place N", "City": "Maple Grove"},
    {"Address": "6040 Goldenrod Lane N", "City": "Plymouth"},
    {"Address": "2200 Nevada Avenue S #211", "City": "St Louis Park"},
    {"Address": "4520 Rosewood Lane N", "City": "Plymouth"},
    {"Address": "4046 Xerxes Avenue S", "City": "Minneapolis"},
    {"Address": "3637 Bloomington Avenue", "City": "Minneapolis"},
    {"Address": "4600 Boone Avenue N", "City": "New Hope"},
    {"Address": "2509 N 6th Street", "City": "Minneapolis"},
    {"Address": "6929 James Avenue S", "City": "Richfield"},
    {"Address": "22570 129th Place N", "City": "Rogers"},
    {"Address": "15630 26th Avenue N #B", "City": "Plymouth"},
    {"Address": "2508 11th Avenue S", "City": "Minneapolis"},
    {"Address": "6739 Yucca Lane N", "City": "Maple Grove"},
    {"Address": "5733 Scenic Heights Drive", "City": "Minnetonka"},
    {"Address": "10920 Oak Knoll Terrace N", "City": "Hopkins"},
    {"Address": "649 Madison Street NE", "City": "Minneapolis"},
    {"Address": "222 Ferndale Road S ##105", "City": "Wayzata"},
    {"Address": "2670 Commerce Boulevard #202", "City": "Mound"},
    {"Address": "3101 Tyler Street NE", "City": "Minneapolis"},
    {"Address": "8533 Rich Avenue S", "City": "Bloomington"},
    {"Address": "8401 32nd Avenue N", "City": "Crystal"},
    {"Address": "3036 15th Avenue S", "City": "Minneapolis"},
]

DOWNLOAD_DIR = Path("downloads")
COUNTY_ORDER = [
    "HENNEPIN",
    "DAKOTA",
    "RAMSEY",
    "WASHINGTON",
    "ANOKA",
    "SCOTT",
    "CARVER",
]


def resolve_csv_paths() -> list[Path]:
    csv_paths = list(DOWNLOAD_DIR.glob("AUTO_SEARCH_V1*_1_25_26.csv"))
    if not csv_paths:
        return []
    ordered: list[Path] = []
    remaining = {path.name: path for path in csv_paths}
    for county in COUNTY_ORDER:
        for suffix in list(remaining):
            if county in suffix:
                ordered.append(remaining.pop(suffix))
    ordered.extend(sorted(remaining.values(), key=lambda path: path.name))
    return ordered


def load_workflow_records() -> list[dict[str, str]]:
    csv_paths = resolve_csv_paths()
    if not csv_paths:
        return [dict(record) for record in ADDRESSES]
    records: list[dict[str, str]] = []
    for csv_path in csv_paths:
        with csv_path.open("r", newline="", encoding="utf-8-sig") as source_file:
            reader = csv.DictReader(source_file)
            for row in reader:
                if not row:
                    continue
                address = (row.get("Address") or "").strip()
                city = (row.get("City") or "").strip()
                if not address and not city:
                    continue
                records.append(dict(row))
    return records


def run_mode(mode: str, records: list[dict[str, str]]) -> list[tuple[str, str, float | None]]:
    batch_size = 5
    results: list[tuple[str, str, float | None]] = []
    total = len(records)
    for index, record in enumerate(records, start=1):
        address = (record.get("Address") or "").strip()
        city = (record.get("City") or "").strip()
        json_address = f"{address}, {city}" if address and city else address or city
        try:
            match = lot_lookup(record, mode=mode)
        except SystemExit as exc:
            print(f"[fuzzy] timeout after 10s on record {index}: {json_address}")
            print(f"[fuzzy] {exc}")
            match = None

        formatted = match.get("formatted_address") if match else ""
        score = match.get("match_score") if match else None
        results.append((json_address, formatted, score))

        label = formatted if formatted else "NO MATCH"
        score_str = f"{score:.4f}" if score is not None else "n/a"
        print(
            f"[fuzzy] {mode} processed {index}/{total}: {json_address} -> {label}"
            f" (score {score_str})"
        )

        if index % batch_size == 0:
            matched = sum(1 for _, formatted_addr, _ in results[-batch_size:] if formatted_addr)
            print(
                f"[fuzzy] {mode} batch summary {index - batch_size + 1}-{index}: matched {matched},"
                f" unmatched {batch_size - matched}"
            )

    results.sort(key=lambda item: item[2] if item[2] is not None else -1)
    return results


def main() -> None:
    records = load_workflow_records()
    modes = ["improved", "number_zip"]
    combined_results: list[tuple[str, str, str, float | None]] = []
    for mode in modes:
        results = run_mode(mode, records)
        for json_address, formatted, score in results:
            combined_results.append((mode, json_address, formatted, score))

    output_path = Path("MatchingSummary.csv")
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["mode", "json_address", "lots_table_match", "match_score"])
        for mode, json_address, formatted, score in combined_results:
            writer.writerow(
                [mode, json_address, formatted, "" if score is None else f"{score:.4f}"]
            )
    print(f"[fuzzy] wrote {len(combined_results)} rows to {output_path}")


if __name__ == "__main__":
    main()
