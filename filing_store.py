# filing_store.py
"""
Filing index & storage manager for SEC filings.

Storage layout:
    SEC_Filings/<TICKER>/
        index.json          ← master index (metadata for every known filing)
        10-K/               ← annual reports
        10-Q/               ← quarterly reports
        20-F/               ← foreign annual
        6-K/                ← foreign interim

index.json schema:
    {
        "ticker": "AAPL",
        "cik": "0000320193",
        "updated": "2026-04-11T...",
        "filings": [
            {
                "accession": "0000320193-24-...",
                "form": "10-K",
                "report_date": "2024-09-28",
                "filing_date": "2024-11-01",
                "primary_doc": "aapl-20240928.htm",
                "downloaded": true,
                "local_path": "10-K/2024-09-28_10-K_aapl-20240928.htm",
                "has_excerpt": true,
                "excerpt_tokens": 12345
            },
            ...
        ]
    }
"""

import json
import os
from datetime import datetime


class FilingStore:
    BASE_DIR = os.path.join(os.path.dirname(__file__), "SEC_Filings")

    def __init__(self, ticker: str):
        self.ticker = ticker.upper()
        self.store_dir = os.path.join(self.BASE_DIR, self.ticker)
        os.makedirs(self.store_dir, exist_ok=True)
        self._index_path = os.path.join(self.store_dir, "index.json")
        self._index = self._load_index()

    # ── Index I/O ────────────────────────────────────────────────────
    def _load_index(self) -> dict:
        if os.path.exists(self._index_path):
            with open(self._index_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            # Migrate old entries that predate excerpt tracking
            _repo_root = os.path.dirname(self.BASE_DIR)
            for entry in data.get("filings", []):
                if "has_excerpt" not in entry:
                    entry["has_excerpt"] = False
                    entry["excerpt_tokens"] = 0
                    lp = entry.get("local_path", "")
                    if lp:
                        cache_p = os.path.join(
                            _repo_root, "cache", "SEC_Filings", self.ticker,
                            os.path.splitext(lp)[0] + "_financial.txt",
                        )
                        if os.path.exists(cache_p):
                            entry["has_excerpt"] = True
            return data
        return {
            "ticker": self.ticker,
            "cik": None,
            "updated": None,
            "filings": [],
        }

    def save(self):
        self._index["updated"] = datetime.now().isoformat()
        with open(self._index_path, "w", encoding="utf-8") as fh:
            json.dump(self._index, fh, indent=2, ensure_ascii=False)

    @property
    def cik(self):
        return self._index.get("cik")

    @cik.setter
    def cik(self, value):
        self._index["cik"] = value

    @property
    def filings(self) -> list:
        return self._index["filings"]

    # ── Query helpers ────────────────────────────────────────────────
    def has_filing(self, accession: str) -> bool:
        return any(f["accession"] == accession for f in self.filings)

    def is_downloaded(self, accession: str) -> bool:
        for f in self.filings:
            if f["accession"] == accession and f.get("downloaded"):
                # Double-check file actually exists
                lp = f.get("local_path", "")
                return os.path.exists(os.path.join(self.store_dir, lp))
        return False

    def get_annual_filings(self) -> list:
        """Return all 10-K / 20-F filings, sorted by report_date desc."""
        annual = [f for f in self.filings if f["form"] in ("10-K", "20-F")]
        return sorted(annual, key=lambda x: x.get("report_date", ""), reverse=True)

    def get_quarterly_filings(self, year: int = None) -> list:
        """Return 10-Q / 6-K filings, optionally filtered by year."""
        qs = [f for f in self.filings if f["form"] in ("10-Q", "6-K")]
        if year:
            qs = [f for f in qs if f.get("report_date", "").startswith(str(year))]
        return sorted(qs, key=lambda x: x.get("report_date", ""), reverse=True)

    def count_downloaded(self, form_types=None) -> int:
        filings = self.filings
        if form_types:
            filings = [f for f in filings if f["form"] in form_types]
        return sum(1 for f in filings if f.get("downloaded"))

    # ── Registration ─────────────────────────────────────────────────
    def register_filing(self, accession, form, report_date, filing_date,
                        primary_doc, downloaded=False, local_path=""):
        """Add or update a filing entry."""
        for f in self.filings:
            if f["accession"] == accession:
                f["downloaded"] = downloaded
                f["local_path"] = local_path
                return
        self.filings.append({
            "accession": accession,
            "form": form,
            "report_date": report_date,
            "filing_date": filing_date,
            "primary_doc": primary_doc,
            "downloaded": downloaded,
            "local_path": local_path,
            "has_excerpt": False,
            "excerpt_tokens": 0,
        })

    def mark_downloaded(self, accession: str, local_path: str):
        for f in self.filings:
            if f["accession"] == accession:
                f["downloaded"] = True
                f["local_path"] = local_path
                return

    def needs_download(self, accession: str) -> bool:
        """True only when raw file absent AND excerpt not cached — safe to skip download."""
        for f in self.filings:
            if f["accession"] == accession:
                if f.get("has_excerpt"):
                    return False  # excerpt exists → raw file not needed
                lp = f.get("local_path", "")
                return not os.path.exists(os.path.join(self.store_dir, lp))
        return True

    # ── Excerpt tracking ─────────────────────────────────────────────
    def _excerpt_cache_path(self, local_path: str) -> str:
        """Compute the expected excerpt cache file path for a given local_path."""
        repo_root = os.path.dirname(self.BASE_DIR)
        return os.path.join(
            repo_root, "cache", "SEC_Filings", self.ticker,
            os.path.splitext(local_path)[0] + "_financial.txt",
        )

    def mark_excerpted(self, accession: str, tokens: int = 0):
        """Record that a filing's financial excerpt has been cached to disk."""
        for f in self.filings:
            if f["accession"] == accession:
                f["has_excerpt"] = True
                f["excerpt_tokens"] = tokens
                self.save()
                return

    def mark_excerpted_by_path(self, file_path: str, tokens: int = 0):
        """Look up filing by its local file path and mark as excerpted."""
        try:
            rel = os.path.relpath(file_path, self.store_dir)
        except ValueError:
            rel = file_path  # different drive on Windows — use as-is
        for f in self.filings:
            lp = f.get("local_path", "")
            if lp and (lp == rel or os.path.normpath(lp) == os.path.normpath(rel)):
                self.mark_excerpted(f["accession"], tokens)
                return

    def delete_raw_filings(self) -> int:
        """Delete raw files for all excerpted filings. Returns number deleted."""
        deleted = 0
        for f in self.filings:
            if not f.get("has_excerpt"):
                continue
            lp = f.get("local_path", "")
            if not lp:
                continue
            full_path = os.path.join(self.store_dir, lp)
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                    f["downloaded"] = False
                    deleted += 1
                except OSError:
                    pass
        if deleted:
            self.save()
        return deleted

    # ── Summary for UI ───────────────────────────────────────────────
    def summary(self) -> dict:
        """Quick stats: how many annual / quarterly downloaded."""
        return {
            "annual_total": len(self.get_annual_filings()),
            "annual_downloaded": self.count_downloaded(("10-K", "20-F")),
            "quarterly_total": len([f for f in self.filings if f["form"] in ("10-Q", "6-K")]),
            "quarterly_downloaded": self.count_downloaded(("10-Q", "6-K")),
        }


def get_sec_filings_total_size() -> int:
    """Return total disk usage of all files in SEC_Filings/, in bytes."""
    base = FilingStore.BASE_DIR
    if not os.path.exists(base):
        return 0
    total = 0
    for root, _, files in os.walk(base):
        for fname in files:
            try:
                total += os.path.getsize(os.path.join(root, fname))
            except OSError:
                pass
    return total
