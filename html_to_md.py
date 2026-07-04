# html_to_md.py
"""Convert downloaded SEC filings (HTML) into clean Markdown for AI tools.

SEC 10-K/10-Q/20-F/6-K documents are HTML with heavy inline-XBRL tagging
(`<ix:...>` wrappers, hidden fact blocks, per-cell <span style="..."> noise).
Raw `.htm` is hard for AI tools to read; this module strips the XBRL/styling
junk and renders the remaining structure — crucially the financial tables —
into GitHub-flavored Markdown.

Usage
-----
As a library:
    from html_to_md import convert_file, convert_html
    md_path = convert_file("SEC_Filings/AAPL/10-K/....htm")   # writes sibling .md
    md_text = convert_html(html_string)

As a CLI (batch-convert every downloaded filing):
    python html_to_md.py                 # all filings under SEC_Filings/
    python html_to_md.py AAPL PDD        # only these tickers
    python html_to_md.py --force         # re-convert even if .md is up to date
    python html_to_md.py --root HK_Filings
"""

import os
import re
import sys
import warnings

from bs4 import BeautifulSoup, Comment, XMLParsedAsHTMLWarning
from markdownify import MarkdownConverter

# SEC documents are XHTML; parsing with the HTML parser is intentional here.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Tags that hold no readable content but bloat / break conversion.
_DROP_TAGS = ("script", "style", "head", "meta", "link")
# Inline-XBRL "hidden" fact containers carry no visible text.
_HIDDEN_RE = re.compile(r"display\s*:\s*none", re.IGNORECASE)


class _FilingConverter(MarkdownConverter):
    """markdownify converter tuned for SEC filings."""

    def convert_table(self, el, text, parent_tags):
        # markdownify renders tables fine; just make sure we don't emit a
        # table that collapsed to nothing.
        md = super().convert_table(el, text, parent_tags)
        return md if md.strip().strip("|").strip() else ""


def _strip_xbrl(soup: BeautifulSoup) -> None:
    """Unwrap inline-XBRL tags and remove hidden/empty noise in place."""
    # Drop comments.
    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()

    # Remove non-content tags entirely.
    for tag in soup.find_all(_DROP_TAGS):
        tag.decompose()

    # Remove explicitly hidden elements (inline-XBRL hidden fact blocks etc.).
    for tag in soup.find_all(style=_HIDDEN_RE):
        tag.decompose()
    for tag in soup.find_all(attrs={"aria-hidden": "true"}):
        tag.decompose()

    # Inline-XBRL element tags use namespaced names like "ix:nonfraction".
    # Header/hidden/reference blocks carry no visible text → drop them;
    # value-bearing wrappers (ix:nonfraction, ix:nonnumeric) → keep text, drop tag.
    _drop_local = ("header", "hidden", "references", "resources")
    for tag in soup.find_all(lambda t: t.name and ":" in t.name):
        if tag.name.split(":", 1)[1] in _drop_local:
            tag.decompose()
        else:
            tag.unwrap()


def _split_row(line: str):
    """Split a Markdown table row '| a | b |' into ['a', 'b']."""
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _clean_md_tables(md: str) -> str:
    """Drop all-empty spacer columns that SEC's cell-per-glyph layout produces.

    SEC tables split each value across several <td> (e.g. '$' | '180,683' | '')
    plus blank alignment columns, yielding `| | | $ | 180,683 | |` rows. We parse
    each table, delete columns that are empty in every row, and rebuild it.
    """
    lines = md.split("\n")
    out = []
    i = 0
    n = len(lines)
    while i < n:
        # A table starts with a header row followed by a |---|---| separator.
        if (lines[i].lstrip().startswith("|") and i + 1 < n
                and re.match(r"^\s*\|[-: |]+\|\s*$", lines[i + 1])):
            start = i
            while i < n and lines[i].lstrip().startswith("|"):
                i += 1
            block = lines[start:i]
            rows = [_split_row(ln) for ln in block]
            sep_idx = 1
            ncol = max(len(r) for r in rows)
            rows = [r + [""] * (ncol - len(r)) for r in rows]
            # Keep a column if any non-separator row has content there.
            keep = [
                any(rows[r][c] for r in range(len(rows)) if r != sep_idx)
                for c in range(ncol)
            ]
            if not any(keep):
                continue
            for r, row in enumerate(rows):
                cells = [row[c] for c in range(ncol) if keep[c]]
                if r == sep_idx:
                    cells = ["---"] * len(cells)
                out.append("| " + " | ".join(cells) + " |")
        else:
            out.append(lines[i])
            i += 1
    return "\n".join(out)


def convert_html(html: str) -> str:
    """Convert a raw SEC filing HTML string to Markdown text."""
    soup = BeautifulSoup(html, "lxml")
    _strip_xbrl(soup)

    md = _FilingConverter(
        heading_style="ATX",        # use "# Heading" form
        strip=["a", "img"],          # links/images add noise, no value here
        bullets="-",
    ).convert_soup(soup)

    # Collapse runs of blank lines and trailing whitespace.
    md = re.sub(r"[ \t]+\n", "\n", md)
    md = re.sub(r"\n{3,}", "\n\n", md)
    # SEC tables often leave rows of empty cells (| | | |); drop them.
    md = re.sub(r"^\s*\|(?:\s*\|)+\s*$\n?", "", md, flags=re.MULTILINE)
    md = _clean_md_tables(md)
    return md.strip() + "\n"


def convert_file(html_path: str, md_path: str = None, force: bool = False) -> str:
    """Convert one .htm filing to a sibling .md file. Returns the .md path.

    Skips work if an up-to-date .md already exists (unless force=True).
    """
    if md_path is None:
        base, _ = os.path.splitext(html_path)
        md_path = base + ".md"

    if (not force and os.path.exists(md_path)
            and os.path.getmtime(md_path) >= os.path.getmtime(html_path)):
        return md_path

    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    md = convert_html(html)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    return md_path


def _iter_html_files(root: str, tickers=None):
    for dirpath, _dirs, files in os.walk(root):
        if tickers:
            # Top-level folder under root is the ticker/code.
            rel = os.path.relpath(dirpath, root).split(os.sep)[0].upper()
            if rel not in tickers:
                continue
        for name in files:
            if name.lower().endswith((".htm", ".html")):
                yield os.path.join(dirpath, name)


def batch_convert(root: str = "SEC_Filings", tickers=None, force: bool = False,
                  log=print) -> int:
    """Convert every HTML filing under `root` to Markdown. Returns count converted."""
    tickers = {t.upper() for t in tickers} if tickers else None
    converted = 0
    for html_path in _iter_html_files(root, tickers):
        try:
            base, _ = os.path.splitext(html_path)
            md_path = base + ".md"
            existed = os.path.exists(md_path)
            convert_file(html_path, md_path, force=force)
            if force or not existed or os.path.getmtime(md_path) >= os.path.getmtime(html_path):
                if not existed or force:
                    converted += 1
                    log(f"✅ {os.path.relpath(md_path, root)}")
        except Exception as e:
            log(f"❌ {os.path.relpath(html_path, root)}: {e}")
    return converted


if __name__ == "__main__":
    args = sys.argv[1:]
    force = "--force" in args
    args = [a for a in args if a != "--force"]
    root = "SEC_Filings"
    if "--root" in args:
        i = args.index("--root")
        root = args[i + 1]
        del args[i:i + 2]
    tickers = args or None
    n = batch_convert(root=root, tickers=tickers, force=force)
    print(f"\n🎉 转换完成：{n} 个文件已生成 Markdown。")
