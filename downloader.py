 # downloader.py
import os
import time
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from filing_store import FilingStore

try:
    from html_to_md import convert_file as _convert_to_md
except Exception:  # markdownify not installed — conversion silently disabled
    _convert_to_md = None


def _make_markdown(html_path, log_func=None):
    """Best-effort: write a sibling .md for a downloaded .htm filing. Never raises."""
    if _convert_to_md is None:
        return
    if not html_path.lower().endswith((".htm", ".html")):
        return
    try:
        _convert_to_md(html_path)
    except Exception as e:
        if log_func:
            log_func(f"  ⚠️ Markdown 转换失败: {os.path.basename(html_path)}: {e}")


def _safe_filename(s, max_len=80):
    """Strip characters illegal in Windows filenames and trim length."""
    s = re.sub(r'[<>:"/\\|?*\r\n\t]', '', s or '').strip()
    s = re.sub(r'\s+', ' ', s)
    return s[:max_len].strip()


def _name_tag(ticker, company):
    """Build the '<TICKER>_<公司名>' prefix prepended to every saved file."""
    parts = [p for p in (_safe_filename(ticker, 30), _safe_filename(company, 60)) if p]
    return "_".join(parts)


class SmartSECDownloader:
    DEFAULT_FORMS = ['10-K', '10-Q', '20-F', '6-K']
    # 招股说明书 (IPO prospectus): S-1 family (domestic) + F-1 family (foreign issuers),
    # including their amendments. Exhibits/attachments are downloaded in full.
    IPO_FORMS = ['S-1', 'S-1/A', 'F-1', 'F-1/A']

    def __init__(self, email):
        self.headers = {"User-Agent": f"ResearchApp {email}", "Host": "www.sec.gov"}
        self.data_headers = {"User-Agent": f"ResearchApp {email}", "Host": "data.sec.gov"}

    def get_cik(self, ticker, manual_url=None):
        if manual_url:
            match = re.search(r"CIK=(\d+)", manual_url, re.IGNORECASE) or \
                    re.search(r"/data/(\d+)", manual_url, re.IGNORECASE)
            if match: return match.group(1).zfill(10)

        try:
            r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=self.headers, timeout=10)
            data = r.json()
            ticker = ticker.upper().strip()
            for entry in data.values():
                if entry['ticker'] == ticker: return str(entry['cik_str']).zfill(10)
            raise Exception("未找到该代码，请尝试提供 SEC 网址")
        except Exception as e:
            raise e

    def _get_exhibits(self, base_url, main_doc_content):
        exhibits = []
        soup = BeautifulSoup(main_doc_content, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            if re.search(r'ex\d+|exhibit|pressrelease|press-release', href, re.IGNORECASE):
                if href.endswith(('.htm', '.html', '.txt', '.pdf')):
                    full_url = requests.compat.urljoin(base_url, href)
                    exhibits.append(full_url)
        return list(set(exhibits))

    def _download_attachments(self, cik, acc, save_dir, prefix, primary_doc, log_func):
        """Download every document (exhibits / 附件) in an accession folder.

        Used for IPO prospectus (S-1/F-1) filings where the cover document is
        only part of the package — exhibits hold the substantive material.
        """
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/index.json"
        try:
            time.sleep(0.12)
            ri = requests.get(idx_url, headers=self.headers, timeout=15)
            if ri.status_code != 200:
                return 0
            items = ri.json().get("directory", {}).get("item", [])
        except Exception as e:
            log_func(f"  ⚠️ 附件索引获取失败: {e}")
            return 0

        prefix = prefix.replace("/", "-")
        count = 0
        for it in items:
            name = it.get("name", "")
            low = name.lower()
            if not low.endswith((".htm", ".html", ".txt", ".pdf")):
                continue
            if name == primary_doc or low.endswith("-index.htm"):
                continue
            ex_path = os.path.join(save_dir, f"{prefix}_EX_{name}")
            if os.path.exists(ex_path):
                continue
            ex_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{name}"
            time.sleep(0.12)
            try:
                er = requests.get(ex_url, headers=self.headers, timeout=20)
                if er.status_code == 200:
                    with open(ex_path, "wb") as f:
                        f.write(er.content)
                    count += 1
                    log_func(f"  🔗 附件: {name}")
            except Exception as e:
                log_func(f"  ❌ 附件失败 {name}: {e}")
        return count

    def _process_batch(self, filings, cik, ticker, log_func, form_filter=None,
                       company_name=""):
        allowed = self.DEFAULT_FORMS
        if form_filter:
            keywords = [k.strip().upper() for k in form_filter.split(',') if k.strip()]
            if keywords:
                allowed = keywords
        count = 0
        tag = _name_tag(ticker, company_name)
        base_dir = os.path.join(os.getcwd(), "SEC_Filings", ticker.upper())
        total = len(filings['accessionNumber'])
        for i in range(total):
            form = filings['form'][i]
            if form not in allowed: continue
            r_date = filings['reportDate'][i] or filings['filingDate'][i]
            f_date = filings['filingDate'][i] or r_date
            if r_date < "2005-01-01": continue

            save_dir = os.path.join(base_dir, form.replace("/", "-"))
            os.makedirs(save_dir, exist_ok=True)
            doc = filings['primaryDocument'][i]
            acc = filings['accessionNumber'][i].replace("-", "")

            fname = f"{tag}_{r_date}_{f_date}_{form}_{doc}".replace("/", "-")
            path = os.path.join(save_dir, fname)
            main_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}"

            if not os.path.exists(path):
                time.sleep(0.12)
                try:
                    rr = requests.get(main_url, headers=self.headers, timeout=15)
                    if rr.status_code == 200:
                        with open(path, "wb") as f: f.write(rr.content)
                        log_func(f"⬇️ 下载主文件: {fname}")
                        _make_markdown(path, log_func)
                        count += 1
                        if form in self.IPO_FORMS:
                            count += self._download_attachments(
                                cik, acc, save_dir, f"{tag}_{r_date}_{f_date}_{form}", doc, log_func)
                        if form == '6-K':
                            ex_urls = self._get_exhibits(main_url, rr.text)
                            for ex_url in ex_urls:
                                ex_name = ex_url.split('/')[-1]
                                ex_path = os.path.join(save_dir, f"{tag}_{r_date}_EX_{ex_name}")
                                if not os.path.exists(ex_path):
                                    time.sleep(0.12)
                                    ex_r = requests.get(ex_url, headers=self.headers, timeout=15)
                                    with open(ex_path, "wb") as f: f.write(ex_r.content)
                                    log_func(f"  🔗 附件已保存: {ex_name}")
                except Exception as e:
                    log_func(f"❌ 下载失败 {fname}: {e}")
        return count

    def download_all(self, cik, ticker, log_func, form_filter=None):
        api_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        log_func(f"📡 获取索引数据: {ticker} (CIK: {cik})...")
        r = requests.get(api_url, headers=self.data_headers)
        if r.status_code != 200: raise Exception(f"无法获取索引 (Code {r.status_code})")
        data = r.json()
        company_name = data.get("name", "")
        if company_name:
            log_func(f"🏢 公司: {company_name}")
        total_dl = self._process_batch(data['filings']['recent'], cik, ticker, log_func,
                                       form_filter, company_name)
        if 'files' in data['filings']:
            for h_file in data['filings']['files']:
                time.sleep(0.15)
                r_h = requests.get(f"https://data.sec.gov/submissions/{h_file['name']}", headers=self.data_headers)
                if r_h.status_code == 200:
                    total_dl += self._process_batch(r_h.json(), cik, ticker, log_func,
                                                    form_filter, company_name)
        return total_dl

    # ── Smart download (uses FilingStore) ─────────────────────────────
    def _collect_all_submission_batches(self, cik):
        """Fetch all submission batches from SEC (recent + historical)."""
        api_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(api_url, headers=self.data_headers, timeout=15)
        if r.status_code != 200:
            raise Exception(f"无法获取索引 (Code {r.status_code})")
        data = r.json()
        company_name = data.get("name", "")
        batches = [data["filings"]["recent"]]
        if "files" in data["filings"]:
            for h_file in data["filings"]["files"]:
                time.sleep(0.15)
                r_h = requests.get(
                    f"https://data.sec.gov/submissions/{h_file['name']}",
                    headers=self.data_headers, timeout=15,
                )
                if r_h.status_code == 200:
                    batches.append(r_h.json())
        return company_name, batches

    def smart_download_us(self, ticker, log_func, sec_url=None):
        """Download all 10-K/20-F + current-year 10-Q/6-K, with dedup.

        Returns: FilingStore instance (call .summary() for stats).
        """
        store = FilingStore(ticker)
        cik = self.get_cik(ticker, sec_url)
        store.cik = cik
        log_func(f"📡 CIK: {cik} | 正在扫描 SEC 索引...")

        company_name, batches = self._collect_all_submission_batches(cik)
        tag = _name_tag(ticker, company_name)
        if company_name:
            log_func(f"🏢 公司: {company_name}")

        # Phase 1: Register all filings into the index
        # SEC submissions are in reverse-chronological order, so we see newest first.
        total_registered = 0
        quarterly_count = 0
        MAX_QUARTERLY = 4  # only need the most recent 3-4 quarters
        for filings in batches:
            n = len(filings["accessionNumber"])
            for i in range(n):
                form = filings["form"][i]
                # We want: all annual (10-K, 20-F) + a few recent quarterly (10-Q, 6-K)
                # + all IPO prospectus (S-1/F-1 family, with attachments).
                is_annual = form in ("10-K", "20-F")
                is_quarterly = form in ("10-Q", "6-K")
                is_ipo = form in self.IPO_FORMS
                if not is_annual and not is_quarterly and not is_ipo:
                    continue

                r_date = filings["reportDate"][i] or filings["filingDate"][i]

                # For quarterly, keep only the most recent MAX_QUARTERLY filings
                if is_quarterly:
                    if quarterly_count >= MAX_QUARTERLY:
                        continue
                    quarterly_count += 1

                acc = filings["accessionNumber"][i]
                store.register_filing(
                    accession=acc,
                    form=form,
                    report_date=r_date,
                    filing_date=filings["filingDate"][i],
                    primary_doc=filings["primaryDocument"][i],
                )
                total_registered += 1

        log_func(f"📋 索引完成: {total_registered} 份报告已注册")

        # Phase 2: Download missing filings
        download_count = 0
        for entry in store.filings:
            if store.is_downloaded(entry["accession"]):
                continue

            form = entry["form"]
            r_date = entry["report_date"]
            f_date = entry.get("filing_date") or r_date
            doc = entry["primary_doc"]
            acc = entry["accession"].replace("-", "")

            form_dir = form.replace("/", "-")  # e.g. "S-1/A" -> "S-1-A"
            save_dir = os.path.join(store.store_dir, form_dir)
            os.makedirs(save_dir, exist_ok=True)
            fname = f"{tag}_{r_date}_{f_date}_{form}_{doc}".replace("/", "-")
            local_path = os.path.join(form_dir, fname)
            full_path = os.path.join(store.store_dir, local_path)

            if os.path.exists(full_path):
                store.mark_downloaded(entry["accession"], local_path)
                continue

            main_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}"
            time.sleep(0.12)
            try:
                rr = requests.get(main_url, headers=self.headers, timeout=15)
                if rr.status_code == 200:
                    with open(full_path, "wb") as f:
                        f.write(rr.content)
                    store.mark_downloaded(entry["accession"], local_path)
                    _make_markdown(full_path, log_func)
                    download_count += 1
                    log_func(f"⬇️ {form} {r_date}: {doc}")
                    if form in self.IPO_FORMS:
                        n_ex = self._download_attachments(
                            cik, acc, save_dir, f"{tag}_{r_date}_{f_date}_{form}", doc, log_func)
                        if n_ex:
                            log_func(f"  📎 {form} 招股说明书附件共 {n_ex} 份")
                else:
                    log_func(f"⚠️ {form} {r_date}: HTTP {rr.status_code}")
            except Exception as e:
                log_func(f"❌ {form} {r_date}: {e}")

        store.save()
        s = store.summary()
        ipo_dl = sum(1 for f in store.filings
                     if f["form"] in self.IPO_FORMS and f.get("downloaded"))
        log_func(
            f"🎉 完成! 年报 {s['annual_downloaded']}/{s['annual_total']} | "
            f"季报 {s['quarterly_downloaded']}/{s['quarterly_total']} | "
            f"招股说明书 {ipo_dl} 份 | "
            f"本次新下载 {download_count} 份"
        )
        return store

class CninfoDownloader:
    def __init__(self):
        self.search_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        self.suggest_url = "http://www.cninfo.com.cn/new/information/topSearch/query"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }

    def get_org_info(self, code):
        """Return (orgId, 公司简称) for a stock code, or (None, '')."""
        try:
            res = requests.post(self.suggest_url, data={'keyWord': code, 'maxNum': 5}, headers=self.headers)
            for s in res.json():
                if s['code'] == code:
                    return s['orgId'], s.get('zwjc', '')
            return None, ''
        except:
            return None, ''

    def get_org_id(self, code):
        return self.get_org_info(code)[0]

    def download_cn_reports(self, code, keyword, log_func):
        org_id, company_name = self.get_org_info(code)
        if not org_id:
            log_func(f"❌ 无法识别股票代码 {code}")
            return 0
        if company_name:
            log_func(f"🏢 公司: {company_name}")
        tag = _name_tag(code, company_name)

        base_dir = os.path.join(os.getcwd(), "CN_Filings", code)
        os.makedirs(base_dir, exist_ok=True)

        total = 0
        # Pass 1: 年报 (caller-provided keyword/category)
        total += self._search_and_download(code, org_id, keyword, base_dir, log_func, tag)
        # Pass 2: 招股说明书及其附件 (IPO prospectus + attachments)
        log_func("📑 开始检索招股说明书...")
        total += self._search_and_download(
            code, org_id, "招股", base_dir, log_func, tag, keep_all=True)
        return total

    def _search_and_download(self, code, org_id, keyword, base_dir, log_func, tag="",
                             keep_all=False):
        """Paginate 巨潮 full-text search for `keyword` and download every PDF hit.

        keep_all=True keeps every match (used for 招股说明书附录/附件 so attachments
        are not filtered out); otherwise 摘要/提示性公告 are skipped.
        """
        # 不设时间上限：从 A 股有史以来（沪市 1990 年开市）到今天
        end_date = datetime.now().strftime("%Y-%m-%d")
        se_date = f"1990-01-01 ~ {end_date}"

        # Default category set (annual/semi-annual/Q1/Q3) only when no keyword given
        if keyword:
            categories = ""
        else:
            categories = "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh;"

        page_num = 1
        total_dl_count = 0
        while True:
            log_func(f"📡 正在检索第 {page_num} 页 ({keyword or '年报'})...")
            payload = {
                "pageNum": page_num, "pageSize": 30, "column": "szse",
                "tabName": "fulltext", "stock": f"{code},{org_id}",
                "searchkey": keyword, "category": categories,
                "isStandardSearching": "true", "seDate": se_date
            }

            try:
                r = requests.post(self.search_url, data=payload, headers=self.headers, timeout=15)
                data = r.json()
                announcements = data.get('announcements', [])

                if not announcements:
                    log_func("🏁 已到达最后一页或无更多符合条件的报告。")
                    break

                for info in announcements:
                    title = info['announcementTitle'].replace("<em>", "").replace("</em>", "").replace("/", "-")
                    if not keep_all and ("摘要" in title or "提示性" in title):
                        continue

                    pdf_url = f"http://static.cninfo.com.cn/{info['adjunctUrl']}"
                    fname = f"{tag}_{title}" if tag else title
                    save_path = os.path.join(base_dir, f"{fname}.pdf")

                    if not os.path.exists(save_path):
                        log_func(f"  ⬇️ 下载: {fname}")
                        resp = requests.get(pdf_url, timeout=30)
                        with open(save_path, "wb") as f: f.write(resp.content)
                        total_dl_count += 1
                        time.sleep(0.5)

                if data.get('hasMore') == False: break
                page_num += 1
                time.sleep(1)

            except Exception as e:
                log_func(f"❌ 第 {page_num} 页请求失败: {e}")
                break

        return total_dl_count