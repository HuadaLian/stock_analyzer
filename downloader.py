 # downloader.py
import os
import time
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from filing_store import FilingStore

class SmartSECDownloader:
    DEFAULT_FORMS = ['10-K', '10-Q', '20-F', '6-K']

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

    def _process_batch(self, filings, cik, ticker, log_func, form_filter=None):
        allowed = self.DEFAULT_FORMS
        if form_filter:
            keywords = [k.strip().upper() for k in form_filter.split(',') if k.strip()]
            if keywords:
                allowed = keywords
        count = 0
        base_dir = os.path.join(os.getcwd(), "SEC_Filings", ticker.upper())
        total = len(filings['accessionNumber'])
        for i in range(total):
            form = filings['form'][i]
            if form not in allowed: continue
            r_date = filings['reportDate'][i] or filings['filingDate'][i]
            f_date = filings['filingDate'][i] or r_date
            if r_date < "2005-01-01": continue

            save_dir = os.path.join(base_dir, form)
            os.makedirs(save_dir, exist_ok=True)
            doc = filings['primaryDocument'][i]
            acc = filings['accessionNumber'][i].replace("-", "")
            
            fname = f"{r_date}_{f_date}_{form}_{doc}"
            path = os.path.join(save_dir, fname)
            main_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}"

            if not os.path.exists(path):
                time.sleep(0.12)
                try:
                    rr = requests.get(main_url, headers=self.headers, timeout=15)
                    if rr.status_code == 200:
                        with open(path, "wb") as f: f.write(rr.content)
                        log_func(f"⬇️ 下载主文件: {fname}")
                        count += 1
                        if form == '6-K':
                            ex_urls = self._get_exhibits(main_url, rr.text)
                            for ex_url in ex_urls:
                                ex_name = ex_url.split('/')[-1]
                                ex_path = os.path.join(save_dir, f"{r_date}_EX_{ex_name}")
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
        total_dl = self._process_batch(data['filings']['recent'], cik, ticker, log_func, form_filter)
        if 'files' in data['filings']:
            for h_file in data['filings']['files']:
                time.sleep(0.15)
                r_h = requests.get(f"https://data.sec.gov/submissions/{h_file['name']}", headers=self.data_headers)
                if r_h.status_code == 200:
                    total_dl += self._process_batch(r_h.json(), cik, ticker, log_func, form_filter)
        return total_dl

    # ── Smart download (uses FilingStore) ─────────────────────────────
    def _collect_all_submission_batches(self, cik):
        """Fetch all submission batches from SEC (recent + historical)."""
        api_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(api_url, headers=self.data_headers, timeout=15)
        if r.status_code != 200:
            raise Exception(f"无法获取索引 (Code {r.status_code})")
        data = r.json()
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
        return batches

    def smart_download_us(self, ticker, log_func, sec_url=None):
        """Download all 10-K/20-F + current-year 10-Q/6-K, with dedup.

        Returns: FilingStore instance (call .summary() for stats).
        """
        store = FilingStore(ticker)
        cik = self.get_cik(ticker, sec_url)
        store.cik = cik
        log_func(f"📡 CIK: {cik} | 正在扫描 SEC 索引...")

        current_year = datetime.now().year
        batches = self._collect_all_submission_batches(cik)

        # Phase 1: Register all filings into the index
        total_registered = 0
        for filings in batches:
            n = len(filings["accessionNumber"])
            for i in range(n):
                form = filings["form"][i]
                # We want: all annual (10-K, 20-F) + current-year quarterly (10-Q, 6-K)
                is_annual = form in ("10-K", "20-F")
                is_quarterly = form in ("10-Q", "6-K")
                if not is_annual and not is_quarterly:
                    continue

                r_date = filings["reportDate"][i] or filings["filingDate"][i]
                if r_date < "2005-01-01":
                    continue

                # For quarterly, only current year and previous year
                if is_quarterly:
                    try:
                        yr = int(r_date[:4])
                    except ValueError:
                        continue
                    if yr < current_year - 1:
                        continue

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

            save_dir = os.path.join(store.store_dir, form)
            os.makedirs(save_dir, exist_ok=True)
            fname = f"{r_date}_{f_date}_{form}_{doc}"
            local_path = os.path.join(form, fname)
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
                    download_count += 1
                    log_func(f"⬇️ {form} {r_date}: {doc}")
                else:
                    log_func(f"⚠️ {form} {r_date}: HTTP {rr.status_code}")
            except Exception as e:
                log_func(f"❌ {form} {r_date}: {e}")

        store.save()
        s = store.summary()
        log_func(
            f"🎉 完成! 年报 {s['annual_downloaded']}/{s['annual_total']} | "
            f"季报 {s['quarterly_downloaded']}/{s['quarterly_total']} | "
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

    def get_org_id(self, code):
        try:
            res = requests.post(self.suggest_url, data={'keyWord': code, 'maxNum': 5}, headers=self.headers)
            suggestions = res.json()
            for s in suggestions:
                if s['code'] == code: return s['orgId']
            return None
        except:
            return None

    def download_cn_reports(self, code, keyword, log_func):
        org_id = self.get_org_id(code)
        if not org_id:
            log_func(f"❌ 无法识别股票代码 {code}")
            return 0

        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
        se_date = f"{start_date} ~ {end_date}"

        # Default: annual/semi-annual/Q1/Q3 reports; override with keyword if provided
        if keyword:
            categories = ""
        else:
            categories = "category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh;"

        page_num = 1
        total_dl_count = 0
        base_dir = os.path.join(os.getcwd(), "CN_Filings", code)
        os.makedirs(base_dir, exist_ok=True)

        while True:
            log_func(f"📡 正在检索第 {page_num} 页报告...")
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
                    if "摘要" in title or "提示性" in title: continue 
                    
                    pdf_url = f"http://static.cninfo.com.cn/{info['adjunctUrl']}"
                    save_path = os.path.join(base_dir, f"{title}.pdf")
                    
                    if not os.path.exists(save_path):
                        log_func(f"  ⬇️ 下载: {title}")
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