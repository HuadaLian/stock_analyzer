import os
import time
import requests
import re
from bs4 import BeautifulSoup
from PyQt6.QtCore import QThread, pyqtSignal

class SmartSECDownloader:
    def __init__(self, email):
        self.headers = {"User-Agent": f"ResearchApp {email}", "Host": "www.sec.gov"}
        self.data_headers = {"User-Agent": f"ResearchApp {email}", "Host": "data.sec.gov"}
        self.target_forms = ['10-K', '10-Q', '20-F', '6-K']

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
            # 针对 6-K 常见的附件关键词进行匹配
            if re.search(r'ex\d+|exhibit|pressrelease|press-release', href, re.IGNORECASE):
                if href.endswith(('.htm', '.html', '.txt', '.pdf')):
                    full_url = requests.compat.urljoin(base_url, href)
                    exhibits.append(full_url)
        return list(set(exhibits))

    def _process_batch(self, filings, cik, ticker, log_signal):
        count = 0
        base_dir = os.path.join(os.getcwd(), "SEC_Filings", ticker.upper())
        
        total = len(filings['accessionNumber'])
        for i in range(total):
            form = filings['form'][i]
            if form not in self.target_forms: continue

            r_date = filings['reportDate'][i] or filings['filingDate'][i]
            if r_date < "2005-01-01": continue

            save_dir = os.path.join(base_dir, form)
            os.makedirs(save_dir, exist_ok=True)

            doc = filings['primaryDocument'][i]
            acc_raw = filings['accessionNumber'][i]
            acc = acc_raw.replace("-", "")
            
            fname = f"{r_date}_{form}_{doc}"
            path = os.path.join(save_dir, fname)
            main_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}"

            if not os.path.exists(path):
                time.sleep(0.12)
                try:
                    rr = requests.get(main_url, headers=self.headers, timeout=15)
                    if rr.status_code == 200:
                        with open(path, "wb") as f: f.write(rr.content)
                        log_signal.emit(f"⬇️ 下载主文件: {fname}")
                        count += 1
                        
                        # 如果是 6-K，抓取里面的 Exhibit
                        if form == '6-K':
                            exhibit_urls = self._get_exhibits(main_url, rr.text)
                            for ex_url in exhibit_urls:
                                ex_name = ex_url.split('/')[-1]
                                ex_path = os.path.join(save_dir, f"{r_date}_EX_{ex_name}")
                                if not os.path.exists(ex_path):
                                    time.sleep(0.12)
                                    ex_r = requests.get(ex_url, headers=self.headers, timeout=15)
                                    with open(ex_path, "wb") as f: f.write(ex_r.content)
                                    log_signal.emit(f"  🔗 附件已保存: {ex_name}")
                except Exception as e:
                    log_signal.emit(f"❌ 下载失败 {fname}: {e}")
        return count

    def download_all(self, cik, ticker, log_signal):
        api_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        log_signal.emit(f"📡 获取索引数据: {ticker} (CIK: {cik})...")
        
        r = requests.get(api_url, headers=self.data_headers)
        if r.status_code != 200: raise Exception(f"无法获取索引 (Code {r.status_code})")
        
        data = r.json()
        total_dl = 0
        total_dl += self._process_batch(data['filings']['recent'], cik, ticker, log_signal)

        if 'files' in data['filings']:
            for h_file in data['filings']['files']:
                fname = h_file['name']
                time.sleep(0.15)
                h_url = f"https://data.sec.gov/submissions/{fname}"
                r_h = requests.get(h_url, headers=self.data_headers)
                if r_h.status_code == 200:
                    total_dl += self._process_batch(r_h.json(), cik, ticker, log_signal)
        return total_dl

# --- 必须保留这个类，否则 GUI 无法导入 ---
class DownloadWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, ticker, url, email):
        super().__init__()
        self.ticker = ticker
        self.url = url
        self.email = email

    def run(self):
        try:
            dl = SmartSECDownloader(self.email)
            cik = dl.get_cik(self.ticker, self.url)
            self.log_signal.emit(f"✅ 锁定目标 CIK: {cik}")
            
            count = dl.download_all(cik, self.ticker, self.log_signal)
            self.log_signal.emit(f"🎉 全部完成! 共下载 {count} 份文件。")
        except Exception as e:
            self.log_signal.emit(f"❌ 错误: {str(e)}")
        finally:
            self.finished_signal.emit()