# app.py
import streamlit as st
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from downloader import SmartSECDownloader, CninfoDownloader
# How to start: run anaconda prompt and execute: streamlit run app.py

# 页面基础配置
st.set_page_config(page_title="Stock Analyzer Pro", layout="wide")
st.title("📈 Stock Analyzer Pro (Value Investing v8.0)")

# 创建主选项卡，彻底分离美股与A股
tab_us, tab_cn = st.tabs(["🇺🇸 美股分析中心", "🇨🇳 A股分析中心"])

# --- 美股模块 ---
with tab_us:
    st.header("SEC 报告下载 & 长期价值图表")
    
    col1, col2 = st.columns(2)
    with col1:
        us_ticker = st.text_input("美股代码 (Ticker)", value="AAPL")
    with col2:
        sec_url = st.text_input("SEC URL (可选用于精准锁定)")
        
    action_col1, action_col2 = st.columns(2)
    
    # ==========================================
    # 动作 1：绘图与美化逻辑
    # ==========================================
    if action_col1.button("📊 生成专业价格图表", use_container_width=True):
        if not us_ticker:
            st.warning("请输入有效的 Ticker。")
        else:
            with st.spinner(f"正在获取 {us_ticker} 过去15年的日线数据..."):
                try:
                    stock = yf.Ticker(us_ticker.upper())
                    # 注意：为了计算 EMA10 和 EMA250，必须使用日线(1d)数据
                    df = stock.history(period="15y", interval="1d")
                    
                    if df is None or df.empty or len(df) < 250:
                        st.error("数据不足 (可能上市不足250天) 或 Yahoo Finance 未返回数据。")
                    else:
                        # 数据清洗
                        df.index = df.index.tz_localize(None)
                        df = df[df['Volume'] > 0].copy()
                        
                        # 计算指数移动平均线 (EMA)
                        df['EMA10'] = df['Close'].ewm(span=10, adjust=False).mean()
                        df['EMA250'] = df['Close'].ewm(span=250, adjust=False).mean()

                        # --- 图表美化 (Matplotlib) ---
                        plt.style.use('bmh') # 使用更加整洁、现代的内置绘图风格
                        # 设置画布比例，主图占 3，副图(成交量)占 1
                        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), dpi=150, sharex=True, gridspec_kw={'height_ratios': [3, 1]})
                        
                        # 主图：收盘价与 EMA
                        # 15年日线数据点密集，用半透明细线绘制收盘价，突出 EMA
                        ax1.plot(df.index, df['Close'], color='#2c3e50', lw=1.0, alpha=0.6, label='Close Price')
                        ax1.plot(df.index, df['EMA10'], color='#e74c3c', lw=1.0, alpha=0.9, label='EMA 10 (Short-term Momentum)')
                        ax1.plot(df.index, df['EMA250'], color='#8e44ad', lw=2.0, alpha=0.9, label='EMA 250 (Bull/Bear Line)')
                        
                        ax1.set_title(f"【{us_ticker.upper()}】 15-Year Trend & Exponential Moving Averages", fontsize=16, fontweight='bold', pad=15)
                        ax1.set_ylabel("Price (USD)", fontsize=12)
                        ax1.legend(loc='upper left', frameon=True, shadow=True, fontsize=10)
                        
                        # 副图：成交量
                        # 日线成交量使用面积图填充更美观
                        ax2.fill_between(df.index, df['Volume'], color='#7f8c8d', alpha=0.5)
                        ax2.set_ylabel("Volume", fontsize=12)
                        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
                        ax2.xaxis.set_major_locator(mdates.YearLocator(2))
                        
                        fig.tight_layout()
                        
                        # 在网页中渲染图表
                        st.pyplot(fig)
                        
                        # ==========================================
                        # 动态解读模块 (增强细节)
                        # ==========================================
                        latest_close = df['Close'].iloc[-1]
                        latest_ema10 = df['EMA10'].iloc[-1]
                        latest_ema250 = df['EMA250'].iloc[-1]
                        
                        # 简单判断当前状态
                        trend_term = "长期牛市 / 主升浪" if latest_close > latest_ema250 else "长期熊市 / 深度调整"
                        mom_term = "强劲" if latest_close > latest_ema10 else "疲软"
                        
                        st.markdown(f"""
                        ### 📝 图表指标深度解析
                        
                        **当前 {us_ticker.upper()} 最新收盘价：`${latest_close:.2f}`**
                        
                        * **🌑 收盘价走势 (灰色半透明细线)**：记录了该股票过去 15 年（约 3700 个交易日）的实际价格波动。使用半透明处理是为了避免密集的日线掩盖了核心的趋势线。
                        * **🔴 EMA 10 (红色细线 - 最新值 `${latest_ema10:.2f}`)**：10日指数移动平均线。它赋予了最近几天的价格更高的权重，对市场的短期动能非常敏感。
                            * *当前状态*：价格相对 EMA10 表现 **{mom_term}**。如果红线快速向上交叉紫线，通常被视为短线买入的动能信号。
                        * **🟣 EMA 250 (紫色粗线 - 最新值 `${latest_ema250:.2f}`)**：250日指数移动平均线，大致相当于一年的交易日总和，被称为长线投资的**“牛熊分界线”**。
                            * *当前状态*：目前价格位于 EMA250 之{"上" if latest_close > latest_ema250 else "下"}，从长线价值投资的角度来看，该股票当前处于 **{trend_term}** 状态。在历史走势中，每次价格回踩这根紫线且没有跌破，往往是非常好的长线建仓机会。
                        * **📊 下方灰色区域 (成交量 Volume)**：展示了历史资金的活跃度。如果在价格突破紫线（EMA250）时伴随着下方灰色面积的激增（放量突破），说明大资金正在进场，信号的可靠性极高。
                        """)
                        
                except Exception as e:
                    st.error(f"图表渲染出错: {e}")

    # ==========================================
    # 动作 2：美股下载逻辑 (保持上个版本的稳定代码)
    # ==========================================
    if action_col2.button("🚀 下载 SEC 报告", use_container_width=True):
        if not us_ticker:
            st.warning("请输入有效的 Ticker。")
        else:
            st.write("### 📥 下载日志")
            log_container = st.empty()
            log_data = [] 
            
            def sec_logger(msg):
                log_data.append(msg)
                log_container.text_area("实时日志", value="\n".join(log_data), height=300)

            with st.spinner("任务执行中，请勿关闭页面..."):
                try:
                    dl = SmartSECDownloader(email="lianhdff@gmail.com")
                    cik = dl.get_cik(us_ticker, sec_url)
                    sec_logger(f"✅ 锁定目标 CIK: {cik}")
                    count = dl.download_all(cik, us_ticker, sec_logger)
                    sec_logger(f"🎉 任务结束! 总计处理 {count} 份文件。")
                    st.success("SEC 报告下载完毕！")
                except Exception as e:
                    sec_logger(f"❌ 发生错误: {str(e)}")
                    st.error("下载中断。")

# --- A股模块 (保持稳定) ---
with tab_cn:
    st.header("巨潮资讯报告批量提取")
    
    col_cn1, col_cn2 = st.columns(2)
    with col_cn1:
        cn_code = st.text_input("A股代码 (6位)", value="")
    with col_cn2:
        cn_keyword = st.text_input("筛选关键字 (可选)")
        
    if st.button("🔍 批量下载 A股定期报告", use_container_width=True):
        if not cn_code:
            st.warning("请输入 A 股代码。")
        else:
            st.write("### 📥 下载日志")
            log_container_cn = st.empty()
            log_data_cn = []
            
            def cn_logger(msg):
                log_data_cn.append(msg)
                log_container_cn.text_area("实时日志", value="\n".join(log_data_cn), height=300)
                
            with st.spinner("正在检索巨潮资讯，请稍候..."):
                try:
                    dl_cn = CninfoDownloader()
                    count = dl_cn.download_cn_reports(cn_code, cn_keyword, cn_logger)
                    cn_logger(f"🎉 任务结束! 总计处理 {count} 份文件。")
                    st.success("A股报告下载完毕！")
                except Exception as e:
                    cn_logger(f"❌ 发生错误: {str(e)}")
                    st.error("下载中断。")