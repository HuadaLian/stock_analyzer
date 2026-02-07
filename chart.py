# chart.py
import yfinance as yf
import matplotlib
matplotlib.use('Agg') # 关键：使用非交互式后端
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg
import matplotlib.dates as mdates
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QImage

class ChartWorker(QThread):
    # 信号传递：QImage (图片数据), str (错误信息)
    image_signal = pyqtSignal(object, str)
    
    def __init__(self, ticker):
        super().__init__()
        self.ticker = ticker

    def run(self):
        try:
            print(f"ChartWorker: 获取数据 {self.ticker}...")
            stock = yf.Ticker(self.ticker)
            # Value Line 看长期，取 10-15 年
            df = stock.history(period="15y", interval="1mo")
            
            if df.empty:
                self.image_signal.emit(None, "Yahoo Finance 未返回数据 (可能代码错误)")
                return

            # 数据清洗
            df = df[df['Volume'] > 0]
            # 模拟 Value Line 趋势线 (12月均线)
            df['ValueLine_Proxy'] = df['Close'].rolling(window=12).mean()

            print("ChartWorker: 开始绘图...")
            # 创建纯内存画布
            fig = Figure(figsize=(10, 6), dpi=100, facecolor='white')
            
            # 子图布局
            ax1 = fig.add_subplot(211)
            ax2 = fig.add_subplot(212, sharex=ax1)

            # 主图 (High-Low Bar)
            ax1.vlines(x=df.index, ymin=df['Low'], ymax=df['High'], color='black', lw=1.2, label='Range')
            ax1.scatter(df.index, df['Close'], s=15, c='black', marker='_', label='Close')
            ax1.plot(df.index, df['ValueLine_Proxy'], c='black', lw=1.0, label='Trend')
            
            ax1.set_title(f"{self.ticker.upper()} - Value Line Style Analysis", fontweight='bold')
            ax1.grid(True, alpha=0.3, linestyle='--')
            ax1.tick_params(labelbottom=False) # 隐藏上图X轴标签

            # 副图 (Volume)
            ax2.bar(df.index, df['Volume'], color='gray', alpha=0.5, width=20)
            ax2.set_ylabel("Volume")
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax2.xaxis.set_major_locator(mdates.YearLocator(2)) # 每2年一个刻度

            # 渲染为图片流
            canvas = FigureCanvasAgg(fig)
            canvas.draw()
            
            width, height = int(fig.get_figwidth() * fig.get_dpi()), int(fig.get_figheight() * fig.get_dpi())
            buffer = canvas.buffer_rgba()
            
            # 生成 Qt 图片对象 (深拷贝防止内存释放)
            img = QImage(buffer, width, height, QImage.Format.Format_RGBA8888).copy()
            
            self.image_signal.emit(img, "")
            
        except Exception as e:
            print(f"ChartWorker Error: {e}")
            self.image_signal.emit(None, str(e))