# gui.py
from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                             QLineEdit, QPushButton, QTextEdit, QMessageBox, 
                             QGroupBox, QTabWidget, QScrollArea)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap, QFont
from downloader import DownloadWorker
from chart import ChartWorker

class ValueLineTab(QWidget):
    def __init__(self, parent_app):
        super().__init__()
        self.parent_app = parent_app
        layout = QVBoxLayout()
        
        self.btn_plot = QPushButton("📊 生成图表")
        self.btn_plot.clicked.connect(self.request_chart)
        layout.addWidget(self.btn_plot)
        
        self.status_label = QLabel("准备就绪")
        layout.addWidget(self.status_label)

        # 图片显示区
        self.scroll_area = QScrollArea()
        self.image_label = QLabel("图表显示区域")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scroll_area.setWidget(self.image_label)
        self.scroll_area.setWidgetResizable(True)
        layout.addWidget(self.scroll_area)
        
        self.setLayout(layout)

    def request_chart(self):
        self.parent_app.run_chart()

    def update_image(self, qimage):
        pixmap = QPixmap.fromImage(qimage)
        self.image_label.setPixmap(pixmap)
        self.image_label.adjustSize()

class SECApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Stock Analyzer Pro (Modular v5.0)')
        self.resize(1000, 750)
        self.setFont(QFont("Microsoft YaHei", 10))
        
        main_layout = QVBoxLayout()

        # --- 顶部设置栏 ---
        top_group = QGroupBox("全局设置")
        top_layout = QHBoxLayout()
        
        self.input_ticker = QLineEdit()
        self.input_ticker.setPlaceholderText("AAPL")
        # 自动大写
        self.input_ticker.textChanged.connect(lambda: self.input_ticker.setText(self.input_ticker.text().upper()))
        
        self.input_url = QLineEdit()
        self.input_url.setPlaceholderText("可选: SEC网址 (精准定位)")
        
        self.input_email = QLineEdit()
        self.input_email.setText("lianhdff@gmail.com") # 默认邮箱
        
        top_layout.addWidget(QLabel("代码:"))
        top_layout.addWidget(self.input_ticker)
        top_layout.addWidget(QLabel("网址:"))
        top_layout.addWidget(self.input_url)
        top_layout.addWidget(QLabel("邮箱:"))
        top_layout.addWidget(self.input_email)
        top_group.setLayout(top_layout)
        main_layout.addWidget(top_group)

        # --- 选项卡 ---
        self.tabs = QTabWidget()
        
        # Tab 1: 下载
        self.tab_dl = QWidget()
        dl_layout = QVBoxLayout()
        self.btn_dl = QPushButton("🚀 开始下载 (含历史归档)")
        self.btn_dl.clicked.connect(self.run_download)
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setStyleSheet("font-family: Consolas;")
        dl_layout.addWidget(self.btn_dl)
        dl_layout.addWidget(self.log_box)
        self.tab_dl.setLayout(dl_layout)
        
        # Tab 2: 图表
        self.tab_chart = ValueLineTab(self)
        
        self.tabs.addTab(self.tab_dl, "📥 财报下载")
        self.tabs.addTab(self.tab_chart, "📈 价格图表")
        main_layout.addWidget(self.tabs)
        self.setLayout(main_layout)

    def run_download(self):
        t = self.input_ticker.text().strip()
        e = self.input_email.text().strip()
        u = self.input_url.text().strip()
        
        if not t or "@" not in e:
            return QMessageBox.warning(self, "提示", "请填写代码和邮箱")
            
        self.btn_dl.setEnabled(False)
        self.log_box.clear()
        
        self.dl_worker = DownloadWorker(t, u, e)
        self.dl_worker.log_signal.connect(self.log_box.append)
        self.dl_worker.finished_signal.connect(lambda: self.btn_dl.setEnabled(True))
        self.dl_worker.start()

    def run_chart(self):
        t = self.input_ticker.text().strip()
        if not t: return QMessageBox.warning(self, "提示", "请填写代码")
        
        self.tab_chart.btn_plot.setEnabled(False)
        self.tab_chart.status_label.setText("正在后台生成图片...")
        
        self.chart_worker = ChartWorker(t)
        self.chart_worker.image_signal.connect(self.handle_chart_result)
        self.chart_worker.start()

    def handle_chart_result(self, qimage, err):
        self.tab_chart.btn_plot.setEnabled(True)
        if qimage:
            self.tab_chart.status_label.setText("图表生成成功")
            self.tab_chart.update_image(qimage)
        else:
            self.tab_chart.status_label.setText(f"错误: {err}")