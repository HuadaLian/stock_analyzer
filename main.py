import sys
from PyQt6.QtWidgets import QApplication, QWidget, QPushButton, QVBoxLayout, QLabel
from PyQt6.QtCore import QUrl
from PyQt6.QtGui import QDesktopServices, QFont

class SECApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        # 1. 设置窗口基本属性
        self.setWindowTitle('SEC 财报下载助手')
        self.resize(400, 200) # 宽400，高200

        # 2. 创建布局 (垂直布局)
        layout = QVBoxLayout()

        # 3. 添加一段说明文字
        label = QLabel('点击下方按钮访问 SEC 官网')
        label.setFont(QFont('Arial', 12))
        layout.addWidget(label)

        # 4. 创建按钮
        self.btn = QPushButton('打开 SEC 主页')
        self.btn.setFont(QFont('Arial', 10))
        self.btn.setFixedSize(200, 50) # 设置按钮大小
        
        # KEY STEP: 将按钮的 "点击信号" 连接到 "open_browser" 这个函数
        self.btn.clicked.connect(self.open_browser)
        
        # 将按钮添加到布局中
        layout.addWidget(self.btn)

        # 应用布局
        self.setLayout(layout)

    def open_browser(self):
        # 使用 PyQt 自带的桌面服务打开 URL，这会自动调用你的默认浏览器（Chrome/Edge等）
        url = QUrl("https://www.sec.gov")
        QDesktopServices.openUrl(url)

if __name__ == '__main__':
    # 每个 PyQt 程序都需要一个 QApplication 对象
    app = QApplication(sys.argv)
    
    # 创建并显示我们的窗口
    window = SECApp()
    window.show()
    
    # 进入程序主循环
    sys.exit(app.exec())