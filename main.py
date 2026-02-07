# main.py
import sys
from PyQt6.QtWidgets import QApplication
from gui import SECApp

# 确保在 Windows 下图标显示正常
try:
    import ctypes
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("my.stock.analyzer.1.0")
except:
    pass

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = SECApp()
    window.show()
    sys.exit(app.exec())