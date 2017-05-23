#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
from PyQt4 import QtGui


class Example(QtGui.QWidget):
    
    def __init__(self):
        super(Example, self).__init__()
        
        self.initUI()
        
    def initUI(self):               
        self.lb=QtGui.QLabel(self)
        self.lb.setText('测试消息')
        
        self.btn=QtGui.QPushButton('按钮',self)
        self.btn.clicked.connect(self.btnclk)
        
        vbox = QtGui.QVBoxLayout()
        vbox.addWidget(self.lb)
        vbox.addWidget(self.btn)
        
        self.setLayout(vbox)
        
        self.setGeometry(300, 300, 250, 150)        
        self.setWindowTitle('......')    
        self.show()
        
    def btnclk(self):
        reply = QtGui.QMessageBox.warning(self, '警告','这是一个警告！', \
            QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)
        if reply == QtGui.QMessageBox.Yes:
            self.lb.setText('warning button/yes')
        elif reply == QtGui.QMessageBox.No:
            self.lb.setText('warning button/no')
        else:
            self.lb.setText('unknow?impossible!')
        
        
def main():
    
    app = QtGui.QApplication(sys.argv)
    ex = Example()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
   