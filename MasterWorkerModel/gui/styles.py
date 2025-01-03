app_style_sheet=("""
                    QWidget {
                        background-color: #f5f5f5;
                        font-family: Arial, sans-serif;
                    }
                    QGroupBox {
                        border: 2px solid #ccc;
                        border-radius: 8px;
                        margin-top: 10px;
                        padding: 10px;
                        background-color: #fff;
                    }
                    QGroupBox::title {
                        subcontrol-origin: margin;
                        subcontrol-position: top center;
                        padding: 0 3px;
                        background-color: #fff;
                        border: 1px solid #ccc;
                        border-radius: 4px;
                        font-weight: bold;
                    }
                    QLabel {
                        font-size: 14px;
                        color: #333;
                    }
                    QLineEdit {
                        border: 1px solid #ccc;
                        border-radius: 4px;
                        padding: 6px;
                        background-color: #fff;
                        font-size: 14px;
                    }
                    QPushButton {
                        background-color: #4CAF50;
                        color: white;
                        border: none;
                        border-radius: 4px;
                        padding: 8px 16px;
                        font-size: 14px;
                        font-weight: bold;
                    }
                    QPushButton:hover {
                        background-color: #45a049;
                    }
                    QPushButton:pressed {
                        background-color: #367c39;
                    }
                    QPushButton:checked {
                        background-color: #367c39;
                        color: white;
                    }
                    QRadioButton {
                        font-size: 14px;
                    }
                    QTableView {
                        border: 1px solid #ccc;
                        background-color: #fff;
                        font-size: 14px;
                    }
                    QHeaderView::section {
                        background-color: #eee;
                        padding: 4px;
                        border: 1px solid #ccc;
                        font-weight: bold;
                    }
                    QDateEdit {
                        border: 1px solid #ccc;
                        border-radius: 4px;
                        padding: 6px;
                        font-size: 14px;
                        background-color: #fff;
                    }
                    QComboBox {
                        border: 1px solid #ccc;
                        border-radius: 4px;
                        padding: 6px;
                        font-size: 14px;
                        background-color: #fff;
                    }

                """)