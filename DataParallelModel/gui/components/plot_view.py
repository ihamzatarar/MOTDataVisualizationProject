from PyQt5.QtWidgets import QGroupBox, QVBoxLayout
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

class PlotGroup(QGroupBox):
    def __init__(self):
        super().__init__("Plot")

        layout = QVBoxLayout()
        self.plot_canvas = MatplotlibCanvas(self)
        layout.addWidget(self.plot_canvas)
        self.setLayout(layout)

class MatplotlibCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.figure = Figure(figsize=(width, height), dpi=dpi)
        super().__init__(self.figure)
