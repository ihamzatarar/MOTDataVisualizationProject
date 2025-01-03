from PyQt5.QtCore import Qt, QAbstractTableModel
import pandas as pd
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg
from matplotlib.backends.backend_template import FigureCanvas
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure



class PandasModel(QAbstractTableModel):
    """
    A custom table model to display Pandas DataFrames in QTableView.
    """

    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parent=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Vertical:
                return str(self._data.index[section])
        return None



# In gui/utils.py
def draw_figure(canvas, result, analysis_type, make, model):
    # ... your plotting code ...
    # Example:
    fig = Figure()
    axis = fig.add_subplot(111)
    if analysis_type == "age":
        x_values = list(result.keys())
        y_values = list(result.values())
        axis.plot(x_values, y_values)
        axis.set_xlabel("Age (Years)")
    else:  # analysis_type == "mileage"
        x_values = list(result.keys())
        y_values = list(result.values())
        axis.plot(x_values, y_values)
        axis.set_xlabel("Mileage")

    axis.set_ylabel("Pass Rate")
    axis.set_title(f"Pass Rate by {analysis_type.capitalize()} for {make} {model}")  # Use make and model in title
    canvas.figure = fig  # Set the figure on the canvas
    canvas.draw()

class MatplotlibCanvas(FigureCanvas):
    """
    A custom widget to display Matplotlib figures in PyQt5.
    """

    def __init__(self, parent=None, width=5, height=4, dpi=100):
        fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = fig.add_subplot(111)
        super(MatplotlibCanvas, self).__init__(fig)
