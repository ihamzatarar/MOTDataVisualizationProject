# MOTDataVisualizationProject

## Overview
MOTDataVisualizationProject is a Python-based application designed to load, clean, and visualize data related to vehicle testing. The project leverages MPI for parallel processing and includes a GUI for data interaction.

## Project Structure
- `DataParallelModel/`: Contains the main application code for the data parallel approach.
- `MasterWorkerModel/`: Contains the main application code for the master-worker approach.
- `database/`: Directory where data files are stored.
- `data/modules/`: Contains modules for data cleaning and loading.
- `gui/`: Contains the GUI code.

## Prerequisites
- Python 3.x
- `mpi4py` for MPI support
- `pandas` for data manipulation
- `scalene` for profiling (optional)

## Installation
1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/MOTDataVisualizationProject.git
    cd MOTDataVisualizationProject
    ```

2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

## Data Preparation
Place the `test_result_2022` directory in the `database` directory. Ensure that the data files are correctly formatted and accessible.

## Running the Application

### Approach 1: Data Parallel Model
1. Run the application using MPI:
    ```sh
    mpiexec -n 4 python DataParallelModel/app.py
    ```

2. Optionally, run the application with Scalene for profiling:
    ```sh
    mpiexec -n 4 python -m scalene DataParallelModel/app.py
    ```

### Approach 2: Master-Worker Model
1. Run the application using MPI:
    ```sh
    mpiexec -n 4 python MasterWorkerModel/app.py
    ```

2. Optionally, run the application with Scalene for profiling:
    ```sh
    mpiexec -n 4 python -m scalene MasterWorkerModel/app.py
    ```

## Code Explanation

### `DataParallelModel/app.py`
This script initializes the data cleaner and loader, distributes the data loading tasks across multiple processes using MPI, and starts the GUI.

### `MasterWorkerModel/app.py`
This script follows a master-worker approach to distribute tasks among multiple processes using MPI and then starts the GUI.

## GUI
The GUI is initialized in the `gui/gui_main.py` file and is responsible for providing an interactive interface for data visualization and analysis.

## Contributing
1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

## License
This project is licensed under the MIT License. See the `LICENSE` file for more details.

## Contact
For any questions or issues, please open an issue on GitHub or contact the project maintainer at your.email@example.com.
