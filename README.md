# MOTDataVisualizationProject
Place "test_result_2022" in database dir.
Then run app.py for either of approach from MOTDataVisualizationProject" because relative paths are used

# Approach 1
1. mpiexec -n 4 python DataParallelModel/app.py
2. mpiexec -n 4 python -m scalene DataParallelModel/app.py

# Approach 2

1. mpiexec -n 4 python MasterWorkerModel/app.py
2. mpiexec -n 4 python -m scalene MasterWorkerModel/app.py