# MR
MapReduce stuff is located in directory ``mr``.
To build and run the project launch the script ``makefile.sh`` using:
    
    cd mr
    sh makefile.sh <n_reducers> <combiners_on>

where pars:
 - n_reducers = number of reducers to be used
 - combiners_on = it enables the usage of combiners; 0 -> off; any other -> on 

The script will:
- Compile all src files
- Create the jar
- Delete old jar in HDFS (if already present)
- Put the new jar in HDFS
- Run all jobs
- Show res directory
- Take and display execution time

The expected intput path containing the dataset is ``dataset/vehicles.csv``.
Results are contained in directories ``out``, ``out2a``, ``out2b``, ``out3``.

# Spark
Spark stuff is located in directory ``spark``.
To run the project launch the script ``usedcars.sh`` using:
    
    cd spark
    sh usedcars.sh <n_executors> <cores_per_executor>

The script will:

- Remove out directory (if already existing)
- Run the spark jobs
- Show res directory

The expected intput path containing the dataset is ``dataset/vehicles.csv``.
Results are contained in directory ``spark-out``.

In the end, file ``usedcars.ipynb`` contains the same jobs saved as a project notebook.
