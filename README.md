# How to run the application

##### Prerequisites
##### You need an installed JVM in your environment for spark!!!


#### For mac/linux/windows wsl, you should choose one of option: 

- Run with default (date 2023-01): 
bash run_ingestion.sh

-  Run for one date, for example :
bash run_ingestion.sh 2023-02

-  Run for range of dates, for example:
bash run_ingestion.sh 2023-01 2023-10


#### Where see results:

- Ingested data appears in project folder:
"core/temp/data"

- Data after spark transformations appears in  project folder:
"result"


#### For run tests for spark functions separately

python -m unittest test/test.py