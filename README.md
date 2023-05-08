# pyspark_assignment
pysprk assignment


docker-compose.yaml defines services and volumes for pyspark docker applications. 

Below are all mounted local paths to container 
./data = folder that stores incoming csv files that are loaded into delta table. Location of delta table is in /data/delta folder
./logs = custom logs are stored at this location
/events = this is where event logs (history) is stored. These logs are then pulled by history server
	
I have defined 3 services.
1. spark-job
	This service run the spark job via spar-submit command. spark-job-1.py python file under source directory containes the pyspark code which reads data from csv files stored at /data folder. Delta files are stored at /data/delta

2. spark-history-server
	Spark History server to keep the history of event logs for all applications submitted via spark-submit also submitted via notebook
	
3. jupyter
	jupyter notebook for any runtime analysis. Refer to example notebook under notebook folder. This will help to read the table entries once spark submit job is over
	

spark-job-1.py
This is the file where core pyspark data ingestion logic is stored. It reads all csv (one by one) from /data folder. Each csv files is ingested as one batch job and eacha batch job is assigned with unique job ID (UUID). Also associated timestamp for when csv data loaded is also stored in delta table. It is assumed that all incoming files are with same structure. It may have or may not have column header. Thus SCHEMA was defined and enforced while reading is csv file. If csv file has header then first row was ommited before ingestion.
