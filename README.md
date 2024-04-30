# FOREX_DATABRICKS

###### In this project I worked with the API of https://openexchangerates.org/., which provides daily information about currency exchanges with respect to the dollar. The architecture starts from the API request using python and orchestrated from AIRFLOW. Every time the information is requested, it is saved in a Data Lake, activating a trigger that processes the data in pyspark to be stored in delta tables, creating 3 data lakes, bronze, silver and gold. Below I leave the architecture that I followed.

##### Architecture
![alt text](https://github.com/dariog721/FOREX_DATABRICKS/blob/main/Architecture/Arch.png)

##### Airflow 
![alt text](https://github.com/dariog721/FOREX_DATABRICKS/blob/main/Images/airflow/dags.png)

#### Pipelines
![alt text](https://github.com/dariog721/FOREX_DATABRICKS/blob/main/Images/Pipelines/Ingestion_forex_data.png)
![alt text](https://github.com/dariog721/FOREX_DATABRICKS/blob/main/Images/Pipelines/Exc1.png)
