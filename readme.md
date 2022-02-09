### Airflow ETL pipeline for gathering and storing data from Kaggle   
-----------------------------------------------------------

This pipelines is a good example of ETL pipeline using Airflow framework.  
Processing is done using pandas library, which is not a perfect choice for working with large datasets,
initial processing was made by *pySpark* which is a python library for *Spark Engine*, but for some reasons it was 
quite complicated for me to run Spark JVM localy, so I took in consideration the size of datasets and decided to change processing engine to 
pandas.

-----------------------------------

Pipeline gather data from three Kaggle datasets (suicide statistics, usage of internet statistics and countries GDP statistics), preprocess this data, 
and stores it into Postgres.

----------------------------------

#### Pipeline structure:

<img src="https://github.com/kinfi4/ELT-for-gathering-suicide-and-internet-usage-stats/blob/master/docs/screenshots/pipelines-structure.png?raw=true">



-------------------------------------
#### Project structure:
        - source
            -- dags (folder with all source files)
                -- common (folder for storing common logic)
                    --- callables.py (common callales using in pipeline)
                    --- processors.py (stores functions with pandas processing)
                    --- const.py (needed constants)
                    --- operators.py (stores custom opertors)
                -- dag_data_ETL.py (Airflow file with structure of pipeline)
                -- config.py (config file)
            -- data (folder for storing processed and processing data)
                -- data-lake
                -- processing-storage
                -- processed-data

-----------------------------------------------
@kinfi4
