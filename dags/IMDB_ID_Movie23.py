# Airflow library
from airflow.decorators import dag, task
from datetime import datetime

# Astro SDK library
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

@dag(
    start_date= datetime(2023, 12, 20)
    ,schedule= "@daily"
    ,catchup= False
    ,tags= ['IMDB 2023 Indonesia Film']
)

def imdb_movie():
    load_data_to_snowflake = aql.load_file(
        task_id= "load_data_to_snowflake"
        ,input_file= File(
            path= "https://raw.githubusercontent.com/andriankharisma/imdb-scraping-selenium-idfilm2023/main/Dataset/imdb_id_movie_2023.csv"
        )
        ,output_table= Table(
            name= "IMDB ID Movie 2023"
            ,conn_id=  "snowflake"
        )
    )
    
    load_data_to_snowflake

imdb_movie()