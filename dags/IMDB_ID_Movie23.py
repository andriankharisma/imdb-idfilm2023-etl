# Airflow library
from airflow.decorators import dag, task
from datetime import datetime
from airflow.models.baseoperator import chain

# Pandas
import pandas as pd

# Astro SDK library
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

@aql.transform
def clean_table(input_table : Table):
    return """
        SELECT
            "TITLE",
            
            TRY_CAST(SUBSTRING("RATING", 1, POSITION('(' IN "RATING") - 1) AS FLOAT) AS CLEANED_RATING,
            
            CASE
                WHEN POSITION('K' IN "RATING") > 0 THEN TRY_CAST(SUBSTRING("RATING", POSITION('(' IN "RATING") + 1, POSITION('K' IN "RATING") - POSITION('(' IN "RATING") - 1) AS FLOAT) * 1000
                ELSE TRY_CAST(SUBSTRING("RATING", POSITION('(' IN "RATING") + 1, POSITION(')' IN "RATING") - POSITION('(' IN "RATING") - 1) AS FLOAT)
            END AS CLEANED_VOTE_COUNT,
            
            CASE
                WHEN POSITION('h' IN "DURATION") > 0 AND POSITION('m' IN "DURATION") > 0 THEN
                    TRY_CAST(SUBSTRING("DURATION", 1, POSITION('h' IN "DURATION") - 1) AS INTEGER) * 60 + TRY_CAST(SUBSTRING("DURATION", POSITION('h' IN "DURATION") + 1, POSITION('m' IN "DURATION") - POSITION('h' IN "DURATION") - 1) AS INTEGER)
                WHEN POSITION('h' IN "DURATION") > 0 THEN
                    TRY_CAST(SUBSTRING("DURATION", 1, POSITION('h' IN "DURATION") - 1) AS INTEGER) * 60
                WHEN POSITION('m' IN "DURATION") > 0 THEN
                    TRY_CAST(SUBSTRING("DURATION", 1, POSITION('m' IN "DURATION") - 1) AS INTEGER)
            END AS CLEANED_DURATION
        
        FROM {{input_table}}
        
         WHERE "TITLE" NOT IN ('Operation Fortune: Ruse de Guerre','Tiger Stripes','Sweet Dreams','Dreaming & Dying','Anwar: The Untold Story','Kleinkinderen van de Oost');
    """


@aql.transform
def top_10_movies(input_table : Table):
    return """
        SELECT "TITLE", "CLEANED_VOTE_COUNT", "CLEANED_RATING"
        FROM {{input_table}}
        ORDER BY "CLEANED_VOTE_COUNT" DESC, "CLEANED_RATING" DESC
        LIMIT 10
    """

@aql.transform
def top_10_longest_movie(input_table : Table):
    return """
        SELECT "TITLE", "CLEANED_DURATION"
        FROM {{input_table}}
        ORDER BY "CLEANED_DURATION" DESC
        LIMIT 10
    """

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
            name= "IMDB_ID_Movie_2023"
            ,conn_id= "snowflake"
        )
    )
    
    cleaned_table = clean_table(
        input_table = Table(
            name= "IMDB_ID_Movie_2023"
            ,conn_id= "snowflake"
        )
        ,output_table = Table(
            name= "IMDB_ID_Movie_2023_Clean"
            ,conn_id= "snowflake"
        )
    )
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_cleaned_table(scan_name='check_cleaned_table', checks_subpath='tables', data_source='snowflake_db') :
        from include.soda.check_function import check
        
        return check(scan_name, checks_subpath, data_source)
    
    top_10_movies_table = top_10_movies(
        input_table = Table(
            name= "IMDB_ID_Movie_2023_Clean"
            ,conn_id= "snowflake"
        )
        ,output_table = Table(
            name= "Top_10_Movies_2023"
            ,conn_id = "snowflake"
        )
    )
    
    top_10_longest_movie_table = top_10_longest_movie(
        input_table = Table(
            name= "IMDB_ID_Movie_2023_Clean"
            ,conn_id= "snowflake"
        )
        ,output_table = Table(
            name= "Top_10_Longest_Movies_2023"
            ,conn_id = "snowflake"
        )
    )
    
    chain(load_data_to_snowflake, cleaned_table, check_cleaned_table(), top_10_movies_table, top_10_longest_movie_table)

imdb_movie()