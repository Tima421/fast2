import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, insert, select,text,Integer
from dotenv import load_dotenv
load_dotenv()
# Create an in-memory SQLite database
engine = create_engine("sqlite:///:memory:")
metadata_obj = MetaData()

# Define the table structure
table_name = "DATA_INFO"
DATA_INFO_table = Table(
    table_name,
    metadata_obj,
    Column("primary_key_val", String(1000), primary_key=True),
    Column("data_source", String(16), nullable=False),
    Column("sector", String(16), nullable=False),
    Column("region", String(16), nullable=False),
    Column("country_category", String(100), nullable=False),
    
    Column("country", String(40), nullable=False),
    Column("category", String(16), nullable=False),
    Column("freq", String(16), nullable=False),
    Column("latest_period", String(16), nullable=False),
    Column("next_period", String(16), nullable=False),
    Column("last_data_recieved_date", DateTime, nullable=False),
    Column("next_data_receiving_date", DateTime, nullable=False),
    Column("is_delayed", Integer, nullable=False),
    Column("database_refresh_date", DateTime, nullable=False),
    Column("latest_common_sector_period", String(16), nullable=False),
    Column("latest_available_local_period", String(16), nullable=False),
    Column("latest_global_period", String(16), nullable=False),
    Column("comments", String(5000), nullable=False)
)

# Create the table in the database
metadata_obj.create_all(engine)

# Define the path and read the Excel file
#path = r"C:\Users\80341013\OneDrive - Pepsico\Desktop\sqlite"
df = pd.read_excel('SQL_DI.xlsx')

rows_list=[]
for index,row in df.iterrows():
    r=row.to_dict()
    rows_list.append(r)
for row in rows_list:
    stmt = insert(DATA_INFO_table).values(**row)
    with engine.begin() as connection:
        cursor = connection.execute(stmt)
        
import os
import openai
import pandas as pd
from llama_index.core import SQLDatabase
from llama_index.llms.openai import OpenAI


openai.api_key = os.getenv("OPENAI_API_KEY")

llm = OpenAI(temperature=0.1, model="gpt-3.5-turbo")
sql_database = SQLDatabase(engine, include_tables=["DATA_INFO"])    
    
from llama_index.core.indices.struct_store.sql_query import (
    SQLTableRetrieverQueryEngine,
)
from llama_index.core.objects import (
    SQLTableNodeMapping,
    ObjectIndex,
    SQLTableSchema,
)
from llama_index.core import VectorStoreIndex

# manually set context text
DATA_INFO_text = (
    "we have a database where we mantain information about the country,category and data source among other informations."
    " A data source like(Nielsen,Shopper) along with country and Category creates a unique record."
    "Each record includes the following details:\n"
    "- **primary_key_val**: A unique identifier for each record. it is a combination of datasource-country-category\n"
    "- **data_source**: The source of the data (e.g., Nielsen, Shopper).\n"
    "- **sector**: sector is a geographic division where we operate. it is further subdivided into region aand countries.\n"
    "- **region**: A region is a subdivision of sector.It is also called as Clusters. It amy or may not be same as sector. It is further divided into countries  .\n"
    "- **country_category**: it is combined field containing Country and category. For every data source it will be be unique.\n"
    "- **country**: The specific country (e.g., France, Germany).\n"
    "- **category**: The category of the data (e.g., Beverages, Snacks).\n"
    "- **freq**: This is defined for eached Datasource,country and category combination. it can be of type POR(444w,544w,445w) or monthly,quartely.\n"
    "- **latest_period**: The latest period for which data is available (e.g., P11, P10). P9 is also read as Period 9\n"
    "- **next_period**: The next period for which data is expected (e.g., P12, P11).\n"
    "- **last_data_recieved_date**: The date when the last data was received (e.g., 12/28/2024). for eg p9 was for country category was recieved on last recieved date.\n"
    "- **next_data_receiving_date**: The date when the next data is expected to be received (e.g., 01/28/2025).for eg p9 was for country category was recieved on last recieved date. Always refrence primary key value,while returning answers. \n"
    "- **is_delayed**: Indicates if the data is delayed. If date today is greater than next_recieving date that measn that the data has been delayed. 1 INDICATES THAT DATA IS DELAYED\n"
    "- **database_refresh_date**: The date when the database was last refreshed (e.g., 01/16/2025).\n"
    "- **latest_common_sector_period**: The latest common sector period (e.g., P9).\n"
    "- **latest_available_local_period**: The latest available local period (e.g., P11).\n"
    "- **latest_global_period**: The latest global period (e.g., P9).\n"
    "- **comments**: Any comments related to the data (e.g., DATA RECEIVED AS EXPECTED, THIS IS THE REASON).\n"
)



# set Logging to DEBUG for more detailed outputs
table_node_mapping = SQLTableNodeMapping(sql_database)
table_schema_objs = [
    (SQLTableSchema(table_name="DATA_INFO", context_str=DATA_INFO_text))
]
obj_index = ObjectIndex.from_objects(
    table_schema_objs,
    table_node_mapping,
    VectorStoreIndex,
)
query_engine = SQLTableRetrieverQueryEngine(
    sql_database, obj_index.as_retriever(similarity_top_k=1)
)

def resp(query):
    response = query_engine.query(query)
    print(response.response)
    print("--------------------------------------")
    print(response.metadata['sql_query'])
    return(response.response , response.metadata['sql_query'])
#query_str = "list all country  available for  Shopper and Beverages??"
#resp(query_str)