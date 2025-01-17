from fastapi import FastAPI
import pandas as pd
from sql import resp
app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}
@app.get("/query")
async def root(q):
    msg,sql_query = resp(q)
    return {"message" : msg , "query" : sql_query}
#input_query = input()
