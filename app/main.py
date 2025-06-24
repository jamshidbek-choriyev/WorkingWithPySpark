from fastapi import FastAPI
from app.data_processor import read_csv_and_get_summary

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Welcome to FastAPI + PySpark"}

@app.get("/summary")
def get_summary():
    file_path = "data/sample.csv"
    summary = read_csv_and_get_summary(file_path)
    return {"summary": summary}
