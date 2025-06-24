from .spark_session import get_spark_session

def read_csv_and_get_summary(file_path: str):
    spark = get_spark_session()
    df = spark.read.option("header", "true").csv(file_path)
    
    summary = df.describe().toPandas().to_dict(orient="records")
    return summary
