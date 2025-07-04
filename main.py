from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum, max, min, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
import io
import json
from typing import Dict, List, Optional
import uvicorn

# FastAPI app yaratish
app = FastAPI(
    title="PySpark Data Processing API",
    description="PySpark yordamida ma'lumotlarni qayta ishlash API",
    version="1.0.0"
)

# Spark Session yaratish (global o'zgaruvchi)
spark = None

def init_spark():
    """Spark Session yaratish funksiyasi"""
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("FastAPI-PySpark-Integration") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    return spark

# Startup event - Spark Session yaratish
@app.on_event("startup")
async def startup_event():
    """Dastur ishga tushganda Spark Session yaratish"""
    init_spark()
    print("✅ Spark Session yaratildi!")

# Shutdown event - Spark Session yopish
@app.on_event("shutdown")
async def shutdown_event():
    """Dastur yopilganda Spark Session yopish"""
    global spark
    if spark:
        spark.stop()
        print("✅ Spark Session yopildi!")

# Health check endpoint
@app.get("/")
async def root():
    """API ishlab turganini tekshirish"""
    return {"message": "PySpark + FastAPI servisi ishlab turibdi!", "status": "OK"}

# Sample data yaratish endpoint
@app.post("/create-sample-data")
async def create_sample_data():
    """Namuna ma'lumotlari yaratish"""
    try:
        # Sample data schema
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
            StructField("city", StringType(), True) 
        ])
        
        # Sample ma'lumotlar
        sample_data = [
            (1, "Ali Karimov", 25, 15000.0, "IT", "Toshkent"),
            (2, "Dilorom Nazarova", 30, 18000.0, "HR", "Samarqand"),
            (3, "Bobur Umarov", 28, 16000.0, "IT", "Toshkent"),
            (4, "Nilufar Rahimova", 35, 22000.0, "Finance", "Buxoro"),
            (5, "Sardor Ahmedov", 32, 19000.0, "IT", "Toshkent"),
            (6, "Madina Tursunova", 27, 14000.0, "HR", "Samarqand"),
            (7, "Jasur Mirzaev", 29, 17000.0, "Marketing", "Toshkent"),
            (8, "Gulnora Usmonova", 31, 20000.0, "Finance", "Buxoro"),
            (9, "Otabek Salimov", 26, 15500.0, "IT", "Toshkent"),
            (10, "Feruza Xasanova", 33, 21000.0, "Marketing", "Samarqand")
        ]
        
        # DataFrame yaratish
        df = spark.createDataFrame(sample_data, schema)
        
        # Temporary view yaratish (SQL uchun)
        df.createOrReplaceTempView("employees")

        
        
        return {
            "message": "Sample data muvaffaqiyatli yaratildi!",
            "total_records": df.count(),
            "columns": df.columns,
            "sample_preview": df.limit(3).toPandas().to_dict('records')
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Ma'lumotlarni ko'rish endpoint
@app.get("/data/view")
async def view_data(limit: int = 10):
    """Ma'lumotlarni ko'rish"""
    try:
        # employees view mavjudligini tekshirish
        tables = spark.catalog.listTables()
        if not any(table.name == "employees" for table in tables):
            raise HTTPException(status_code=404, detail="Ma'lumotlar topilmadi. Avval /create-sample-data ni chaqiring")
        
        # Ma'lumotlarni olish
        df = spark.table("employees")
        result = df.limit(limit).toPandas().to_dict('records')
        
        return {
            "total_records": df.count(),
            "displayed_records": len(result),
            "data": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Statistika olish endpoint
@app.get("/data/statistics")
async def get_statistics():
    """Ma'lumotlar statistikasi"""
    try:
        # employees view mavjudligini tekshirish
        tables = spark.catalog.listTables()
        if not any(table.name == "employees" for table in tables):
            raise HTTPException(status_code=404, detail="Ma'lumotlar topilmadi")
        
        df = spark.table("employees")
        
        # Asosiy statistikalar
        total_count = df.count()
        avg_age = df.select(avg("age")).collect()[0][0]
        avg_salary = df.select(avg("salary")).collect()[0][0]
        max_salary = df.select(max("salary")).collect()[0][0]
        min_salary = df.select(min("salary")).collect()[0][0]
        
        # Bo'limlarga ko'ra statistika
        dept_stats = df.groupBy("department") \
            .agg(count("*").alias("count"),
                 avg("salary").alias("avg_salary"),
                 avg("age").alias("avg_age")) \
            .orderBy("count", ascending=False) \
            .toPandas().to_dict('records')
        
        # Shaharlarga ko'ra statistika
        city_stats = df.groupBy("city") \
            .agg(count("*").alias("count"),
                 avg("salary").alias("avg_salary")) \
            .orderBy("count", ascending=False) \
            .toPandas().to_dict('records')
        
        return {
            "general_statistics": {
                "total_employees": total_count,
                "average_age": round(avg_age, 2) if avg_age else 0,
                "average_salary": round(avg_salary, 2) if avg_salary else 0,
                "max_salary": max_salary,
                "min_salary": min_salary
            },
            "department_statistics": dept_stats,
            "city_statistics": city_stats
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Filtrlar bilan qidirish
@app.get("/data/filter")
async def filter_data(department: Optional[str] = None, city: Optional[str] = None, min_salary: Optional[float] = None, max_salary: Optional[float] = None, min_age: Optional[int] = None, max_age: Optional[int] = None):
    """Ma'lumotlarni filtrlash"""
    try:
        tables = spark.catalog.listTables()
        if not any(table.name == "employees" for table in tables):
            raise HTTPException(status_code=404, detail="Ma'lumotlar topilmadi")
        
        df = spark.table("employees")
        
        # Filtrlar qo'llash
        if department:
            df = df.filter(col("department") == department)
        if city:
            df = df.filter(col("city") == city)
        if min_salary:
            df = df.filter(col("salary") >= min_salary)
        if max_salary:
            df = df.filter(col("salary") <= max_salary)
        if min_age:
            df = df.filter(col("age") >= min_age)
        if max_age:
            df = df.filter(col("age") <= max_age)
        
        result = df.toPandas().to_dict('records')
        
        return {
            "filters_applied": {
                "department": department,
                "city": city,
                "salary_range": f"{min_salary or 'min'} - {max_salary or 'max'}",
                "age_range": f"{min_age or 'min'} - {max_age or 'max'}"
            },
            "total_matching_records": len(result),
            "data": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# SQL query bajarish
@app.post("/data/sql-query")
async def execute_sql_query(query_data: Dict[str, str]):
    """SQL query bajarish"""
    try:
        query = query_data.get("query")
        if not query:
            raise HTTPException(status_code=400, detail="Query parametri kerak")
        
        # Xavfsizlik uchun faqat SELECT querylarni ruxsat berish
        if not query.strip().upper().startswith("SELECT"):
            raise HTTPException(status_code=400, detail="Faqat SELECT querylar ruxsat etilgan")
        
        # Query bajarish
        result_df = spark.sql(query)
        result = result_df.toPandas().to_dict('records')
        
        return {
            "query": query,
            "result_count": len(result),
            "data": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Spark Session ma'lumotlari
@app.get("/spark/info")
async def spark_info():
    """Spark Session ma'lumotlari"""
    try:
        return {
            "spark_version": spark.version,
            "app_name": spark.sparkContext.appName,
            "master": spark.sparkContext.master,
            "spark_ui_url": spark.sparkContext.uiWebUrl,
            "default_parallelism": spark.sparkContext.defaultParallelism,
            "active_tables": [table.name for table in spark.catalog.listTables()]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Uvicorn server ishga tushirish
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )