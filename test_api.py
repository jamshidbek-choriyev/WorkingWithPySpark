import requests
import json
import time

# API Base URL
BASE_URL = "http://localhost:8000"

def test_api():
    """API ni test qilish funksiyasi"""
    
    print("🔥 PySpark + FastAPI Test Boshlanmoqda...\n")
    
    # 1. Health Check
    print("1️⃣ Health Check...")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print("✅ API ishlab turibdi!")
            print(f"   Response: {response.json()}")
        else:
            print("❌ API ishlamayapti!")
            return
    except Exception as e:
        print(f"❌ Xatolik: {e}")
        return
    
    print("\n" + "="*50 + "\n")
    
    # 2. Sample data yaratish
    print("2️⃣ Sample data yaratish...")
    try:
        response = requests.post(f"{BASE_URL}/create-sample-data")
        if response.status_code == 200:
            data = response.json()
            print("✅ Sample data yaratildi!")
            print(f"   Jami yozuvlar: {data['total_records']}")
            print(f"   Ustunlar: {data['columns']}")
            print("   Namuna ma'lumotlar:")
            for record in data['sample_preview']:
                print(f"     {record}")
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # 3. Ma'lumotlarni ko'rish
    print("3️⃣ Ma'lumotlarni ko'rish...")
    try:
        response = requests.get(f"{BASE_URL}/data/view?limit=5")
        if response.status_code == 200:
            data = response.json()
            print("✅ Ma'lumotlar olinishi muvaffaqiyatli!")
            print(f"   Jami yozuvlar: {data['total_records']}")
            print(f"   Ko'rsatilgan yozuvlar: {data['displayed_records']}")
            print("   Ma'lumotlar:")
            for i, record in enumerate(data['data'], 1):
                print(f"     {i}. {record['name']} - {record['department']} - {record['salary']} so'm")
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # 4. Statistikalar
    print("4️⃣ Statistikalar olish...")
    try:
        response = requests.get(f"{BASE_URL}/data/statistics")
        if response.status_code == 200:
            data = response.json()
            print("✅ Statistikalar olinishi muvaffaqiyatli!")
            
            general = data['general_statistics']
            print("   📊 Umumiy statistikalar:")
            print(f"     • Jami hodimlar: {general['total_employees']}")
            print(f"     • O'rtacha yosh: {general['average_age']}")
            print(f"     • O'rtacha maosh: {general['average_salary']:,.0f} so'm")
            print(f"     • Eng yuqori maosh: {general['max_salary']:,.0f} so'm")
            print(f"     • Eng past maosh: {general['min_salary']:,.0f} so'm")
            
            print("\n   🏢 Bo'limlarga ko'ra:")
            for dept in data['department_statistics']:
                print(f"     • {dept['department']}: {dept['count']} kishi, o'rtacha maosh: {dept['avg_salary']:,.0f}")
            
            print("\n   🏙️ Shaharlarga ko'ra:")
            for city in data['city_statistics']:
                print(f"     • {city['city']}: {city['count']} kishi, o'rtacha maosh: {city['avg_salary']:,.0f}")
                
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # 5. Filtrlash
    print("5️⃣ Ma'lumotlarni filtrlash (IT bo'limi, maosh > 15000)...")
    try:
        params = {
            "department": "IT",
            "min_salary": 15000
        }
        response = requests.get(f"{BASE_URL}/data/filter", params=params)
        if response.status_code == 200:
            data = response.json()
            print("✅ Filtrlash muvaffaqiyatli!")
            print(f"   Topilgan yozuvlar: {data['total_matching_records']}")
            print("   Natijalar:")
            for record in data['data']:
                print(f"     • {record['name']} - {record['salary']:,.0f} so'm")
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # 6. SQL Query
    print("6️⃣ SQL Query bajarish...")
    try:
        sql_query = {
            "query": "SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC"
        }
        response = requests.post(f"{BASE_URL}/data/sql-query", json=sql_query)
        if response.status_code == 200:
            data = response.json()
            print("✅ SQL Query muvaffaqiyatli bajarildi!")
            print(f"   Query: {data['query']}")
            print(f"   Natijalar soni: {data['result_count']}")
            print("   Natijalar:")
            for record in data['data']:
                print(f"     • {record['department']}: {record['count']} kishi, o'rtacha: {record['avg_salary']:,.0f}")
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # 7. Spark ma'lumotlari
    print("7️⃣ Spark ma'lumotlari...")
    try:
        response = requests.get(f"{BASE_URL}/spark/info")
        if response.status_code == 200:
            data = response.json()
            print("✅ Spark ma'lumotlari olinishi muvaffaqiyatli!")
            print(f"   Spark versiyasi: {data['spark_version']}")
            print(f"   Dastur nomi: {data['app_name']}")
            print(f"   Master: {data['master']}")
            print(f"   Parallelism: {data['default_parallelism']}")
            print(f"   Faol jadvallar: {data['active_tables']}")
            if data['spark_ui_url']:
                print(f"   Spark UI: {data['spark_ui_url']}")
        else:
            print(f"❌ Xatolik: {response.text}")
    except Exception as e:
        print(f"❌ Xatolik: {e}")
    
    print("\n🎉 Test yakunlandi!")

if __name__ == "__main__":
    test_api()