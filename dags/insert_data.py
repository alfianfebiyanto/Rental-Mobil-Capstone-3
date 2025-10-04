import psycopg2
import logging
from src.generate_data import (generate_cars, generate_customers, generate_payments, generate_rentals)


# setting connection
def db_conn():
    return psycopg2.connect(
        host="localhost",
        port="5400",
        database="rental_mobil_db",
        user="admin_de",
        password="admin_password"
    )

def create_tables():
    conn = db_conn()
    tables = [
        """
        CREATE TABLE IF NOT EXISTS customers (
            id VARCHAR(10) PRIMARY KEY,
            first_name VARCHAR(25),
            last_name VARCHAR(25),
            age INT,
            phone TEXT,
            address TEXT,
            email TEXT,
            created_at TIMESTAMP);
        """, 
        """
        CREATE TABLE IF NOT EXISTS cars (
            id VARCHAR(10) PRIMARY KEY,
            brand TEXT,
            model TEXT,
            color TEXT,
            year INT,
            created_at TIMESTAMP);
        """,
        """
        CREATE TABLE IF NOT EXISTS rentals (
            id VARCHAR(10) PRIMARY KEY,
            customer_id VARCHAR(10),
            car_id VARCHAR(10),
            rental_start DATE,
            rental_end_plan DATE,
            days INT,
            price NUMERIC,
            created_at TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers (id),
            FOREIGN KEY (car_id) REFERENCES cars (id));
        """,
         """
        CREATE TABLE IF NOT EXISTS payments (
            id VARCHAR(10) PRIMARY KEY,
            rental_id VARCHAR(10),
            days INT,
            price NUMERIC,
            method TEXT,
            created_at TIMESTAMP,
            FOREIGN KEY (rental_id) REFERENCES rentals (id));
        """     
    ]
    try:
       with conn.cursor() as cur:
            for table in tables:
               cur.execute(table)
            conn.commit()
            print('semua tabel sudah dibuat')
    except Exception as e:
        conn.rollback()  # Rollback kalau ada error
        print(f"❌ Error saat membuat tabel: {e}")
        raise  # Re-raise error supaya caller tahu ada masalah
    finally:
        conn.close()
        
        
def insert_data():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            customers = generate_customers(50)
            cars = generate_cars(50)
            rentals = generate_rentals(customers,cars)
            payments =generate_payments(rentals)
            
            # insert data customers
            for c in customers:
                cur.execute("""
                    INSERT INTO customers (id, first_name, last_name, age, phone, address, email, created_at)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )
                    ON CONFLICT (id) DO NOTHING;
                """,(c["id"],c["first_name"],c["last_name"],c["age"],c["phone"],c["address"],c["email"],c["created_at"]))
                
            # insert data cars
            for car in cars:
                cur.execute("""
                    INSERT INTO cars (id, brand, model, color, year, created_at)
                    VALUES ( %s, %s, %s, %s, %s, %s )
                    ON CONFLICT (id) DO NOTHING;
                """,(car["id"],car["brand"],car["model"],car["color"],car["year"],car["created_at"]))
            
            # insert data rentals
            for r in rentals:
                cur.execute("""
                    INSERT INTO rentals (id, customer_id, car_id, rental_start, rental_end_plan, days, price, created_at)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )
                    ON CONFLICT (id) DO NOTHING;
                """,(r["id"],r["customer_id"],r["car_id"],r["rental_start"],r["rental_end_plan"],r["days"],r["price"],r["created_at"]))
            
            # insert data payments
            for p in payments:
                cur.execute("""
                    INSERT INTO payments (id, rental_id, days, price, method, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING;
                """, (p["id"], p["rental_id"], p["days"], p["price"], p["method"], p["created_at"]))

            conn.commit()
            print("✅ Semua data berhasil dimasukkan.")
    finally:
        conn.close()
        
# --- main ---
if __name__ == "__main__":
    create_tables()
    insert_data()
    