import psycopg2
from psycopg2.extras import execute_values
import os

DB_URL = os.getenv("DB_URL", "postgresql://postgres:1234@localhost:5432/postgres")

def get_db_connection():
    return psycopg2.connect(DB_URL)


def create_campground_table():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            print("Dropping campgrounds table in case it already exists...")
            cur.execute("DROP TABLE IF EXISTS campgrounds;")

            print("Creating a campgrounds table...")
            cur.execute("""
                CREATE TABLE campgrounds (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    region_name TEXT,
                    administrative_area TEXT,
                    nearest_city_name TEXT,
                    accommodation_type_names TEXT[],
                    bookable BOOLEAN,
                    camper_types TEXT[],
                    operator TEXT,
                    photo_url TEXT,
                    photo_urls TEXT[],
                    photos_count INTEGER,
                    rating DOUBLE PRECISION,
                    reviews_count INTEGER,
                    slug TEXT,
                    price_low DOUBLE PRECISION,
                    price_high DOUBLE PRECISION,
                    availability_updated_at TIMESTAMP,
                    address TEXT
                );
            """)
        conn.commit()
        print("Table recreated.")
