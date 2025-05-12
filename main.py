import pandas as pd
import time
import httpx
import csv
import json
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from db import get_db_connection, create_campground_table
from campground import Campground
#fff
# Config
BASE_URL = "https://thedyrt.com/api/v6/locations/search-results"
BBOX_USA = "-125,24,-66,50"
PAGE_SIZE = 500
RAW_CSV = "campgrounds.csv"
CLEANED_CSV = "campgrounds_cleaned.csv"
FINAL_CSV = "campgrounds_with_addresses.csv"

# Fetch All Campgrounds from API
def fetch_all_campgrounds():
    all_data = []
    page = 1
    while True:
        print(f"Fetching page {page} with size {PAGE_SIZE}...")
        try:
            response = httpx.get(
                BASE_URL,
                params={
                    "filter[search][bbox]": BBOX_USA,
                    "page[number]": page,
                    "page[size]": PAGE_SIZE
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json().get("data", [])
        # Handle HTTP status errors for last page which contains less data than PAGE_SIZE
        except httpx.HTTPStatusError:
            print(f"Failed at page {page} with size {PAGE_SIZE}. Trying decrement fallback...")
            data = []
            for size in range(PAGE_SIZE - 1, 99, -1):
                try:
                    response = httpx.get(
                        BASE_URL,
                        params={
                            "filter[search][bbox]": BBOX_USA,
                            "page[number]": page,
                            "page[size]": size
                        },
                        timeout=15
                    )
                    response.raise_for_status()
                    data = response.json().get("data", [])
                    print(f"Fallback success at size {size} → {len(data)} items")
                    break
                except httpx.HTTPStatusError:
                    continue
            else:
                print(f"All fallback sizes failed for page {page}. Ending fetch.")
                break

        if not data:
            print("No more data.")
            break

        all_data.extend(data)
        print(f"Page {page}: {len(data)} items")

        if len(data) < PAGE_SIZE:
            print("Reached final page.")
            break

        page += 1
        time.sleep(1)
    return all_data

# Save raw JSON to CSV
def save_raw_to_csv(data, path):
    validated_rows = []
    fields = [
        "id", "name", "latitude", "longitude", "region_name", "administrative_area",
        "nearest_city_name", "accommodation_type_names", "bookable", "camper_types",
        "operator", "photo_url", "photo_urls", "photos_count", "rating", "reviews_count",
        "slug", "price_low", "price_high", "availability_updated_at"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for entry in data:
            try:
                raw = entry.get("attributes", {})
                raw["id"] = entry["id"]
                raw["type"] = entry["type"]
                raw["links"] = entry["links"]
                validated = Campground(**raw)
                item = validated.model_dump()
                item["photo_url"] = str(item.get("photo_url") or "")
                item["photo_urls"] = ";".join(str(u) for u in item.get("photo_urls") or [])
                item["accommodation_type_names"] = ";".join(item.get("accommodation_type_names") or [])
                item["camper_types"] = ";".join(item.get("camper_types") or [])
                writer.writerow({k: item.get(k, "") for k in fields})
                validated_rows.append(item)
            except Exception as e:
                print(f"Validation failed for ID {entry.get('id')}: {e}")
    print(f"Saved {len(validated_rows)} validated rows to CSV → {path}")

# Clean the duplicate data and save to a new CSV
def deduplicate_csv(input_path, output_path):
    df = pd.read_csv(input_path)
    df_clean = df.drop_duplicates(subset="id", keep="first")
    df_clean.to_csv(output_path, index=False)
    print(f"Deduplicated → {len(df_clean)} rows saved to {output_path}")
    return df_clean

# Reverse Geocode Using geopy + Nominatim
geolocator = Nominatim(user_agent="campground-geocoder")

# Reverse geocode a single latitude and longitude
def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), timeout=10)
        return location.address if location else None
    except GeocoderTimedOut:
        time.sleep(2)
        return reverse_geocode(lat, lon)
    except Exception as e:
        print(f"Error for ({lat}, {lon}): {e}")
        return None

# Add Addresses to DataFrame from latitude and longitude data via reverse geocoding
def add_addresses(df):
    addresses = []
    for i, row in df.iterrows():
        print(f"[{i+1}/{len(df)}] Reverse geocoding: ({row['latitude']}, {row['longitude']})")
        address = reverse_geocode(row["latitude"], row["longitude"])
        addresses.append(address)

    df["address"] = addresses
    df.to_csv(FINAL_CSV, index=False)
    print(f"Saved with addresses to: {FINAL_CSV}")
    return df

# Insert the updated and cleaned data into PostgreSQL
def insert_to_db(csv_path):
    df = pd.read_csv(csv_path)
    success, failed = 0, 0
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                try:
                    row["photo_urls"] = row["photo_urls"].split(";") if pd.notna(row["photo_urls"]) else []
                    row["accommodation_type_names"] = row["accommodation_type_names"].split(";") if pd.notna(row["accommodation_type_names"]) else []
                    row["camper_types"] = row["camper_types"].split(";") if pd.notna(row["camper_types"]) else []

                    cur.execute("""
                        INSERT INTO campgrounds (
                            id, name, latitude, longitude, region_name, administrative_area,
                            nearest_city_name, accommodation_type_names, bookable, camper_types,
                            operator, photo_url, photo_urls, photos_count, rating, reviews_count,
                            slug, price_low, price_high, availability_updated_at, address
                        )
                        VALUES (
                            %(id)s, %(name)s, %(latitude)s, %(longitude)s, %(region_name)s,
                            %(administrative_area)s, %(nearest_city_name)s, %(accommodation_type_names)s,
                            %(bookable)s, %(camper_types)s, %(operator)s, %(photo_url)s, %(photo_urls)s,
                            %(photos_count)s, %(rating)s, %(reviews_count)s, %(slug)s, %(price_low)s,
                            %(price_high)s, %(availability_updated_at)s, %(address)s
                        )
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            latitude = EXCLUDED.latitude,
                            longitude = EXCLUDED.longitude,
                            address = EXCLUDED.address;
                    """, row)

                    success += 1
                except Exception as e:
                    print(f"DB insert failed for ID {row.get('id')}: {e}")
                    failed += 1
        conn.commit()
    print(f"DB Inserted: {success} | Failed: {failed}")

# Main Execution Flow
if __name__ == "__main__":
    print("📦 Creating DB table if not exists...")
    create_campground_table()

    print("Fetching all campgrounds...")
    raw_data = fetch_all_campgrounds()

    print("Saving raw data to CSV...")
    save_raw_to_csv(raw_data, RAW_CSV)

    print("Deduplicating data...")
    deduped_df = deduplicate_csv(RAW_CSV, CLEANED_CSV)

    print("Reverse geocoding addresses...")
    enriched_df = add_addresses(deduped_df)

    print("Inserting into PostgreSQL...")
    insert_to_db(FINAL_CSV)

    print("All campgrounds data fetched, cleaned, updated then saved into db.")
