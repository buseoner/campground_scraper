# campground_scraper
This project automates the collection, cleaning, enrichment, and storage of campground data across the United States using The Dyrt's internal API and OpenStreetMap Nominatim geocoding.

Data Collection: Campground data is fetched from The Dyrt API using a bounding box that spans the entire USA. Results are paginated with a size of 500 per page. A fallback mechanism is implemented to handle API errors on the final page by retrying with smaller page sizes.

Validation & Deduplication: Each record is validated using a Pydantic model. Duplicate entries (based on ID) are removed to ensure clean data.

Reverse Geocoding: For each unique campground, its latitude and longitude are converted to a human-readable address using OpenStreetMap’s Nominatim service.

Storage: The final dataset — including the resolved address — is saved to CSV and inserted into a PostgreSQL database.

Containerized Workflow: The full pipeline runs inside Docker containers. PostgreSQL and the scraper are defined in docker-compose.yml, making the setup reproducible and portable.
