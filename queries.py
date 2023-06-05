from sqlalchemy import text

FETCH_MAX_TIMESTAMP = text("SELECT MAX(timestamp_utc) AS max_timestamp FROM store_status")

FETCH_DISTINCT_STORE_IDS = text("SELECT DISTINCT store_id FROM store_status")

FETCH_STORE_DATA = text("SELECT * FROM store_status WHERE store_id = :store_id")

FETCH_TIMEZON = text("SELECT timezone_str FROM bq_results WHERE store_id = :store_id")

FETCH_MENU_HOURS_DATA = text("SELECT * FROM menu_hours WHERE store_id = :store_id")