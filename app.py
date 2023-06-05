from flask import Flask, jsonify
import pandas as pd
from sqlalchemy import create_engine
import pytz
from datetime import datetime, timedelta
import random
import string
import queries

app = Flask(__name__)

# Set the database connection string
db_connection_str = 'mysql+pymysql://root:root@localhost:3306/loop_kitchen'
engine = create_engine(db_connection_str)

@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    report_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    generate_report(report_id)
    return jsonify({'report_id': report_id})

@app.route('/get_report/<report_id>', methods=['GET'])
def get_report(report_id):
    report_file = f'report_{report_id}.csv'
    try:
        with open(report_file, 'r') as file:
            return file.read()
    except FileNotFoundError:
        return jsonify({'status': 'Running'})

def generate_report(report_id):
    # Get the current timestamp as the maximum timestamp in store_status table
    with engine.connect() as con:
        result = con.execute(queries.FETCH_MAX_TIMESTAMP)
        row = result.fetchone()
        if row is not None:
            current_timestamp = pd.to_datetime(row[0])
        else:
            # Handle the case when the query doesn't return any result
            # raise an exception
            raise ValueError("No max timestamp found in store_status table.")

    # Fetch store IDs from store_status table
    with engine.connect() as con:
        result = con.execute(queries.FETCH_DISTINCT_STORE_IDS)
        store_ids = [row[0] for row in result.fetchall()]

    #Output 
    report = []

    # Iterate over each store
    # count = 0  #Hard dode just for showing demo because app will take long time for read all database and create output
    for store_id in store_ids:
        # Fetch menu hours for the store
        with engine.connect() as con:
            result = con.execute(queries.FETCH_MENU_HOURS_DATA, {"store_id": store_id})
            menu_hours_df = pd.DataFrame(result.fetchall(), columns=['store_id', 'dayOfWeek', 'start_time_local', 'end_time_local'])

        # Fetch timezone for the store
        with engine.connect() as con:
            result = con.execute(queries.FETCH_TIMEZON, {"store_id": store_id})
            row = result.fetchone()
            if row is None:
                timezone = 'America/Chicago'
            else:
                timezone = row[0]

        # Filter store_status table for the current store
        with engine.connect() as con:
            result = con.execute(queries.FETCH_STORE_DATA, {"store_id": store_id})
            store_status_df = pd.DataFrame(result.fetchall(), columns=['store_id', 'status', 'timestamp_utc'])

        # Convert the timestamp_utc column to datetime
        store_status_df['timestamp_utc'] = pd.to_datetime(store_status_df['timestamp_utc'], format='mixed')

        # Calculate uptime and downtime for the past hour, day, and week
        uptime_last_hour = calculate_uptime(store_status_df, current_timestamp - timedelta(hours=1),
                                            current_timestamp, menu_hours_df, timezone)
        uptime_last_day = calculate_uptime(store_status_df, current_timestamp - timedelta(days=1),
                                           current_timestamp, menu_hours_df, timezone)
        uptime_last_week = calculate_uptime(store_status_df, current_timestamp - timedelta(weeks=1),
                                            current_timestamp, menu_hours_df, timezone)
        downtime_last_hour = calculate_downtime(store_status_df, current_timestamp - timedelta(hours=1),
                                                current_timestamp, menu_hours_df, timezone)
        downtime_last_day = calculate_downtime(store_status_df, current_timestamp - timedelta(days=1),
                                               current_timestamp, menu_hours_df, timezone)
        downtime_last_week = calculate_downtime(store_status_df, current_timestamp - timedelta(weeks=1),
                                                current_timestamp, menu_hours_df, timezone)

        # Append the report data to the report DataFrame
        report.append([store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week])
        # count += 1
        # if count > 5:
        #     break

    # Generate the report CSV file
    report_df = pd.DataFrame(report, columns=['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                                      'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])
    report_file = f'report_{report_id}.csv'
    report_df.to_csv(report_file, index=False)


def calculate_uptime(store_status_df, start_time, end_time, menu_hours_df, timezone):
    start_time_utc = start_time
    end_time_utc = end_time

    # Filter store_status_df based on start_time_utc and end_time_utc
    filtered_df = store_status_df[(store_status_df['timestamp_utc'] >= start_time_utc) &
                                  (store_status_df['timestamp_utc'] <= end_time_utc)]

    # Filter menu_hours_df for the day of week of start_time
    day_of_week = start_time.weekday()
    menu_hours = menu_hours_df[menu_hours_df['dayOfWeek'] == day_of_week]

    # Calculate total menu hours for the day
    total_menu_hours = 0
    for _, row in menu_hours.iterrows():
        start_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['start_time_local']).time())
        end_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['end_time_local']).time())
        start_time_utc = start_time_local - start_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
        end_time_utc = end_time_local - end_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
        total_menu_hours += (end_time_utc - start_time_utc).total_seconds() / 3600

    # Calculate total uptime within menu hours
    total_uptime = 0
    for _, row in filtered_df.iterrows():
        if row['status'] == 'active':
            # Convert the timestamp to local time
            timestamp_local = row['timestamp_utc'].astimezone(pytz.timezone(timezone))
            for _, row in menu_hours.iterrows():
                start_time_local = datetime.combine(timestamp_local.date(), pd.to_datetime(row['start_time_local']).time()).astimezone(pytz.timezone(timezone))
                end_time_local = datetime.combine(timestamp_local.date(), pd.to_datetime(row['end_time_local']).time()).astimezone(pytz.timezone(timezone))
                if start_time_local <= timestamp_local <= end_time_local:
                    # Convert the start and end times to UTC
                    start_time_utc = start_time_local - start_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
                    end_time_utc = end_time_local - end_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
                    total_uptime += (end_time_utc - start_time_utc).total_seconds() / 3600
                    break

    # Extrapolate uptime to the total menu hours
    uptime_ratio = total_uptime / total_menu_hours if total_menu_hours > 0 else 0
    extrapolated_uptime = uptime_ratio * 24  # 24 hours in a day

    return extrapolated_uptime

def calculate_downtime(store_status_df, start_time, end_time, menu_hours_df, timezone):
    start_time_utc = start_time
    end_time_utc = end_time

    # Filter store_status_df based on start_time_utc and end_time_utc
    filtered_df = store_status_df[(store_status_df['timestamp_utc'] >= start_time_utc) &
                                  (store_status_df['timestamp_utc'] <= end_time_utc)]

    # Filter menu_hours_df for the day of week of start_time
    day_of_week = start_time.weekday()
    menu_hours = menu_hours_df[menu_hours_df['dayOfWeek'] == day_of_week]

    # Calculate total menu hours for the day
    total_menu_hours = 0
    for _, row in menu_hours.iterrows():
        start_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['start_time_local']).time())
        end_time_local = datetime.combine(start_time.date(), pd.to_datetime(row['end_time_local']).time())
        start_time_utc = start_time_local - start_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
        end_time_utc = end_time_local - end_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
        total_menu_hours += (end_time_utc - start_time_utc).total_seconds() / 3600

    # Calculate total downtime within menu hours
    total_downtime = 0
    for _, row in filtered_df.iterrows():
        if row['status'] == 'inactive':
            # Convert the timestamp to local time
            timestamp_local = row['timestamp_utc'].astimezone(pytz.timezone(timezone))
            for _, row in menu_hours.iterrows():
                start_time_local = datetime.combine(timestamp_local.date(), pd.to_datetime(row['start_time_local']).time()).astimezone(pytz.timezone(timezone))
                end_time_local = datetime.combine(timestamp_local.date(), pd.to_datetime(row['end_time_local']).time()).astimezone(pytz.timezone(timezone))
                if start_time_local <= timestamp_local <= end_time_local:
                    start_time_utc = start_time_local - start_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
                    end_time_utc = end_time_local - end_time_local.replace(tzinfo=pytz.timezone(timezone)).utcoffset()
                    total_downtime += (end_time_utc - start_time_utc).total_seconds() / 3600
                    break

    # Extrapolate downtime to the total menu hours
    downtime_ratio = total_downtime / total_menu_hours if total_menu_hours > 0 else 0
    extrapolated_downtime = downtime_ratio * 24  # 24 hours in a day

    return extrapolated_downtime

if __name__ == '__main__':
    app.run(debug=True, port=8000)
