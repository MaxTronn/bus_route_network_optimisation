import pandas as pd
import sqlite3
import logging
from io import StringIO
import requests
from datetime import datetime


def column_exists(conn, table_name, column):
    # Create a cursor object
    cursor = conn.cursor()

    # Get information about the columns in the table
    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()

    # Extract the column names
    table_columns = [info[1] for info in table_info]

    # Check if the required column exists
    column_exists = column in table_columns

    return column_exists


def get_scheduled_duties():
    # gives the duty master schedule for today
    try:
        current_date = datetime.now().strftime('%d/%m/%Y')
        url = 'http://143.110.182.192:8090/depot_tool_duty_master.txt'
        response = requests.get(url)
        data = response.text
        df = pd.read_csv(StringIO(data))

        df = df[['Duty ID', 'Plate No.', 'Trip Start Time', 'Trip End Time', 'Shift Id']]

        df['Trip Start Time'] = pd.to_datetime(df['Trip Start Time'], format='%H:%M:%S')
        df['Trip End Time'] = pd.to_datetime(df['Trip End Time'], format='%H:%M:%S')

        # Convert the 'start_time' column to the desired format
        df['Trip Start Time'] = pd.to_datetime(df['Trip Start Time'].dt.strftime(f'{current_date}, %H:%M:%S'))
        df['Trip End Time'] = pd.to_datetime(df['Trip End Time'].dt.strftime(f'{current_date}, %H:%M:%S'))

        # duty_start_df = df.groupby(['Duty ID', 'Plate No.', 'Shift Id']).apply(lambda x: x.iloc[0])
        duty_start_df = df.groupby(['Duty ID', 'Plate No.', 'Shift Id']).min().reset_index()

        # Splitting "Duty ID" column into two columns named "Duty" and "ID" based on '/' character
        # Example, Duty ID = "479/15"
        # then , Duty = "479" and ID = "15"
        duty_start_df[['Duty', 'ID']] = duty_start_df['Duty ID'].str.rsplit('/', n=1, expand=True)
        duty_start_df = duty_start_df.drop('Duty ID', axis=1)

        # Data cleaning
        # Remove spaces, dots, underscores (str.replace(" |\.|_", "", regex=True))
        # Remove leading zeroes

        # MORE DATA CLEANING NEEDS TO BE DONE TO IMPROVE DUTY EFFICIENCY

        duty_start_df['duty_id'] = duty_start_df['Duty'].str.replace(" |\.|_", "", regex=True).str.lstrip('0') \
                                   + '/' + duty_start_df['ID'].str.replace(" |\.|_", "", regex=True).str.lstrip('0')

        return duty_start_df
    except Exception as e:
        logging.exception("Error occured during getting scheduled fleet data handled\n")
        return pd.DataFrame(columns=['Duty ID', 'Plate No.', 'Trip Start Time', 'Trip End Time', 'Shift Id'])


def get_depot_outsheds():
    # for a given date, the function returns the outshedding details
    url = "https://depot.chartr.in/get_all_depot_data"
    try:
        data = requests.get(url)
        outshed_df = pd.DataFrame.from_records(data.json())
        outshed_df = outshed_df[outshed_df["duty"] != ""]
        outshed_df["shift"] = outshed_df["shift"].map({"m": 1, "e": 2})
        outshed_df["ot"] = pd.to_datetime(outshed_df["ot"])
        outshed_df["it"] = pd.to_datetime(outshed_df["it"])
    except Exception as e:
        logging.exception("Error occured during getting depot outshed data handled\n")
        return pd.DataFrame(columns=['bus_number', 'duty', 'duty_id', 'ot', 'it', 'shift'])
    return outshed_df


def get_fleet_data():
    try:
        fleet_data = requests.get("https://depot.chartr.in/all_fleet/")
        fleet_data = fleet_data.json()
        fleet_df = pd.DataFrame.from_records(fleet_data)
        return fleet_df
    except Exception as e:
        logging.exception("Error occured during getting fleet data handled\n")
        return pd.DataFrame(columns=['vehicle_id', 'ac', 'depot', 'agency'])


if __name__ == '__main__':
    f_name = datetime.now().strftime("%Y_%m_%d")

    # Create DB File and Logging File
    db_file = 'data/schedule_adherence_' + f_name + ".db"

    logging.basicConfig(filename='data/logs_schedule_adherence_' + f_name + '.log', level=logging.DEBUG)

    conn = sqlite3.connect(db_file)
    schedule_df = get_scheduled_duties()
    outshed_df = get_depot_outsheds()
    merged_df = schedule_df.reset_index(drop=True).merge(outshed_df, left_on=['Plate No.', 'Shift Id'],
                                                         right_on=['bus_number', 'shift'], how='inner')

    shift_1_mask = (merged_df["shift"] == 1) & (merged_df["ot"] != "")
    shift_2_mask = (merged_df["shift"] == 2) & (merged_df["ot"] != "")

    shift_1_merged_df = merged_df[shift_1_mask].drop_duplicates(subset=["Plate No."], keep='first')
    shift_2_merged_df = merged_df[shift_2_mask].drop_duplicates(subset=["Plate No."], keep='first')
    shift_1_merged_df["shift_1_schedule_adherence"] = (shift_1_merged_df["ot"] - shift_1_merged_df["Trip Start Time"]).dt.total_seconds()
    shift_2_merged_df["shift_2_schedule_adherence"] = (shift_2_merged_df["ot"] - shift_2_merged_df["Trip Start Time"]).dt.total_seconds()

    fleet_df = get_fleet_data()
    fleet_df["shift_1_schedule_adherence"] = fleet_df["vehicle_id"].map(dict(shift_1_merged_df.set_index("Plate No.")["shift_1_schedule_adherence"])).fillna(0)
    fleet_df["shift_2_schedule_adherence"] = fleet_df["vehicle_id"].map(dict(shift_2_merged_df.set_index("Plate No.")["shift_2_schedule_adherence"])).fillna(0)

    fleet_df["shift_1_scheduled_start_time"] = fleet_df["vehicle_id"].map(dict(shift_1_merged_df.set_index("Plate No.")["Trip Start Time"])).fillna("")
    fleet_df["shift_1_actual_start_time"] = fleet_df["vehicle_id"].map(dict(shift_1_merged_df.set_index("Plate No.")["ot"])).fillna("")

    fleet_df["shift_2_scheduled_start_time"] = fleet_df["vehicle_id"].map(dict(shift_2_merged_df.set_index("Plate No.")["Trip Start Time"])).fillna("")
    fleet_df["shift_2_actual_start_time"] = fleet_df["vehicle_id"].map(dict(shift_2_merged_df.set_index("Plate No.")["ot"])).fillna("")
    fleet_df.to_sql("schedule_adherence", con=conn, if_exists="replace", index=False)
    print("done")
