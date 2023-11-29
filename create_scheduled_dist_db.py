import requests
import logging
import pandas as pd
from datetime import datetime
import time
from io import StringIO
import sqlite3
import os



def get_fleet_data():
    try:
        fleet_data = requests.get("https://depot.chartr.in/all_fleet/")
        fleet_data = fleet_data.json()
        fleet_df = pd.DataFrame.from_records(fleet_data)
        return fleet_df
    except Exception as e:
        logging.exception("Error occured during getting fleet data handled\n")
        return pd.DataFrame(columns=['vehicle_id', 'ac', 'depot', 'agency'])



# Returns a dataframe with the columns = ['Plate No.', 'Route No.'] from Duty Master Schedule.
# Here 'Route No.' contains the route long name
def get_scheduled_routes():
    try:
        url = 'http://143.110.182.192:8090/depot_tool_duty_master.txt'
        response = requests.get(url)
        data = response.text
        df = pd.read_csv(StringIO(data))

        df = df[['Plate No.', 'Route No.']]
        return df

    except Exception as e:
        logging.exception("Error occured during getting duty master schedule handled\n")
        return pd.DataFrame(columns=['Plate No.', 'Route No.'])



# Returns the route_length_table as a dataframe
def get_route_lengths():
    try :
        db_file_path = 'data/route_lengths.db'

        conn = sqlite3.connect(db_file_path)
        query = "SELECT * FROM route_lengths_table"
        df = pd.read_sql_query(query, conn)
        conn.close()
        # Convert 'agency' column values to lowercase
        df['agency'] = df['agency'].str.lower()
        df = df[['route_long_name', 'agency' ,'total_route_length']]
        return df

    except Exception as e:
        logging.exception("Error occured during getting route_lengths database file handled\n")
        return pd.DataFrame(columns=['route_long_name', 'agency' , 'total_route_length'])



# Returns a dataframe with the columns ['Plate No.', 'route_long_name', 'total_route_length']
def get_scheduled_dist():
    try:
        # scheduled_routes_df contains the columns = ['Plate No.', 'Route No.']
        # This 'Route No.' is same as Route Long Name
        scheduled_routes_df = get_scheduled_routes()
        scheduled_routes_df.rename(columns={'Route No.': 'route_long_name'}, inplace=True)

        # fleet_df contains the columns = ['vehicle_id', 'ac', 'depot', 'agency']
        fleet_df = get_fleet_data()
        fleet_df.rename(columns={'vehicle_id': 'Plate No.'}, inplace=True)

        # merge scheduled_routes_df and fleet_df
        merged_df = pd.merge(scheduled_routes_df, fleet_df, on='Plate No.', how='left')

        # route_lengths_df contains the columns = ['route_long_name', 'agency' , 'total_route_length']
        route_lengths_df = get_route_lengths()

        # Perform the merge using ['route_long_name','agency'] as the common column
        merged_df = pd.merge(merged_df, route_lengths_df, on=['route_long_name','agency'], how='left')
        # merged_df.sort_values(by=['Plate No.'], inplace=True)

        # Group by 'route_long_name' and sum the 'total_route_length'
        grouped_df = merged_df.groupby(['Plate No.', 'agency'], as_index=False).agg({
            'total_route_length': 'sum',
            'route_long_name': lambda x: ', '.join(x)
        })

        # Rename column names
        grouped_df.rename(columns={'Plate No.': 'vehicle_id'}, inplace=True)
        grouped_df.rename(columns={'total_route_length': 'total_sum_route_length_km'}, inplace=True)
        grouped_df.rename(columns={'route_long_name': 'route_list'}, inplace=True)

        grouped_df['total_sum_route_length_km'] = grouped_df['total_sum_route_length_km'] / 1000


        return grouped_df

    except Exception as e:
        logging.exception("Error occured during creating scheduled km dataframe handled\n")
        return pd.DataFrame(columns=['vehicle_id', 'agency', 'total_sum_route_length_km', 'route_list'])




def main() :
    start = time.time()
    today_date = datetime.now().strftime('%Y_%m_%d')

    logging.basicConfig(filename='data/logs_scheduled_dist_' + today_date + '.log', level=logging.DEBUG)

    scheduled_dist_df = get_scheduled_dist()

    # Save the DataFrame to a SQLite database file

    conn = sqlite3.connect(f"data/scheduled_dist_{today_date}.db")
    scheduled_dist_df.to_sql("scheduled_dist_table", conn, index=False, if_exists="replace")
    conn.close()

    end = time.time()
    logging.info("\nTime Taken for Iter = {}".format(end - start))

    current_time = datetime.now()
    current_time_string = current_time.strftime("%Y-%m-%d %H:%M:%S")
    logging.info("\nCurrent Time = " + current_time_string + "\n\n")



if __name__ == "__main__":
    main()

    # start = time.time()
    # main()
    # end = time.time()
    # time_taken = end - start
    #
    # print("time taken = {}".format(end - start))