
# Fleet Scheduled : http://143.110.182.192:8090/depot_tool_duty_master.txt

import requests
import pandas as pd
import logging
import time
import os
import sqlite3
from datetime import datetime,timedelta

# This function returns a dictionary of buses scheduled
# Key = vehicle_id
# Value = List of unique duties scheduled for the bus
def get_scheduled_duties():
    while (True):
        try:
            df = pd.read_csv('http://143.110.182.192:8090/depot_tool_duty_master.txt')
            df = df[['Duty ID', 'Plate No.']]

            # Splitting "Duty ID" column into two columns named "Duty" and "ID" based on '/' character
            # Example, Duty ID = "479/15"
            # then , Duty = "479" and ID = "15"
            df[['Duty', 'ID']] = df['Duty ID'].str.rsplit('/', n=1 ,expand=True)
            df = df.drop('Duty ID', axis=1)

            # Data cleaning
            # Remove spaces, dots, underscores (str.replace(" |\.|_", "", regex=True))
            # Remove leading zeroes

            # MORE DATA CLEANING NEEDS TO BE DONE TO IMPROVE DUTY EFFICIENCY

            df['duty_id'] = df['Duty'].str.replace(" |\.|_", "", regex=True).str.lstrip('0') \
                            + '/' + df['ID'].str.replace(" |\.|_", "", regex=True).str.lstrip('0')
            scheduled_duty_dict = df.groupby('Plate No.')['duty_id'].unique().apply(list).to_dict()

            return scheduled_duty_dict

        except:
            logging.exception("Error occured during getting scheduled fleet data handled\n")



# This function gives a list of dictionaries
# Eg = {"vehicle_id": "DL1PC7002", "ac": "ac", "depot": "and", "agency": "dtc"}
# "ac" means bus is Air-Conditioned and "nac" means bus is not Air-Conditioned
def get_fleet_data():
    while(True):
        try :
            fleet_data = requests.get("https://depot.chartr.in/all_fleet/")
            fleet_data = fleet_data.json()
            return fleet_data

        except:
            logging.exception("Error occured during getting fleet data handled\n")


# This function returns a dictionary of buses on road
# Key = vehicle_id
# Value = List of unique duties scheduled for the bus
def get_actual_duties() :
    while (True):
        try:
            actual_duty_dict = {}

            # Get manual inshed outshed Data from API Call
            url = "https://depot.chartr.in/get_all_depot_data/" + datetime.now().strftime("%Y/%m/%d")
            data = requests.get(url)
            data = data.json()

            for row in data:
                if(len(row['ot']) != 0 or len(row['it']) != 0):
                    duty = row['duty']
                    id = row['duty_id']

                    # Data cleaning
                    # Remove spaces, dots, underscores (str.replace(" |\.|_", "", regex=True))
                    # Remove leading zeroes
                    duty = duty.lstrip('0')
                    id = id.lstrip('0')
                    duty_id = (duty + '/' + id).replace(" ", "").replace(".", "").replace("_","")

                    if(row['bus_number'] in actual_duty_dict) :
                        if duty_id not in actual_duty_dict[row['bus_number']]:
                            actual_duty_dict[row['bus_number']].append(duty_id)
                    else :
                        actual_duty_dict[row['bus_number']] = [duty_id]


            return actual_duty_dict

        except :
            logging.exception("Error occured during getting inshed / outshed data handled\n")



def main():

    f_name = datetime.now().strftime("%Y_%m_%d")
    
    # Create DB File and Logging File
    db_file = 'data/fleet_utilisation_' + f_name + ".db"

    logging.basicConfig(filename='data/logs_fleet_utilisation_' + f_name + '.log', level=logging.DEBUG)

    conn = None
    table_exists = False

    # check if the database file exists in the current directory
    if os.path.isfile(db_file):

        # create a connection to the existing database file
        conn = sqlite3.connect(db_file)

        # create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Check if fleet_utilisation table exists in db file
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fleet_utilisation'")
        if cursor.fetchone() is None:
            cursor.execute('''CREATE TABLE fleet_utilisation
                            (vehicle_id text, ac text, depot text, agency text, 
                            scheduled_duties text, actual_duties text, duty_efficiency text)''')
        else:

            # Checking if table is non - empty
            cursor.execute('SELECT COUNT(*) FROM fleet_utilisation')

            num_records = cursor.fetchall()
            num_records = num_records[0][0]
            if(num_records > 0) :
                table_exists = True

        # commit the changes to the database and close the connection
        conn.commit()
        
    else:

        # create a connection to the new database file
        conn = sqlite3.connect(db_file)

        # create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # create a table named fleet_utilisation
        cursor.execute('''CREATE TABLE fleet_utilisation
                        (vehicle_id text, ac text, depot text, agency text, 
                        scheduled_duties text, actual_duties text, duty_efficiency text)''')

        # commit the changes to the database and close the connection
        conn.commit()

    cursor = conn.cursor()

    if(table_exists):
        cursor.execute('DELETE FROM fleet_utilisation')
        table_exists = False

    if(table_exists == False):
        # fleet_data is a list of dictionaries :
        # Eg = {"vehicle_id": "DL1PC7002", "ac": "ac", "depot": "and", "agency": "dtc"}
        fleet_data = get_fleet_data()


        # scheduled_duty_dict and actual_duty_dict are dictionaries with
        # key = vehicle_id
        # value  = list of unique duty_id assigned to the vehicle_id
        scheduled_duty_dict = get_scheduled_duties()
        actual_duty_dict = get_actual_duties()
        



        for bus in fleet_data :
            insert_list = [bus['vehicle_id'], bus['ac'], bus['depot'],bus['agency']]
            actual_duties_str = None
            scheduled_duties_str = None
            actual_duties_list = []
            scheduled_duties_list = []

            if(bus['vehicle_id'] in scheduled_duty_dict):
                scheduled_duties_list = scheduled_duty_dict[bus['vehicle_id']]
                scheduled_duties_str = ", ".join(scheduled_duties_list)

            if (bus['vehicle_id'] in actual_duty_dict):
                actual_duties_list = actual_duty_dict[bus['vehicle_id']]
                actual_duties_str = ", ".join(actual_duties_list)
                
            # if( len(scheduled_duties_list) == 0 and len(actual_duties_list) > 0):
            #     logging.error("Bus = {} is not present in Scheduled Duty Table but "
            #                   "is outshed / inshed in manual outshed inshed API".format(bus['vehicle_id']))
            
            insert_list.append(scheduled_duties_str)
            insert_list.append(actual_duties_str)

            # common_duties is a list of duties common in actual_duties_list and scheduled_duties_list
            common_duties = [string for string in actual_duties_list if string in scheduled_duties_list]
            num_common_duties = len(common_duties)

            duty_efficiency = 0
            if(len(scheduled_duties_list) > 0) :
                duty_efficiency = num_common_duties / len(scheduled_duties_list)

            insert_list.append(str(duty_efficiency))

            cursor.execute('INSERT INTO fleet_utilisation VALUES(?,?,?,?,?,?,?)', insert_list)

        conn.commit()

        logging.info("New Iteration finished at {} \n\n".format(datetime.now()))


if __name__ == '__main__':
    main()

    # BELOW CODE IS FOR TESTING

    # f_name = datetime.now().strftime("%Y_%m_%d")
    # db_file = 'data/fleet_utilisation_' + f_name + ".db"
    # log_file= 'data/fleet_utilisation_' + f_name + '.log'
    #
    # if (os.path.exists(db_file)) :
    #     os.remove(db_file)
    # if (os.path.exists(log_file)):
    #     os.remove(log_file)
    #
    #
    #
    # while(True):
    #     start = time.time()
    #     main()
    #     end = time.time()
    #     time_taken = end - start
    #
    #     print("time taken = {}".format(end - start))
    #
    #     if(time_taken < 10):
    #         time.sleep(10 - time_taken)