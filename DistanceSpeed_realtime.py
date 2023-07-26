# API = http://165.232.181.23:10123/2023_05_17/DL1PC0613/time_filter/2023-05-17%2011:00:00

import json
import sys
import time
from datetime import datetime,timedelta
from geopy.distance import geodesic
import requests
import os
import sqlite3
import logging



prev_recorded_data = {}
data_dict = {}
fleet_dict = {}


# Check if a table exists in the database
def table_exists(db_name,table_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    return c.fetchone() is not None

# Returns all the buses in the fleet as dictionary in which key = bus_registration_number and value = depot
def get_fleet_depot():
    data = requests.get("https://depot.chartr.in/all_fleet/")
    data = data.json()

    fleet_dict = {}
    for bus in data:
        fleet_dict[bus['vehicle_id']] = bus['depot']

    return fleet_dict

# Get realtime gps data of all the buses
def get_bus_gps_data():
    gps_data = None
    dimts_data = None
    gps_status_code = 0
    dimts_status_code = 0
    while(gps_status_code != 200  and dimts_status_code != 200):
        try :
            gps_data = requests.get("http://143.110.182.192:8090/tcil_all_buses_db.txt")
            dimts_data = requests.get("http://143.110.182.192:8090/tcil_all_dimts_buses_db.txt")
            gps_status_code = gps_data.status_code
            dimts_status_code = dimts_data.status_code

        except:
            logging.error("\nError occured during API http call and handled\n")
    gps_data = gps_data.text.split("\n")
    dimts_data = dimts_data.text.split("\n")
    gps_data = [gps_data[i].split(',') for i in range(len(gps_data))]
    dimts_data = [dimts_data[i].split(',') for i in range(len(dimts_data))]
    gps_data += dimts_data

    gps_data_dict = {}
    for i in range(0,len(gps_data)):
        if len(gps_data[i]) >= 3:
            # Dict key = vehicle_id, values = latitude, longitude, vehicle_id, timestamp
            gps_data_dict[gps_data[i][2]] = [gps_data[i][0], gps_data[i][1], gps_data[i][2] ,gps_data[i][5]]

    return gps_data_dict

# Calculate timestamped distance and speed of each bus
def calculate_distance_speed(cursor):
    global prev_recorded_data
    current_gps_data = get_bus_gps_data()

    for vehicle_id, val in current_gps_data.items():

        # If there exists a previous record of the vehicle
        if vehicle_id  in prev_recorded_data.keys():
            prev_gps = prev_recorded_data[vehicle_id]
            cur_dist = geodesic(val[:2], prev_gps[:2]).km
            prev_dist = prev_gps[4]

            speed = 0

            overspeed_flag = False

            if cur_dist > 0 :
                prev_time = datetime.strptime(prev_gps[3], "%Y-%m-%d %H:%M:%S")
                cur_time = datetime.strptime(val[3], "%Y-%m-%d %H:%M:%S")
                time_taken = cur_time - prev_time
                if time_taken.total_seconds() < 0 :
                    continue;
                if time_taken.total_seconds() > 0 :
                    speed = cur_dist * 60 * 60 / time_taken.total_seconds()

                if (speed > 40) :
                    overspeed_flag = True

                if (speed < 0):
                    logging.error("\nSpeed Negative")
                    logging.error("Previous Data :", prev_gps)
                    logging.error("Current Data :", current_gps_data[vehicle_id])
                    logging.error("Distance :", cur_dist)
                    logging.error("Speed :", speed)

            dist = cur_dist + float(prev_dist)
            overspeed_flag = str(overspeed_flag)
            current_gps_data[vehicle_id].extend([str(dist),str(speed),prev_gps[6],overspeed_flag])



        # If there does not exist a previous record, add else functionality here
        else :

            if vehicle_id in fleet_dict :
                vehicle_depot_name = fleet_dict[vehicle_id]
            else:
                logging.error("VEHICLE ID = " + vehicle_id + " NOT FOUND IN FLEET INFORMATION API")
                vehicle_depot_name = None

            current_gps_data[vehicle_id].extend(["0", "0", vehicle_depot_name, "False"])

        cur_val = current_gps_data[vehicle_id]

        # Update Previous Recorded Data
        prev_recorded_data[vehicle_id] = cur_val
        cursor.execute('INSERT INTO distance_speed_realtime VALUES(?,?,?,?,?,?)', cur_val[2:])




# Get realtime inshed and outshed data of the all the buses
def get_it_ot_data(cursor):
    global fleet_dict
    while(True) :
        try :
            # Get inshed outshed Data from API Call
            url = "http://dtcbuses.chartr.in:11222/get_all_depot_data/" + datetime.now().strftime("%Y/%m/%d")
            data = requests.get(url)
            data = data.json()
            break
        except :
            logging.error("Error occured during getting inshed outshed data handled")



    # Store Data in data_dict
    for row in data:

        dist_data = []
        inshed_detected = False
        outshed_detected = False
        data_present = False

        # Check if depot value is present in fleet_dict

        if (row['bus_number'] in fleet_dict):
            bus_depot = fleet_dict[row['bus_number']]
        else:
            logging.error("VEHICLE ID = " + row['bus_number'] + " NOT FOUND IN FLEET INFORMATION API")
            bus_depot = None

        # If data for a bus is already present, check if inshed or outshed is detected
        try:

            if row['bus_number'] + row['shift'] in data_dict.keys():
                data_present = True

                # data_dict needs to have two key values, [row['bus_number'], shift
                prev_data = data_dict[row['bus_number'] + row['shift']]
                cur_data = row

                if (prev_data[1] != cur_data['ot'] or prev_data[2] != cur_data['it']):

                    if (prev_data[1] != cur_data['ot']):
                        logging.info("Outshed Detected for bus = {}".format(row['bus_number']))
                        outshed_detected = True
                    if prev_data[2] != cur_data['it']:
                        logging.info("Inshed Detected for bus = {}".format(row['bus_number']))
                        inshed_detected = True

                    # dist_data format =
                    # [ latitude, longitude, vehicle_id, timestamp, distance_km, speed_kmph, depot, overspeeding]

                    dist_data = prev_recorded_data[row['bus_number']]

                    if (dist_data == None):
                        logging.error("ERROR : call to DistanceSpeed_realtime did not return data for bus =", row['bus_number'])
                    elif (len(dist_data) == 4):
                        logging.error("call to DistanceSpeed_realtime did not have distance and speed for bus =",
                              row['bus_number'])
                        logging.error("Returned Data = ")
                        logging.error(dist_data)

        except:
            logging.error("Error Occured for data row = ", row)
            raise

        # Data_dict format
        # key = bus_number
        # value = [bus_number, ot, it, shift, distance at ot (km), distance travelled during shift(km), depot]
        if ((dist_data == None or len(dist_data) == 0) and not data_present):
            data_dict[row['bus_number'] + row['shift']] = [row['bus_number'], row['ot'], row['it'], row['shift'], '0',
                                                           '0', bus_depot]
        elif outshed_detected:
            data_dict[row['bus_number'] + row['shift']] = [row['bus_number'], row['ot'], row['it'], row['shift'],
                                                           str(dist_data[4]), '0', bus_depot]
        elif inshed_detected:
            data_dict_cur_bus_val = data_dict[row['bus_number'] + row['shift']]
            distance_travelled_km = float(dist_data[4]) - float(data_dict_cur_bus_val[4])
            data_dict[row['bus_number'] + row['shift']][5] = str(distance_travelled_km)
            data_dict[row['bus_number'] + row['shift']][2] = row['it']


        if (data_present == False or inshed_detected or outshed_detected):
            cursor.execute('DELETE FROM shiftwise_bus_distance where vehicle_id = ? and shift = ?',
                           (row['bus_number'], row['shift']))
            cursor.execute('INSERT INTO shiftwise_bus_distance VALUES(?,?,?,?,?,?,?)',
                           data_dict[row['bus_number'] + row['shift']])


def main():

    global prev_recorded_data
    global fleet_dict
    prev_recorded_data = {}
    fleet_dict = {}

    # if time < 3:00 am, we don't want to create a new file and want to continue into previous date's file
    current_time = datetime.now()
    if current_time.hour < 3:
        yesterday = datetime.now() - timedelta(days=1)
        f_name = yesterday.strftime('%Y_%m_%d')
    else :
        f_name = datetime.now().strftime("%Y_%m_%d")

    db_file = 'data/bus_movements_' + f_name + ".db"
    logging.basicConfig(filename='data/logs_bus_movements_' + f_name + '.log', level=logging.DEBUG)

    # Cache file stores the data from previous iteration of code run
    cache_f_name = "data/cache_" + f_name + ".json"


    if(os.path.exists(cache_f_name)) :
        with open(cache_f_name) as f:
            data = json.load(f)
            prev_recorded_data = data

    fleet_dict = get_fleet_depot()

    conn = None

    # check if the database file exists in the current directory
    if os.path.isfile(db_file):

        # create a connection to the existing database file
        conn = sqlite3.connect(db_file)

        # create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # Check if distance_speed_table exists in db file
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='distance_speed_realtime'")
        if cursor.fetchone() is None:
            cursor.execute('''CREATE TABLE distance_speed_realtime
                                (vehicle_id text, timestamp text, distance_km text, speed_kmph text, 
                                depot text, overspeeding text)''')


        # Check if shiftwise_bus_distance exists in db file
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='shiftwise_bus_distance'")
        if cursor.fetchone() is None:
            cursor.execute('''CREATE TABLE shiftwise_bus_distance
                                (vehicle_id text, ot text, it text, shift text, 
                                distance_before_ot_km, distance_km text, depot text)''')

        # commit the changes to the database and close the connection
        conn.commit()


    else:

        # create a connection to the new database file
        conn = sqlite3.connect(db_file)

        # create a cursor object to execute SQL commands
        cursor = conn.cursor()

        # create a table named distance_speed_realtime
        cursor.execute('''CREATE TABLE distance_speed_realtime
                        (vehicle_id text, timestamp text, distance_km text, speed_kmph text, 
                        depot text, overspeeding text)''')

        # create a table named shiftwise_bus_distance
        cursor.execute('''CREATE TABLE shiftwise_bus_distance
                            (vehicle_id text, ot text, it text, shift text, 
                            distance_before_ot_km, distance_km text, depot text)''')


        # commit the changes to the database and close the connection
        conn.commit()


    # while True:
    try :
        cursor = conn.cursor()
        start = time.time()
        logging.info("Started Iter")

        if len(prev_recorded_data) == 0:

            prev_recorded_data = get_bus_gps_data()

            # val = [ latitude , longitude , vehicle_id , timestamp ]
            # This loop adds distance, speed, depot, overspeeding values into val and then
            # stores them in prev_recorded_data dict and sql table
            for vehicle_id, val in prev_recorded_data.items() :
                val.append("0")     # distance
                val.append("0")     # speed
                if(vehicle_id in fleet_dict):
                    val.append(fleet_dict[vehicle_id])   # depot
                else:
                    logging.error("VEHICLE ID = " + vehicle_id + " NOT FOUND IN FLEET INFORMATION API")
                    val.append(None)

                val.append("False");      # overspeeding = false

                # we do not add latitude ,longitude values into distance_speed_realtime table.
                cursor.execute('INSERT INTO distance_speed_realtime VALUES(?,?,?,?,?,?)', val[2:])


        else:
            calculate_distance_speed(cursor)
            get_it_ot_data(cursor)

        conn.commit()
        end = time.time()
        logging.info("\nTime Taken for Iter = {}".format(end - start))

        current_time = datetime.now()
        current_time_string = current_time.strftime("%Y-%m-%d %H:%M:%S")
        logging.info("\nCurrent Time = " + current_time_string + "\n\n")


        # Update prev_recorded_data into cache file
        # Check if cache file exists :
        with open(cache_f_name, 'w') as cache_file:
            json.dump(prev_recorded_data, cache_file)


    except :
        logging.error("Error occured in try-except block in main, handled")






if __name__ == '__main__':
    main()

    # f_name = datetime.now().strftime("%Y_%m_%d")
    # db_file = 'data/bus_movements_' + f_name + ".db"
    # log_file='data/logs_bus_movements_' + f_name + '.log'
    # cache_f_name = "data/cache_" + f_name + ".json"
    # if (os.path.exists(db_file)) :
    #     os.remove(db_file)
    # if (os.path.exists(log_file)):
    #     os.remove(log_file)
    # if (os.path.exists(cache_f_name)):
    #     os.remove(cache_f_name)
    #
    #
    # for i in range(10):
    #     start = time.time()
    #     main()
    #     end = time.time()
    #     print(end - start)
    #     if end - start < 10:
    #         time.sleep(10 - (end - start))


