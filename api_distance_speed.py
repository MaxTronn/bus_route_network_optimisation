# API Endpoint
# https://depot-monitoring.chartr.in/
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os
from datetime import datetime, timedelta
from schedule_adherence import column_exists
import json

app = Flask(__name__)
CORS(app)

# API Call Example URL = http://127.0.0.1:5000/overspeeding_data/2023-04-20
# In Example, IP Address:Port = 127.0.0.1:5000
# In Example, date = 2023-04-20
# Use Date Format = YYYY-MM-DD    (iso format)
# Use Timestamp Format = YYYY-MM-DDTHH:MM:SS  (iso format)
# Example Timestamp = 2023-05-18T12:43:52



# Returns all overspeeding records on a given date
@app.route('/overspeeding_data/<date>')
@app.route('/overspeeding_data/<date>/<bus_num>')
@app.route('/overspeeding_data/<date>/<int:speed>')
@app.route('/overspeeding_data/<date>/<bus_num>/<int:speed>')
@app.route('/overspeeding_data/<date>/depot/<depot_name>')
@app.route('/overspeeding_data/<date>/<int:speed>/depot/<depot_name>')
def overspeeding_filter(date, bus_num = None, speed = None, depot_name = None):

    date = date.replace('-','_')

    conn = sqlite3.connect(f'data/bus_movements_{date}.db')

    if date:
        query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                'FROM distance_speed_realtime ' \
                'WHERE overspeeding = "True" ' \
                'GROUP BY vehicle_id;'
    if bus_num:
        if speed :
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                    'FROM distance_speed_realtime ' \
                    f'WHERE vehicle_id = "{bus_num}" ' \
                    'AND overspeeding = "True" ' \
                    f'AND CAST(speed_kmph AS float) >= {speed} ;'

        else :
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph)' \
                    'FROM distance_speed_realtime ' \
                    'WHERE overspeeding = "True" ' \
                    f'AND vehicle_id = "{bus_num}";'

    if (not bus_num) and speed :
        if depot_name :
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                    'FROM distance_speed_realtime ' \
                    f'WHERE CAST(speed_kmph AS float) >= {speed} ' \
                    f'AND depot = "{depot_name}" ' \
                    'AND overspeeding = "True" ' \
                    'GROUP BY vehicle_id;'
        else :
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                    'FROM distance_speed_realtime ' \
                    f'WHERE CAST(speed_kmph AS float) >= {speed} ' \
                    'AND overspeeding = "True" ' \
                    'GROUP BY vehicle_id;'

    if (not bus_num) and (not speed) and depot_name :
        query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                'FROM distance_speed_realtime ' \
                f'WHERE depot = "{depot_name}"' \
                'AND overspeeding = "True" ' \
                'GROUP BY vehicle_id;'


    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    if (len(rows) > 0 and rows[0][0] != None):
        for i in range(len(rows)) :
            temp_dict = {
                'vehicle_id' : rows[i][0],
                'timestamps' : rows[i][1].split(','),
                'speeds' : rows[i][2].split(',')
            }
            rows[i] = temp_dict
    else:
        response = "No entries found for given parameters"
        return response


    return jsonify(rows)






# Returns all overspeeding records on a given date for a given bus between two given timestamps
# Giving Bus Number is optional for the user
# Giving two timestamps is optional for the user. User needs to give atleast 1 timestamp
# Timestamp2 must be greater than timestamp1
# Use Date Format = YYYY-MM-DD    (iso format)
# Use Timestamp Format = YYYY-MM-DDTHH:MM:SS    (iso format)
# Example Timestamp = 2023-05-18T12:43:52
@app.route('/overspeeding_data/<date>/time_filter/<timestamp1>')
@app.route('/overspeeding_data/<date>/time_filter/<timestamp1>/<timestamp2>')
@app.route('/overspeeding_data/<date>/depot/<depot_name>/time_filter/<timestamp1>')
@app.route('/overspeeding_data/<date>/depot/<depot_name>/time_filter/<timestamp1>/<timestamp2>')
@app.route('/overspeeding_data/<date>/<bus_num>/time_filter/<timestamp1>')
@app.route('/overspeeding_data/<date>/<bus_num>/time_filter/<timestamp1>/<timestamp2>')
def overspeeding_time_filter(date, bus_num = None, timestamp1 = None, timestamp2 = None, depot_name = None):
    date = date.replace('-', '_')
    conn = sqlite3.connect(f'data/bus_movements_{date}.db')
    cursor = conn.cursor()
    if(timestamp1) :
        timestamp1 = (str)(datetime.strptime(timestamp1, '%Y-%m-%dT%H:%M:%S'))
    if(timestamp2) :
        timestamp2 = (str)(datetime.strptime(timestamp2, '%Y-%m-%dT%H:%M:%S'))



    if(bus_num) :
        if(timestamp1) :
            if(timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                                'FROM distance_speed_realtime '
                                f'WHERE vehicle_id = "{bus_num}" '
                                f'AND timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               'AND overspeeding = "True" '
                                'GROUP BY vehicle_id;')
            else :
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE vehicle_id = "{bus_num}" '
                               f'AND timestamp >=  "{timestamp1}"'
                               'AND overspeeding = "True" '
                               'GROUP BY vehicle_id;')

    elif depot_name :
        if (timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               f'AND depot = "{depot_name}"'
                               'AND overspeeding = "True" '
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               f'AND depot = "{depot_name}"'
                               'AND overspeeding = "True" '
                               'GROUP BY vehicle_id;')

    else:
        if(timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               'AND overspeeding = "True" '
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               'AND overspeeding = "True" '
                               'GROUP BY vehicle_id;')

    rows = cursor.fetchall()
    if (len(rows) > 0 and rows[0][0] != None):
        for i in range(len(rows)):
            temp_dict = {
                'vehicle_id': rows[i][0],
                'timestamps': rows[i][1].split(','),
                'speeds': rows[i][2].split(',')
            }
            rows[i] = temp_dict
    else:
        response = "No entries found "
        return response

    return jsonify(rows)





# Returns timestamped distance data for given date with optional parameters of bus number and timestamps and depot
@app.route('/distance_data/<date>')
@app.route('/distance_data/<date>/time_filter/<timestamp1>')
@app.route('/distance_data/<date>/time_filter/<timestamp1>/<timestamp2>')

# BUS NUMBER FILTER
@app.route('/distance_data/<date>/<bus_num>')
@app.route('/distance_data/<date>/<bus_num>/time_filter/<timestamp1>')
@app.route('/distance_data/<date>/<bus_num>/time_filter/<timestamp1>/<timestamp2>')

# DEPOT FILTER
@app.route('/distance_data/<date>/depot/<depot_name>/')
@app.route('/distance_data/<date>/depot/<depot_name>/time_filter/<timestamp1>')
@app.route('/distance_data/<date>/depot/<depot_name>/time_filter/<timestamp1>/<timestamp2>')
def distance_data_filter(date, bus_num = None, timestamp1 = None, timestamp2 = None, depot_name = None):
    date = date.replace('-', '_')
    conn = sqlite3.connect(f'data/bus_movements_{date}.db')
    cursor = conn.cursor()
    if(timestamp1) :
        timestamp1 = (str)(datetime.strptime(timestamp1, '%Y-%m-%dT%H:%M:%S'))
    if(timestamp2) :
        timestamp2 = (str)(datetime.strptime(timestamp2, '%Y-%m-%dT%H:%M:%S'))



    if(bus_num) :
        if(timestamp1) :
            if(timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                                'FROM distance_speed_realtime '
                                f'WHERE vehicle_id = "{bus_num}" '
                                f'AND timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}";')

            else :
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                               'FROM distance_speed_realtime '
                               f'WHERE vehicle_id = "{bus_num}" '
                               f'AND timestamp >=  "{timestamp1}"')

        else : # when only bus_num is given and no timestamp is given
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km)'
                            'FROM distance_speed_realtime '
                            f'WHERE vehicle_id = "{bus_num}";')

    elif(depot_name) :
        if (timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               f'AND depot = "{depot_name}"'
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               f'AND depot = "{depot_name}"'
                               'GROUP BY vehicle_id;')

        else:  # When only date and depot_name is given and nothing else
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                           'FROM distance_speed_realtime '
                           f'WHERE depot = "{depot_name}"'
                           'GROUP BY vehicle_id;')


    else:
        if(timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               'GROUP BY vehicle_id;')

        else : # When only date is given and nothing else
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(distance_km) '
                           'FROM distance_speed_realtime '
                           'GROUP BY vehicle_id;')

    rows = cursor.fetchall()
    if (len(rows) > 0 and rows[0][0] != None):
        for i in range(len(rows)):
            temp_dict = {
                'vehicle_id': rows[i][0],
                'timestamps': rows[i][1].split(','),
                'distance_km': rows[i][2].split(',')
            }
            rows[i] = temp_dict
    else:
        response = "No entries found "
        return response

    return jsonify(rows)






# Returns timestamped speed data for given date with optional parameters of bus number and timestamps and depot
@app.route('/speed_data/<date>')
@app.route('/speed_data/<date>/time_filter/<timestamp1>')
@app.route('/speed_data/<date>/time_filter/<timestamp1>/<timestamp2>')

# BUS NUMBER FILTER
@app.route('/speed_data/<date>/<bus_num>')
@app.route('/speed_data/<date>/<bus_num>/time_filter/<timestamp1>')
@app.route('/speed_data/<date>/<bus_num>/time_filter/<timestamp1>/<timestamp2>')

# DEPOT FILTER
@app.route('/speed_data/<date>/depot/<depot_name>/')
@app.route('/speed_data/<date>/depot/<depot_name>/time_filter/<timestamp1>')
@app.route('/speed_data/<date>/depot/<depot_name>/time_filter/<timestamp1>/<timestamp2>')
def speed_data_filter(date, bus_num = None, timestamp1 = None, timestamp2 = None, depot_name = None):
    date = date.replace('-', '_')
    conn = sqlite3.connect(f'data/bus_movements_{date}.db')
    cursor = conn.cursor()
    if(timestamp1) :
        timestamp1 = (str)(datetime.strptime(timestamp1, '%Y-%m-%dT%H:%M:%S'))
    if(timestamp2) :
        timestamp2 = (str)(datetime.strptime(timestamp2, '%Y-%m-%dT%H:%M:%S'))



    if(bus_num) :
        if(timestamp1) :
            if(timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                                'FROM distance_speed_realtime '
                                f'WHERE vehicle_id = "{bus_num}" '
                                f'AND timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}";')

            else :
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE vehicle_id = "{bus_num}" '
                               f'AND timestamp >=  "{timestamp1}"')

        else : # when only bus_num is given and no timestamp is given
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph)'
                            'FROM distance_speed_realtime '
                            f'WHERE vehicle_id = "{bus_num}";')

    elif depot_name :
        if (timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               f'AND depot = "{depot_name}"'
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               f'AND depot = "{depot_name}"'
                               'GROUP BY vehicle_id;')

        else:  # When only date is given and nothing else
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                           'FROM distance_speed_realtime '
                           f'WHERE depot = "{depot_name}"'
                           'GROUP BY vehicle_id;')


    else:
        if(timestamp1):
            if (timestamp2):
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}"'
                               'GROUP BY vehicle_id;')

            else:
                cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                               'FROM distance_speed_realtime '
                               f'WHERE timestamp >=  "{timestamp1}"'
                               'GROUP BY vehicle_id;')

        else : # When only date is given and nothing else
            cursor.execute('SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) '
                           'FROM distance_speed_realtime '
                           'GROUP BY vehicle_id;')

    rows = cursor.fetchall()
    if (len(rows) > 0 and rows[0][0] != None):
        for i in range(len(rows)):
            temp_dict = {
                'vehicle_id': rows[i][0],
                'timestamps': rows[i][1].split(','),
                'speed_kmph': rows[i][2].split(',')
            }
            rows[i] = temp_dict
    else:
        response = "No entries found "
        return response

    return jsonify(rows)






# BUS LIST FILTER
# - If the user wants to extract records of a list of specific buses, use this filter.
# - Send a POST request to the API with the list of buses in the JSON format in the request body.

# Correct set of values for data_type = ("overspeeding_data", "distance_data", "speed_data")
@app.route('/<data_type>/<date>/bus_list', methods=["POST"])
@app.route('/<data_type>/<date>/bus_list/<int:speed>', methods=["POST"])
@app.route('/<data_type>/<date>/bus_list/time_filter/<timestamp1>', methods=["POST"])
@app.route('/<data_type>/<date>/bus_list/time_filter/<timestamp1>/<timestamp2>', methods=["POST"])
def bus_list_filter(data_type, date, speed=None, timestamp1=None, timestamp2=None):
    date = date.replace('-', '_')

    conn = sqlite3.connect(f'data/bus_movements_{date}.db')
    data = request.get_json()

    if 'bus_list' not in data:
        response = "Please provide a list of buses in json format in request body"
        return response

    bus_list = data['bus_list']


    # Speed filter query has no use when the user asks for distance data
    if speed and data_type != "distance_data":
        query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                'FROM distance_speed_realtime ' \
                'WHERE vehicle_id IN ({}) ' \
                'AND overspeeding = "True" ' \
                f'AND CAST(speed_kmph AS float) >= {speed} ' \
                'GROUP BY vehicle_id;'.format(', '.join('?' * len(bus_list)))
    else:
        query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph)' \
                'FROM distance_speed_realtime ' \
                'WHERE vehicle_id IN ({}) ' \
                'AND overspeeding = "True" ' \
                'GROUP BY vehicle_id;'.format(', '.join('?' * len(bus_list)))

    if (timestamp1):
        timestamp1 = (str)(datetime.strptime(timestamp1, '%Y-%m-%dT%H:%M:%S'))
    if (timestamp2):
        timestamp2 = (str)(datetime.strptime(timestamp2, '%Y-%m-%dT%H:%M:%S'))

    if (timestamp1):
        if (timestamp2):
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                    'FROM distance_speed_realtime ' \
                    'WHERE vehicle_id IN ({}) ' \
                    f'AND timestamp BETWEEN  "{timestamp1}" AND "{timestamp2}" ' \
                    'AND overspeeding = "True" ' \
                    'GROUP BY vehicle_id;'.format(', '.join('?' * len(bus_list)))
        else:
            query = 'SELECT vehicle_id, GROUP_CONCAT(timestamp), GROUP_CONCAT(speed_kmph) ' \
                    'FROM distance_speed_realtime ' \
                    'WHERE vehicle_id IN ({}) ' \
                    f'AND timestamp >=  "{timestamp1}" ' \
                    'AND overspeeding = "True" ' \
                    'GROUP BY vehicle_id;'.format(', '.join('?' * len(bus_list)))

    # If data_type = speed_data OR distance_data, we remove the substring "AND overspeeding = "True""
    # from the string query
    # This will make all the queries suitable for speed_data
    if (data_type == "speed_data" or data_type == "distance_data"):
        query = query.replace('AND overspeeding = "True" ', '')


    elif (data_type != "overspeeding_data"):
        response = "Please enter correct data_type " \
                   "Correct set of values for data_type = " \
                   "('overspeeding_data', 'distance_data', 'speed_data')"
        return response

    # To make the queries suitable for distance data, just replace the
    # substring "speed_kmph" with "distance_km"
    if (data_type == "distance_data"):
        query = query.replace('speed_kmph', 'distance_km')


    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    cursor.execute(query, bus_list)

    rows = cursor.fetchall()
    if (len(rows) > 0 and rows[0][0] != None):
        for i in range(len(rows)):
            if(data_type != "distance_data") :
                temp_dict = {
                    'vehicle_id': rows[i][0],
                    'timestamps': rows[i][1].split(','),
                    'speed_kmph': rows[i][2].split(',')
                }
            else:
                temp_dict = {
                    'vehicle_id': rows[i][0],
                    'timestamps': rows[i][1].split(','),
                    'distance_km': rows[i][2].split(',')
                }
            rows[i] = temp_dict
    else:
        response = "No entries found "
        return response

    return jsonify(rows)





# FLEET UTILISATION
# - This returns the values for {Fleet Held, Scheduled Fleet, Actual Fleet on Road, Fleet Utilisation}
@app.route('/fleet_utilisation/<date>')
@app.route('/fleet_utilisation/<date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/fleet_utilisation/<date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/fleet_utilisation/<date>/bus_type/<bus_type>')
def fleet_utilisation_filter(date, depot_name=None, agency = None, bus_type = None):
    date = date.replace('-', '_')

    conn = sqlite3.connect(f'data/fleet_utilisation_{date}.db')
    cursor = conn.cursor()


    # FLEET HELD
    query = 'SELECT COUNT(vehicle_id) FROM fleet_utilisation'
    if(depot_name) :
        query += f' WHERE depot = "{depot_name}"'
    if(agency):
        query += f' WHERE agency = "{agency}"'
    if (bus_type):
        if (bus_type == "electric") :
            query += f' WHERE vehicle_id LIKE "DL51GD%"'
        else :
            query += f" WHERE ac = '{bus_type}'"

    cursor.execute(query)
    fleet_held = cursor.fetchall()[0][0]



    # FLEET SCHEDULED
    query = 'SELECT COUNT(vehicle_id) FROM fleet_utilisation ' \
            'WHERE scheduled_duties IS NOT NULL'
    if(depot_name):
        query += f' AND depot = "{depot_name}"'
    if (agency):
        query += f' AND agency = "{agency}"'
    if (bus_type):
        if (bus_type == "electric") :
            query += f' AND vehicle_id LIKE "DL51GD%"'
        else :
            query += f" AND ac = '{bus_type}'"
    cursor.execute(query)
    fleet_scheduled = cursor.fetchall()[0][0]



    # FLEET ON ROAD
    query = 'SELECT COUNT(vehicle_id) FROM fleet_utilisation ' \
            'WHERE actual_duties IS NOT NULL'
    if (depot_name):
        query += f' AND depot = "{depot_name}"'
    if (agency):
        query += f' AND agency = "{agency}"'
    if (bus_type):
        if (bus_type == "electric") :
            query += f' AND vehicle_id LIKE "DL51GD%"'
        else :
            query += f" AND ac = '{bus_type}'"
    cursor.execute(query)
    fleet_on_road = cursor.fetchall()[0][0]



    # FLEET UTILISATION
    fleet_utilised = 0
    if(fleet_held > 0):
        fleet_utilised = fleet_on_road / fleet_held

    fleet_utilised = "{:.2f}%".format(fleet_utilised * 100)


    temp_dict = {
        'fleet_held' : fleet_held,
        'fleet_scheduled' : fleet_scheduled,
        'fleet_on_road' : fleet_on_road,
        'fleet_utilisation' : fleet_utilised
    }

    if(depot_name):
        temp_dict['depot'] = depot_name
    if (agency):
        temp_dict['agency'] = agency
    if (bus_type):
        temp_dict['bus_type'] = bus_type


    return jsonify(temp_dict)





# DUTY EFFICIENCY
# - This function returns duty efficiency (Number of Actual Duties == Scheduled Duties) / Number of Scheduled Duties
@app.route('/duty_efficiency/<date>')
@app.route('/duty_efficiency/<date>/<bus_num>')
@app.route('/duty_efficiency/<date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/duty_efficiency/<date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/duty_efficiency/<date>/bus_type/<bus_type>')
def duty_efficiency_filter(date, bus_num = None, depot_name=None, agency = None, bus_type = None):
    date = date.replace('-', '_')

    conn = sqlite3.connect(f'data/fleet_utilisation_{date}.db')
    cursor = conn.cursor()

    query = "SELECT AVG(CAST(duty_efficiency AS decimal)) FROM fleet_utilisation"
    if(bus_num):
        query += f' WHERE vehicle_id = "{bus_num}"'
    if (depot_name):
        query += f' WHERE depot = "{depot_name}"'
    if (agency):
        query += f' WHERE agency = "{agency}"'
    if (bus_type):
        if (bus_type == "electric"):
            query += f' WHERE vehicle_id LIKE "DL51GD%"'
        else:
            query += f" WHERE ac = '{bus_type}'"

    cursor.execute(query)
    duty_efficiency = cursor.fetchall()[0][0]
    duty_efficiency = "{:.2f}%".format(duty_efficiency*100)

    temp_dict = {'duty_efficiency': duty_efficiency}

    if (bus_num):
        temp_dict['vehicle_id'] = bus_num
    if (depot_name):
        temp_dict['depot'] = depot_name
    if (agency):
        temp_dict['agency'] = agency
    if (bus_type):
        temp_dict['bus_type'] = bus_type

    return jsonify(temp_dict)





# SCHEDULE ADHERENCE
# - This function returns schedule adherence - scheduled duty start time vs actual outshed
@app.route('/schedule_adherence/<date>')
@app.route('/schedule_adherence/<date>/<bus_num>')
@app.route('/schedule_adherence/<date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/schedule_adherence/<date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/schedule_adherence/<date>/bus_type/<bus_type>')
def schedule_adherence_filter(date, bus_num = None, depot_name=None, agency = None, bus_type = None):
    date = date.replace('-', '_')

    conn = sqlite3.connect(f'data/schedule_adherence_{date}.db')
    cursor = conn.cursor()

    shift_start_column_exists = column_exists(conn, "schedule_adherence", "shift_1_scheduled_start_time")
    query = "SELECT vehicle_id, shift_1_schedule_adherence, shift_2_schedule_adherence FROM schedule_adherence"
    if shift_start_column_exists:
        query = "SELECT vehicle_id, shift_1_schedule_adherence, shift_2_schedule_adherence, shift_1_scheduled_start_time, shift_1_actual_start_time, shift_2_scheduled_start_time, shift_2_actual_start_time FROM schedule_adherence"
    if(bus_num):
        query += f' WHERE vehicle_id = "{bus_num}"'
    if (depot_name):
        query += f' WHERE depot = "{depot_name}"'
    if (agency):
        query += f' WHERE agency = "{agency}"'
    if (bus_type):
        if (bus_type == "electric"):
            query += f' WHERE vehicle_id LIKE "DL51GD%"'
        else:
            query += f" WHERE ac = '{bus_type}'"

    cursor.execute(query)
    result = cursor.fetchall()

    # Get the column names from the cursor's description
    columns = [description[0] for description in cursor.description]

    # Create a DataFrame from the fetched data and column names
    schedule_adherence_df = pd.DataFrame(result, columns=columns)
    if not shift_start_column_exists:
        schedule_adherence_df["shift_1_scheduled_start_time"] = ""
        schedule_adherence_df["shift_1_actual_start_time"] = ""
        schedule_adherence_df["shift_2_scheduled_start_time"] = ""
        schedule_adherence_df["shift_2_actual_start_time"] = ""
    temp_dict = schedule_adherence_df.to_dict(orient="records")


    conn.close()
    return jsonify(temp_dict)





# SCHEDULED AND ACTUAL DISTANCE
# - This function returns scheduled distance and actual distance covered for a bus on a day
@app.route('/scheduled_and_actual_dist/<date>/<bus_num>')
def scheduled_and_actual_dist_filter(date, bus_num):
    date = date.replace('-', '_')

    # Scheduled Distance
    conn = sqlite3.connect(f'data/scheduled_dist_{date}.db')
    cursor = conn.cursor()
    query = 'SELECT total_sum_route_length_km FROM scheduled_dist_table' \
            f' WHERE vehicle_id = "{bus_num}"'
    cursor.execute(query)
    scheduled_dist_km = cursor.fetchall()[0][0]


    # Actual Distance
    actual_dist_km = 0
    json_file_name = f"data/cache_{date}.json"

    # Load the JSON data from the file
    with open(json_file_name, 'r') as file:
        data = json.load(file)

    if(bus_num in data):
        actual_dist_km = data[bus_num][4]


    temp_dict = {
        'vehicle_id' : bus_num,
        'scheduled_dist_km' : scheduled_dist_km,
        'actual_dist_km' : (float)(actual_dist_km)
    }

    return jsonify(temp_dict)




# AVERAGE DISTANCE COVERED
# - This function return average distance (km) covered by a bus between two date values
@app.route('/calculate_average_distance/<start_date>/<end_date>/<bus_num>')
def calculate_average_distance(start_date,end_date,bus_num):

    start_date_str = start_date
    end_date_str = end_date

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Initialize variables to store sum and count for calculating the average
    total_sum = 0
    count = 0
    not_found_dates = []

    # Iterate through the date range
    current_date = start_date
    while current_date <= end_date:
        # Construct the file name using the current date
        file_name = f"data/cache_{current_date.strftime('%Y_%m_%d')}.json"

        # Check if the file exists
        if os.path.isfile(file_name):
            # Read the JSON file
            with open(file_name, 'r') as file:
                data = json.load(file)

            # Check if the bus number is a key in the JSON data
            if bus_num in data:
                # Extract the 5th value from the list
                values = data[bus_num]
                if len(values) >= 5:
                    value = values[4]
                    # Add to the sum and increment the count
                    total_sum += (float)(value)
                    count += 1
        else:
            not_found_dates.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next day
        current_date += timedelta(days=1)

    # Calculate the average
    if count > 0:
        average = total_sum / count
    else:
        average = 0

    temp_dict = {
        "vehicle_id": bus_num,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "average_distance_covered_km": average,
        "not_found_dates" : not_found_dates
    }


    return jsonify(temp_dict)





# AVERAGE DUTY EFFICIENCY
# - This function return average duty efficiency between two date values
@app.route('/calculate_average_duty_efficiency/<start_date>/<end_date>/')

@app.route('/calculate_average_duty_efficiency/<start_date>/<end_date>/<bus_num>')
@app.route('/calculate_average_duty_efficiency/<start_date>/<end_date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/calculate_average_duty_efficiency/<start_date>/<end_date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/calculate_average_duty_efficiency/<start_date>/<end_date>/bus_type/<bus_type>')
def calculate_average_duty_efficiency(start_date,end_date,bus_num = None, depot_name=None, agency = None, bus_type = None):

    start_date_str = start_date
    end_date_str = end_date

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Initialize variables to store sum and count for calculating the average
    total_sum = 0
    count = 0
    not_found_dates = []

    # Iterate through the date range
    current_date = start_date
    while current_date <= end_date:
        # Construct the file name using the current date
        file_name = f"data/fleet_utilisation_{current_date.strftime('%Y_%m_%d')}.db"

        # Check if the file exists
        if os.path.isfile(file_name):
            # Connect to the .db file
            conn = sqlite3.connect(file_name)
            cursor = conn.cursor()

            query = "SELECT AVG(CAST(duty_efficiency AS decimal)) FROM fleet_utilisation"
            if (bus_num):
                query += f' WHERE vehicle_id = "{bus_num}"'
            if (depot_name):
                query += f' WHERE depot = "{depot_name}"'
            if (agency):
                query += f' WHERE agency = "{agency}"'
            if (bus_type):
                if (bus_type == "electric"):
                    query += f' WHERE vehicle_id LIKE "DL51GD%"'
                else:
                    query += f" WHERE ac = '{bus_type}'"

            cursor.execute(query)
            duty_efficiency = cursor.fetchall()[0][0]

            total_sum += (float)(duty_efficiency)
            count += 1

        else:
            not_found_dates.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next day
        current_date += timedelta(days=1)

    # Calculate the average
    if count > 0:
        average = total_sum / count
    else:
        average = 0

    average = "{:.2f}%".format(average * 100)

    temp_dict = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "average_duty_efficiency": average,
        "not_found_dates" : not_found_dates
    }

    if (bus_num):
        temp_dict['vehicle_id'] = bus_num
    if (depot_name):
        temp_dict['depot'] = depot_name
    if (agency):
        temp_dict['agency'] = agency
    if (bus_type):
        temp_dict['bus_type'] = bus_type


    return jsonify(temp_dict)





# AVERAGE FLEET UTILISATION
# - This function return average fleet utilisation between two date values
@app.route('/calculate_average_fleet_utilisation/<start_date>/<end_date>/')

@app.route('/calculate_average_fleet_utilisation/<start_date>/<end_date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/calculate_average_fleet_utilisation/<start_date>/<end_date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/calculate_average_fleet_utilisation/<start_date>/<end_date>/bus_type/<bus_type>')
def calculate_average_fleet_utilisation(start_date,end_date, depot_name=None, agency = None, bus_type = None):
    start_date_str = start_date
    end_date_str = end_date

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Initialize variables to store sum and count for calculating the average
    total_sum = 0
    count = 0
    not_found_dates = []

    # Iterate through the date range
    current_date = start_date
    while current_date <= end_date:

        file_name = f"data/fleet_utilisation_{current_date.strftime('%Y_%m_%d')}.db"

        # Check if the file exists
        if os.path.isfile(file_name):
            # Connect to the .db file
            conn = sqlite3.connect(file_name)
            cursor = conn.cursor()

            # FLEET HELD
            query = 'SELECT COUNT(vehicle_id) FROM fleet_utilisation'
            if(depot_name) :
                query += f' WHERE depot = "{depot_name}"'
            if(agency):
                query += f' WHERE agency = "{agency}"'
            if (bus_type):
                if (bus_type == "electric") :
                    query += f' WHERE vehicle_id LIKE "DL51GD%"'
                else :
                    query += f" WHERE ac = '{bus_type}'"

            cursor.execute(query)
            fleet_held = cursor.fetchall()[0][0]

            # FLEET ON ROAD
            query = 'SELECT COUNT(vehicle_id) FROM fleet_utilisation ' \
                    'WHERE actual_duties IS NOT NULL'
            if (depot_name):
                query += f' AND depot = "{depot_name}"'
            if (agency):
                query += f' AND agency = "{agency}"'
            if (bus_type):
                if (bus_type == "electric"):
                    query += f' AND vehicle_id LIKE "DL51GD%"'
                else:
                    query += f" AND ac = '{bus_type}'"
            cursor.execute(query)
            fleet_on_road = cursor.fetchall()[0][0]

            # FLEET UTILISATION
            fleet_utilised = 0
            if (fleet_held > 0):
                fleet_utilised = fleet_on_road / fleet_held

            print(current_date)
            print(fleet_utilised)

            total_sum += (float)(fleet_utilised)
            count += 1

        else:
            not_found_dates.append(current_date.strftime("%Y-%m-%d"))

        # Move to the next day
        current_date += timedelta(days=1)

    # Calculate the average
    if count > 0:
        average = total_sum / count
    else:
        average = 0

    average = "{:.2f}%".format(average * 100)

    temp_dict = {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "average_fleet_utilisation": average,
        "not_found_dates" : not_found_dates
    }


    if (depot_name):
        temp_dict['depot'] = depot_name
    if (agency):
        temp_dict['agency'] = agency
    if (bus_type):
        temp_dict['bus_type'] = bus_type


    return jsonify(temp_dict)





# Helper Function for calculate_total_distance
def get_bus_list(depot = None, agency = None, bus_type = None):
    file_path = 'data/all_buses_delhi.csv'
    df = pd.read_csv(file_path)

    if(depot):
        # Filter the DataFrame based on the specified depot
        filtered_df = df[df['depot'] == depot]
    elif(agency):
        # Filter the DataFrame based on the specified agency
        filtered_df = df[df['agency'] == agency]

    elif(bus_type):
       if(bus_type == 'electric'):
           # Specify the prefix to search for
           prefix_to_search = 'DL51GD'

           # Filter rows based on the specified prefix in the 'reg_num' column
           filtered_df = df[df['reg_num'].str.startswith(prefix_to_search)]
       else:
           filtered_df = df[df['ac'] == bus_type]
    else:
        filtered_df = df

    # Extract the 'reg_num' column as a list
    reg_num_list = filtered_df['reg_num'].tolist()

    return reg_num_list



# TOTAL DISTANCE COVERED
# - This function returns total distance covered by a bus between two date values
@app.route('/calculate_total_distance/<start_date>/<end_date>/')

@app.route('/calculate_total_distance/<start_date>/<end_date>/<bus_num>')
@app.route('/calculate_total_distance/<start_date>/<end_date>/depot/<depot_name>')

# Correct set of values for agency = {'dtc', 'dimts'}
@app.route('/calculate_total_distance/<start_date>/<end_date>/agency/<agency>')

# Correct set of values for bus_type = {'ac', 'nac', 'electric'}
@app.route('/calculate_total_distance/<start_date>/<end_date>/bus_type/<bus_type>')
def calculate_total_distance(start_date, end_date, bus_num = None, depot_name=None, agency = None, bus_type = None):
    start_date_str = start_date
    end_date_str = end_date

    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    response_df = pd.DataFrame(columns=["vehicle_id", "start_date", "end_date", "total_distance_covered_km",
                                        "not_found_dates"])

    # Get a list of bus numbers for the filter
    if(bus_num):
        bus_list = [bus_num,]
    elif(depot_name):
        bus_list = get_bus_list(depot=depot_name)
    elif (agency):
        bus_list = get_bus_list(agency=agency)
    elif (bus_type):
        bus_list = get_bus_list(bus_type=bus_type)
    else:
        bus_list = get_bus_list()

    # Iterate through the date range
    current_date = start_date
    while current_date <= end_date:
        # Construct the file name using the current date
        file_name = f"data/cache_{current_date.strftime('%Y_%m_%d')}.json"

        # Check if the file exists
        if os.path.isfile(file_name):
            # Read the JSON file
            with open(file_name, 'r') as file:
                data = json.load(file)

            for bus_number in bus_list:

                # Check if the bus number is a key in the JSON data
                if bus_number in data:
                    # Extract the 5th value from the list
                    values = data[bus_number]

                    if len(values) >= 5:
                        value = values[4]
                        # print(current_date)
                        # print(values[4])
                        # print()
                        # Add to the sum and increment the count
                        dist = (float)(value)

                        if bus_number in response_df['vehicle_id'].values:
                            response_df.loc[
                                response_df['vehicle_id'] == bus_number, 'total_distance_covered_km'] += dist
                        else:
                            new_entry = {
                                "vehicle_id": bus_number,
                                "start_date": start_date_str,
                                "end_date": end_date_str,
                                "total_distance_covered_km": dist,
                                "not_found_dates": ""
                            }

                            new_df = pd.DataFrame([new_entry])
                            response_df = pd.concat([response_df, new_df], ignore_index=True)

                else:  # bus number is not present in json data, so add entry to "not_found_dates" column
                    if bus_number in response_df['vehicle_id'].values:
                        response_df.loc[
                            response_df['vehicle_id'] == bus_number, "not_found_dates"] += current_date.strftime(
                            "%Y-%m-%d") + ","
                    else:
                        new_entry = {
                            "vehicle_id": bus_number,
                            "start_date": start_date_str,
                            "end_date": end_date_str,
                            "total_distance_covered_km": 0,
                            "not_found_dates": ""
                        }

                        new_df = pd.DataFrame([new_entry])
                        response_df = pd.concat([response_df, new_df], ignore_index=True)

        else:  # json file does not exist, so add entry to "not_found_dates" column
            for bus_number in bus_list:
                if bus_number in response_df['vehicle_id'].values:
                    response_df.loc[
                        response_df['vehicle_id'] == bus_number, "not_found_dates"] += current_date.strftime(
                        "%Y-%m-%d") + ","

                else:
                    new_entry = {
                        "vehicle_id": bus_number,
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        "total_distance_covered_km": 0,
                        "not_found_dates": ""
                    }

                    new_df = pd.DataFrame([new_entry])
                    response_df = pd.concat([response_df, new_df], ignore_index=True)

        # Move to the next day
        current_date += timedelta(days=1)


    # Convert "not_found_dates" to a list of strings without empty strings
    response_df["not_found_dates"] = response_df["not_found_dates"].apply(
        lambda x: [item for item in x.split(',') if item])

    if (depot_name):
        response_df["depot"] = depot_name
    elif (agency):
        response_df["agency"] = agency
    elif (bus_type):
        response_df["bus_type"] = bus_type

    # Convert the DataFrame to a list of dictionaries
    list_of_dicts = response_df.to_dict(orient='records')

    return jsonify(list_of_dicts)



# This ensures API does not create favicon.ico file
@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('empty.ico')


if __name__ == '__main__':
    app.run(debug=True)
