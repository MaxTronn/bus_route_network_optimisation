# API Endpoint
# https://depot-monitoring.chartr.in/
import pandas as pd
from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os
from datetime import datetime
from schedule_adherence import column_exists

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






# Returns timestamped distance data for given date with optional parameters of bus number and timestamps and depot
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

    # temp_dict = {'schedule_adherence': schedule_adherence}
    #
    # if (bus_num):
    #     temp_dict['vehicle_id'] = bus_num
    # if (depot_name):
    #     temp_dict['depot'] = depot_name
    # if (agency):
    #     temp_dict['agency'] = agency
    # if (bus_type):
    #     temp_dict['bus_type'] = bus_type
    conn.close()
    return jsonify(temp_dict)


# This ensures API does not create favicon.ico file
@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('empty.ico')


if __name__ == '__main__':
    app.run(debug=True)
