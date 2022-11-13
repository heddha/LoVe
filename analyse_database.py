import time
from importlib import reload
import csv
from pathlib import Path
import sqlite3 as sl
import h3

def print_content(db):
     with db:
         data = db.execute("SELECT * FROM COORDINATES")
         for row in data:
             print(row)


# 21916 Sensors alltogether
def get_no_of_sensors(db):
    with db:
        data = db.execute("SELECT COUNT(DISTINCT sensor_id) FROM COORDINATES")
        for row in data:
            print(row)

# 58491974 
def get_no_of_coords(db):
    with db:
        data = db.execute("SELECT sensor_id, COUNT(lat) FROM COORDINATES GROUP BY sensor_id")
        for row in data:
            print(row)

# 2668.916 coordinates per sensor

def get_avg_no_of_coords(db):
    with db:
        counter = 0
        b_sum = 0
        data = db.execute("SELECT sensor_id, COUNT(lat) FROM COORDINATES GROUP BY sensor_id")
        for row in data:
            counter += 1
            a, b = row
            b_sum += b
        print("Counted" , counter, "distinct sensors. On average, every sensor sensed", b_sum / counter, "coordinates. The total number of ADS-B records recorded is", b_sum )

def get_cell(db):
    with db:
        data = db.execute("SELECT * FROM COORDINATES")
        for row in data:
            id, sid, lat, lng = row
            print(sid, lat, lng, h3.latlng_to_cell(lat,lng,6))



# We have 58491973 distinct rows and 58382552 distinct hashes
def get_no_of_hashes(db):
    with db:
        counter = 0
        data = db.execute("SELECT DISTINCT hash FROM COORDINATES")
        data2 = db.execute("SELECT id FROM COORDINATES ORDER BY id DESC LIMIT 1;")
        for row in data:
            counter += 1
#            print(row)
        for row in data2:
            print("We have", row[0], "distinct rows and", counter, "distinct hashes")


# call like so: analyse_database.construct_h3_helptable(testdb, "H3SENSORS_ALL_CELLSIZE_5", 5)
# already modified to work with postgresql

def construct_h3_helptable(db, tablename, cellsize):
    with db:
        db.execute("CREATE TABLE " + tablename + " (id SERIAL PRIMARY KEY, h3id TEXT, sensor_id INTEGER);")
        counter = 0
        db.execute("SELECT id, sensor_id, lat, long FROM COORDINATES")
        data = db.fetchall()
        for row in data:
            counter += 1
            sensor_id = row[1]
            lat = row[2]
            lng = row[3]
            h3id = h3.latlng_to_cell(lat, lng, cellsize)
            for i in sensor_id:
                db.execute("INSERT INTO "+ tablename + "(h3id, sensor_id) values ('" + h3id + "', " + str(i) + ")")
            if (counter % 100000) == 0:
                print("managed",  counter,  "entries")
        print("transferred all entries to" , tablename)
        #db.commit()


def construct_h3_table_reduced(db, tablename_in, tablename_out, cellsize):
    with db:
        db.execute("CREATE TABLE " + tablename_out + " (id SERIAL PRIMARY KEY, h3id TEXT, sensor_id INTEGER, amount INTEGER);")
        counter = 0
        db.execute("SELECT id, sensor_set, lat, long FROM " + tablename_in)
        data = db.fetchall()
        h3dict = {}
        for row in data:
            counter += 1
            sensor_id = row[1]
            lat = row[2]
            lng = row[3]
            h3id = h3.latlng_to_cell(lat, lng, cellsize)
            for id in sensor_id:
                tmpid = h3id + "," + str(id)
                try:
                    h3dict[tmpid] = h3dict[tmpid]+1
                except(KeyError):
                    h3dict[tmpid] = 1
            if (counter % 100000) == 0:
                print("managed",  counter,  "entries")
        #print(h3dict)
        for e in h3dict:
            h3id, id = e.split(",")
            amount = h3dict[e]
            db.execute("INSERT INTO " + tablename_out + " (h3id, sensor_id, amount) values ('" + h3id + "', '" + id + "', " + str(amount) + ")")
        print("transferred all entries to", tablename_out)
        #db.commit()'''




# analyse_database.construct_h3_amounttable(adsbdb, "H3SENSORS_ALL_CELLSIZE_5", "H3SENSORS_CELLSIZE_5")
def construct_h3_amounttable(db, transfertablename, newtablename):
    db.execute("CREATE TABLE "+ newtablename + " (id SERIAL PRIMARY KEY, h3id TEXT, sensor_id INTEGER, amount INTEGER);")
    with db:
        db.execute("insert into " + newtablename + " (h3id, sensor_id, amount) SELECT h3id, sensor_id, count(h3id) from "+ transfertablename + " group by h3id, sensor_id")
        #db.commit()


def how_many_in_region(db, h3id, cellsize):
    data = db.execute("SELECT id, sensor_id, lat, long from coordinates")
    count = 0
    for row in data:
        h3res = h3.latlng_to_cell(row[2], row[3], cellsize)
        if h3res == h3id:
            count = count + 1
    print(count)


def get_all_entries_for_h3id(db, table, h3id):
    data = db.execute("select h3id, sensor_id, amount from " + table + " where h3id = \"" + h3id + "\"")
    for row in data:
        print(row)
    return data


def get_all_sensor_ids(db, table):
    data = db.execute("select distinct sensor_id from "+table)
    for row in data:
        print(row)


# analyse_database.get_no_of_distinct_h3ids(adsbdb, "H3SENSORS_CELLSIZE_5")
# result on big database: 373448 for cellsize 5, 1479017 for cellsize 6
def get_no_of_distinct_h3ids(db, table):
    data = db.execute("select count(distinct h3id) from " +table)
    for row in data:
        print(row)


def get_meaningfulness_sensor_coords(db, table, sensor_id, lat, lng, cellsize):
    h3id = h3.latlng_to_cell(lat, lng, cellsize)
    #data = get_all_entries_for_h3id(db, table, h3id)
    db.execute("select h3id, sensor_id, amount from " + table + " where h3id = '" + h3id + "'")
    data = db.fetchall()
    datalength = 0 # amount of rows found in query
    msgs_in_area = 0 # all messages recorded in area
    valueset = set() # a set of all amounts contained in the query
    value_in_question = 0 # amount of ADS-B messages for this sensor in this hexagon
    for row in data:
        datalength += 1
        msgs_in_area += row[2]
        valueset.add(row[2])
        #print(row)
        if row[1] == sensor_id:
            value_in_question = row[2]
    return msgs_in_area, datalength, valueset, value_in_question


# for flightradar-db:
# analyse_database.evaluate_trust(analyse_database.get_meaningfulness_sensor_coords(europe, "H3SENSORS_CELLSIZE_5", 1922, 30.81409, -12.6626, 5))
# for openSkyNetwork:
# analyse_database.evaluate_trust(analyse_database.get_meaningfulness_sensor_coords(cur, "H3SENSORS_5", -1408234233, 40.522745823890354, -3.5540420836791293, 5),  cur, -1408234233)

def evaluate_trust_verbose(inputlist, db, sensor_id):
    msgs_in_area, datalength, values, value_in_question = inputlist
    #print("alltogether, there are", msgs_in_area, "messages in the hexagon in question. They were recorded by", datalength, "sensors.")
    lowest_value = min(values)
    highest_value = max(values)
    if value_in_question == 0:
        print("The sensor in question has not recorded a single signal in this area yet. The signal might have been spoofed.")
        db.execute("SELECT * from COORDINATES where " + str(sensor_id) + " = ANY (sensor_id)")
        data = db.fetchall()
        lat_min = 500
        lat_max = 500
        long_min = 500
        long_max = 500
        count_rows = 0
        for row in data:
            count_rows += 1
            if lat_min == 500:
                lat_min = row[2]
                lat_max = row[2]
                long_min = row[3]
                long_max = row[3]
            lat_min = min(lat_min, row[2])
            lat_max = max(lat_max, row[2])
            long_min = min(long_min, row[3])
            long_max = max(long_max, row[3])
            #if row[3] < long_min:
            #    long_min = row[3]
            #elif row[3] > long_max:
            #    long_max = row[3]
        if count_rows == 0:
            print("Sensor doesn't exist in data set")
            return 0
        print("It typically recorded signals in the following region:")
        print("lat: ", lat_min, "to", lat_max)
        print("long: ", long_min, "to", long_max)
        return 0
    elif value_in_question == highest_value:
        print("The receiving sensor has the highest trust score in this area. There were", datalength, "sensors,", msgs_in_area, "messages in the hexagon in question and this sensor has been received", value_in_question,  "times. The signal was likely not spoofed.")
        return 4
    elif value_in_question >= round(msgs_in_area/datalength):
        print("The receiving sensor is among the more trustworthy sensors in this area. There were", datalength, "sensors,", msgs_in_area, "messages in the hexagon in question and this sensor has been received", value_in_question,  "times. The signal was likely not spoofed.")
        return 3
    elif value_in_question == lowest_value:
        print("The receiving sensor is among the least trustworthy sensors in this area. There were", datalength, "sensors,", msgs_in_area, "messages in the hexagon in question and this sensor has been received", value_in_question,  "times. The signal was likely not spoofed.")
        return 1
    elif value_in_question < round(msgs_in_area/datalength):
        print("The receiving sensor is among the less trustworthy sensors in this area. There were", datalength, "sensors,", msgs_in_area, "messages in the hexagon in question and this sensor has been received", value_in_question,  "times. The signal was likely not spoofed.")
        return 2
    return 0

def evaluate_trust(inputlist, db, sensor_id):
    msgs_in_area, datalength, values, value_in_question = inputlist
    #print("alltogether, there are", msgs_in_area, "messages in the hexagon in question. They were recorded by", datalength, "sensors.")
    if value_in_question == 0:
        # The sensor in question has not recorded a single signal in this area yet.
        return 0
    lowest_value = min(values)
    highest_value = max(values)
    if value_in_question == highest_value:
        # The receiving sensor has the highest trust score in this area.
        return 4
    elif value_in_question >= round(msgs_in_area/datalength):
        # The receiving sensor is among the more trustworthy sensors in this area.
        return 3
    elif value_in_question == lowest_value:
        # The receiving sensor is among the least trustworthy sensors in this area.
        return 1
    elif value_in_question < round(msgs_in_area/datalength):
        # The receiving sensor is among the less trustworthy sensors in this area.
        return 2
    return 0


def evaluate_trust_minimal(db, table, sensor_id, h3id):
    db.execute("select amount from " + table + " where h3id = '" + h3id + "' and sensor_id = " + str(sensor_id) + ";")
    data = db.fetchone()
    try:
        value_in_question = data[0]  # amount of ADS-B messages for this sensor in this hexagon
    except(TypeError):
        return 0  # The sensor in question has not recorded a single signal in this area yet.
    if value_in_question > 0:
        return 1
    else:
        return 0




'''
Very high confidence = 4
High confidence = 3
Medium confidence = 2
Low confidence = 1
No confidence = 0
Sensor doesn't exist = -1
'''

def get_sensor_region(db, sensor_id):
    #db.execute("SELECT sensor_set, lat, long from COORDINATES_hour3 where array[" + str(sensor_id) + "] && sensor_set")
    db.execute("SELECT sensor_set, lat, long from flightradar_coordinates_reduced where array[" + str(sensor_id) + "] && sensor_set")
    data = db.fetchall()
    lat_min = 500
    lat_max = 500
    long_min = 500
    long_max = 500
    count_rows = 0
    #latlonglist = []
    last_lat = 0
    last_long = 0
    for row in data:
        #print("got the following values for sensor_id ", sensor_id, ":\n")
        #print(row)
        count_rows += 1
        if lat_min == 500:
            lat_min = row[1]
            lat_max = row[1]
            long_min = row[2]
            long_max = row[2]
        lat_min = min(lat_min, row[1])
        lat_max = max(lat_max, row[1])
        long_min = min(long_min, row[2])
        long_max = max(long_max, row[2])
    if count_rows == 0:
        print("Sensor doesn't exist in data set")
        return 0
    return lat_min, lat_max, long_min, long_max

# fill coordinates_europe table from coordinates:
# insert into COORDINATES_EUROPE (sensor_id, lat, long, hash) SELECT sensor_id, lat, long, hash from COORDINATES WHERE lat>=30 AND lat<=75 AND long>=-25 AND long<=45;




