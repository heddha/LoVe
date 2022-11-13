import csv
import random

import h3
import psycopg2
import analyse_database
from random import randrange



rootdb = psycopg2.connect(
    host="localhost",
    database="root",
    user="root",
    password="root",
    port=5433)
cur = rootdb.cursor()

opensky = psycopg2.connect(
    host="localhost",
    database="opensky",
    user="root",
    password="root",
    port=5433)
cur2 = opensky.cursor()


# creates a set of untrustworthy coordinates for specific sensors
def construct_false_testdata(cur1, cur2, length, tablename):
    cur1.execute("select distinct sensor_id from coordinates_hour3_h34")
    data = cur1.fetchmany(length)
    print("got all sensor_ids and h3ids from coordinates_h35-table")
    tmp_storage = []
    counter = 0
    h3id = ""
    for i in data:
        sensor_id = i[0]
        #h3id = i[1]
        #print("got to line 39")
        lat_min, lat_max, long_min, long_max = analyse_database.get_sensor_region(cur1, sensor_id)
        tmp_storage.append([sensor_id, lat_min, lat_max, long_min, long_max])
        counter += 1
        if counter % 30 == 0:
            print("Managed to map" , counter, "entries to their coordinates")
    '''
    tmp_storage = [[1805968165, 48.69047520524364, 54.56089860301906, -6.100540161132812, 3.020220075334822], [1801273114, 43.54468394134004, 53.23820986990201, 3.530484713040865, 13.79830850137247], [91083, 44.95530985169491, 52.22919011520127, -3.554077148437557, 8.655606783353365], [-1408233805, 50.3966612735037, 54.46523375430349, -4.664481026785666, 2.719192504882812], [-1408234138, 49.10394287109375, 55.71414184570312, 3.209969656808036, 14.03724670410156], [955906058, 48.45878471762447, 53.16502975205243, 1.395189439928209, 9.97161865234375], [-1408233984, 45.32597351074219, 51.60589599609375, -4.924404746607763, 4.614691483347039]]
    '''
    cur1.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_id INTEGER, lat FLOAT, long FLOAT, label INTEGER);")
    print("Done getting sensor-coordinate-map. How moving on to create the data set")
    counter = 0
    while length > 0:
        counter += 1
        if counter % 1000 == 0:
            print("Managed to insert", counter, "entries into database")
        length -= 1
        rand_no = randrange(len(tmp_storage))
        rand_sensor_array = tmp_storage[rand_no]
        rand_sensor, lat_min, lat_max, long_min, long_max = rand_sensor_array
        label = 0
        #if label:
        #new_lat, new_long = latlonglist
        #new_lat = random.uniform(lat_min, lat_max)
        #new_long = random.uniform(long_min, long_max)
        rand1 = randrange(2)
        rand2 = randrange(2)
        new_lat = 0
        new_long = 0
        latrand = random.uniform(0.1, 10.0)
        longrand = random.uniform(0.1, 10.0)
        if rand1:
            new_lat = lat_min - latrand
        else:
            new_lat = lat_max + latrand
        if rand2:
            new_long = long_min - longrand
        else:
            new_long = long_max + longrand
        #print(rand_sensor, lat_min, "value =", new_lat, lat_max, long_min, "value =", new_long,  long_max, "label =", label )
        cur1.execute("insert into " + tablename + " (sensor_id, lat, long, label) values ( " + str(rand_sensor) + ", " + str(new_lat) + ", " + str(new_long) + ", " + str(label) + " )")


def construct_true_testdata(db, length, tablename, myfile):
    db.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_id INTEGER, lat FLOAT, long FLOAT, label INTEGER);")
    construct_true_testdata_no_db(db, length, tablename, myfile)

def construct_true_testdata_no_db(db, length, tablename, myfile):
    with open(myfile, newline='\n') as csvfile:
        allelements = csv.DictReader(csvfile, delimiter=',')
        counter = 0
        myindex = 0
        for row in allelements:
            lati = row['lat']
            longi = row['lon']
            sensor_id = row['group_concat(cast(serials.item as string)']
            label = 1
            counter += 1
            sql = "INSERT INTO " + tablename + " (sensor_id, lat, long, label) values( '" + str(sensor_id) + "', " + str(lati) + ', ' + str(longi) + ', ' + str(label) + ')'
            db.execute(sql)
            if counter % 10000 == 0:
                print("managed", counter, "entries")
            if counter == length:
                break

def get_mixed_trainingset(cur1, cur2, length, tablename, myfile):
    construct_false_testdata(cur1, cur2, round(length/2), tablename)
    construct_true_testdata_no_db(cur1, round(length/2), tablename, myfile)



def construct_testdata_flightradar(cur1, cur2, length, tablename):
    cur1.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_id INTEGER, lat FLOAT, long FLOAT, label INTEGER);")
    cur1.execute("insert into " + tablename + "(sensor_id, lat, long, label) select sensor_id, lat, long, label from true_testdata_flightradar_500000;")
    rootdb.commit()
    print("done copying over the true data")
    length1 = round(length / 2)
    cur1.execute("select distinct sensor_id from flightradar_coordinates_reduced_h32")
    data = cur1.fetchmany(length1)
    print("got all sensor_ids from coordinates_h32-table")
    tmp_storage = []
    counter = 0
    h3id = ""
    for i in data:
        sensor_id = i[0]
        #h3id = i[1]
        #print("got to line 39")
        lat_min, lat_max, long_min, long_max = analyse_database.get_sensor_region(cur1, sensor_id)
        tmp_storage.append([sensor_id, lat_min, lat_max, long_min, long_max])
        counter += 1
        if counter % 30 == 0:
            print("Managed to map" , counter, "entries to their coordinates")
    print("Done getting sensor-coordinate-map. How moving on to create the data set")
    counter = 0
    length2 = length1
    while length1 > 0:
        counter += 1
        if counter % 10000 == 0:
            print("Managed to insert", counter, "entries into database")
        length1 -= 1
        rand_no = randrange(len(tmp_storage))
        rand_sensor_array = tmp_storage[rand_no]
        rand_sensor, lat_min, lat_max, long_min, long_max = rand_sensor_array
        label = 0
        rand1 = randrange(2)
        rand2 = randrange(2)
        new_lat = 0
        new_long = 0
        latrand = random.uniform(0.1, 10.0)
        longrand = random.uniform(0.1, 10.0)
        if rand1:
            new_lat = lat_min - latrand
        else:
            new_lat = lat_max + latrand
        if rand2:
            new_long = long_min - longrand
        else:
            new_long = long_max + longrand
        #print(rand_sensor, lat_min, "value =", new_lat, lat_max, long_min, "value =", new_long,  long_max, "label =", label )
        cur1.execute("insert into " + tablename + " (sensor_id, lat, long, label) values ( " + str(rand_sensor) + ", " + str(new_lat) + ", " + str(new_long) + ", " + str(label) + " )")

def construct_true_dataset_flightradar(db, cur1,  orig_table, test_table, length):
    cur1.execute("select id as exact_count from "+ orig_table)
    all_ids = [x[0] for x in cur1.fetchall()]
    #print(all_ids)
    len_all_ids = len(all_ids)
    print(len_all_ids)
    rand_list = random.sample(all_ids, length)
    print("rand_list=", rand_list, "\n len(rand_list)=", len(rand_list))
    formatted_rand_list = "(" + str(rand_list)[1:-1] + ")"
    cur1.execute("create table " + test_table + " (id SERIAL PRIMARY KEY, sensor_id INTEGER, lat FLOAT, long FLOAT, label INTEGER);")
    cur1.execute("WITH moved_rows AS (DELETE FROM " + orig_table + " where id in " + formatted_rand_list + " returning id, sensor_set, lat, long) insert into " + test_table + " select id, sensor_set[1], lat, long, 1 from moved_rows;")

def construct_testdata_flightradar_short_helper(cur1, cur2, tablename):
    cur1.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_id INTEGER, lat FLOAT, long FLOAT, label INTEGER);")
    cur1.execute("insert into " + tablename + "(sensor_id, lat, long, label) select sensor_id, lat, long, label from true_testdata_flightradar_500000;")
    cur1.execute("insert into " + tablename + "(sensor_id, lat, long, label) select sensor_id, lat, long, label from false_trainingdata_flightradar_500000;")
    rootdb.commit()



#get_mixed_trainingset(cur, cur2, 20000, "mixed_trainingdata_24_00", "/home/heddha/jul_24_2021/1627084800.csv")

#construct_true_testdata(cur, 200000, "true_trainingdata_24_06", "/home/heddha/openSkyNetworkData/jul_24_2021/1627106400.csv")

#construct_false_testdata(cur, cur2, 200000, "false_trainingdata_coordinates")

#get_mixed_trainingset(cur, cur2, 1000000, "mixed_trainingdata_24_12_coordinates_hour3_second_1mil", "/home/heddha/openSkyNetworkData/jul_24_2021/1627120800.csv")
#get_mixed_trainingset(cur, cur2, 200000, "mixed_trainingdata_41600_coordinates_hour3", "/home/heddha/openSkyNetworkData/jul_23_2021/1627041600.csv")


#construct_testdata_flightradar(cur, cur2, 1000000, "mixed_trainingdata_flightradar_1000000")

#construct_true_dataset_flightradar(rootdb, cur, "dupe_flightradar_coordinates_reduced", "true_testdata_flightradar_500000", 500000)
#construct_true_dataset_flightradar(rootdb, cur, "tiny_coordinates", "tiny_testdata", 3)

construct_testdata_flightradar_short_helper(cur, cur2, "mixed_trainingdata_flightradar_1000000")

rootdb.commit()
cur.close()
cur2.close()

