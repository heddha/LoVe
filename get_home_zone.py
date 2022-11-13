import psycopg2
import h3

'''
opensky = psycopg2.connect(
    host="localhost",
    database="opensky",
    user="root",
    password="root",
    port=5433)

baseline = "tiny_h34"
'''

opensky = psycopg2.connect(
    host="localhost",
    database="root",
    user="root",
    password="root",
    port=5433)



cur = opensky.cursor()




def get_homezone(db, h3table, sensor_id):
    db.execute("select h3id, sensor_id, amount from " + h3table + " where sensor_id = " + str(sensor_id) + " order by amount desc;")
    first = db.fetchone()
    rest = db.fetchall()
    homezone = first[0]
    highest_distance = 0
    overall_amount = 0
    count = 1
    for row in rest:
        count += 1
        #print(homezone, row[0])
        try:
            distance = h3.grid_distance(homezone, row[0])
        except(h3._cy.error_system.H3FailedError):
            for i in {50, 45, 40, 35, 30, 25, 20, 15, 10, 5}:
                if row[0] in h3.grid_disk(homezone, i):
                    distance = i
                    #print("didn't work for sensor", row[1], "in" , homezone, "and", row[0])
        if distance > highest_distance:
            highest_distance = distance
        overall_amount += distance
    average_distance = overall_amount / count
    #print("highest_distance = ", highest_distance, "average distance = ", average_distance)
    return homezone, highest_distance, average_distance

def get_homezone_for_all(db, baseline):
    db.execute("select distinct sensor_id from "+ baseline)
    sensor_set = db.fetchall()
    for sensor in sensor_set:
        get_homezone(db, baseline, sensor[0])

def insert_homezone_into_table(db, baseline, table, cellsize):
    db.execute("create table " + table + str(cellsize) + " (sensor_id int PRIMARY KEY, homezone TEXT, highest_distance INT);")
    print("constructed table " +  table +  str(cellsize))
    db.execute("select distinct sensor_id from " + baseline)
    sensor_set = db.fetchall()
    for sensor in sensor_set:
        sensor_id = sensor[0]
        homezone, highest_distance, average_distance = get_homezone(db, baseline, sensor_id)
        db.execute("insert into " + table + str(cellsize) + " values(" + str(sensor_id) + ", '" + homezone + "', " + str(highest_distance) + ");")


def insert_multiple(curs, baseline_prefix, table_prefix, min_cellsize=2, max_cellsize=7):
    for i in range(min_cellsize, max_cellsize+1):
        cellsize = i
        print("working on the " + str(cellsize) + "'s")
        baseline = baseline_prefix + str(cellsize)
        insert_homezone_into_table(cur, baseline, table_prefix, cellsize)
        opensky.commit()

#get_homezone(cur, baseline, -1408237567)
#get_homezone(cur, baseline, e)
#get_homezone_for_all(cur, baseline)


#baseline = "flightradar_coordinates_reduced_h35"
#cellsize = 5
#insert_homezone_into_table(cur, baseline, "flightradar_homezone_h3", cellsize)



#insert_multiple(cur, "coordinates_h3", "homezone_h3", 2, 7)

#opensky.commit()
#cur.close()
