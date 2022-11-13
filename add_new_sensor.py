import psycopg2


def add_new_sensors(db, old_h3_table, new_coordinates_table):
    db.execute("select distinct sensor_id from " + old_h3_table)
    old_sensors_tmp = db.fetchall()
    old_sensors = set()
    print(old_sensors_tmp)
    for row in old_sensors_tmp:
        old_sensors.add(row[0])
    #print(old_sensors)
    db.execute("select distinct unnest(sensor_set) from " + new_coordinates_table)
    new_sensors = set()
    new_sensors_tmp = db.fetchall()
    for row in new_sensors_tmp:
        new_sensors.add(row[0])
    print(new_sensors)
    sensor_diff = new_sensors.difference(old_sensors)
    print(sensor_diff)
    if 1922535842 in new_sensors:
        print(1922535842, "in new_sensors")
    if 1922535842 in old_sensors:
        print(1922535842, "in old_sensors")
    for sensor in sensor_diff:
        db.execute("select sensor_set from " + new_coordinates_table + " where " + str(sensor) + "  = any (sensor_set);")
        # get the other sensors and check whether they're trusted. If so, add the h3id + new sensor to old_h3_table