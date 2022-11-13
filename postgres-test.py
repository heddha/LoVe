import psycopg2

import add_new_sensor
import db_scripts
import fill_database
import analyse_database
import generate_false_data

#import generate_false_data

'''
opensky = psycopg2.connect(
    host="localhost",
    database="opensky",
    user="postgres",
    password="postgres",
    port=5432)
cur = opensky.cursor()
'''
opensky = psycopg2.connect(
    host="localhost",
    database="root",
    user="root",
    password="root",
    port=5433)
cur = opensky.cursor()


#cur.execute(""" CREATE TABLE COORDINATES(id SERIAL PRIMARY KEY, sensor_id INTEGER[], lat FLOAT, long FLOAT, hash BIGINT)""")

#cur.execute("INSERT INTO COORDINATES (sensor_id, lat, long, hash) values ('{-1408235984, 1408235984, -1408235984}', 12319.312, 123.5532, 12315)")
#fill_database.fill_from_openSkyNetworkData("/home/heddha/jul_23_2021/1627070400.csv", opensky,  cur, "coordinates_70400")
#fill_database.iterate_over_multiple_opensky("/home/heddha/openSkyNetworkData/testfolder", opensky, cur, "COORDINATES")
# fill_database.iterate_over_multiple_opensky("/home/heddha/jul_23_2021", opensky, cur, "COORDINATES")


#generate_false_data.construct_true_testdata(cur, 2000, "trainingdata_trueset", "/home/heddha/openSkyNetworkData/jul_23_2021/1626998400.csv")

#generate_false_data.get_mixed_trainingset(cur, 2000, "trainingdata_mixedset", "/home/heddha/openSkyNetworkData/jul_23_2021/1626998400.csv")

#cur.execute("select * from COORDINATES")
#analyse_database.construct_h3_helptable(cur, "H3SENSORS_5_ALL", 5)
# analyse_database.construct_h3_amounttable(cur, "H3SENSORS_5_ALL", "H3SENSORS_5")
#data = cur.fetchall()
#for row in data:
#    print(row)
#analyse_database.construct_h3_helptable_reduced(cur, "NEW_H3SENSORS", 5)


#analyse_database.get_meaningfulness_sensor_coords(cur, "H3SENSORS_5", -1408234233, 40.522745823890354, -3.5540420836791293, 5)
#analyse_database.evaluate_trust(analyse_database.get_meaningfulness_sensor_coords(cur, "H3SENSORS_5", -1408234233, 40.522745823890354, -3.5540420836791293, 5),  cur, -1408234233)
#analyse_database.evaluate_trust(analyse_database.get_meaningfulness_sensor_coords(cur, "H3SENSORS_5", -1408236786, 40.52745823890354, -3.5540420836791293, 5),  cur, -1408236786)

#print(analyse_database.get_sensor_region(cur, -1408236786))

#generate_false_data.trainingdata_coordinates(cur, 20000, "TRAININGDATA_MEDIUMSIZE")

#analyse_database.construct_h3_table_reduced(cur, "COORDINATES", "h3sensors_cellsize5", 5)
#generate_false_data.construct_true_testdata(cur, 20000, "trainingdata_tiny", "/home/heddha/openSkyNetworkData/jul_23_2021/1627081200.csv")
#add_new_sensor.add_new_sensors(cur, "new_h3sensors", "coordinates_hour3")


#fill_database.add_h3_columns(opensky, cur, "coordinates_hour3")

#fill_database.add_h3_columns(opensky, cur, "coordinates")

#db_scripts.fill_h3_table(opensky, cur, "flightradar_coordinates_reduced", 4)
#db_scripts.fill_h3_table(opensky, cur, "flightradar_coordinates_reduced", 5)
#db_scripts.fill_h3_table(opensky, cur, "flightradar_coordinates_reduced", 6)
#db_scripts.fill_h3_table(opensky, cur, "flightradar_coordinates_reduced", 7)

#fill_database.fill_from_openSkyNetworkData_no_db("/home/heddha/openSkyNetworkData/jul_23_2021/1627020000.csv", opensky, cur, "coordinates_23_06")

#fill_database.construct_database(cur, "flightradar_coordinates_reduced")

#fill_database.iterate_over_multiple("/home/heddha/Nextcloud/MicrosoftVM_SharedFolder/ADS-B data/unzipped_positions_folder", opensky, cur, "flightradar_coordinates")

#fill_database.add_h3_columns(opensky, cur, "flightradar_coordinates_reduced")



#db_scripts.create_indeces_for_tables(opensky, cur, "coordinates", "coordinates_h3", "homezone_h3", 2, 7)
#db_scripts.create_indeces_for_tables(opensky, cur, "flightradar_coordinates_reduced", "flightradar_coordinates_reduced_h3", "flightradar_homezone_h3")
#db_scripts.create_indices_for_testdata(opensky, cur, "mixed_trainingdata_flightradar_3")




opensky.commit()


cur.close()



