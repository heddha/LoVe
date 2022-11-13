import analyse_database
import psycopg2
from time import perf_counter
from get_home_zone import get_homezone
from h3 import latlng_to_cell, grid_distance, grid_disk, H3FailedError

'''
opensky = psycopg2.connect(
    host="localhost",
    database="opensky",
    user="root",
    password="root",
    port=5433)
h3_table = "coordinates_23_06_h35"
'''


def get_database():
    return psycopg2.connect(
        host="localhost",
        database="root",
        user="root",
        password="root",
        port=5433)


def execute_test(cur, h3_table, trainingdata_table, cellsize):
    overall_time_a = perf_counter()
    cur.execute("select sensor_id, lat, long, label from " + trainingdata_table + " order by sensor_id, lat, long;")
    data = cur.fetchall()
    no_of_testrows = no_of_TP = no_of_TN = no_of_FP = no_of_FN = no_of_labeled_data = old_sensor_id = 0
    old_result = 1
    old_h3id = ""
    cur.execute("select distinct sensor_id from " + homezonetable_prefix + "2")
    sensor_list = {x[0] for x in cur.fetchall()}
    # print("Took", perf_counter() - overall_time_a, "until the distinct sensor list was done")
    nonex_msg_counter = old_data_counter = 0
    nonexistent_sensors = set()
    count_time = 0
    trustdict = {}
    cur.execute("select h3id, sensor_id from " + h3_table)
    trust_stuff = cur.fetchall()
    for hid, sid in trust_stuff:
        trustdict[hid + str(sid)] = 1
    for row in data:
        no_of_testrows += 1
        sensor_id = row[0]
        lat = row[1]
        lng = row[2]
        label = row[3]
        no_of_labeled_data += label
        h3id = latlng_to_cell(lat, lng, cellsize)
        if old_sensor_id == sensor_id and old_h3id == h3id:
            result = old_result
            old_data_counter += 1
        else:
            try:
                result = trustdict[h3id + str(sensor_id)]
            except KeyError:
                result = 0
        if result == 0:
            if sensor_id not in sensor_list:
                nonexistent_sensors.add(sensor_id)
                nonex_msg_counter += 1
        if result == 0:
            if label == 0:
                no_of_TN += 1
            else:
                no_of_FN += 1
        else:
            if label == 1:
                no_of_TP += 1
            else:
                no_of_FP += 1
        old_result = result
        old_h3id = h3id
        old_sensor_id = sensor_id
    print("TP:", no_of_TP, "FP:", no_of_FP, "TN:", no_of_TN, "FN:", no_of_FN, "overall amount:", no_of_testrows)
    TP_plus_FN = no_of_FN + no_of_TP
    TPR = no_of_TP / TP_plus_FN if TP_plus_FN != 0 else 0
    FNR = no_of_FN / TP_plus_FN if TP_plus_FN != 0 else 0
    print("FPR =", no_of_FP / no_of_testrows, "TPR =", TPR, "FNR =", FNR)
    time_needed = perf_counter() - overall_time_a
    print(nonexistent_sensors)
    print(nonex_msg_counter)
    return time_needed


def get_homezonedict(cur, cellsize):
    cur.execute("select sensor_id, homezone, highest_distance from " + homezonetable_prefix + str(cellsize))
    all_homezones_retrieved = cur.fetchall()
    homezonedict = {}
    for e in all_homezones_retrieved:
        sensor_id = e[0]
        homezone = e[1]
        highest_distance = e[2]
        homezonedict[sensor_id] = [homezone, highest_distance]
    return homezonedict


def get_misclassified_sensors(FN_list, db, flagged_sensors, cellsize):
    misclassified_FN = 0
    time_a = perf_counter()
    db.execute("select sensor_id, homezone, highest_distance from " + homezonetable_prefix + str(
        cellsize) + " where ARRAY[sensor_id] && '" + str(flagged_sensors) + "'")
    all_homezones_retrieved = db.fetchall()
    homezonedict = {}
    for e in all_homezones_retrieved:
        sensor_id = e[0]
        homezone = e[1]
        highest_distance = e[2]
        homezonedict[sensor_id] = [homezone, highest_distance]
    flagged_sensors_counter = 0
    # distance = highest_distance + 1
    time_b = perf_counter()
    for element in FN_list:
        sensor_id = element[0]
        lat = element[1]
        lng = element[2]
        h3id = latlng_to_cell(lat, lng, cellsize)
        try:
            homezone, highest_distance = homezonedict[sensor_id]
            try:
                distance = grid_distance(homezone, h3id)
            except H3FailedError:
                # print(homezone, h3id)
                cellarray = []
                match cellsize:
                    case 2:
                        cellarray = [5, 4, 3, 2, 1]
                    case 3:
                        cellarray = [10, 8, 6, 4, 2]
                    case 4:
                        cellarray = [10, 8, 6, 4, 2]
                    case 5:
                        cellarray = [10, 8, 6, 4, 2]
                    case 6:
                        cellarray = [10, 8, 6, 4, 2]
                    case 7:
                        cellarray = [10, 8, 6, 4, 2]
                    case 8:
                        cellarray = [10, 8, 6, 4, 2]
                    case 9:
                        cellarray = [10, 8, 6, 4, 2]
                for i in cellarray:
                    if h3id in grid_disk(homezone, i):
                        distance = i
            if distance < highest_distance:
                misclassified_FN += 1
        except KeyError:
            flagged_sensors_counter += 1
    # print("needed", perf_counter() - time_b, "to map all FN_list entries to their homezones")
    # print("flagged_sensors_counter: ", flagged_sensors_counter)
    # print("misclassified_FN: ", misclassified_FN)
    return misclassified_FN


def is_misclassified(cur, homezonedict, flagged_sensor, h3id, cellsize):
    result = 0
    time_a = perf_counter()
    flagged_sensors_counter = 0
    time_b = perf_counter()
    homezone, highest_distance = homezonedict[flagged_sensor]
    distance = highest_distance + 1
    try:
        distance = grid_distance(homezone, h3id)
    except H3FailedError:
        # print(homezone, h3id)
        cellarray = []
        match cellsize:
            case 2:
                cellarray = [5, 4, 3, 2, 1]
            case 3:
                cellarray = [10, 8, 6, 4, 2]
            case 4:
                cellarray = [10, 8, 6, 4, 2]
            case 5:
                cellarray = [10, 8, 6, 4, 2]
            case 6:
                cellarray = [10, 8, 6, 4, 2]
            case 7:
                cellarray = [10, 8, 6, 4, 2]
            case 8:
                cellarray = [10, 8, 6, 4, 2]
            case 9:
                cellarray = [10, 8, 6, 4, 2]
        for i in cellarray:
            if h3id in grid_disk(homezone, i):
                distance = i
    if distance < (highest_distance / 2):
        result = 1
    # print("needed", perf_counter() - time_b, "to map all FN_list entries to their homezones")
    # print("flagged_sensors_counter: ", flagged_sensors_counter)
    # print("misclassified_FN: ", misclassified_FN)
    return result


def get_nonexistent_sensors(db, flagged_sensors, h3_table):
    nonexistent_sensors = set()
    for sensor_id in flagged_sensors:
        db.execute("select count(sensor_id) from " + h3_table + " where " + str(sensor_id) + " = sensor_id;")
        amount_occurences_sensor = db.fetchone()[0]
        if amount_occurences_sensor == 0:
            nonexistent_sensors.add(sensor_id)
    # print("nonexistent_sensors:", nonexistent_sensors)
    return nonexistent_sensors


def check_exists(db, flagged_sensor, h3_table):
    db.execute("select count(sensor_id) from " + h3_table + " where " + str(flagged_sensor) + " = sensor_id;")
    amount_occurences_sensor = db.fetchone()[0]
    if amount_occurences_sensor == 0:
        return 0
    return 1


def run_test(h3_table_prefix="coordinates_h3", trainingdata_table="true_trainingdata_24_00", cellsize=4):
    cur = get_database().cursor()
    print("\nresolution:", cellsize)
    execute_test(cur, h3_table, trainingdata_table, cellsize)


def run_tests(h3_table_prefix="coordinates_h3", trainingdata_table="true_trainingdata_24_00", min_cellsize=2,
              max_cellsize=9):
    cur = get_database().cursor()
    for i in range(min_cellsize, max_cellsize + 1):
        # for i in [2, 3, 8, 9]:
        print("\nresolution:", i)
        h3_table = h3_table_prefix + str(i)
        execute_test(cur, h3_table, trainingdata_table, i)
    # h3_table = h3_table_prefix+str(cellsize)
    # execute_test(cur, h3_table, trainingdata_table, cellsize)


def run_test_x_times(h3_table_prefix="coordinates_h3", trainingdata_table="true_trainingdata_24_00", min_cellsize=2,
                     max_cellsize=7, x=100):
    cur = get_database().cursor()
    for turn in range(min_cellsize, max_cellsize + 1):
        cellsize = turn
        h3_table = h3_table_prefix + str(cellsize)
        print("\nresolution:", cellsize)
        time_total = 0
        for i in range(0, x):
            time_needed = execute_test(cur, h3_table, trainingdata_table, cellsize)
            time_total += time_needed
        print(x, "runs required on average", time_total / x, "seconds")


homezonetable_prefix = "flightradar_homezone_h3"
# run_tests(h3_table_prefix="flightradar_coordinates_reduced_h3", trainingdata_table="mixed_trainingdata_flightradar_3", min_cellsize=2, max_cellsize=9)

# run_test_x_times(h3_table_prefix="flightradar_coordinates_reduced_h3", trainingdata_table="mixed_trainingdata_flightradar_3", min_cellsize=2, max_cellsize=7, x=1)


# run_tests(h3_table_prefix="flightradar_coordinates_reduced_h3", trainingdata_table="true_testdata_flightradar")
# print("old result: \nresolution: 2\nTP: 94259 FP: 201 TN: 99799 FN: 5741 overall amount: 200000\nFPR = 0.001005 TPR = 0.94259 FNR = 0.05741\n1 runs required on average 3.2966653540206607 seconds")
run_test_x_times(h3_table_prefix="flightradar_coordinates_reduced_h3",
                 trainingdata_table="mixed_trainingdata_flightradar_1000000", min_cellsize=2, max_cellsize=7, x=1)

homezonetable_prefix = "homezone_h3"
# run_tests(h3_table_prefix="coordinates_h3", trainingdata_table="mixed_trainingdata_24_06_coordinates_hour3", min_cellsize=2, max_cellsize=7)

print("\n\n\n")
# print("old result:\n resolution: 2\n TP: 96916 FP: 190 TN: 99810 FN: 3084 overall amount: 200000\nFPR = 0.00095 TPR = 0.96916 FNR = 0.03084\n 1 runs required on average 1.2469815599906724 seconds")

run_test_x_times(h3_table_prefix="coordinates_h3",
                 trainingdata_table="mixed_trainingdata_24_06_coordinates_hour3_1_mil", min_cellsize=2,
                 max_cellsize=7, x=1)
