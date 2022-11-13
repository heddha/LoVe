import csv
from pathlib import Path
import h3

# call like so:
# testdb = fill_database.construct_database("testdb")

def construct_database(curs, tablename):
    with curs:
        curs.execute("CREATE TABLE " + tablename + " (id SERIAL PRIMARY KEY, sensor_set integer[], lat FLOAT, long FLOAT, h34 TEXT, h35 TEXT, h36 TEXT, h37 TEXT);")


# call like so:
# fill_database.fill_from_one_file("../examplesOfPositionalData/2SensorsOnly.csv", testdb, 1) 

def fill_from_one_file(myfile, cur, tablename):
    with open(myfile, newline='\n') as csvfile:
        allelements = csv.DictReader(csvfile, delimiter=',')
        for row in allelements:
            sensor_set = row['radar_id']
            lati = row['latitude']
            longi = row['longitude']
            h34 = h3.latlng_to_cell(float(lati),float(longi), 4)
            h35 = h3.latlng_to_cell(float(lati),float(longi), 5)
            h36 = h3.latlng_to_cell(float(lati), float(longi), 6)
            h37 = h3.latlng_to_cell(float(lati), float(longi), 7)
            sql = "INSERT INTO " + tablename + " (sensor_set, lat, long, h34, h35, h36, h37) values( '{" + str(sensor_set) + "}', " + str(lati) + ', ' + str(longi) + ", '"  + str(h34) + "', '" + str(h35) + "', '" + str(h36) + "', '" + str(h37) + "');"
            cur.execute(sql)
            #sql = 'INSERT INTO COORDINATES (id, sensor_set, lat, long) values(' + str(counter) + ', ' + str(current_sensor) + ', ' + str(lati) + ', ' + str(longi) + ')'



def print_content(db):
    with db:
        data = db.execute("SELECT * FROM COORDINATES")
        for row in data:
            print(row)


#  call like so: 
#  fill_database.iterate_over_multiple("../testfolder", testdb)

def iterate_over_multiple(folder, db, curs, tablename):
    p = Path(folder)
    files = list(p.glob('*'))
    for myfile in files:
        fill_from_one_file(myfile, curs, tablename)
        db.commit()
        print("done with", myfile)


def construct_database_with_hash(dbname):
    db_id = sl.connect(dbname)
    with db_id:
        db_id.execute("""
            CREATE TABLE COORDINATES (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                sensor_id INTEGER [],
                lat FLOAT,
                long FLOAT,
                hash INTEGER
            );
        """)
    return db_id


def fill_from_one_file_with_hash(myfile, db, counter):
    with open(myfile, newline='\n') as csvfile:
        allelements = csv.DictReader(csvfile, delimiter=',')
        for row in allelements:
            current_sensor = row['radar_id']
            lati = row['latitude']
            longi = row['longitude']
            # myhash = hash(str(lati)+str(longi))
            myhash = hash(str(row['altitude']) + str(row['heading']) + str(lati) + str(longi) + str(row['speed']))
            sql = 'INSERT INTO COORDINATES (id, sensor_id, lat, long, hash) values(' + str(counter) + ', ' + str(
                current_sensor) + ', ' + str(lati) + ', ' + str(longi) + ', ' + str(myhash) + ')'
            db.execute(sql)
            counter = counter + 1
    return counter


def iterate_over_multiple_with_hash(folder, db):
    counter = 1
    p = Path(folder)
    files = list(p.glob('*'))
    for myfile in files:
        counter = fill_from_one_file_with_hash(myfile, db, counter)
    db.commit()
    return counter


def construct_h3_table(dbname):
    # db_id = sl.connect(dbname)
    with dbname:
        dbname.execute("""
            CREATE TABLE H3SENSORS (
                id SERIAL PRIMARY KEY,
                h3id TEXT,
                sensor_id INTEGER,
                amount INTEGER
            );
        """)
    return dbname

def fill_from_openSkyNetworkData(myfile, db, cur, tablename):
    #no hash included. include the hash in the values table with hash BIGINT
    cur.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_set INTEGER[], lat FLOAT, long FLOAT);")
    fill_from_openSkyNetworkData_no_db(myfile, db, cur, tablename)


def fill_from_openSkyNetworkData_no_db(myfile, db, cursorname, tablename):
    with open(myfile, newline='\n') as csvfile:
        allelements = csv.DictReader(csvfile, delimiter=',')
        counter = 0
        for row in allelements:
            counter += 1
            if counter % 100000 == 0:
                print("managed", counter, "entries")
            lati = row['lat']
            longi = row['lon']
            sensor_set = set()
            sensor_set.add(int(row['group_concat(cast(serials.item as string)']))
            if (row[" '"]):
                sensor_set.add(int(row[" '"]))
            if (row["')"]):
                sensor_set.add(int(row["')"]))
            try:
                sensor_set.update([int(x) for x in row[None]])
            except KeyError:
                pass
            h34 = h3.latlng_to_cell(float(lati),float(longi), 4)
            h35 = h3.latlng_to_cell(float(lati),float(longi), 5)
            h36 = h3.latlng_to_cell(float(lati), float(longi), 6)
            h37 = h3.latlng_to_cell(float(lati), float(longi), 7)
            '''
            hash-version:
            myhash = hash(
                str(row['baroaltitude']) + str(row['heading']) + str(lati) + str(longi) + str(row['velocity']))
            sql = "INSERT INTO " + tablename + " (sensor_set, lat, long, hash) values( '" + str(sensor_set) + "', " + str(
                lati) + ', ' + str(longi) + ', ' + str(myhash) + ')'
            '''
            sql = "INSERT INTO " + tablename + " (sensor_set, lat, long, h34, h35, h36, h37) values( '" + str(sensor_set) + "', " + str(lati) + ', ' + str(longi) + ", '"  + str(h34) + "', '" + str(h35) + "', '" + str(h36) + "', '" + str(h37) + "');"
            cursorname.execute(sql)
    db.commit()


def iterate_over_multiple_opensky(folder, db, cursorname, tablename):
    #cursorname.execute("CREATE TABLE " + tablename + "(id SERIAL PRIMARY KEY, sensor_set INTEGER[], lat FLOAT, long FLOAT, h34 TEXT, h35 TEXT, h36 TEXT, h37 TEXT);")
    counter = 1
    p = Path(folder)
    files = list(p.glob('*'))
    amount_of_files = len(files)
    print("there are", amount_of_files, "files to be added")
    for myfile in files:
        print("\n\ncurrently working on", myfile)
        counter +=1
        fill_from_openSkyNetworkData_no_db(myfile, db, cursorname, tablename)
        print("added", myfile, "to table", tablename)
        amount_of_files -= 1
        print("There are", amount_of_files, "files left to be scanned.")
    #db.commit()
    return counter


def add_h3_column(db, cursor,  table, new_columnname, cellsize):
    with cursor:
        cursor.execute("ALTER TABLE " + table + " ADD COLUMN " + new_columnname + " TEXT;")
        cursor.execute("SELECT id, lat, long FROM " + table)
        data = cursor.fetchall()
        print("got all entries from the database")
        counter = 0
        for row in data:
            id = row[0]
            lat = row[1]
            lng = row[2]
            h3id = h3.latlng_to_cell(lat, lng, cellsize)
            cursor.execute("UPDATE " + table + " SET " + new_columnname + " = '" + str(h3id) + "' WHERE id = " + str(id) + ";")
            counter += 1
            if counter % 10000 == 0:
                print("managed", counter, "entries")
    db.commit()

def add_h3_columns(db, cursor, table):
    with cursor:
        cursor.execute("ALTER TABLE " + table + " ADD COLUMN h34 TEXT, ADD COLUMN h35 TEXT, ADD COLUMN h36 TEXT, ADD COLUMN h37 TEXT;")
        cursor.execute("SELECT count(*) as exact_count FROM " + table)
        amount_of_entries = cursor.fetchone()[0]
        print(amount_of_entries)
        counter = 0
        partition = 100000
        while (amount_of_entries / partition) > 0:
            #print("got another", partition, "entries from the database")
            cursor.execute("SELECT id, lat, long FROM " + table + " WHERE ID >= " + str(counter) + " ORDER BY ID LIMIT " + str(partition))
            amount_of_entries -= 1
            data = cursor.fetchall()
            for row in data:
                row_id = row[0]
                #print(row_id)
                lat = row[1]
                lng = row[2]
                h34 = h3.latlng_to_cell(lat, lng, 4)
                h35 = h3.latlng_to_cell(lat, lng, 5)
                h36 = h3.latlng_to_cell(lat, lng, 6)
                h37 = h3.latlng_to_cell(lat, lng, 7)
                cursor.execute("UPDATE " + table + " SET h34 = '" + str(h34) + "', h35 = '" + str(h35) + "', h36 = '" + str(h36) + "', h37 = '" + str(h37) + "' WHERE id = " + str(row_id) + ";")
                counter += 1
                if counter % partition == 0:
                    print("managed", counter, "entries")
            db.commit()

def construct_h3_table_reduced(db, tablename_in, tablename_out, cellsize):
    with db:
        db.execute("CREATE TABLE " + tablename_out + " (id SERIAL PRIMARY KEY, h3id TEXT, sensor_id INTEGER, amount INTEGER);")
        counter = 0
        db.execute("SELECT id, sensor_set, lat, long FROM " + tablename_in)
        data = db.fetchall()
        h3dict = {}
        for row in data:
            counter += 1

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
