import psycopg2


# insert into tiny_h34_2
# select h34, unnest(sensor_id) as s_id, count(h34) from tiny_coordinates tc group by s_id, tc.h34;

# select distinct unnest(sensor_id) from tiny_coordinates;  --285 rows
# select unnest(sensor_id) from tiny_coordinates;  --399 rows
# select distinct h34 from tiny_coordinates;  --39 rows

# CREATE INDEX coordinates_sensor_set_h35_idx on coordinates(sensor_set, h35);


def fill_h3_table(db, curs, orig_table, cellsize):
    h3table = orig_table + "_h3" + str(cellsize)
    h3_columnname = "h3" + str(cellsize)
    curs.execute(
        "create table " + h3table + " (h3id TEXT, sensor_id INTEGER, amount INTEGER, primary key (h3id, sensor_id));")
    print("Created table", h3table)
    curs.execute(
        "insert into " + h3table + " select " + h3_columnname + ", unnest(sensor_set) as s_id, count(" + h3_columnname + ") from " + orig_table + "  tc WHERE tc.lat>=30 AND tc.lat<=75 AND tc.long>=-25 AND tc.long<=45 group by s_id, tc." + h3_columnname + ";")
    print("inserted all entries into table")
    db.commit()
    print("committed everything to db")


def create_indeces_for_tables(db, cur, coordinates_prefix, h3table_prefix, homezone_prefix, min_cellsize=2, max_cellsize=9):
    for i in range(min_cellsize, max_cellsize+1):
        print("working on the " + str(i) + "'s")
        current_h3_table = h3table_prefix + str(i)
        current_homezone_table = homezone_prefix + str(i)
        cur.execute("CREATE index " + current_h3_table + "_h3id_idx on " + current_h3_table + " USING BTREE(h3id);")
        cur.execute("CREATE index " + current_h3_table + "_sensor_id_idx on " + current_h3_table + " USING BTREE(sensor_id);")
        cur.execute("CREATE index " + current_h3_table + "_h3id_sensor_id_amount_idx on " + current_h3_table + " USING BTREE(h3id, sensor_id, amount);")
        cur.execute("CREATE index " + current_h3_table + "_amount_idx on " + current_h3_table + " USING BTREE(amount);")
        cur.execute("CREATE index " + current_homezone_table + "_sensor_id_homezone_highest_distance_idx on " + current_homezone_table + " USING BTREE(sensor_id, homezone, highest_distance);")
        cur.execute("CREATE index " + current_homezone_table + "_sensor_id_idx on " + current_homezone_table + "(sensor_id);")
        #cur.execute("drop index " + current_homezone_table + "_sensor_id_idx")
        db.commit()

def create_indices_for_testdata(db, cur, testdata_table):
    cur.execute("CREATE index " + testdata_table + "_sensor_id_idx on " + testdata_table + "(sensor_id);")
