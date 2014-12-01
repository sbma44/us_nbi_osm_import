import sys
import csv
import os
import subprocess
import tempfile
import webbrowser
import us
import psycopg2, psycopg2.extras
from progressbar import ProgressBar, Bar, ETA, Percentage, Counter

DBNAME = 'nbi'

def createdb():
    print('# creating database')    

    cmd = 'createdb {}'.format(DBNAME)
    subprocess.call(cmd.split(' '))

    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    cur = conn.cursor()
    sql = "CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology; CREATE EXTENSION hstore;"
    cur.execute(sql)
    
    sql = """
    CREATE TABLE nbi_bridges (
        state text,
        over_clearance real,
        under_clearance real,
        under_clearance_type text,
        operating_load real,
        structure_number text,
        record_type text,
        toll_status text,
        location geometry(point, 4326),
        osm_way_id real
    );

    CREATE INDEX nbi_bridges_structure_number_index ON nbi_bridges(structure_number) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_state_index ON nbi_bridges(state) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_location_index ON nbi_bridges USING gist (location) WITH (fillfactor=100);    
    """
    cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()

def dropdb():
    print('# dropping database')
    subprocess.call('dropdb --if-exists {}'.format(DBNAME).split(' '))

def load_osm(state):
    # load state OSM
    filename = 'us_osm/{state}-latest.osm.pbf'.format(state=state.name.lower().replace(' ', '-'))
    if not os.path.exists(filename):
        print('Skipping {filename}'.format(filename=filename))
        return False

    print('Loading {filename}'.format(filename=filename))
    cmd = 'osm2pgsql --hstore --slim --drop -E 4326 --latlong -c -d {dbname} {filename}'.format(dbname=DBNAME, filename=filename)
    subprocess.call(cmd.split(' '))

    return True

def file_length(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def load_nbi(state):
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    state_filename = 'nbi/nbi_{state_abbr}.csv'.format(state_abbr=state.abbr)
    
    pbar = ProgressBar(widgets=['{state:15}'.format(state=state.name), Percentage(), '  ', ETA(), Bar()], maxval=file_length(state_filename)).start()                    

    with open(state_filename, 'r') as csvfile:
        i = 0
        matched_bridges = 0
        reader = csv.reader(csvfile)
        for row in reader:

            # @TODO: skip records with problems

            structure_number = row[1]

            # record type
            # 1 = route carried on structure
            # 2 = single route goes under structure
            # 3 = multiple routes go under structure
            record_type = row[2]

            # clearance above bridge in meters; can be coded 99 if >30m
            over_clearance = row[14] 

            # type of route under bridge
            # H = highway
            # R = railroad
            # N = other
            under_clearance = None
            try:
                under_clearance_type = row[15]
                under_clearance = float(row[16]) # meters; >30m is 9999
            except:
                pass

            # toll status
            # 1 = toll bridge
            # 2 = on toll road
            # 3 = free road
            # 4 = ? - "On Interstate toll segment under Secretarial Agreement. Structure functions as a part of the toll segment."
            # 5 = ? - "Toll bridge is a segment under Secretarial Agreement. Structure is separate agreement from highway segment."
            toll_status = row[22]

            # find weight limit
            operating_rating = None
            try:
                operating_rating = row[72]
            except:
                pass

            # extract coordinates
            try:                    
                lat_row = row[19]
                lat = float(lat_row[0:2]) + (float(lat_row[2:4]) / 60.0) + (float(lat_row[4:8]) / 360000.0)
                lon_row = row[20]
                
                # correct miscoded longitudes -- this is hardcoded for the US
                if lon_row[0] == '0':
                    lon_row = list(lon_row)
                    lon_row.pop(0)
                    lon_row.append('0')    
                    lon_row = ''.join(lon_row)
                lon = -1 * (float(lon_row[0:2]) + (float(lon_row[2:4]) / 60.0) + (float(lon_row[4:8]) / 360000.0))            

                 # are least-significant digits non-zero? if so, it's high precision
                high_precision = (lon_row[6:8] != '00') and (lat_row[6:8] != '00')

                # @TODO: horizontal clearances                   
            except:
                continue
    
            sql = """INSERT INTO nbi_bridges

            (
                state,
                over_clearance,
                under_clearance,
                operating_load,
                structure_number,
                record_type,
                toll_status,
                location
            )

            VALUES

            (
                %(state)s,
                %(over_clearance)s,
                %(under_clearance)s,
                %(operating_load)s,
                %(structure_number)s,
                %(record_type)s,
                %(toll_status)s,
                ST_GeomFromText('POINT(%(lon)s %(lat)s)', 4326)   
            )
            """

            params = {
                'state': state.abbr,
                'over_clearance': over_clearance,
                'under_clearance': under_clearance,
                'operating_load': operating_rating,
                'structure_number': structure_number,
                'record_type': record_type,
                'toll_status': toll_status,
                'lat': lat,
                'lon': lon
            }
            for p in params:
                if params[p]=='':
                    params[p] = None

            cur.execute(sql, params)

            i = i + 1
            pbar.update(i)

    conn.commit()

    cur.close()
    conn.close()     
    pbar.finish()

def match_ways_to_bridges(state):
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = """
    SELECT osm_id, ST_AsText(way) AS wkt FROM planet_osm_roads WHERE bridge='yes'
    """
    cur.execute(sql)
    while True:
        way = cur.fetchone()
        if not way:
            break

        sql = """
            SELECT 
                structure_number,
                ST_Distance(location, ST_GeomFromText(%(wkt)s, 4326)) AS dist
            FROM 
                nbi_bridges
            WHERE
                state = %(state)s
            AND
                ST_DWithin(
                    location,
                    ST_GeomFromText(%(wkt)s, 4326),
                    0.001
                )
            ORDER BY dist
            """
        cur2.execute(sql)

        # @TODO: record results

    cur.close()
    conn.close() 

def main():
    # writefile = open('build/nbi_way_matches.csv', 'w')
    # writer = csv.writer(writefile)
    # writer.writerow(['state', 'structure_number', 'osm_id', 'lon', 'lat', 'dist', 'maxheight', 'maxweight'])

    for state in (us.states.FL,):
    # for state in us.states.STATES:
            
        dropdb()
        createdb()

        # try to load OSM -- if it fails, skip this state        
        if not load_osm(state):
            continue

        load_nbi(state)


        

               
                       


if __name__ == '__main__':
    main()