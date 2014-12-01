import sys
import csv
import os
import subprocess
import tempfile
import json
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
        nbi_bridge_id SERIAL PRIMARY KEY,
        state text,
        over_clearance real,
        under_clearance real,
        under_clearance_type text,
        operating_load real,
        structure_number text,
        record_type text,
        toll_status text,
        location geometry(point, 4326)
    );

    CREATE INDEX nbi_bridges_structure_number_index ON nbi_bridges(structure_number) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_state_index ON nbi_bridges(state) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_location_index ON nbi_bridges USING gist (location) WITH (fillfactor=100);    

    CREATE TABLE nbi_bridge_osm_ways (
        nbi_bridge_id integer,
        osm_id integer,
        distance real
    );

    CREATE INDEX nbi_bridge_osm_ways_nbi_bridge_id ON nbi_bridge_osm_ways(nbi_bridge_id);
    CREATE INDEX nbi_bridge_osm_ways_osm_id ON nbi_bridge_osm_ways(osm_id);
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
    print('# loading NBI for {}'.format(state.name))

    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    state_filename = 'nbi/nbi_{state_abbr}.csv'.format(state_abbr=state.abbr)
    
    pbar = ProgressBar(widgets=['NBI load/{state:15}'.format(state=state.name), Percentage(), '  ', ETA(), Bar()], maxval=file_length(state_filename)).start()                    

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
                under_clearance_type = row[61]
                under_clearance = float(row[62]) # meters; >30m is 9999
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
    cur3 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    matched_bridges = 0
    processed_records = 0

    sql = "SELECT COUNT(*) AS total FROM planet_osm_roads WHERE bridge='yes'"
    cur.execute(sql)
    result = cur.fetchone()

    pbar = ProgressBar(widgets=['matching/{state:15}'.format(state=state.name), Percentage(), '  ', ETA(), Bar()], maxval=int(result['total'])).start()                    

    sql = """
    SELECT osm_id, ST_AsText(way) AS wkt FROM planet_osm_roads WHERE bridge='yes'
    """
    cur.execute(sql)
    while True:
        way = cur.fetchone()
        if not way:
            break

        # look for a nearby bridge
        sql = """
            SELECT 
                nbi_bridge_id,
                ST_Distance(location, ST_GeomFromText(%(wkt)s, 4326)) AS distance
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
            ORDER BY distance ASC
            """
        params = {
            'wkt': way['wkt'],
            'state': state.abbr
        }
        cur2.execute(sql, params)

        # if a matching bridge was found, record it
        result = cur2.fetchone()
        if result is not None:
            sql = """
            INSERT INTO nbi_bridge_osm_ways 
            (
                nbi_bridge_id,
                osm_id,
                distance
            )
            VALUES
            ( %(nbi_bridge_id)s, %(osm_id)s, %(distance)s );
            """
            params = {
                'nbi_bridge_id': result['nbi_bridge_id'],
                'osm_id': way['osm_id'],
                'distance': result['distance']

            }
            cur3.execute(sql, params)
            match_ways_to_bridges = matched_bridges + 1

        processed_records = processed_records + 1
        pbar.update(processed_records)

    pbar.finish()

    conn.commit()
    cur3.close()
    cur2.close()
    cur.close()
    conn.close() 

def makegeojson(state):
    if not os.path.exists('build/{}'.format(state.abbr)):
        os.mkdir('build/{}'.format(state.abbr))

    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = "SELECT COUNT(*) AS total FROM nbi_bridge_osm_ways"
    cur.execute(sql)
    result = cur.fetchone()

    pbar = ProgressBar(widgets=['GeoJSON/{state:15}'.format(state=state.name), Percentage(), '  ', ETA(), Bar()], maxval=int(result['total'])).start()                    

    sql = """
            SELECT 
                nbi_bridge_osm_ways.osm_id AS osm_id,
                nbi_bridges.nbi_bridge_id AS nbi_bridge_id,
                nbi_bridges.structure_number AS nbi_structure_number,
                ST_AsGeoJSON(location) AS ptjson, 
                ST_AsGeoJSON(way) AS wayjson
            FROM
                nbi_bridge_osm_ways
            INNER JOIN
                planet_osm_roads 
                    ON planet_osm_roads.osm_id=nbi_bridge_osm_ways.osm_id
            INNER JOIN
                nbi_bridges
                    ON nbi_bridges.nbi_bridge_id=nbi_bridge_osm_ways.nbi_bridge_id
        """
    cur.execute(sql)
    i = 0
    while True:
        result = cur.fetchone()
        if result is None:
            break

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "state": state.abbr,
                        "nbi_bridge_id": result['nbi_bridge_id'],
                        "structure_number": result['nbi_structure_number']
                    },
                    "geometry": json.loads(result['ptjson'])   
                },
                {
                    "type": "Feature",
                    "properties": {
                        "state": state.abbr,
                        "osm_id": result['osm_id']
                    },
                    "geometry": json.loads(result['wayjson'])    
                }
            ]
        }

        with open('build/{}/{}.json'.format(state.abbr, result['osm_id']), 'w') as jsonout:
            json.dump(geojson, jsonout, indent=4) 

        i = i + 1
        pbar.update(i)

    pbar.finish()       
    
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

        match_ways_to_bridges(state)

        makegeojson(state)
        

               
                       


if __name__ == '__main__':
    main()