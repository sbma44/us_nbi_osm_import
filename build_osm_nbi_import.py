import sys
import csv
import os
import math
import subprocess
import tempfile
import json
import webbrowser
import us
import psycopg2, psycopg2.extras
from progressbar import ProgressBar, Bar, ETA, Percentage, Counter

DBNAME = 'nbi'
DISTANCE_THRESHOLD = 0.0005
ACCEPTABLE_HIGHWAY_TYPES = [
    'motorway',
    'trunk',
    'primary',
    'secondary',
    'tertiary',
    'unclassified',
    'residential',
    'service'
]

def createdb():
    cmd = 'createdb {}'.format(DBNAME)
    subprocess.call(cmd.split(' '))

    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    cur = conn.cursor()
    sql = "CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology; CREATE EXTENSION hstore;"
    cur.execute(sql)
    
    sql = """
    CREATE TABLE nbi_bridges (
        nbi_bridge_id SERIAL PRIMARY KEY,
        structure_number text,
        state text,
        over_clearance real,
        under_clearance real,
        under_clearance_type text,
        operating_rating real,        
        record_type text,
        toll_status text,
        location geometry(point, 4326)
    );

    CREATE INDEX nbi_bridges_structure_number_index ON nbi_bridges(structure_number) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_state_index ON nbi_bridges(state) WITH (fillfactor=100);
    CREATE INDEX nbi_bridges_location_index ON nbi_bridges USING gist (location) WITH (fillfactor=100);    

    CREATE TABLE nbi_bridge_osm_ways (        
        nbi_bridge_id integer,
        structure_number text,        
        osm_id integer,
        distance real
    );

    CREATE INDEX nbi_bridge_osm_ways_nbi_bridge_id ON nbi_bridge_osm_ways(nbi_bridge_id);
    CREATE INDEX nbi_bridge_osm_ways_osm_id ON nbi_bridge_osm_ways(osm_id);
    
    CREATE TABLE intersecting_ways (
        nbi_bridge_id integer,
        nbi_bridge_osm_way_id integer,
        osm_id integer,
        over_clearance real
    );
    
    CREATE INDEX intersecting_ways_nbi_bridge_id ON intersecting_ways(nbi_bridge_id);
    CREATE INDEX intersecting_ways_nbi_bridge_osm_way_id ON intersecting_ways(nbi_bridge_osm_way_id);
    CREATE INDEX intersecting_ways_osm_id ON intersecting_ways(osm_id);
    """
    cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()


def dropdb():
    subprocess.call('dropdb --if-exists {}'.format(DBNAME).split(' '))


def file_length(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


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


def load_nbi(state):
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    state_filename = 'nbi/nbi_{state_abbr}.csv'.format(state_abbr=state.abbr)
    
    pbar = ProgressBar(widgets=['NBI load                ', Percentage(), '  ', ETA(), Bar()], maxval=file_length(state_filename)).start()                    

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

            # clearance above bridge way in meters; can be coded 99 if >30m
            over_clearance = None
            try:
                over_clearance = row[14] 
                # is value zero, 99.x or >30?
                if (math.floor(float(over_clearance)) in [0, 99]) or (float(over_clearance)>=30):
                    over_clearance = None
            except:
                pass

            # type of route under bridge
            # H = highway
            # R = railroad
            # N = other
            under_clearance = None
            try:
                under_clearance_type = row[61]
                under_clearance = float(row[62]) # meters; >30m is 9999
                if under_clearance_type.strip()=='N' or float(under_clearance)>=30:
                    under_clearance = None
            except:
                pass

            # toll status
            # 1 = toll bridge
            # 2 = on toll road
            # 3 = free road
            # 4 = ? - "On Interstate toll segment under Secretarial Agreement. Structure functions as a part of the toll segment."
            # 5 = ? - "Toll bridge is a segment under Secretarial Agreement. Structure is separate agreement from highway segment."
            toll_status = row[22].strip()
            if len(toll_status)==0:
                toll_status = None

            # find weight limit
            operating_rating = row[72].strip()
            if len(operating_rating)==0:
                operating_rating = None

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
                under_clearance_type,
                operating_rating,
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
                %(under_clearance_type)s,
                %(operating_rating)s,
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
                'under_clearance_type': under_clearance_type,
                'operating_rating': operating_rating,
                'structure_number': structure_number,
                'record_type': record_type.strip(),
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


def match_ways_to_bridges():
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur3 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    matched_bridges = 0
    processed_records = 0

    sql = "SELECT COUNT(*) AS total FROM planet_osm_line WHERE bridge='yes'"
    cur.execute(sql)
    result = cur.fetchone()

    pbar = ProgressBar(widgets=['NBI->OSM                ', Percentage(), '  ', ETA(), Bar()], maxval=int(result['total'])).start()                    

    sql = """
    SELECT 
        osm_id, 
        ST_AsText(way) AS wkt 
    FROM 
        planet_osm_line 
    WHERE 
        bridge='yes' 
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
                record_type = '1'
            AND
                ST_DWithin(
                    location,
                    ST_GeomFromText(%(wkt)s, 4326),
                    %(dist_threshold)s
                )
            ORDER BY distance ASC
            """
        params = {
            'wkt': way['wkt'],
            'dist_threshold': DISTANCE_THRESHOLD
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


def find_intersecting_ways():
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur3 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = "SELECT COUNT(*) AS total FROM nbi_bridge_osm_ways"
    cur.execute(sql)
    result = cur.fetchone()
    total = int(result['total'])

    sql = """
    SELECT 
        nbi_bridges.state,
        nbi_bridges.structure_number,
        nbi_bridge_osm_ways.nbi_bridge_id, 
        nbi_bridge_osm_ways.osm_id,
        ST_AsText(way) AS wkt
    FROM 
        nbi_bridge_osm_ways 
    INNER JOIN 
        planet_osm_line 
            ON nbi_bridge_osm_ways.osm_id=planet_osm_line.osm_id
    INNER JOIN
        nbi_bridges
            ON nbi_bridges.nbi_bridge_id=nbi_bridge_osm_ways.nbi_bridge_id
            
    """
    cur.execute(sql)

    pbar = ProgressBar(widgets=['intersections           ', Percentage(), '  ', ETA(), Bar()], maxval=total).start()                    

    i = 0
    while True:
        result = cur.fetchone()
        if result is None:
            break

        sql = """
        SELECT 
            osm_id,
            ST_AsText(way) AS wkt
        FROM 
            planet_osm_line 
        WHERE 
            ST_Intersects(way, ST_Line_SubString(ST_GeomFromText(%(geom)s, 4326), 0.01, 0.99)) 
        AND
            osm_id!=%(osm_id)s   
        AND
            highway IS NOT NULL
        """

        params = {
            'geom': result['wkt'], 
            'osm_id': result['osm_id'],
            'ACCEPTABLE_HIGHWAY_TYPES': ACCEPTABLE_HIGHWAY_TYPES
        }
        cur2.execute(sql, params)
        while True:
            result2 = cur2.fetchone()
            if result2 is None:
                break

            # find vertical clearance from nearest record in NBI
            sql = """
            SELECT                 
                over_clearance
            FROM 
                nbi_bridges 
            WHERE 
                structure_number=%(structure_number)s
            AND
                record_type != '1'
            ORDER BY 
                ST_Distance(ST_GeomFromText(%(wkt)s, 4326), location) ASC
            LIMIT 1
            """            
            params = {'wkt': result2['wkt'], 'structure_number': result['structure_number']}
            cur3.execute(sql, params)
            result3 = cur3.fetchone()
            over_clearance = None
            if result3 is not None:
                over_clearance = result3['over_clearance']
                if over_clearance is not None:
                    try:
                        over_clearance = float(over_clearance)
                        if (over_clearance == 0) or (over_clearance > 30):
                            over_clearance = None
                    except:
                        over_clearance = None


            sql = """
            INSERT INTO intersecting_ways             
                (nbi_bridge_id, nbi_bridge_osm_way_id, osm_id, over_clearance) 
            VALUES 
                (%(nbi_bridge_id)s, %(nbi_bridge_osm_way_id)s, %(osm_id)s, %(over_clearance)s)
            """
            params = {
                'nbi_bridge_id': result['nbi_bridge_id'],
                'nbi_bridge_osm_way_id': result['osm_id'],
                'osm_id': result2['osm_id'],
                'over_clearance': over_clearance
            }
            cur3.execute(sql, params)

        i = i + 1
        pbar.update(i)

    pbar.finish()
    conn.commit()
    cur.close()
    cur2.close()
    cur3.close()
    conn.close()


def geojson(state=us.states.FL):
    if not os.path.exists('build/{}'.format(state.abbr)):
        os.mkdir('build/{}'.format(state.abbr))

    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    sql = "SELECT COUNT(*) AS total FROM nbi_bridge_osm_ways"
    cur.execute(sql)
    result = cur.fetchone()

    pbar = ProgressBar(widgets=['GeoJSON                 ', Percentage(), '  ', ETA(), Bar()], maxval=int(result['total'])).start()                    

    # fetch OSM->NBI mappings
    sql = """
            SELECT 
                nbi_bridges.over_clearance,
                nbi_bridges.under_clearance,
                nbi_bridges.under_clearance_type,
                nbi_bridges.toll_status,
                nbi_bridges.operating_rating,
                nbi_bridge_osm_ways.osm_id AS osm_id,                
                nbi_bridges.nbi_bridge_id AS nbi_bridge_id,
                nbi_bridges.structure_number AS nbi_structure_number,
                EXIST(planet_osm_line.tags, 'maxweight') as maxweight_set,
                EXIST(planet_osm_line.tags, 'maxheight') as maxheight_set,
                EXIST(planet_osm_line.tags, 'toll') as toll_set,
                ST_AsGeoJSON(location) AS ptjson, 
                ST_AsGeoJSON(way) AS wayjson            
            FROM
                nbi_bridge_osm_ways
            INNER JOIN
                planet_osm_line 
                    ON planet_osm_line.osm_id=nbi_bridge_osm_ways.osm_id
            INNER JOIN
                nbi_bridges
                    ON nbi_bridges.nbi_bridge_id=nbi_bridge_osm_ways.nbi_bridge_id
        """
    cur.execute(sql)
    iteration = 0
    while True:
        result = cur.fetchone()
        if result is None:
            break

        # construct top-level output GeoJSON
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "state": state.abbr,
                        "nbi_bridge_id": result['nbi_bridge_id'],
                        "nbi_structure_number": result['nbi_structure_number']
                    },
                    "geometry": json.loads(result['ptjson'])   
                },
                {
                    "type": "Feature",
                    "properties": {
                        "state": state.abbr,
                        "nbi_bridge_id": result['nbi_bridge_id'],                        
                        "osm_id": result['osm_id'],
                        "nbi_structure_number": result['nbi_structure_number']
                    },
                    "geometry": json.loads(result['wayjson'])    
                }
            ]
        }

        # fetch previously calculated intersecting ways
        intersecting_ways = []
        sql = """
        SELECT 
            DISTINCT intersecting_ways.osm_id,
            intersecting_ways.over_clearance,
            ST_AsGeoJSON(planet_osm_line.way) as wayjson,
            EXIST(planet_osm_line.tags, 'maxheight') as maxheight_set
        FROM
            intersecting_ways
        INNER JOIN
            planet_osm_line
                ON intersecting_ways.osm_id=planet_osm_line.osm_id
        WHERE
            intersecting_ways.nbi_bridge_osm_way_id=%(nbi_bridge_osm_way_id)s
        """
        params = {'nbi_bridge_osm_way_id': result['osm_id']}
        cur2.execute(sql, params)
        while True:
            result2 = cur2.fetchone()
            if result2 is None:
                break
            intersecting_ways.append({
                "type": "Feature",
                "properties": {
                    "state": state.abbr,
                    "osm_id": result2['osm_id'],
                    "maxheight_set": result2['maxheight_set'],
                    "maxheight_nbi": result2['over_clearance']
                },
                "geometry": json.loads(result2['wayjson'])  
            })

        # apply operating load, if valid and not set
        if not result['maxweight_set']:
            if result['operating_rating'] is not None:
                if (float(result['operating_rating']) > 0) and (float(result['operating_rating']) < 99):
                    geojson['features'][1]['properties']['maxweight'] = result['operating_rating']

        # apply toll status, if valid and not set
        if not result['toll_set']:            
            if result['toll_status'] == '1': # '1' = toll bridge in NBI
                geojson['features'][1]['properties']['toll'] = 'yes'

        # apply bridge way vertical clearance, if valid and not set
        if not result['maxheight_set']:
            if result['over_clearance'] is not None:
                if (float(result['over_clearance']) > 0) and (float(result['over_clearance']) < 99):                
                    geojson['features'][1]['properties']['maxheight'] = result['over_clearance']                

        # apply intersecting way vertical clearance to roads underneath
        # if under clearance type is highway or rail and maxheight not set
        if result['under_clearance_type'] in ['H', 'R']:
            # set the previously-found underclearance value            
            for (i, way) in enumerate(intersecting_ways):
                if not intersecting_ways[i]['properties']['maxheight_set']:
                    if intersecting_ways[i]['properties']['maxheight_nbi'] is not None:
                        intersecting_ways[i]['properties']['maxheight'] = intersecting_ways[i]['properties']['maxheight_nbi']

        # remove all maxheight_set attributes
        for (i, way) in enumerate(intersecting_ways):
            del intersecting_ways[i]['properties']['maxheight_set']
            del intersecting_ways[i]['properties']['maxheight_nbi']

        geojson['features'] = geojson['features'] + intersecting_ways

        with open('build/{}/{}.json'.format(state.abbr, result['osm_id']), 'w') as jsonout:
            json.dump(geojson, jsonout, indent=4) 

        iteration = iteration + 1
        pbar.update(iteration)

    pbar.finish()       
    
    cur.close()
    conn.close()


def unmatched_bridges(state):
    conn = psycopg2.connect('dbname={}'.format(DBNAME))
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print('# Storing unmatched NBI items')

    with open('build/{}-unmatched.csv'.format(state.abbr), 'w') as outfile:
        writer = csv.writer(outfile)

        sql = """
        SELECT 
            nbi_bridges.structure_number, 
            ST_AsText(nbi_bridges.location) as wkt
        FROM 
            nbi_bridges 
        LEFT JOIN 
            nbi_bridge_osm_ways 
                ON nbi_bridge_osm_ways.nbi_bridge_id=nbi_bridges.nbi_bridge_id 
        WHERE 
            nbi_bridge_osm_ways.osm_id IS NULL 
        AND
            nbi_bridges.record_type = '1'
        """
        cur.execute(sql)
        while True:
            result = cur.fetchone()
            if result is None:
                break

            writer.writerow([
                state.fips,
                result['structure_number'],
                result['wkt']
            ])

    cur.close()
    conn.close()

def main():
    states = us.states.STATES
    if (len(sys.argv)>1) and len(sys.argv[1]):
        selected_state = getattr(us.states, sys.argv[1].upper(), None)
        if selected_state is not None:
            states = [ selected_state ]


    for state in states:
            
        # reset the database
        dropdb()
        createdb()

        print('# Processing {}'.format(state.name))

        # try to load OSM -- if it fails, skip this state        
        if not load_osm(state):
            continue

        # load NBI records for current state
        load_nbi(state)

        # find matching OSM ways
        match_ways_to_bridges()

        # find OSM ways that cross the identified bridge way
        find_intersecting_ways()

        # emit geojson for each bridge & associated ways
        geojson(state)

        # record CSV of states for later tracing
        unmatched_bridges(state)
        


if __name__ == '__main__':
    main()
