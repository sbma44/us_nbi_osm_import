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
    conn.commit()
    cur.close()
    conn.close()

def dropdb():
    print('# dropping database')
    subprocess.call('dropdb --if-exists {}'.format(DBNAME).split(' '))

def loadstate(state):
    # load state OSM
    dropdb()
    createdb()

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

# detect when a pair of bridges have been grouped with a pair of ways
# in a duplicative manner
# each way must have at most one bridge
# each bridge may have multiple ways
def mergebridges(filename):
    # tf = tempfile.NamedTemporaryFile(delete=False)
    # tf_name = tf.name
    # writer = csv.writer(tf)    

    way_ids = {}
    i = 0
    with open(filename, 'r') as readerfile:      
        reader = csv.DictReader(readerfile)
        for row in reader:
            if not row['osm_id'] in way_ids:
                way_ids[row['osm_id']] = []
            way_ids[row['osm_id']].append((row['structure_number'], row['dist'], i))
            i = i + 1

    for (way_id, bridges) in way_ids.items():
        print('# {}'.format(way_id))
        for bridge in sorted(bridges):
            print('  {}'.format(bridge))

    # for way_id in way_ids:
    #     comp_a = set(map(lambda x: x[0], way_ids[way_id]))
    #     for way_id2 in way_ids:
    #         if way_id == way_id2:
    #             continue
    #         comp_b = set(map(lambda x: x[0], way_ids[way_id2]))
    #         if comp_a == comp_b:
    #             # ... something?
    #             print('OSM way IDs {} and {} are pointing at the same bridges'.format(way_id, way_id2))

def main():
    writefile = open('build/nbi_way_matches.csv', 'w')
    writer = csv.writer(writefile)
    writer.writerow(['state', 'structure_number', 'osm_id', 'lon', 'lat', 'dist', 'maxheight', 'maxweight'])

    for state in (us.states.FL,):
    # for state in us.states.STATES:
        
        # try to load OSM -- if it fails, skip this one
        if not '--noload' in sys.argv:
            if not loadstate(state):
                continue

        conn = psycopg2.connect('dbname={}'.format(DBNAME))
        psycopg2.extras.register_hstore(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        sql = "SELECT COUNT(*) AS total FROM planet_osm_roads WHERE bridge='yes'"
        cur.execute(sql)
        total_osm_bridges = cur.fetchone()['total']

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

               
                # by default, query within ~90 meters for bridges
                distance = 0.002
                if high_precision:
                    distance = 0.001

                sql = """
                SELECT 
                    osm_id, 
                    tags,
                    ST_Distance(ST_Centroid(way), ST_GeomFromText('POINT(%s %s)', 4326)) as dist

                FROM planet_osm_roads
                
                WHERE 
                    bridge='yes'
                
                AND
                    ST_DWithin(
                        way,
                        ST_GeomFromText('POINT(%s %s)', 4326),
                        %s
                    )
                """
                cur.execute(sql, (lon, lat, lon, lat, distance))                    

                # @TODO: save geometry to make it easy to check matches
                results = cur.fetchall()
                                    
                if len(results)>0:

                    maxheight = over_clearance
                    maxweight = operating_rating

                    for r in results:
                        for tag in r['tags']:
                            if tag[0:len('maxheight')] == 'maxheight':
                                maxheight = None
                            if tag[0:len('maxweight')] == 'maxweight':
                                maxweight = None

                        # skip well-tagged bridges
                        if maxheight is not None and maxweight is not None:
                            continue

                        writer.writerow([state.abbr, structure_number, r['osm_id'], lon, lat, r['dist'], maxheight, maxweight])

                        matched_bridges = matched_bridges + 1
                        
                        # url = 'http://www.openstreetmap.org/edit#map=19/{lat}/{lon}'.format(lat=lat, lon=lon)
                        

                # @TODO: save clearance, query for ways under bridge
                # @TODO: save weight limit for application to bridge

                i = i + 1

                pbar.update(i)
                        

        pbar.finish()                       

        cur.close()
        conn.close()        

        print('# matched {} bridges to ways (of {} ways) in {}'.format(matched_bridges, total_osm_bridges, state.name))

    writefile.close()
    mergebridges('build/nbi_way_matches.csv')

if __name__ == '__main__':
    main()