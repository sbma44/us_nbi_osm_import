import sys
import csv
import os
import subprocess
import webbrowser
import us
import psycopg2
from progressbar import ProgressBar, Bar, ETA, Percentage, Counter

def execsql(sql, **params):
    conn = psycopg2.connect("dbname=nbi")
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()

def createdb():
    print('Creating database')    
    cmd = 'createdb nbi'
    subprocess.call(cmd.split(' '))

    execsql("CREATE EXTENSION postgis; CREATE EXTENSION postgis_topology;")

def dropdb():
    print('Dropping database')
    subprocess.call('dropdb --if-exists nbi'.split(' '))

def loadstate(state):
    # load state OSM
    dropdb()
    createdb()

    filename = 'us_osm/{state}-latest.osm.pbf'.format(state=state.name.lower().replace(' ', '-'))
    if not os.path.exists(filename):
        print('Skipping {filename}'.format(filename=filename))
        return False

    print('Loading {filename}'.format(filename=filename))
    cmd = 'osm2pgsql -E 4326 -c -d nbi {filename}'.format(filename=filename)
    subprocess.call(cmd.split(' '))

    return True

def file_length(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def main():
     with open('build/nbi_way_matches.csv', 'w') as outfile:
        writer = csv.writer(outfile)

        for state in us.states.STATES:
            
            # try to load OSM -- if it fails, skip this one
            if not loadstate(state):
                continue

            conn = psycopg2.connect("dbname=nbi")
            cur = conn.cursor()

            state_filename = 'nbi/nbi_{state_abbr}.csv'.format(state_abbr=state.abbr)

            pbar = ProgressBar(widgets=['{state:15}'.format(state=state.name), Percentage(), '  ', ETA(), Bar()], maxval=file_length(state_filename)).start()                    

            with open(state_filename, 'r') as csvfile:
                i = 0
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
                    under_clearance_type = row[15]
                    under_clearance = row[16] # meters; >30m is 9999

                    # toll status
                    # 1 = toll bridge
                    # 2 = on toll road
                    # 3 = free road
                    # 4 = ? - "On Interstate toll segment under Secretarial Agreement. Structure functions as a part of the toll segment."
                    # 5 = ? - "Toll bridge is a segment under Secretarial Agreement. Structure is separate agreement from highway segment."
                    toll_status = row[22]

                    # find weight limit
                    operating_rating = row[72]

                    try:
                        # extract coordinates
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

                   

                    # by default, query within 90 meters for bridges
                    distance = 90
                    if high_precision:
                        distance = 10

                    sql = """
                    SELECT 
                        osm_id
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
                    cur.execute(sql, (lon, lat, distance))
                    
                    # @TODO: save geometry to make it easy to check matches
                    while True:
                        result = cur.fetchone()
                        if result is None:
                            break
                        writer.writerow([state.abbr, structure_number, result[0]])


                    # @TODO: save clearance, query for ways under bridge
                    # @TODO: save weight limit for application to bridge

                    i = i + 1

                    pbar.update(i)
                

                    # if i==252:
                    #     url = 'http://www.openstreetmap.org/edit#map=19/{lat}/{lon}'.format(lat=lat, lon=lon)
                    #     print(url)
                    #     webbrowser.open(url)
                    #     # print('-' * len(url))
                    #     # for record in row:
                    #     #     print(record)

                    #     return

            pbar.finish()                       

            cur.close()
            conn.close()



if __name__ == '__main__':
    main()