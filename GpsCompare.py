import sqlite3 as lite
import time
from texttable import Texttable
from tabulate import tabulate
from datetime import datetime
from math import radians, cos, sin, asin, sqrt
from decimal import *


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    km = 6367 * c
    m=km*1000
    
    return m



dbPath="c:\\py\\places.db"
dbCon = lite.connect(dbPath)
dbCon2 = lite.connect(":memory:")
dbCur2=dbCon2.cursor()
dbCur = dbCon.cursor()
dbCur.execute('SELECT ID, timestamp, coords, name FROM coords order by id asc')
coords=dbCur.fetchall()
    
i=0
j=1
table=Texttable()

table.header(["ID", "Lon", "Lat", "Timestamp", "Dst", "T(s)", "Spd"])
table.set_cols_width([4,18,18,18,7,5,7])
#coords.reverse()
for rec in coords:
    i+=    1
    if i==1:
        lon1=float(rec[2].split(",",)[0])
        lat1=float(rec[2].split(",",)[1])
        id1=rec[0]
        timestamp1=rec[1]
        
        continue
        

    lon2=float(rec[2].split(",",)[0])
    lat2=float(rec[2].split(",",)[1])
    id2=rec[0]
    timestamp2=rec[1]
    
    
    
    
    sql="select strftime('%s','%s') - strftime('%s','%s')" % ("%s", timestamp1, "%s", timestamp2)
    
    dbCur2.execute(sql)
    timediff = dbCur2.fetchone()
    
    distance=haversine(lon1, lat1, lon2, lat2)
    #if distance > 9999999999:
#        continue
    
    try:
        speed=distance/timediff[0]
    except (ZeroDivisionError):
        continue
        
    speed=speed*2.236936
    
    
    
    
    
    
    
    row=["%s\n%s" % (id1, id2), "%s\n%s" % (lon1, lon2),  "%s\n%s" % (lat1, lat2), "%s\n%s" % (timestamp1[2:19], timestamp2[2:19]), "%.2f\nmtrs" % distance, "%s\nsecs" % timediff[0], "%.2f\nmph" % speed ]

    table.add_row([row][0])

    sql="insert into comps (ids, lon1, lon2, lat1, lat2, time1, time2, distance, elapsed, speed) values ('%s,%s', '%s','%s', '%s','%s', '%s','%s', '%.2f','%s', '%.2f')" % (id1, id2, lon1, lon2, lat1, lat2, timestamp1, timestamp2, distance, timediff[0], speed)
    
    
    dbCur.execute(sql)
    
    lon1=lon2
    lat1=lat2
    id1=id2
    timestamp1=timestamp2
    if j == 50000:
        dbCon.commit()
        j=1
        print ("%s\n" % i)
    
    j+=1    
dbCon.commit()
print (table.draw() + "\n")
#print tabulate(table, headers="firstrow")
    

        
        
