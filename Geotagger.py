import sys
import xml.etree.ElementTree as ET
import sqlite3 as lite
import os
import subprocess
import time
import math
from argparse import ArgumentParser, ArgumentTypeError

exifToolPath="\"c:\\py\\exiftool.exe\""
dbPath="c:\\py\\places.db"

def dbImport(inFile, sourceName):
    
    recLimit=10000
    
    log=logger("dbImport")
    
    log.wl("DBpath: %s" % dbPath)
    
    dbCon = lite.connect(dbPath)
    dbCur = dbCon.cursor()
    dbCur.execute("SELECT COUNT() FROM coords")
    nCurRows=dbCur.fetchone()

    
    log.wl("Current rows in DB: %s" % nCurRows)
    
    xml=ET.parse(inFile)
    tree=xml.getroot()
    times=tree.findall('.//{http://www.opengis.net/kml/2.2}when')
    iTimes=len(times)
    
    places=tree.findall('.//{http://www.google.com/kml/ext/2.2}coord')
    iPlaces=len(places)
    
    
    log.wl("Found %s times and %s places" % (iTimes, iPlaces))
    
    startTime= time.time()
    batchStart=time.time()
    batchStartRows=nCurRows
    if iTimes != iPlaces:  
        log.wl('Houston, we have a problem. # times/places mismatch')
    
    j=1
    
    try:
        for i in range(0, iPlaces):
        
            #log.wl("%s--%s" % (times[i].text, places[i].text))
        
            rtime=times[i].text.replace('.000','')
            place=places[i].text.replace(" ", ",")
            print(i)   
            sql= "INSERT INTO coords(name, coords, timestamp) SELECT '%s', '%s', '%s' WHERE NOT EXISTS(Select 1 from coords where timestamp ='%s')" % (sourceName, place, rtime, rtime)
            #log.wl(sql)
            dbCur.execute(sql)
            
            if j == recLimit:
              
                dbCon.commit()
                
                j=1
              
                dbCur.execute("SELECT COUNT() FROM coords")
                nCurRows=dbCur.fetchone()
                
                batchElapsed=time.time()-batchStart
                elapsed=time.time()-startTime
                
                dbRowsAdded=nCurRows[0]-batchStartRows[0]
                
                dbCur.execute("SELECT COUNT() FROM coords")
                batchStartRows=dbCur.fetchone()
                
                log.wl("Reached %s. Current record: %s. Current rows in DB: %s. %s/%s batch/total." % (recLimit, i, nCurRows[0], round(batchElapsed,3), round(elapsed,3)))
                
                dbCon.close()
                
                dbCon = lite.connect(dbPath)
                dbCur = dbCon.cursor()
                
                batchStart=time.time()
            else:
                j+=1
    
    except KeyboardInterrupt:
        log.wl("Keyboard Interrupt, Stopping")
        
    finally:
    
        dbCon.commit()
    
    
        dbCur.execute("SELECT COUNT() FROM coords")
        nNewRows=dbCur.fetchone()
        log.wl("Done. Current rows in DB: %s" % nNewRows)
        nAdded=nNewRows[0] - nCurRows[0]
        print("")
        nMarks=str(iTimes)
        log.wl( "Found %s placemarks in \"%s\". Wrote %s rows to the DB" % (nMarks, inFile, nAdded))
        print("")

       
       
def exifWriter(folderPath, limit):
    
    log=logger("exifwriter")
    
    con = None
    con = lite.connect(dbPath) 
    
    nPics=0
    nFiles=0
    #parse through picture folder
    startTime=time.time()
    
    if limit == False:
        limit = 9999
        
    log.wl("Start time: %s" % startTime)
    log.wl("Parsing %s" % folderPath)
    
    
    
    #build the files list first so it can be sorted.
    filesList=[]
    for root, subFolders, files in os.walk(folderPath):
        
        for filename in files:
            
            filesList.append(os.path.join(root, filename))


    
    filesList=sorted(filesList, reverse=True)
    
    for filePath in filesList:
        print("-------------------------------------")
        if nFiles > int(limit):
            log.wl("Reached file limit:%s" % limit)
            break
        #filePath = os.path.join(root, filename)
        filename=os.path.split(filePath)[1]
        
        allowedFiles=["jpg", "JPG", "CR2"]
        if filename[-3:] not in allowedFiles: 
            log.wl("%s Invalid filetype" % filename[-3:])
            continue
        
        #log.wl("---")
        #log.wl("")
        #log.wl("---")
        #log.wl(filename, "File:")
        #get exif timestamp and possibly gpslat
        executeThis="%s \"-DateTimeOriginal\" \"-TimeZone\" \"-GPSLatitude\" \"%s\" -S" % (exifToolPath, filePath)
    
        sp=subprocess.Popen(executeThis, stdout=subprocess.PIPE)
        output, _ = sp.communicate()
        
        #log.wl(output)
        
        lOutput=output.splitlines()
        
        try:
            
            #print ("timestamp: %s tz: %s" % (timestamp, timezone))
            
            output=str(lOutput)
            log.wl(output, 'Exif')
            exp=output.split(' ')
            timezone=exp[4][:6]
            timestamp=exp[1]+' '+exp[2][:8]
            #timestamp=stroutput[0].partition(":")[2].lstrip()
            #timezone=stroutput[1].partition(":")[2].lstrip()
            #log.wl(timestamp, 'ts')
            #log.wl(timezone, 'tz')
            
        except:
            e = sys.exc_info()[0]
            print( "%s" % e )
            log.wl( "skipping %s probably because no exif data" % filename)
            continue
            
        try:
            gpsLat=lOutput[2].partition(":")[2].lstrip()
            hasGps=True
        except:
            hasGps=False

        if hasGps: 
                                                                
            log.wl( "skipping %s for it has this latitudical information: %s" % (filename, gpsLat))
        else:
            
            #get the timestamp out of the output string and format it                
            timestamp=timestamp+timezone
            
            timestamp=timestamp.replace(":", "-", 2)
            timestamp=timestamp.replace(" ", "T")
            
            exiftimestamp=timestamp
         
            with con:
                con.row_factory = lite.Row 
                cur = con.cursor() 
                
                #find closest match to exif timestamp
                sql="SELECT ABS( strftime('%s',timestamp) - strftime('%s','%s')) AS accuracy, name, coords, timestamp from coords ORDER BY accuracy ASC limit 1" % ("%s", "%s", exiftimestamp)
            
               
                log.wl(sql, "sql")
                cur.execute(sql)
                rows = cur.fetchall()
                
                for row in rows:
                    
                    #breakup coords field
                    sCoords=row["coords"].split(",")
                    lat=sCoords[1]
                    lon=sCoords[0]
                    log.wl("%s, exifTime:%s, Found:%s, Accuracy:%s, coords:%s,%s, src:%s" % (filePath, exiftimestamp, row["timestamp"], str(row['accuracy']), lat, lon, row["name"]))
                    
                    executeThis="%s \"-GPSLatitude=%s\" \"-GPSLongitude=%s\" \"-GPSLongitudeRef=West\" \"-GPSLatitudeRef=North\" \"%s\" -overwrite_original" % (exifToolPath, lat, lon, filePath)
                    log.wl(executeThis, "call")
                    sp=subprocess.Popen(executeThis, stdout=subprocess.PIPE)
                    output, _ = sp.communicate()
                    nPics += 1
                    print("")
                    
                    
        nFiles += 1    
        
    endTime=time.time()
    log.wl("End time: %s" % endTime)
    log.wl("Edited %s pictures in %s seconds" % (nPics, endTime-startTime))
    del(log)
    
def removeGps(folderPath):
    for root, subFolders, files in os.walk(folderPath):
        log=logger("removegpstags")
        log.wl("Parsing %s" % folderPath)
        
        for filename in files:
            filePath = os.path.join(root, filename)
        
            
            executeThis="%s \"-GPSLatitude=\" \"-GPSLongitude=\" \"%s\" -overwrite_original" % (exifToolPath, filePath)
            log.wl(executeThis)
            sp=subprocess.Popen(executeThis, stdout=subprocess.PIPE)
            output, _ = sp.communicate()
            log.wl(output, "output")
            
                        
                
   # del(log)
    
def removeCr2(picFolderPath, cr2FolderPath):

    #picFolderPath="e:\\pictures\\eos"
    #cr2FolderPath="e:\\pictures\\eos raw"
    log=logger("removeCr2")
    log.wl("Parsing %s and %s (CR2)" % (picFolderPath, cr2FolderPath))
    baseFiles=[]
    for file in os.listdir(picFolderPath):
        
        
        baseFile=file.split(".")[0]
        baseFiles.append(baseFile)
    size=0
    nDeleted=0
    for file in os.listdir(cr2FolderPath):
        
        
        baseFile=file.split(".")[0]
        
        if baseFile not in baseFiles:
            filePath="%s\\%s" % (cr2FolderPath, file)
            size += os.path.getsize(filePath)
            nDeleted +=1
            try:
                os.remove(filePath)
                log.wl("Deleting %s. %s" % (file, size))
                
            except os.error as e:
                log.wl("There was a problem deleting %s" % file)
                print(e.errstr)
            except error as e:
                print(e.errstr)
    
    log.wl("Deleted %s files. %s" % (nDeleted, humansize(size)))

def humansize(nbytes):
#this came from here: http://stackoverflow.com/questions/14996453/python-libraries-to-calculate-human-readable-filesize-from-bytes
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    if nbytes == 0: return '0 B'
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

def distances():
    log=logger('comps')
    log.wl('no. stop.')
    return
    dbCon = None
    dbCon = lite.connect(dbPath)
    dbCur = dbCon.cursor()
    dbCur.execute("SELECT * FROM coords limit 10")

    rows = dbCur.fetchall()
    
    lat1="10.98396"
    lon1="90.28882"
    print(type(lat1))
    print(type(lon1))
    for row in rows:
        log.wl(row, "row")
        coords=row[1].split(",")
        lat2=coords[1]
        lon2=coords[0]
        
        distance = distance_on_unit_sphere(float(lat1), float(lon1), float(lat2), float(lon2))
        result=distance*6378
        result=float(result / 1000)
        print("comparing %s,%s and %s,%s and the result was %s" % (lon1, lat1, lon2, lat2, result)) 
        
        #prepare for next round
        lat1=lat2
        lon1=lon2
        
        
def cleanUp(startDate, endDate, speedLimit):
            log=logger('clean up')
            #startDate=validDate(startDate)
            #endDate=validDate(endDate)
            
            dbCon = lite.connect(dbPath)
            dbCur = dbCon.cursor()
            sql="select * from comps where time1 > '%s' and time1 < '%s' and speed > %s order by time1 asc" % (startDate, endDate, speedLimit)
            log.wl(sql, 'sql')
            dbCur.execute(sql)
                        
            rows=dbCur.fetchall()
            idList=[]
            log.wl('Found %s rows for %s<->%s >%s' % (len(rows), startDate, endDate, speedLimit))
            
            for row in rows:
                
                id1=row[1].split(',')[0]
                id2=row[1].split(',')[1]
                if (id1 not in idList):
                    idList.append(id1)
                    
                if (id2 not in idList):
                    idList.append(id2)
                
                
                
                    
            log.wl('Added %s IDs' % len(idList))
            nonHomePoints=0
            homePoints=0
            for id in idList:
                log.wl('---')
                sql="select * from coords where id=%s and flag is null" % id
                log.wl(sql, 'sql')
                dbCur.execute(sql)
                row=dbCur.fetchone()
                #print (len(row))
                if row is None:
                    continue
                print(row)
                if (row[2].find('118.349')!= -1):
                    if (row[2].find('33.875')!= -1):
                        
                        homePoints +=1
                        
                        sql="update coords set flag=1 where id=%s" % row[0]
                        log.wl(sql, 'sql')
                       
                        dbCur.execute(sql)    
                    else:
                        log.wl("lon matched, lon didn't")
                    
                     
                    sql="select * from comps where ids LIKE '%"+str(row[0])+"%'" 
                    log.wl(sql)
                    dbCur.execute(sql)
                    foundRows=dbCur.fetchall()
                    log.wl("[%s,%s] Coords match rbb. Rows with this ID:" % (row[2],row[3]))
                    
                    for row in foundRows:
                        
                        log.wl("IDs:[%s], Coords: [%s,%s],[%s,%s]. Time: %s--%s, Dst: %s, Tm: %s, Spd: %s" % (row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]))
                    
                else:
                
                    sql="update coords set flag=1 where id=%s" % row[0]
                    log.wl(sql, 'sql')
                       
                    dbCur.execute(sql)    
                    log.wl(row, 'non home')
                    nonHomePoints +=1
            dbCon.commit()
            log.wl("found %s home points, %s non home points" % (homePoints, nonHomePoints))
def generateJS(startDate, endDate, output):
    log=logger('generateJS')
    dbCon = lite.connect(dbPath)
    dbCur = dbCon.cursor()
    
    
    sql="select * from coords where timestamp > '%s' and timestamp < '%s' and flag is null order by timestamp desc" % (startDate, endDate)
    
    log.wl(sql, 'sql')
    dbCur.execute(sql)
                
    rows=dbCur.fetchall()    
    log.wl('Found %s rows for %s<->%s. Out:%s' % (len(rows), startDate, endDate, output))
    f_out = open(output, "w")
    
    f_out.write("window.locationJsonData = ")

    f_out.write("{\"locations\":[")
    first = True
    lTimestamps=[]
    lastLat='0'
    lastLon='0'
    lat='0'
    lon='0'
    for row in rows:
        
        lastLat=lat
        lastLon=lon
        
        
        lat=row[2].split(',')[1]
        
        lon=row[2].split(',')[0]
        
        timestamp = time.mktime(time.strptime(row[1][:20], "%Y-%m-%dT%H:%M:%SZ"));
        timestamp = timestamp*1000
        
        if str(timestamp)[:8] in lTimestamps:
            continue
        else:
            lTimestamps.append(str(timestamp)[:8])
            
        if first:
            first = False
        else:
            f_out.write(",")
        
        if getDistanceFromLatLonInKm(float(lastLat), float(lastLon), float(lat), float(lon)) > 40:
            newTrack=1
        else:
            newTrack=0
        
        
        
        f_out.write("{")
        f_out.write("\"timestampMs\":%s," % str(timestamp))
        #f_out.write("\"latitudeE7\":%s," % lat[:12].replace('.',''))
        f_out.write("\"latitudeE7\":%s," % lat)
        #f_out.write("\"longitudeE7\":%s" % lon[:10].replace('.',''))
        f_out.write("\"longitudeE7\":%s," % lon)
        f_out.write("\"newTrack\":%s" % newTrack)
        f_out.write("}")
    f_out.write("]};")
    f_out.close()
                


#from the internets
def distance_on_unit_sphere(lat1, long1, lat2, long2):

    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
        
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
        
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
        
    # Compute spherical distance from spherical coordinates.
        
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
    
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    return arc
# Haversine formula
def getDistanceFromLatLonInKm(lat1,lon1,lat2,lon2):
    R = 6371 # Radius of the earth in km
    dlat = deg2rad(lat2-lat1)
    dlon = deg2rad(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + \
    math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * \
    math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c # Distance in km
    return d


def deg2rad(deg):
    return deg * (math.pi/180)

    
class logger:
    def __init__(self, logfilename):
        logfilename=logfilename+str(time.time())[:10]+".log"
        self.log=open('logs\\'+logfilename, 'w')
    
    def wl(self, logmsg, title="log"):
        logmsg="%s::%s" % (title, logmsg)
        print(logmsg)
        self.log.write(logmsg+"\n")
        
    def __del__(self):
        self.log.close()
    
    



def validDate(sDate):
    try:
        return datetime.strptime(sDate, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(sDate)
        raise ArgumentTypeError(msg)




    

        
        
        
        
print("")
print("Welcome. Current settings:")
print("exifToolPath: %s" % exifToolPath)
print("dbPath: %s" % dbPath)
print("")
menu = {}
menu['1']="Import Google KML to DB - (Step 1)"
menu['2']="Write EXIF GPS data - (Writes GPS coordinates of closest timestamp in the DB)"
menu['3']="Nothing"
menu['4']="Remove Geotags - (Because only doing it once is no fun)"
menu['5']="Clean up CR2 files - (Delete all CR2's in a folder. Noone really know why this is here)"
menu['6']="Distances - (Write time/distance deltas to comps table)"
menu['7']="Clean Up - (Flag any points for a given timeframe where speed exceeds speedlimit)"
menu['8']="Generate JS - (Generate input.js file for map thing)"
print("")
while True:
    
   
    if len(sys.argv) >= 2:
        selection=str(sys.argv[1])
        inFile=sys.argv[2]
        print(len(sys.argv))
    else:
    
        options=list(menu.keys())
        options.sort()
        for entry in options: 
            print(entry+ ":"+ menu[entry])
        

        #******************************
        selection=input("Select An Option:") 
       
    
    
    print("")
    if selection =='1': 
        print("Ok let's do this!")
        if len(inFile) > 0 :
            fileToParse=inFile
        else:
            fileToParse=input("Enter full filepath of KML file:")
            
        if len(sys.argv) >= 4 :
            sourceName=sys.argv[3]
        else:
            sourceName=input("Enter GPS source name:")
               
        
        dbImport(fileToParse, sourceName)
        
        #if pictureFolder != False:
        #    exifWriter(pictureFolder, limit)
        break
    elif selection == '2': 
        print("Geotagging, eh?")
        print (" %s %s %s" %(sys.argv[1], sys.argv[2], sys.argv[3])) 
        if len(sys.argv[2]):
            pictureFolder=sys.argv[2]
        else:        
            pictureFolder=input("Enter full path of pictures folder:")
        #pictureFolder="e:\\Pictures\\Lightroom Storage\\2014\\2014-06-29"
        
        if len(sys.argv[3]):
            limit=sys.argv[3]
        else:
            limit=input("How many pics would you like to process? Enter for all:") or False
        
        
        exifWriter(pictureFolder, limit)
        
        break
    elif selection == '3':
        print("What exactly are you expecting here? Bye") 
        break
        
        
    elif selection == '4':
    
        if len(sys.argv[2]):
            pictureFolder=sys.argv[2]
        else:
            pictureFolder=input("This will remove GPS tags! Enter full path of pictures folder:")
   #        pictureFolder="e:\\Pictures\\Lightroom Storage\\2014\\2014-06-29"
        removeGps(pictureFolder)
        break
        
        
    elif selection == '5': 
        pictureFolder=input("CR2 Cleanup: Enter full path of pictures folder:")
        cr2Folder=input("Enter full path of CR2 folder:")
        
        removeCr2(pictureFolder, cr2Folder)
        break
        
    elif selection == '6':
        distances()
        break
        
    elif selection == '7': 
        if len(sys.argv)>=2:
            startDate=sys.argv[2]
        else:        
            startDate=input("Enter start date(YYYY-MM-DD):")
        
        if len(sys.argv)>=3:
            endDate=sys.argv[3]
        else:
            endDate=input("Enter end date(YYYY-MM-DD):") or False
        
        if len(sys.argv)>=4:
            speedLimit=sys.argv[4]
        else:
            speedLimit=input("Enter fastest valid line(MPH):") or False
        
        cleanUp(startDate, endDate, speedLimit)
        break
        
    elif selection == '8': 
        if len(sys.argv)>=2:
            startDate=sys.argv[2]
        else:        
            startDate=input("Enter start date(YYYY-MM-DD):")
        
        if len(sys.argv)>=3:
            endDate=sys.argv[3]
        else:
            endDate=input("Enter end date(YYYY-MM-DD):") or False
        
        if len(sys.argv)>=4:
            output=sys.argv[4]
        else:
            output=input("Enter output file name:") or False
        
        generateJS(startDate, endDate, output)
        break
    else: 
        print("Unknown Option Selected!") 




