import savReaderWriter
import numpy
import mysql.connector
import numpy as np
import datetime

#savFileName = "C:/Users/yoni/Desktop/ALL APK/spss iom/TestSpss.sav"
#-----------this line of code print all variable with value
#
#
#
#with savReaderWriter.SavReader(savFileName) as reader:
#    for line in reader:
#        print(line)
#
#
#
#-----------this line of code print the head of spss
#
#
#
#with savReaderWriter.SavHeaderReader(savFileName) as header:
#    report = str(header) # txt report
#    metadata = header.all()
#
#
#-----------this code print var of the spss
#
#
#with savReaderWriter.SavHeaderReader(savFileName) as spssDict:
#        wholeDict = spssDict.dataDictionary()
#        print(spssDict)
#
#
#------------this code create spss file with data
#
#
#savFileName = 'someFile.sav'
#records = [[b'Test1', 1, 1], [b'Test2', 2, 1]]
#varNames = ['var1', 'v2', 'v3']
#varTypes = {'var1': 5, 'v2': 0, 'v3': 0}
#with savReaderWriter.SavWriter(savFileName, varNames, varTypes) as writer:
#    for record in records:
#        writer.writerow(record)
#
#----------this code print the last 4 rows
#
#
#data = savReaderWriter.SavReader(savFileName) 
#print("The last four records look like this: %s" % data.tail(4))
#data.close()
#
#
#---------this code print the length of rows
#print(len(savReaderWriter.SavReader(savFileName)))
#
#
#--------this code retunt the length of rows as the same to len
#data = savReaderWriter.SavReader(savFileName)
#xyz=data.shape.nrows# == len(data) # True
#print(xyz)
#data.close()
#
#
#--------this code return row and col value
#data = savReaderWriter.SavReader("someFile.sav") 
#print("The first six records look like this: %s" % data[:6])
#print("The first record looks like this: %s" % data[0])
#print("First column: %s" % data[..., 0]) # requires numpy
#print("Row 2 & 5, first three cols: %s" % data[1:3, :3])
#data.close()
#
#
#
#
#--------
dbspss = "C:/Users/yoni/Desktop/IMO Nfi Entry/spss iom/TestSpss.sav"#
cnx = mysql.connector.connect(user='root', password='',host='127.0.0.1',database='pty_test')#open MySql database
cursor = cnx.cursor()#load cursor
cursorUPD= cnx.cursor()#load cursor other cursor for update the record
cursor.execute('SELECT * FROM SPSST')#run sql query and put it on cursor

#cursor.execute('SELECT concat("b'",name,"'") FROM spss_val WHERE 1 ORDER BY concat("b'",name,"'") ASC')
remaining_rows = cursor.fetchall()#fetch all every selected rows and put it on cursor

with savReaderWriter.SavHeaderReader(dbspss) as header:#open spss file for collecting meta data
    metadata = header.all()#get all meta data from spss
# the following array declaration help to create [[],[],.....]
myouter=[]#declare array for outer array
myinner=[]#declare array for inner element
# the following nested loop used to create array structor
for r in remaining_rows:#loop itteration for every database selected rows
    myinner=[]#this piece of code help us to clear existion value
    for i in r:#itteration for every data columen of databse 
        if isinstance(i,str):
            
            myinner.append(i.encode('UTF-8'))#every row/column data converted to bytes which is allow us to insert on spss file and append on inner array
        
        elif i==None:#i replace a null value to empity string and append it on the array
            myinner.append(b'')
            #print('i got none')
        else:
            myinner.append(i)
            #print(myinner)
    myouter.append(myinner)#after every inner array preparation i append it on outer array
    #-->19-nov-2018 print(myouter)
YonisavFileName = 'TestSpss.sav'#define on which spss data to be inserted
with savReaderWriter.SavWriter(YonisavFileName,mode=b'ab', *metadata) as writer:#open spss data for insert by append style "b'ab'" work the append
    
    for record in myouter:#do itteration for every row
        #-->19-nov-2018 print(record[2])#This 
        #-->19-nov-2018 print(record[25])
        #Date formating start here----------------------
#The following 4 lines deal with date time conversion
        #i have two spss variable which has date data type for those variable i need to have date formating
        #i manually get a date variable array index which is 2 and 25
        #i create a date value using spssDateTime functon and put value on the record array
        spssDateValue = writer.spssDateTime(record[2], '%d-%m-%Y')
        record[2]=spssDateValue
        spssDateValue = writer.spssDateTime(record[25], '%d-%m-%Y')
        record[25]=spssDateValue
#Date formating end here------------------------------
        # UPDATE and INSERT statements for the old and new salary
        yoni_update_table = ( "UPDATE allsp SET Status = %s WHERE FormNumber = %s")
        #-->19-nov-2018 print(yoni_update_table)
        tezz='t'
        print('next i show you the row info')
        #-->19-nov-2018 print(record[0])#to extract unique key for update the database
        writer.writerow(record)#write every data as row fasshion
        print('i do write the record on SPSS')
        #next i run the update query to the table db %s on script require parameter pass and i pass tezz and record[0] value
        cursorUPD.execute(yoni_update_table, (tezz, record[0])) #run update query the vale passed from array value of form name
        cnx.commit()#to commit the update query
        print('i do update the db record')
cursor.close()#close the cursor
cnx.close()#close MySql connection
