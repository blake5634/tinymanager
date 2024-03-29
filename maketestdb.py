#!/usr/bin/python3
#from  club_database import *
from tinydb import TinyDB, Query
from tinydb.operations import delete


#
#    Make a few test databases for tinymanger.py
#
#

####################################################################
#
#        Test 1:   a "uniform" db
#
dbfname = 'test1.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)
q = Query()

for r in db:
    db.remove(q.key1.exists())  # clear old copy

rec1 = {'key1': 5, 'key2':4, 'key3':3, 'key4':2,'key5':1}
rec2 = {'key1': 5, 'key5':4, 'key3':3, 'key4':2,'key2':1}

for i in range(30):
    if i%10==0:
        r = rec2
    else:
        r = rec1
        
    db.insert(r)

####################################################################
#
#        Test 2:  every 10th rec has an extra key
#
dbfname = 'test2.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)          


for r in db:
    db.remove(q.key1.exists())  # clear old copy

rec1 = {'key1':5, 'key2':4,  'key3':3,  'key4':2, 'key5':1}
rec2 = {'key0':0,  'key1':5, 'key5':4,  'key3':3, 'key4':2, 'key2':1}

for i in range(30):
    if i==0:
        r = rec1
    elif i%10==0:
        r = rec2
    else:
        r = rec1
        
    db.insert(r)

####################################################################
#
#        Test 3: two records have wrong types for key1 (str->int)
#
dbfname = 'test3.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)          


for r in db:
    db.remove(q.key1.exists())  # clear old copy


rec = {'key1':'John Smith', 'key2':'127',  'key3':560,  'key4': ['a', 'b', 'c'], 'key5':4.2379}
ids = []
for i in range(100):
    ids.append(db.insert(rec))
    
print ('Length of test3.json: ', len(db))
#make non uniform key
modids = [ids[3], ids[12]]
print ('modifying ids: ', modids)
db.update({'key1':5},doc_ids=modids) # key1 is now not uniform

    

####################################################################
#
#        Test 4:  some records have missing keys and others wrong types
#
dbfname = 'test4.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)          


for r in db:
    db.remove(q.key1.exists())  # clear old copy if any

rec = {'key1':'John Smith', 'key2':'127',  'key3':560,  'key4': ['a', 'b', 'c'], 'key5':4.2379}
rec2 = {'key1':'John Smith',               'key3':560,  'key4': ['a', 'b', 'c'], 'key5':4.2379} # key2 missing
ids = []
for i in range(100):
    ids.append(db.insert(rec))
    
print ('Length of test3.json: ', len(db))
#make non uniform key
modids = [ids[3], ids[12]]  # 'randomly' pick  3rd, and 12th doc_ids to mess with
print ('modifying ids: ', modids)
db.update({'key1':5},doc_ids=modids) # key1 is now not uniform type
print('deleting key2 from ', ids[14], ids[18])
db.update(delete('key2'), doc_ids=[ids[14],ids[18]])

    


####################################################################
#
#        Test 5:  some records have both missing keys and extra keys
#
dbfname = 'test5.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)          


for r in db:
    db.remove(q.key1.exists())  # clear old copy if any

rec = {'key1':'John Smith', 'key2':'127',  'key3':560,  'key4': ['a', 'b', 'c'], 'key5':4.2379}
rec2 = {'key1':'John Smith',               'key3':560,  'key4': ['a', 'b', 'c'], 'key5':4.2379, 'keyxx': 25} #  
ids = []
for i in range(100):
    ids.append(db.insert(rec))
    
print ('Length of test3.json: ', len(db))
#make non uniform key
modids = [ids[5], ids[27]]  # 'randomly' pick  5th , 27th doc_ids to mess with
print ('modifying ids: ', modids)
for id in modids:
    db.insert(rec2)  # add some wierd records
 
    

    

####################################################################
#
#        Test 6: Test db with multiple tables 
#          (table 2 has flaws)
#
dbfname = 'test6.json'
#dbfname = 'testdb.json'
db = TinyDB(dbfname)
q = Query()

tables = ['test_table1','test_table2','test_table3']

for t in tables:
    dbt = db.table(t)
    dbt.truncate()  #clear it out
     
    rec1 = {'key1': 5, 'key2':4, 'key3':3, 'key4':2,'key5':1}
    rec2 = {'keyZ': 5, 'key5':'777', 'key3':3, 'key4':2,'key2':1}

    r = rec1
    for i in range(30):
        if t == 'test_table2':
            if i%10==0:
                r = rec2
            else:
                r = rec1
        dbt.insert(r)
