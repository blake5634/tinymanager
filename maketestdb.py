#!/usr/bin/python3
#from  club_database import *
from tinydb import TinyDB, Query


#
#    Make a few test databases for tinymanger.py
#
#

####################################################################
#
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

    

    

    
