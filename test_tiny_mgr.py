#!/usr/bin/python3
from tinymanager import *

#
#  Testing pre-made testN.json databasess
#     for each databas, it should fail
#     the validator, then be fixable, then
#     pass the validator.
#
#  
os.system('python3 maketestdb.py')  # initialize the dbs
dbfn_list = []
db_or_table_list = []

#expand all dbs into tables (if they have them)

dbfn_list = ['test1.json', 'test2.json', 'test3.json', 'test4.json', 'test5.json', ]
    
    
for fn in dbfn_list:
    dbf = tdb_file(fn)
    print ('Running test: '+dbf.name)
    #following logic unpacks tinydb's that have non-default tables
    if dbf.db is not None:
        dbf.auto_schema()  # get schema just once
        if len(dbf.tablelist) == 0:
            db_or_table_list.append([dbf, None, dbf.name])
        else:
            for table in dbf.tablelist:
                db_or_table_list.append([dbf, table, dbf.name])
    else:
        print('couldnt open '+fn)
print('list: ', db_or_table_list)
report = []
report.append('\n\n  Testing report: \n')
# now go through all tables
for item in db_or_table_list:
    result = 'Pass'        
    dbf = item[0]
    table = item[1]
    dname = item[2]
    print ('testing: ',dname, table)
    dbparent = dbf.db # the db itself
    if table : # if there is a table
        db = dbparent.table(table)
    else: 
        db = dbparent.table('_default')
        
    v = tdb_validator(dbf,table)
    if v.valid_TF() == True:
        if dbf.name != 'test1.json':  # test1.json is made to be valid!
            result = 'Fail - invalid db tested good:'
        else:
            result = 'Pass'  
    else:
        v.repair_uniformity()
    if v.valid_TF() == False:
        result = 'Fail - repair failed.'
    report.append('    '+dname+'   '+table+'      result: '+result+'\n')

for line in report:
    print(line)
    
        
        
