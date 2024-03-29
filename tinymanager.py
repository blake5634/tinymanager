#!/usr/bin/python3
#from  club_database import *
import sys
from tinydb import TinyDB, Query
import uuid
import shutil
import os
import json
#from tinydb.operations import delete



#dbnamelist = ['test1.json','test2.json', 'test3.json'] #'testRobCldb.json']
#dbfname = 'testRobCldb.json'
#dbfname = 'test1.json'
#dbfname = 'test2.json'
#dbfname = 'testdb.json'

q = Query()

class tdb_file():
    def __init__(self,fname):
        self.schema = {'tables':[], 'table_fields':{}} # list of strings / dict of table 2-lists
        self.tablelist = []
        self.name = fname
        self.db = None
        # check filename
        if not fname.endswith('.json'):
            print (dbfname, ' is not a JSON file')
            print ('ignored')
        # identify the tables if present
        else: # valid filename
            #print('opening ', self.name)
            self.db = TinyDB(self.name)   
            #print(' database: ', self.db)
            self.tablelist = list(self.db.tables()) # don't need _default
            #print('    tables: ', self.tablelist )
            #print ('tdb_files.init(): ' , self.tablelist)
            
            
    def display_schema(self):
        print('\n   Schema Report for',self.name, ', all tables.')
        for t in self.schema['tables']:
            keydat = self.schema['table_fields'][t]
            nrec = len(self.db.table(t))            
            print ('\nTable: {:15} nrecords: {:4}'.format( t, nrec))
            for kd in keydat:
                print('{:20} {:15}'.format(kd[0],kd[1]))  
            
    def read_schema(self):
        schema_fname = self.name+'_SCHEMA_.json'
        if os.path.isfile(schema_fname):
            print('I found a schema description file: ', schema_fname)
            f = open(schema_fname, 'r')
            self.schema = json.load(f)
            return True
        else:
            print('No existing schema file for ', self.name)
            return False
        
    # generate schema based on existing tdb_file.
    def auto_schema(self):  # collect a schema from first document(s) in the db file
        #
        #  Note: this is basically a convenience hack -- assuming the first record
        #     of each table FOLLOWS the schema
        #
        
        # Schema: 
        # tables: ['table1', 'table2' etc] = self.name.tables
        # table_fields = {tablename: [['key1',str(type1)], ['key2',str(type2)], ['key3',str(type3)], etc], table2name: [[],[],etc]}
        if  self.db==None:
            print('auto_schema: no database!')
            quit()
            
        print ('\n--------------------------------------------------------------------------\n')
    
        schema_fname = self.name+'_SCHEMA_.json'
        if os.path.isfile(schema_fname):
            print('I found a schema description file: ', schema_fname)
            f = open(schema_fname, 'r')
            self.schema = json.load(f)
        else:
            print (self.name, ' does not seem to have a schema description file')
            x = input('Do you want to auto-generate schema from first record? (y/N):')
            if x.lower() == 'y':
                # get the tables
                self.schema['tables'] = list(self.db.tables())
                self.schema['table_fields'] = {}
                # go through the tables
                for t in self.schema['tables']:  # includes the _default table
                    #print ('looking at table: ', t) 
                    self.schema['table_fields'][t] = []
                    dbt = self.db.table(t)
                    # get the keys and key types for each table
                    firsttime = True   
                    # get "first" record
                    for r in dbt:
                        #print ('record:', r)
                        if firsttime:
                            for k in r.keys():
                                self.schema['table_fields'][t].append( [k, str(type(r[k]))] ) #  
                            #print ('\n\nInital keys for:', t, ', ',self.schema['table_fields'][t])
                            firsttime = False
                        else:
                            break
                # generate the schema file
                
                f = open(schema_fname, 'w')
                json.dump(self.schema,f) 
                
            else:
                print('Please generate a schema file manually')
                quit()
                #print ('File: ',self.name)
                #print (self.schema)
    
        
        
class tdb_validator():
    def __init__(self,dbf, table): # args:  class tdb_file dbf;  str tablename)
        self.db = dbf.db.table(table) # table could be '_default'
        self.tablename = table
        self.dfile = dbf  #class tdb_file
        self.result = {}
        self.keysAllUniformType = False
        self.keyuniformity = {}
        self.samekeysflag = False
        self.schema_keys = []        # keys from initial record
        self.schema_types = {}        # types of  keys
        self.profdata = {}       # reports from analyses
        self.validation_data = {}                  
        
    def repair_uniformity(self):
        q = Query()
        if self.validation_data == {} or self.schema_keys == []:
            print("Can't repair database")
            quit()
        nmiss = 0
        nextra = 0
        print(' >> repairing: ', self.validation_data['badrecordIDs'])
        for id in self.validation_data['badrecordIDs']: # problem records
            rec2fix = self.db.get(doc_id = id)
            print ('fixing keys: ',id) #, rec2fix)
            eks = [] # place to store the extra keys in this id
            #
            #  fix EXTRA keys
            #
            if id in self.validation_data['extrakeyIDs']:
                print('fixing extra keys: ',id)
                # lets get rid of the extra keys:
                for k in rec2fix.keys():  # go through the keys
                    #print('looking at ',k,' in record ', id,self.schema_keys)
                    if str(k) not in self.schema_keys:
                        print ('found an extra key: ',id, k)
                        eks.append(str(k))  # store this key for deletion
                        nextra += 1
            #
            #  fix MISSING keys
            #
            if id in self.validation_data['missingkeyIDs']:
                print('fixing missing keys: ',id)
                for k in self.schema_keys:
                    if str(k) not in rec2fix.keys():
                        #print ('found a missing key: ',id, k)
                        rec2fix[k] = ''  # add the missing key with null value 
                                         # note could be wrong type!
                        nmiss += 1
            for k in eks:  # now remove any extra keys
                del rec2fix[k]
            self.db.remove(doc_ids=[id])
            newid = self.db.insert(rec2fix)  # update corrected db rec.
            print ('newid: ', newid, '  old: ', id)
            tmp = [newid if x == id else x for x in self.validation_data['badrecordIDs']] # replace id
            self.validation_data['badrecordIDs']= tmp
            tmp = [newid if x == id else x for x in self.validation_data['typeproblemIDs']] # replace id
            self.validation_data['typeproblemIDs']= tmp
        print ('done with missing/extra key repair')
        print ('  repaired ',nmiss, ' missing keys')
        print ('  repaired ',nextra, ' extra keys')
        
        desiredtypes = {}
        for tf in self.dfile.schema['table_fields'][self.tablename]:
            desiredtypes[tf[0]] = tf[1] # store typestrings by key
        ## repair types
        nfixt = 0
        for id in self.validation_data['typeproblemIDs']: 
            rec2fix = self.db.get(doc_id = id)
            print ('fixing type: ',id)   #, rec2fix)
            for k in rec2fix.keys():
                desiredtype = str(desiredtypes[k])
                #print ('     key:', k, '   des type: ['+desiredtype+']  actual: ['+str(type(rec2fix[k]))+'] ')
                if str(type(rec2fix[k])) != desiredtype:
                    #print(' - fixing - ', rec2fix[k])
                    if desiredtype == "<class 'str'>":
                        rec2fix[k] = str(rec2fix[k])
                    elif desiredtype == "<class 'int'>":
                        try:
                            rec2fix[k] = int(rec2fix[k])
                        except:
                            rec2fix[k] = 0
                    elif desiredtype == "<class 'float'>":
                        rec2fix[k] = float(rec2fix[k])
            print('updating record: ', id)
            self.db.update(rec2fix,doc_ids=[id])
            nfixt += 1
            self.db.update(rec2fix,doc_ids=[id])
        print ('\ndone with type error repair')
        print ('   repaired ', nfixt, ' type errors')
    
    def uniformity(self): #self.cr = record(cldb,clschema)
        badrecordIDs = []  # doc_id's of docs (records) with missing or extra keys
        missingkeyIDs = []
        extrakeyIDs = []
        typeproblemIDs = []
        self.samekeysflag = True
        self.keysAllUniformType = True
        self.keyuniformity = {}    # which keys are uniform type throughout db
        self.schema_keys = []        # list of keys from schema
        
        ##  unpack the schema
        if self.dfile.schema['tables'] == []:
            print ('you called uniformity without getting schema first')
            quit()
        this_tbl = self.tablename
        keypair_list = self.dfile.schema['table_fields'][this_tbl]
        for kp in keypair_list:
            key = kp[0]
            type_str = kp[1]
        t = self.dfile.schema['table_fields'][self.db.name]
        self.schema_types = {}        # types of initial keys
        for f in t:
            self.schema_types[f[0]] = f[1]  # make into dictionary
            self.schema_keys.append(f[0])
            self.keyuniformity[f[0]] = True
        #
        #  go through all records
        # 
        for r in self.db:
                #
                #  check for missing/extra keys
                #
                kl = sorted(list(r.keys()))    # this records' keys
                skl = sorted(self.schema_keys) # schema keys
                if kl != skl:   # are the record keys exactly same?
                    self.samekeysflag = False
                    self.keysAllUniformType = False # all docs do not have same keys
                    #print ('dif:', r.keys())
                    badrecordIDs.append(r.doc_id)
                    for sk in skl:
                        if sk not in r.keys():
                            missingkeyIDs.append(r.doc_id) 
                            break
                    for rk in kl:
                        if rk not in self.schema_keys:
                            extrakeyIDs.append(r.doc_id)
                            break
                #
                # Check for invalid type (of each value)
                #
                for k in kl: # go through keys in this record
                    try:
                        if str(type(r[k])) != self.schema_types[k]:
                            self.keysAllUniformType = False  # at least one key mixes types
                            self.keyuniformity[k]   = False    # key k, mixes types
                            typeproblemIDs.append(r.doc_id)
                            badrecordIDs.append(r.doc_id)
                    except:
                        pass # (no need to check type of an EXTRA key (not present in schema))
                    
        for k in self.schema_keys:
            if not self.keyuniformity[k]:
                self.schema_types[k] = 'multiple types'     
        self.validation_data['badrecordIDs']   = list(set(badrecordIDs))  #remove duplicates
        self.validation_data['missingkeyIDs']  = list(set(missingkeyIDs))
        self.validation_data['extrakeyIDs']    = list(set(extrakeyIDs))
        self.validation_data['typeproblemIDs'] = list(set(typeproblemIDs))
        if len(self.validation_data['badrecordIDs']) == 0:
            self.validation_data['valid'] = True
        else:
            self.validation_data['valid'] = False

    def valid_TF(self):
        self.uniformity()
        return (self.validation_data['valid'])
        
    def unif_report(self):
        prob=False
        if self.samekeysflag:
            print('\n      All documents (records) have same keys')
            prob=True
        if self.keysAllUniformType:
            print('      All keys have uniform types')
            prob=True
        if prob:
            #print('      Some keys do not belong to all documents (records)')
            print('club record IDs with key problems: ',   self.validation_data['badrecordIDs'])
            print('\nclub record IDs with missing keys: ', self.validation_data['missingkeyIDs'])
            print('\nclub record IDs with extra keys: ',   self.validation_data['extrakeyIDs'])
            print('\nclub record IDs with type problems: ',   self.validation_data['typeproblemIDs'])
            
    def profile(self):
        hugeint = 9999999999999999999999999999
        if not self.samekeysflag:
            td_error('cant profile unless all docs (records) have same keys')
        nk = len(self.schema_keys)
        # set up profiles of the keys with type int.
        self.profdata = {}
        self.profdata['N']=len(self.db)
        maxint = {}
        minint = {}
        meanint = {}
        nint = {}
        for k in self.schema_keys:
            if self.schema_types[k] == type(5):
                maxint[k] = -hugeint
                minint[k] = hugeint
                meanint[k] = 0
                nint[k] = 0
        else:
            for r in self.db:
                for k in self.schema_keys:
                    if self.schema_types[k] == type(5):
                        t = r[k]
                        if t > maxint[k]:
                            maxint[k] = t
                        if t < minint[k]:
                            minint[k] = t
                        meanint[k] += t
                        nint[k] += 1
        self.profdata['keys'] = self.schema_keys
        self.profdata['types'] = self.schema_types
        self.profdata['int_mins'] = minint
        self.profdata['int_maxs'] = maxint
        for k in self.schema_keys:
            if self.schema_types[k] == type(5):
                meanint[k] = float(meanint[k])/float(nint[k])
        self.profdata['int_means'] = meanint
        
                
        
def td_error(msg):
    print('tinyDB validator: '+msg)
    quit()

def backup_tiny_json(name):
    approxUID = str(uuid.uuid1())[0:10]  # truncate to be nicer but low-risk
    bufilename = name.replace('.','_')+'_BACKUP_'+ approxUID # from db_or_table_list
    print('backing up to: ', bufilename)
    shutil.copy(name, bufilename)
    print('done')

def explainer():
    txt = ''' 
TinyManager

           A management tool/API for tinyDB json databases
           
    ***  Note: it is *assumed* by this SW that you want all documents (records)
    in the db to have 
              1) the same set of keys
              2) the values for each key are all of the same key-specific type
              3) *** it is assumed that the FIRST record in the db/table is correct.
                 In other words the first record corresponds to a "schema" for the db.
                
    These assumptions are not required by tinydb and may or may not be what 
    you want for your application.           
           
    usage:   > python tinymanager.py  [--help] file1.json file2.json ...
    
    
Step 1: 
    Checks "quality" of your tinyDB database. First we determine if all 
    documents (records) have the same set of keys and we list them
    
    if all documents have same keys:
        We analyze the data type of each key and we check if all 
        documents store the same (uniform from doc to doc) type in each key.
        
        
    if not, you will be offered the option to repair your database.   A backup of
    the original .json file will be created automatically for you first. 
    
Step 2: 
    Edit database using an existing tool such as
    
        https://jsoneditoronline.org
        
        
    '''
    print(txt)
    
    
if __name__ == '__main__':
    
    # This is basically a *demo* ap for the tinymanager API. See also test_tiny_mgr.py
    #
         
    #
    # deal with multiple tables in the db!!!
    #        
    if len(sys.argv) ==1 or sys.argv[1].replace('-','').lower() =='help':
        explainer()
        quit()
    
    dbfile_list = []
    db_or_table_list = []
    
    dbfnamelist = sys.argv[1:]   # list of json files.
    
    #expand all dbs into tables (if they have them)
    for dbfname in dbfnamelist:
        dbfile_list.append(tdb_file(dbfname)) 
        
    for dbf in dbfile_list:
        print ('looking at: ', dbf.name)
        sc_present = dbf.read_schema()
        if not sc_present:
            dbf.auto_schema()    # collect schema for this file.
        dbf.display_schema()
        resp = input('Does this schema look good? y/N:')
        if resp.lower() != 'y':
            dbf.db = None
        if dbf.db is not None:
            if len(dbf.tablelist) == 0:
                db_or_table_list.append([dbf, None, dbf.name])
            else:
                for table in dbf.tablelist:
                    db_or_table_list.append([dbf, table, dbf.name])
        else: 
            print('There seems to be no database! somethings wrong, goodbye')
            quit()
    for item in db_or_table_list:
        dbf = item[0]
        table = item[1]
        dname = item[2]
        dbparent = dbf.db # the db itself
        if table : # if there is a table
            db = dbparent.table(table)
        else: 
            db = dbparent.table('_default')

        print ('\n--------------------------------------------------------------------------\n')
        print('\n\n      Testing Database:', dname, '   Table: ', table)  
        
        v = tdb_validator(dbf, table)
        #dbf.display_schema()
        #quit()
        v.uniformity()  # check uniformity of keys
        v.unif_report()
        
        
        print('\n      Key Type analysis\n')
        print('{:<20} {:<30} {}'.format('Key','Type','Uniform?'))
        print('--------------------------------------------------------')
        r = db.all() # first record
        if len(r) == 0:
            print ('There are no documents(records)')
        else:
            #print (r)
            r = r[0] # just use first one
            for k in v.schema_keys:
                ktype = v.schema_types[k]
                print ('{:<20} {:<30} {}'.format(k,str(ktype),v.keyuniformity[k]))
                
        if v.samekeysflag:
            v.profile()            
            print ('\nProfile data:',v.profdata['N'],' documents(records)')
            print ('Mins: ', v.profdata['int_mins'])
            print ('Max:  ', v.profdata['int_maxs'])
            print ('Mean: ', v.profdata['int_means'])
    
        print ('we found ', len(v.validation_data['badrecordIDs']), ' bad records')
        if len(v.validation_data['badrecordIDs']) > 0:
            x = input(' Do you want to repair the database '+dname + '   Table: '+table+ '(y/N) ?')
            if x.lower() == 'y':
                #
                #   make backup before repair
                #
                backup_tiny_json(dname)
                v.repair_uniformity()
                


