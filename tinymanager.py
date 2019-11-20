#!/usr/bin/python3
#from  club_database import *
import sys
from tinydb import TinyDB, Query
import uuid
import shutil
#from tinydb.operations import delete



#dbnamelist = ['test1.json','test2.json', 'test3.json'] #'testRobCldb.json']
#dbfname = 'testRobCldb.json'
#dbfname = 'test1.json'
#dbfname = 'test2.json'
#dbfname = 'testdb.json'

q = Query()


class tdb_validator():
    def __init__(self,db):
        self.db = db
        self.result = {}
        self.keysAllUniformType = False
        self.keyuniformity = {}
        self.samekeysflag = False
        self.firstkeys = []        # keys from initial record
        self.firsttypes = {}        # types of initial keys
        self.profdata = {}       # reports from analyses
        self.unifdata = {}                  
        #self.unifdata['badrecordIDs'] = badrecordIDs
        #self.unifdata['missingkeyIDs'] = missingkeyIDs
        #self.unifdata['extrakeyIDs'] = extrakeyIDs
    
    def repair_uniformity(self):
        q = Query()
        if self.unifdata == {} or self.firstkeys == []:
            print("Can't repair database")
            quit()
        nmiss = 0
        nextra = 0
        for id in self.unifdata['badrecordIDs']: # problem records
            rec2fix = self.db.search(q.ClubID == id)[0]
            if id in self.unifdata['extrakeyIDs']:
                # lets get rid of the extra keys:
                for k in rec2fix.keys():  # go through the keys
                    if k not in self.firstkeys:
                        self.db.remove(q.ClubID == id) # delete the key
                        nextra += 1
            elif id in self.unifdata['missingkeyIDs']:
                for k in self.firstkeys:
                    if k not in rec2fix.keys():
                        self.db.update({k:''}, q.ClubID == id)  # add the key
                        nmiss += 1
        print ('done with missing/extra key repair')
        print ('  repaired ',nmiss, ' missing keys')
        print ('  repaired ',nextra, ' extra keys')
        
    def uniformity(self): #self.cr = record(cldb,clschema)
        badrecordIDs = []  # clubID of records with missing or extra keys
        missingkeyIDs = []
        extrakeyIDs = []
        self.samekeysflag = True
        self.keysAllUniformType = True
        firsttime = True
        self.firstkeys = []        # keys from initial record
        self.firsttypes = {}        # types of initial keys
        self.keyuniformity = {}    # which keys are uniform type throughout db
        for r in self.db:
            if firsttime:
                self.firstkeys = r.keys()
                for k in self.firstkeys:
                    self.firsttypes[k] = type(r[k])
                    self.keyuniformity[k] = True
                #print ('Inital keys:', self.firstkeys)
                firsttime = False
            else:
                if r.keys() != self.firstkeys:   # are the record keys exactly same
                    self.samekeysflag = False
                    self.keysAllUniformType = False # all docs do not have same keys
                    #print ('dif:', r.keys())
                    badrecordIDs.append(r['ClubID'])
                    if len(r.keys()) < len(self.firstkeys):
                        missingkeyIDs.append(r['ClubID'])  # unless ClubID is missing!!
                    else: # extra keys
                        extrakeyIDs.append(r['ClubID'])
                else: # familiar keys
                    for k in self.firstkeys:
                        if type(r[k]) != self.firsttypes[k]:
                            self.keysAllUniformType = False  # at least one key mixes types
                            self.keyuniformity[k] = False    # key k, mixes types
        if self.samekeysflag:
            for k in self.firstkeys:
                if not self.keyuniformity[k]:
                    self.firsttypes[k] = 'multiple types'                    
        self.unifdata['badrecordIDs'] = badrecordIDs
        self.unifdata['missingkeyIDs'] = missingkeyIDs
        self.unifdata['extrakeyIDs'] = extrakeyIDs
                            
    def profile(self):
        hugeint = 9999999999999999999999999999
        if not self.samekeysflag:
            self.error('cant profile unless all docs (records) have same keys')
        nk = len(self.firstkeys)
        # set up profiles of the keys with type int.
        self.profdata = {}
        self.profdata['N']=len(self.db)
        maxint = {}
        minint = {}
        meanint = {}
        nint = {}
        for k in self.firstkeys:
            if self.firsttypes[k] == type(5):
                maxint[k] = -hugeint
                minint[k] = hugeint
                meanint[k] = 0
                nint[k] = 0
        else:
            for r in self.db:
                for k in self.firstkeys:
                    if self.firsttypes[k] == type(5):
                        t = r[k]
                        if t > maxint[k]:
                            maxint[k] = t
                        if t < minint[k]:
                            minint[k] = t
                        meanint[k] += t
                        nint[k] += 1
        self.profdata['keys'] = self.firstkeys
        self.profdata['types'] = self.firsttypes
        self.profdata['int_mins'] = minint
        self.profdata['int_maxs'] = maxint
        for k in self.firstkeys:
            if self.firsttypes[k] == type(5):
                meanint[k] = float(meanint[k])/float(nint[k])
        self.profdata['int_means'] = meanint
        
                
        
    def error(msg):
        print('tinyDB validator: '+msg)
        quit()

def backup_tiny_json(name):
    approxUID = str(uuid.uuid1())[0:10]  # truncate to be nice low-risk
    bufilename = item[0].replace('.','_')+'_BACKUP_'+ approxUID # from db_or_table_list
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
    #
    # deal with multiple tables in the db!!!
    #        
    if len(sys.argv) ==1 or sys.argv[1].replace('-','').lower() =='help':
        explainer()
        quit()
        
    db_or_table_list = []
    dbfnamelist = sys.argv[1:]   # list of json files.
    #expand all dbs into tables (if they have them)
    for dbfname in dbfnamelist:
        if not dbfname.endswith('.json'):
            print (dbfname, ' is not a JSON file')
        db = TinyDB(dbfname)
        tablelist = list(db.tables())
        if len(tablelist) == 0:
            db_or_table_list.append([dbfname, None])
        else:
            for table in tablelist:
                db_or_table_list.append([dbfname, table])

    for item in db_or_table_list:
        dbparent = TinyDB(item[0])
        if item[1]:
            db = dbparent.table(item[1])
        else: 
            db = dbparent

        print('\n\nTesting Database:', item[0], '   Table: ', item[1])
            
        v = tdb_validator(db)
        
        v.uniformity()  # check uniformity of keys

        if v.samekeysflag:
            print('All documents (records) have same keys')
            if v.keysAllUniformType:
                print('All keys have uniform types')
        else:
            print('Some keys do not belong to all documents (records)')
            print('club record IDs with key problems: ', v.unifdata['badrecordIDs'])
            print('\nclub record IDs with missing keys: ', v.unifdata['missingkeyIDs'])
            print('\nclub record IDs with extra keys: ', v.unifdata['extrakeyIDs'])
        if v.samekeysflag:
            print('\n\n      Key Type analysis\n')
            print('{:<20} {:<30} {}'.format('Key','Type','Uniform?'))
            r = db.all() # first record
            if len(r) == 0:
                print ('There are no documents(records)')
            else:
                #print (r)
                r = r[0] # just use first one
                for k in r.keys():
                    ktype = v.firsttypes[k]
                    print ('{:<20} {:<30} {}'.format(k,str(ktype),v.keyuniformity[k]))
                    
                v.profile()
                
                print ('\nProfile data:',v.profdata['N'],' documents(records)')
                print ('Mins: ', v.profdata['int_mins'])
                print ('Max:  ', v.profdata['int_maxs'])
                print ('Mean: ', v.profdata['int_means'])
    
        if len(v.unifdata['badrecordIDs']) > 0:
            x = input(' Do you want to repair the database '+item[0] + '   Table: '+item[1]+ '(y/N) ?')
            if x.lower() == 'y':
                #
                #   make backup before repair
                #
                backup_tiny_json(item[0])
                v.repair_uniformity()

