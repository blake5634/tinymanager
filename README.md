# TinyManager

A basic management tool/API for [tinyDB](https://tinydb.readthedocs.io/en/latest/) `json` databases
        
**Note:** it is *assumed* by this SW that you want all documents (records)
in the db to have 

1) the same set of keys

2) the values for each key are all of the same key-specific type
        
These assumptions are not required by tinydb and may or may not be what 
you want for your application.           
            
usage:   > `python tinymanager.py  [--help] file1.json file2.json ...`
    
    
## Step 1:
Checks "quality" of your tinyDB database. First we look for a schema file
(see below) and if none exists we offer to create it based on a hack that 
*assumes* the first record of the db follows your intended schema exactly. 

Next we determine if all 
documents (records) have the same set of keys and we list them.
    
if all documents have same keys:
We analyze the data type of each key and we check if all 
documents store the same (uniform from doc to doc) type in each key.
        
        
if not, you will be offered the option to repair your database.   A backup of
the original .json file will be created automatically for you first. 
    
## Step 2:
Edit database using an existing tool such as
    
[jsoneditoronline.org](https://jsoneditoronline.org)
        
        
## Schemas
Databases should have a definition of their tables and fields called a Schema.
This package uses json files to represent the schema.  The schema file 
(`filename.json_SCHEMA_.json`) contains: a dictionary:

```
schema1 =   {
            tables: ['table1', 'table2' etc], 
            table_fields: 
                {table1: [['key1',str(type1)], ['key2',str(type2)], ['key3',str(type3)], etc],
                 table2: [['key4',str(type4)], ['key5',str(type5)], ['key6',str(type6)], etc],
                }
            } 
```

in `table_fields`, each field has a 2-element list containing

1) the key (as a string)

2) the type of the string (converted to string: example:  `str(type(x)`)



(tinydb databases have a table called `_default` plus optional other tables.)
## Tests
The program `maketestdb.py` generates a set of test databases `testX.json`. 
With the exception of `test1.json`, these databases have various "flaws" which 
are documented in the comments of `maketestdb.py`.   The program `test_tiny_mgr.py` 
validates each test database, then tries to repair it, and then validates it again. 
Thus each database should fail validation the first time, and then pass.  If the test passes, then it went through both validations in the sequence [`Fail, Pass`]. 

`maketestdb.py` resets the database prior to each run. 

Current test databases:

1) `test1.json`  a few simple keys and all the records are same. (no db flaws)

2) `test2.json`  every 10th rec has an extra key

3) `test3.json` two records have wrong types for key1 (str->int)

4) `test4.json`  some records have missing keys and others wrong types

5) `test5.json`  some records have both missing keys and extra keys
