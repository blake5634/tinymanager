# TinyManager

A basic management tool/API for [tinyDB](https://tinydb.readthedocs.io/en/latest/) json databases
        
**Note:** it is *assumed* by this SW that you want all documents (records)
in the db to have 

1) the same set of keys
2) the values for each key are all of the same key-specific type
        
These assumptions are not required by tinydb and may or may not be what 
you want for your application.           
            
usage:   > `python tinymanager.py  [--help] file1.json file2.json ...`
    
    
## Step 1:
Checks "quality" of your tinyDB database. First we determine if all 
documents (records) have the same set of keys and we list them
    
if all documents have same keys:
We analyze the data type of each key and we check if all 
documents store the same (uniform from doc to doc) type in each key.
        
        
if not, you will be offered the option to repair your database.   A backup of
the original .json file will be created automatically for you first. 
    
## Step 2:
Edit database using an existing tool such as
    
[jsoneditoronline.org](https://jsoneditoronline.org)
        
        
