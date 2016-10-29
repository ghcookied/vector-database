# vector-database

Design a data storage system for the Addgene DNA vector collection

Python code to load a set of files derived from addgene.com into a PostgreSQL database.

Dependencies :

    - uses .pgpass file for database access, only database and username required to be supplied
    - developed using Python 3.5.2 and PostgreSQL 8.4.20
    - uses psycopg2 as the client interface to PostgreSQL

Help :

/home2/dajurid1/vector-database$ python vdb_loader.py -h
usage: vdb_loader.py [-h] -dbase DBASE -dbuser DBUSER [-path FILE_PATH]
                     [-type FILE_TYPE]
                     [-loglevel {debug,info,warning,error,critical}]
                     [-mtable MASTER_TABLE]

Load some vector files.

optional arguments:
  -h, --help            show this help message and exit
  -dbase DBASE          database
  -dbuser DBUSER        database user (uses ~/.pgpass for password)
  -path FILE_PATH       location of source files (default: ~)
  -type FILE_TYPE       source file type (default: json)
  -loglevel {debug,info,warning,error,critical}
                        logging level (default: info)
  -mtable MASTER_TABLE  prefix for all tables (default; xyz)

Example run :

python vdb_loader.py -dbase dajurid1_dev -dbuser dajurid1_admin -path $VDB_SOURCE_LOCATION -loglevel info -mtable vdb_vector


**** Recommend not to run loglevel debug on many files/rows ****


