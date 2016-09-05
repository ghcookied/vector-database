import os
import glob
import sys
import argparse
import logging
import psycopg2
import json
import vdb_tools

data_dictionary=[]

#
# Parse commnd line arguments

arg_parser = argparse.ArgumentParser(description='Load some vector files.')
arg_parser.add_argument('-dbase', dest='dbase', required=True, help='database')
arg_parser.add_argument('-dbuser', dest='dbuser', required=True, help='database user (uses ~/.pgpass for password)')
arg_parser.add_argument('-path', dest='file_path', default='~', help='location of source files (default: ~)')
arg_parser.add_argument('-type', dest='file_type', default='json', help='source file type (default: json)')
arg_parser.add_argument('-loglevel', dest='log_level', default='info', help='logging level (default: info)', choices=['debug','info','warning','error','critical'])
args = arg_parser.parse_args()

#
# initialise logging

numeric_level = getattr(logging, args.log_level.upper(), None)
logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s %(message)s')

#
# Log basic run details and command line arguments

logging.info('Database         = %s', args.dbase)
logging.info('Database user    = %s', args.dbuser)
logging.info('Source file path = %s', args.file_path)
logging.info('File type        = %s', args.file_type)
logging.info('Log level        = %s', args.log_level)

files = args.file_path + "/*." + args.file_type

logging.debug("Path for source files is : %s", files)

file_list = glob.glob(files)

if len(file_list) <= 0 :
    logging.warning("No source files found")
    sys.exit(0)

logging.debug("%i files found",(len(file_list)))

#
# Connect to database
connect_string = "dbname=" + args.dbase + " user=" + args.dbuser
logging.debug("Database connect string = %s", connect_string)
try:
    conn = psycopg2.connect(connect_string)
except psycopg2.OperationalError as e:
    logging.critical("Unable to connect to database with connection string '%s'", connect_string)
    logging.critical(e)
    sys.exit(1)
else:
    logging.info("Connected to database %s as %s",args.dbase,args.dbuser)

for file_name in file_list:
    logging.debug("Processing : %s",file_name)

#    try:
    file_object = open(file_name, encoding='utf-8', mode='r')
    line_no = 0

    for line in file_object:
        line_no = line_no + 1
        RawVector = json.loads(line)
        dbase_list = []
        seed_string = "vdb_vector"
        vdb_tools.vdb_flatten(seed_string, dbase_list, RawVector)

        #
        # build data dictionary

        for vdb_tuple in dbase_list:
            new_tuple = (vdb_tuple[0], vdb_tuple[1], vdb_tuple[2])

            if new_tuple not in data_dictionary:
              data_dictionary.append(new_tuple)

    logging.debug("lines processed = %i", line_no)
    file_object.close()

for list_tuple in data_dictionary:
    logging.info("%s", list_tuple)

#    except:
#        logging.critical("Error")
#        sys.exit(1)


#
# Close database connection and leave

conn.close()
        
logging.info("No errors encountered!")
sys.exit(0)




