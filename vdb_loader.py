import os
import glob
import sys
import argparse
import logging
import psycopg2
import json
import vdb_tools
import vdb_non_generic
import vdb_database

dbase_tab_col_row_count = dict()    # Key is a tuple (table, column , type) with value  max(rows)
dbase_tab_row_count = dict()        # Key is a just table name, with value  max(rows)
dbase_tab_col_count = dict()        # Key is a just table name, with value no. of columns
dbase_tab_all_stats = dict()        # Key is table, data is a tuple containing max row and column counts
dbase_tab_child_parent = dict()     # key is child table, value is parent
dbase_new_tab_child_parent = dict() # new version of above after tables are rationalised

#
# Parse command line arguments

arg_parser = argparse.ArgumentParser(description='Load some vector files.')
arg_parser.add_argument('-dbase', dest='dbase', required=True, help='database')
arg_parser.add_argument('-dbuser', dest='dbuser', required=True, help='database user (uses ~/.pgpass for password)')
arg_parser.add_argument('-path', dest='file_path', default='~', help='location of source files (default: ~)')
arg_parser.add_argument('-type', dest='file_type', default='json', help='source file type (default: json)')
arg_parser.add_argument('-loglevel', dest='log_level', default='info', help='logging level (default: info)', choices=['debug','info','warning','error','critical'])
arg_parser.add_argument('-mtable', dest='master_table', default='xyz', help='prefix for all tables (default; xyz)')
args = arg_parser.parse_args()

#
# initialise logging

numeric_level = getattr(logging, args.log_level.upper(), None)
logging.basicConfig(level=numeric_level, format='%(asctime)s %(levelname)s %(message)s')

#
# Log basic run details and command line arguments

logging.info('main: database         = %s', args.dbase)
logging.info('main: database user    = %s', args.dbuser)
logging.info('main: source file path = %s', args.file_path)
logging.info('main: file type        = %s', args.file_type)
logging.info('main: log level        = %s', args.log_level)
logging.info('main: master table     = %s', args.master_table)

#
# Connect to database

connect_string = "dbname=" + args.dbase + " user=" + args.dbuser
logging.debug("main: database connect string = %s", connect_string)

db_conn   = psycopg2.connect(connect_string)

#
# Dropping any existing tables and sequences

logging.info("main: dropping existing tables with prefix %s",args.master_table)
count_tables_dropped = vdb_database.drop_tables_seqs(db_conn, args.master_table)
logging.info("main: %i tables  and %i sequences dropped", count_tables_dropped[0], count_tables_dropped[1])

#
# Get file list to process, based on path, wildcard for filename and file type/extension

files = args.file_path + "/*." + args.file_type
logging.debug("main: Path for source files is : %s", files)
file_list = glob.glob(files)

files_to_process = len(file_list)

if files_to_process <= 0 :
    logging.warning("main: no source files found")
    sys.exit(0)

logging.info("main: %i files found to process", files_to_process)

#
# Derive full set of raw tables/columns/column type, based on contents of  all source files

files_processed = 0

for file_name in file_list:
    logging.info("main: processing file : %s", file_name)

    try:
        file_object = open(file_name, encoding='utf-8', mode='r')
        total_lines = 0
        lines_processed = 0

        for line in file_object:
            total_lines = total_lines + 1
            logging.debug("main: processing line %i", total_lines)
            raw_vector = json.loads(line)              # use provided json parser "loads"
            dbase_rows = []
            vdb_tools.vdb_flatten_dict(args.master_table, dbase_rows, raw_vector, dbase_tab_child_parent)
                                                       # vdb_flatten_dict returns a list of tuples, each tupe contains:
                                                       # (table name, column name, PG column type, value}
                                                       # dbase_tab_child_parent is also maintained but is not ready until all files
                                                       # are processed

            if len(dbase_rows) >= 1 :
                logging.debug("main: dbase_rows from vbd_flatten_dict:")
                for row_tuple in dbase_rows:
                    logging.debug("      %s %s %s %s", row_tuple[0], row_tuple[1], row_tuple[2], row_tuple[3])
                vdb_non_generic.fix_sequences(args.master_table, dbase_rows, dbase_tab_child_parent)
                                                       # non generic code to force sequences onto their own table

                logging.debug("main: dbase_rows after fix_sequences:")
                for row_tuple in dbase_rows:
                    logging.debug("      %s %s %s %s", row_tuple[0], row_tuple[1], row_tuple[2], row_tuple[3])

                vdb_tools.build_tab_col_row_count(dbase_rows, dbase_tab_col_row_count)
                                                       # maintain dbase_tab_col_row_count, ready when all files are processes
                lines_processed = lines_processed + 1

        file_object.close()

        if lines_processed == total_lines:
            logging.info("main: all lines in file processed")
        else:
            logging.warning("main: %i lines processed out of %i", lines_processed, total_lines)

        file_object.close()
        files_processed += 1

    except IOError:
        logging.warning("main: problems reading file %s", file_name)

if files_processed == files_to_process:
    logging.info("main: all files procesed")
else:
    logging.warning("main: %i files processed out of %i", files_processed, files_to_process)

logging.debug("main: resulting dbase_tab_child_parent:")
for tab_col_type in dbase_tab_child_parent:
    logging.debug("      %s %s", tab_col_type, dbase_tab_child_parent[tab_col_type])

logging.debug("main: resulting dbase_tab_col_row_count:")
for tab_col_type in dbase_tab_col_row_count:
    logging.debug("      %s %s", tab_col_type, dbase_tab_col_row_count[tab_col_type])

vdb_tools.build_tab_row_count(dbase_tab_col_row_count, dbase_tab_row_count) 
                                                       # calculate table row count
vdb_tools.build_tab_col_count(dbase_tab_col_row_count, dbase_tab_col_count)
                                                       # calculate table column count
vdb_tools.merge_row_column_data(dbase_tab_row_count, dbase_tab_col_count, dbase_tab_all_stats)
                                                       # merge row/col count into dbase_tab_all_stats

logging.info("main:")
logging.info("main: Intial derived table statistics (table, rows, columns):")
for key in dbase_tab_all_stats:
    logging.info("      %s %i %i", key, dbase_tab_all_stats[key][0], dbase_tab_all_stats[key][1])

dbase_table_moves = dict([])
dbase_array_moves = dict([])

create_statements = vdb_tools.build_create_tables(args.master_table, dbase_tab_child_parent, dbase_new_tab_child_parent, \
                                                  dbase_tab_all_stats,\
                                                  dbase_tab_col_row_count, dbase_table_moves, dbase_array_moves)

seq_name = args.master_table + "_key_seq"

create_statements["key_seq"]="create sequence " + seq_name + ";"

logging.info("main:")
logging.info("main: Simplifications to initial tables:")
logging.info("main: The following single row tables/columns are moving :")
for old_tab_column in dbase_table_moves:
    logging.info("      %s to %s", old_tab_column, dbase_table_moves[old_tab_column])

logging.info("main:")
logging.info("main: The following single column tables are moving to be arrays on:")
for old_tab_column in dbase_array_moves:
    logging.info("      %s to %s", old_tab_column, dbase_array_moves[old_tab_column])

logging.info("main:")
logging.info("main: derived create table statements are:")
for create_table in create_statements:
    logging.info("    %s", create_statements[create_table])

tables_to_create = len(create_statements)
tables_created = vdb_database.db_exec_dict(db_conn, create_statements)

logging.info("main: %i tables created out of %i", tables_created, tables_to_create)

#
# loop through files again to insert the data

logging.info("main: ----- Looping through files again to insert data ----")

files_processed = 0

for file_name in file_list:
    logging.info("main: processing file : %s", file_name)

    try:
        file_object = open(file_name, encoding='utf-8', mode='r')
        total_lines = 0
        lines_processed = 0

        for line in file_object:
            total_lines = total_lines + 1
            logging.debug("main: processing line %i", total_lines)
            raw_vector = json.loads(line)              # use provided json parser "loads"
            dbase_rows = []
            vdb_tools.vdb_flatten_dict(args.master_table, dbase_rows, raw_vector, dbase_tab_child_parent)
                                                       # vdb_flatten_dict returns a list of tuples, each tupe contains:
                                                       # (table name, column name, PG column type, value}
                                                       # dbase_tab_child_parent is also maintained but is not ready until all files
                                                       # are processed

            if len(dbase_rows) >= 1 :
                logging.debug("main: dbase_rows from vbd_flatten_dict:")
                for row_tuple in dbase_rows:
                    logging.debug("      %s %s %s %s", row_tuple[0], row_tuple[1], row_tuple[2], row_tuple[3])
                vdb_non_generic.fix_sequences(args.master_table, dbase_rows, dbase_tab_child_parent)
                                                       # non generic code to force sequences onto their own table

                logging.debug("main: dbase_rows after fix_sequences:")
                for row_tuple in dbase_rows:
                    logging.debug("      %s %s %s %s", row_tuple[0], row_tuple[1], row_tuple[2], row_tuple[3])

                insert_statements = vdb_tools.insert_data(seq_name, db_conn, dbase_rows, dbase_table_moves, dbase_array_moves, \
                                                           dbase_new_tab_child_parent)

                for insert_statement in insert_statements:
                    logging.debug("main: %s",insert_statement)

                rows_inserted = vdb_database.db_exec_list(db_conn, insert_statements)

                lines_processed = lines_processed + 1

        file_object.close()

        if lines_processed == total_lines:
            logging.info("main: all lines in file processed")
        else:
            logging.warning("main: %i lines processed out of %i", lines_processed, total_lines)

        file_object.close()
        files_processed += 1

    except IOError:
        logging.warning("main: problems reading file %s", file_name)


#
# Close database and exit

db_conn.close()
sys.exit(0)




