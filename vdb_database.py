import logging
import psycopg2

def drop_tables_seqs(db_connection, table_prefix):

    logging.debug("drop_tables: in vdb_database.drop_tables")

    sql_string = "select tablename from pg_tables where tablename like '" + table_prefix + "%';"
    logging.debug("drop_tables: select string = %s", sql_string)

    db_cursor = db_connection.cursor()
    db_cursor.execute(sql_string)

    tables_to_drop = db_cursor.fetchall()

    db_cursor.close()

    sql_string = "select relname from pg_class where relkind = 'S' and relname like '" + table_prefix + "%';"
    logging.debug("drop_tables: select string = %s", sql_string)

    db_cursor = db_connection.cursor()
    db_cursor.execute(sql_string)

    sequences_to_drop = db_cursor.fetchall()

    db_cursor.close()

    db_cursor = db_connection.cursor()

    num_tables_dropped = 0
    num_seq_dropped = 0

    for table_name in tables_to_drop:
        drop_sql_cmd = "drop table " +  table_name[0] + ";"
        logging.debug("drop_tables: drop string = %s", drop_sql_cmd)
        db_cursor.execute(drop_sql_cmd)
        logging.debug("drop_tables: dropped table %s", table_name[0])
        num_tables_dropped += 1

    for seq_name in sequences_to_drop:
        drop_sql_cmd = "drop sequence " +  seq_name[0] + ";"
        logging.debug("drop_tables: drop string = %s", drop_sql_cmd)
        db_cursor.execute(drop_sql_cmd)
        logging.debug("drop_tables: dropped sequence %s", seq_name[0])
        num_seq_dropped += 1

    db_connection.commit()
    db_cursor.close()

    logging.debug("drop_tables: leaving vdb_database.drop_tables")

    return (num_tables_dropped, num_seq_dropped)

def db_exec_dict(db_connection, db_statements):

     logging.debug("db_exec_dict: in vdb_database.db_exec_dict")

     db_cursor = db_connection.cursor()
     cmd_count = 0

     for db_statement in db_statements:
         logging.debug("Processing: %s", db_statements[db_statement])
         db_cursor.execute(db_statements[db_statement])
         cmd_count += 1

     db_connection.commit()
     db_cursor.close()

     logging.debug("db_exec_dict: leaving vdb_database.db_exec_dict:")

     return cmd_count

def db_exec_list(db_connection, db_statements):

     logging.debug("db_exec_dict: in vdb_database.db_exec_list")

     db_cursor = db_connection.cursor()
     cmd_count = 0

     for db_statement in db_statements:
         logging.debug("Processing: %s", db_statement)
         db_cursor.execute(db_statement)
         cmd_count += 1

     db_connection.commit()
     db_cursor.close()

     logging.debug("db_exec_dict: leaving vdb_database.db_exec_dict:")

     return cmd_count



def get_next_key(seq, db_connection):

    db_cursor = db_connection.cursor()

    db_cmd = "select nextval('" + seq + "');"
    db_cursor.execute(db_cmd)

    next_seq =  db_cursor.fetchall()[0][0]

    db_cursor.close()

    return next_seq
