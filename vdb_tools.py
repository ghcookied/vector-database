import logging
import vdb_database

basic_type_list = ["<class 'str'>", "<class 'int'>",  "<class 'float'>"]

python_to_pg = { "<class 'int'>"   : "INTEGER",
                 "<class 'float'>" : "DECIMAL",
                 "<class 'str'>"   : "TEXT",
                 "<class 'bool'>"  : "BOOLEAN" }

def vdb_flatten_dict(table_name, result_list, input_object, tab_child_parent):

    logging.debug("vdb_flatten_dict: in vdb_tools.vdb_flatten_dict")

    #
    # will reduce parsed json object to a set of outline database fields, can only handle input of string, dict, dict, dict

    for attribute in input_object:
        attribute_type = str(type(input_object[attribute]))

        if attribute_type in basic_type_list :
            result_list.append((table_name, attribute, python_to_pg[attribute_type], input_object[attribute]))
        elif attribute_type == "<class 'bool'>" :
            result_list.append((table_name, attribute, python_to_pg[attribute_type], (str(input_object[attribute])).upper()))
        elif attribute_type == "<class 'list'>" :
            new_child_table = table_name + "_" + attribute

            if new_child_table not in tab_child_parent:
                tab_child_parent[new_child_table] = table_name

            process_list(input_object[attribute], result_list, table_name + "_" + attribute, attribute, tab_child_parent)
        elif  attribute_type == "<class 'dict'>" :
            new_child_table = table_name + "_" + attribute

            if new_child_table not in tab_child_parent:
                tab_child_parent[new_child_table] = table_name

            vdb_flatten_dict(table_name + "_" + attribute, result_list, input_object[attribute], tab_child_parent)
        else:
            if str(input_object[attribute]) != 'None' :
                logging.warning("vdb_flatten_dict: Unknown type encountered : %s", attribute_type)
                logging.warning("                  attribute : %s",attribute)
                logging.warning("                  value : %s",input_object[attribute])

    logging.debug("vdb_flatten_dict: leaving vdb_flatten_dict")

    return

def process_list(list_object, result_list, table_name, attribute, tab_child_parent) :

    for list_member in list_object:
        list_member_type = str(type(list_member))

        if list_member_type in basic_type_list :
            result_list.append((table_name, attribute, python_to_pg[list_member_type],list_member))
        elif list_member_type == "<class 'bool'>" :
            result_list.append((table_name, attribute, python_to_pg[list_member_type], (str(list_member)).upper()))
        elif list_member_type == "<class 'dict'>" :
            vdb_flatten_dict(table_name, result_list, list_member, tab_child_parent)
        elif list_member_type == "<class 'list'>" :
            result_list.append((table_name, attribute, 'TEXT', str(list_member)))
        else:
            if str(list_member) != 'null' :
                logging.warning("Unknown type encountered : %s", attribute_type)
                logging.warning("    attribute : %s",attribute)
                logging.warning("    value : %s",Input_Object[attribute])

    return

def build_tab_col_row_count(new_rows, global_dict):

    #
    # first collapse new rows into a local dict

    local_dict = dict()

    for vdb_tuple in new_rows:
        new_tuple =  (vdb_tuple[0], vdb_tuple[1], vdb_tuple[2])
        if new_tuple in local_dict:
            local_dict[new_tuple] += 1
        else:
            local_dict[new_tuple] = 1

    #
    # Now update gobal dict where appropriate

    for key_tuple in local_dict:
        if key_tuple in global_dict:
            if local_dict[key_tuple] > global_dict[key_tuple]:
                global_dict[key_tuple] = local_dict[key_tuple]
        else:
            global_dict[key_tuple] = local_dict[key_tuple]

    return

def build_tab_row_count(tab_col_row_count, tab_count):

    for key_tuple in tab_col_row_count:
        tab_key = key_tuple[0]

        if tab_key in tab_count:
            if tab_col_row_count[key_tuple] != tab_count[tab_key]:
                logging.warning("Different max row counts found for table %s, using maximum", key_tuple)

                if tab_col_row_count[key_tuple] > tab_count[tab_key]:
                    tab_count[tab_key] = tab_col_row_count[key_tuple]
        else:
            tab_count[tab_key] = tab_col_row_count[key_tuple]

    return

def build_tab_col_count(tab_col_row_count, tab_col_count):

    #
    # First just build an array of tuples with table/column

    tab_col_array = []

    for key_tuple in tab_col_row_count:
        table = key_tuple[0]
        column = key_tuple[1]

        if (table, column) not in tab_col_array:
            tab_col_array.append((table, column))

    #
    # Now build dict with key = table name and value = no. of columns

    for tab_tuple in tab_col_array:
        table_key = tab_tuple[0]

        if table_key in tab_col_count:
            tab_col_count[table_key] += 1
        else:
            tab_col_count[table_key] = 1

    return
        
def merge_row_column_data(tab_row_count, tab_col_count, tab_counts):

    for tab_key in tab_row_count:
        if tab_key not in tab_col_count:
            logging.critical("Table %s in the row count does not exist in the column count", tab_key)
            sys.exit(1)

        tab_counts[tab_key] = (tab_row_count[tab_key], tab_col_count[tab_key])

    for tab_key in tab_col_count:
        if tab_key not in tab_row_count:
            logging.critical("Table %s in the column count does not exist in the row count", tab_key)
            sys.exit(1)

    return

def get_actual_table(table_name, child_parent_table, all_table_stats):

    #
    # If table has one row and a parent should use parent table. Process is recursive as parent may also have one row

    if table_name not in child_parent_table:   # No parent
        return table_name
    else:
        if all_table_stats[table_name][0] > 1: # Has a parent but more than one row => is a real table
            return table_name
        else:                                  # Has a parent and one row but need to check number of rows in parent
            table_name = get_actual_table(child_parent_table[table_name], child_parent_table, all_table_stats)
            return table_name

    logging.critical("get_actual_table failed to find actual table for table $s", table_name)
    sys.exit(1)

def add_key_column(a_table, tmp_table_string, new_child_parent_table):

    new_tmp_table_string = a_table + "_id INTEGER NOT NULL, " + tmp_table_string
    
    if a_table in new_child_parent_table:
        table_string = add_key_column(new_child_parent_table[a_table], new_tmp_table_string, new_child_parent_table)
    else:
        table_string = new_tmp_table_string
        
    return table_string

def build_create_tables(master_table, child_parent_table, new_child_parent_table, all_table_stats, table_column_type, table_moves, array_moves):

    #
    # Function to build "create table" statements

    #
    # Firstly build a dictionary for each actual table containing a repeating string with format "column type,"

    table_list = dict()             # Dictionary where each row will contain the create table string

    for tab_column in table_column_type:
        table_name    = tab_column[0]
        column_name   = tab_column[1]
        column_type   = tab_column[2]
        column_prefix = ""
        array         = False

        actual_table = get_actual_table(table_name, child_parent_table, all_table_stats)

        if actual_table != table_name:
            column_prefix = table_name[len(actual_table)+1:] + "_"
            table_moves[(table_name, column_name)] = (actual_table, column_prefix + column_name) 
        else:                                         # look for arrays - multi row tables with 1 column
            if all_table_stats[table_name][1] == 1:   # one column and must be multi row as not dealt with in above
                array = True

                # An array must have a parent table, but original parent may have been subsumed by it's parent so check:

                actual_table = get_actual_table(child_parent_table[table_name], child_parent_table, all_table_stats)

                column_prefix = ""

                if actual_table != child_parent_table[table_name]:
                    column_prefix = child_parent_table[table_name][len(actual_table)+1:] + "_"
                tmp_column_type = column_type
                column_type = tmp_column_type + " []"
                array_moves[(table_name, column_name)] = (actual_table, column_prefix + column_name)

        if actual_table in table_list:
            tmp_create_table_string = table_list[actual_table]
            table_list[actual_table] = tmp_create_table_string + " " + column_prefix + column_name + " " + column_type + ","
        else:
             table_list[actual_table] = column_prefix + column_name + " " + column_type + ","

    for a_table in table_list:                    # Last character needs to be changed to a ");"
        tmp_create_table_string = table_list[a_table]
        table_list[a_table] = tmp_create_table_string[:-1] + ");"

    for a_table in table_list:
        parent = get_actual_table(table_name, child_parent_table, all_table_stats)

    for a_table in table_list:
        if a_table != master_table:
            parent = get_actual_table(child_parent_table[a_table], child_parent_table, all_table_stats)
            new_child_parent_table[a_table] = parent

    #
    # Create key columns
    
    for a_table in table_list:
        tmp_table_string = table_list[a_table]
        table_list[a_table] = add_key_column(a_table, tmp_table_string, new_child_parent_table)

    #
    # Finally add create table

    for a_table in table_list:
        tmp_table_string = table_list[a_table]
        table_list[a_table] = "CREATE TABLE " + a_table + " (" + tmp_table_string

    return table_list

def insert_data(seq_name,db_conn,table_col_type_value, table_moves, array_moves, table_hierarchy):

    logging.debug("insert_data: in vdb_tools.insert_data")

    working_columns = dict()
    working_values  = dict()
    insert_statements = []
    key_list = dict()

    initialise_key_list(seq_name, db_conn, key_list, table_hierarchy)

    for key_table in key_list:
        logging.debug("insert_data: %s initial key value = %s", key_table, key_list[key_table])

    array = False
    array_opened = False
    array_table = ""
    array_column = ""

    for table_col_type in table_col_type_value:
        table    = table_col_type[0]
        column   = table_col_type[1]
        col_type = table_col_type[2]
        value    = table_col_type[3]

        if array and (table != array_table or column != array_column):        # close array
            tmp_working_values  = working_values[actual_table][:-1] + "}',"
            working_values[actual_table] = tmp_working_values
            array = False
            array_opened = False

        actual_table = table
        actual_column = column

        if (table, column) in table_moves:
            actual_table  = table_moves[(table, column)][0]
            actual_column = table_moves[(table, column)][1]

        if (table, column) in array_moves:
            actual_table  = array_moves[(table, column)][0]
            actual_column = array_moves[(table, column)][1]
            array = True
            array_table = table
            array_column = column

        if actual_table in working_columns:                             # Table already being populated
            if not array:                                               # don't want to start a new table when loading an array
                cs1 = "(" + actual_column + ","
                cs2 = "," + actual_column + ","
                if working_columns[actual_table].find(cs1) != -1 or working_columns[actual_table].find(cs2) != -1 :
                                                                        # column already present, close current table and start
                                                                        # a new table
                    tmp_working_columns = working_columns[actual_table]
                    tmp_working_values  = working_values[actual_table]
                    working_columns[actual_table] = tmp_working_columns[:-1] + ")" # remove trailing , and add )
                    working_values[actual_table]  = tmp_working_values[:-1] + ")"
                    insert_statements.append(working_columns[actual_table] + " " + working_values[actual_table] + ";")
                    initialise_insert_statement(seq_name, db_conn, working_columns, working_values, \
                                                actual_table, table_hierarchy, key_list)
        else:
            initialise_insert_statement(seq_name, db_conn, working_columns, working_values, \
                                            actual_table, table_hierarchy, key_list)

        value_str = "''"

        if col_type == 'TEXT':
            str1 = str(value).replace("'", "|")
            str2 = str1.replace('"', '|')
            if array:
                value_str = '"' + str2 + '"'
            else:
                value_str = "'" + str2 + "'"
        elif col_type == 'INTEGER' or col_type == 'BOOLEAN':
            value_str = str(value)
        else:
            logging.warning("insert_data: can't handle column type %s, has been set to null", col_type)

        if array:
            if not array_opened:
                cs1 = "(" + actual_column + ","                  # First check if column already exists - if so close current
                cs2 = "," + actual_column + ","                  # table and start a new one
                if working_columns[actual_table].find(cs1) != -1 or working_columns[actual_table].find(cs2) != -1 :
                    tmp_working_columns = working_columns[actual_table]
                    tmp_working_values  = working_values[actual_table]
                    working_columns[actual_table] = tmp_working_columns[:-1] + ")" # remove trailing , and add )
                    working_values[actual_table]  = tmp_working_values[:-1] + ")"
                    insert_statements.append(working_columns[actual_table] + " " + working_values[actual_table] + ";")
                    initialise_insert_statement(seq_name, db_conn, working_columns, working_values, \
                                                 actual_table, table_hierarchy, key_list)
 
                tmp_working_columns = working_columns[actual_table] + actual_column + ","
                tmp_working_values  = working_values[actual_table]  + "'{" + value_str + ","
                array_opened = True
            else:
                tmp_working_columns = working_columns[actual_table]        # unchanged - no new column name required
                tmp_working_values  = working_values[actual_table]  + value_str + ","
        else:
            tmp_working_columns = working_columns[actual_table] + actual_column + ","
            tmp_working_values  = working_values[actual_table]  + value_str + ","
            
        working_columns[actual_table] = tmp_working_columns
        working_values[actual_table] = tmp_working_values

    #
    # Close and store tables working at the end :

    for actual_table in working_columns:
        tmp_working_columns = working_columns[actual_table]
        tmp_working_values  = working_values[actual_table]
        working_columns[actual_table] = tmp_working_columns[:-1] + ")"

        if array:
            working_values[actual_table]  = tmp_working_values[:-1] + "}')"
        else:
            working_values[actual_table]  = tmp_working_values[:-1] + ")"

        insert_statements.append(working_columns[actual_table] + " " + working_values[actual_table] + ";")

    logging.debug("insert_data: leaving vdb_tools.insert_data")

    return insert_statements

    return

def initialise_key_list(seq_name, db_conn, key_list, table_hierarchy):

    for child_table in table_hierarchy:
        key_list[child_table] = vdb_database.get_next_key(seq_name, db_conn)
        
    for child_table in table_hierarchy:
        if table_hierarchy[child_table] not in key_list:
            key_list[table_hierarchy[child_table]] = vdb_database.get_next_key(seq_name, db_conn)

    return           

def initialise_insert_statement(seq_name, db_conn, working_columns, working_values, actual_table, table_hierarchy, key_list):

    key_list[actual_table] = vdb_database.get_next_key(seq_name,db_conn)

    working_columns[actual_table] = "INSERT INTO " + actual_table + " ("
    working_values[actual_table] = "VALUES ("
    
    table_list = [actual_table]
    
    iter_table = actual_table
    
    while iter_table in table_hierarchy:
        table_list.append(table_hierarchy[iter_table])
        iter_table = table_hierarchy[iter_table]

    for table in reversed(table_list):
        tmp_working_columns = working_columns[actual_table] + table + "_id,"
        tmp_working_values = working_values[actual_table] + str(key_list[table]) + ","
        working_columns[actual_table] = tmp_working_columns
        working_values[actual_table] = tmp_working_values
        
    return


