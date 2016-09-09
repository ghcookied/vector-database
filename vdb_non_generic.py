import logging

def fix_sequences(master_table, tab_col_type_value, tab_child_parent):

    #
    # Purpose of this is to put all the sequences onto one table with an extra column containing the type of sequence

    seq_string = master_table + "_sequences"
    seq_string_len = len(seq_string)

    #
    # The idea with this "changing" business is to do one sequence at a time as it's possible the indexing
    # of the list might be affected by the removal/inserts
    #
    # Two parts - first for tab_col_type_value then tab_child_parent

    changing = True

    while changing:

        changing = False

        for tab_col_type_tuple in tab_col_type_value:
            if tab_col_type_tuple[0].find(seq_string) != -1 and len(tab_col_type_tuple[0]) > seq_string_len :
                changing = True
                tab_col_type_value.remove(tab_col_type_tuple)
                new_typ_tuple = (seq_string, "seq_type", 'TEXT', tab_col_type_tuple[0][seq_string_len+1:])
                new_seq_tuple = (seq_string, "sequence", 'TEXT', tab_col_type_tuple[3])
                tab_col_type_value.append(new_typ_tuple)
                tab_col_type_value.append(new_seq_tuple)

                break


    changing = True

    while changing:

        changing = False

        for table_key in tab_child_parent:
            if table_key.find(seq_string) != -1 and len(table_key) > seq_string_len :
                changing = True
                del tab_child_parent[table_key]

                break

    return
