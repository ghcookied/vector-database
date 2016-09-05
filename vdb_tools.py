import logging

basic_type_list = ["<class 'str'>", "<class 'int'>"]

def vdb_flatten(table_name, result_list, Input_Object):

    logging.debug("Entered vdb_flatten, table_name = %s and input object type %s", table_name, str(type(Input_Object)))

    for attribute in Input_Object:
        attribute_type = str(type(Input_Object[attribute]))

        logging.debug("In vdb_flatten, processing attribute %s of type %s", attribute, attribute_type)

        if attribute_type in basic_type_list :
            result_list.append((table_name,attribute,attribute_type,Input_Object[attribute]))
        elif attribute_type == "<class 'bool'>" :
            result_list.append((table_name,attribute,attribute_type,str(Input_Object[attribute]).upper))
        elif attribute_type == "<class 'list'>" :
            process_list(Input_Object[attribute], result_list, table_name + "_" + attribute, attribute)
        elif  attribute_type == "<class 'dict'>" :
            vdb_flatten(table_name + "_" + attribute, result_list, Input_Object[attribute])
        else:
            if str(Input_Object[attribute]) != 'None' :
                logging.warning("Unknown type encountered : %s", attribute_type)
                logging.warning("    attribute : %s",attribute)
                logging.warning("    value : %s",Input_Object[attribute])

    return

def process_list(list_object, result_list, table_name, attribute) :

    for list_member in list_object:
        list_member_type = str(type(list_member))

        if list_member_type in basic_type_list :
            result_list.append((table_name, attribute, list_member_type,list_member))
        elif list_member_type == "<class 'bool'>" :
            result_list.append((table_name, attribute, list_member_type, str(list_member).upper))
        elif list_member_type == "<class 'dict'>" :
            vdb_flatten(table_name, result_list, list_member)
        elif list_member_type == "<class 'list'>" :
            result_list.append((table_name, attribute, list_member_type,list_member))
        else:
            if str(list_member) != 'null' :
                logging.warning("Unknown type encountered : %s", attribute_type)
                logging.warning("    attribute : %s",attribute)
                logging.warning("    value : %s",Input_Object[attribute])

    return


 




