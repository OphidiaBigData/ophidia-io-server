/*
    Ophidia IO Server
    Copyright (C) 2014-2016 CMCC Foundation

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef OPH_QUERY_PARSER_H
#define OPH_QUERY_PARSER_H

#include "hashtbl.h"

/**
 * \brief           Enum with admissible argument types
 */
typedef enum { 
	OPH_QUERY_TYPE_LONG,
	OPH_QUERY_TYPE_DOUBLE,
	OPH_QUERY_TYPE_NULL,
	OPH_QUERY_TYPE_VARCHAR,
	OPH_QUERY_TYPE_BLOB
}oph_query_arg_types;

/**
 * \brief           Enum with admissible field types
 */
typedef enum { 
	OPH_QUERY_FIELD_TYPE_LONG,
	OPH_QUERY_FIELD_TYPE_DOUBLE,
	OPH_QUERY_FIELD_TYPE_STRING,
	OPH_QUERY_FIELD_TYPE_VARIABLE,
	OPH_QUERY_FIELD_TYPE_BINARY,
	OPH_QUERY_FIELD_TYPE_FUNCTION,
	OPH_QUERY_FIELD_TYPE_UNKNOWN
}oph_query_field_types;



/**
 * \brief             Structure to contain a query argument
 * \param arg_type    Type of argument      
 * \param arg_length  Length of argument
 * \param arg_is_null If argument can be null
 * \param arg         Pointer to argument value
 */
typedef struct{
  oph_query_arg_types     arg_type;
  unsigned long           arg_length;
  short int               arg_is_null;
  void                    *arg;
}oph_query_arg;

//Internal functions
/**
 * \brief               Function to validate submission query
 * \param query_string  Submission query to validate
 * \return              0 if successfull, non-0 otherwise
 */
int _oph_query_parser_validate_query(const char *query_string);

/**
 * \brief               Function to load all arguments into an hash table
 * \param query_string  Submission query to load
 * \param hashtbl       Hash table to be loaded
 * \return              0 if successfull, non-0 otherwise
 */
int _oph_query_parser_load_query_params(const char *query_string, HASHTBL *hashtbl);

/**
 * \brief               Function used to parse query string and load all arguments into hash table 
 * \param query_string  Submission query to load
 * \param query_args    Hash table containing args to be created
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_parser(char *query_string, HASHTBL **query_args);

/**
 * \brief               Function to parse and split multiple-value arguments. It modifies the input "values" string 
 * \param values        Values to be splitted
 * \param value_list    Array of pointer to each value
 * \param value_num     Number of values splitted
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_parse_multivalue_arg (char *values, char ***value_list, int *value_num);


/**
 * \brief               Function to get the type related to a field value. This can be used for select, insert, where or group by fields 
 * \param field 		Field to be tested
 * \param field_type    Type of argument evaluated
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_field_type(const char* field, oph_query_field_types *field_type);

#endif /* OPH_QUERY_PARSER_H */
