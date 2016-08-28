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

#ifndef __OPH_QUERY_EXPRESSION_FUNCTIONS_H__
#define __OPH_QUERY_EXPRESSION_FUNCTIONS_H__

#include "oph_query_expression_evaluator.h"

/*These are all the functions that are automatically added to the symtable*/ 

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 2 element array (args[0]=id, args[1]=size)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_id(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 3 element array (args[0]=id, args[1]=size, args[2]=block_size)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_id2(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 3 element array (args[0]=id, args[1]=list, args[2]=block_size)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_id3(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          An array of at least 2 elements (args[0]=id,args[1]=size,...)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_id_to_index (oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 3 element array (args[0]=id, args[1]=block_size, args[2]=size)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_id_to_index2(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 4 element array (args[0]=id, args[1]=start, args[2]=step, args[3]=size)
 * \param num_args      Number of arguments
 * \param name          The name of the function
 * \param descriptor    unused param
 * \param destroy       unused param 
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_is_in_subset(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               A function that handles the invocation of primitives that return double
 * \param args          An array of variable length containing the arguments of the funtion
 * \param num_args      Number of arguments
 * \param name          The name of the primitive that needs to be invocated
 * \param descriptor    A struct containing the allocated space for the structure used for by the primitive
 * \param destroy       Determines the behavior of the generic function. If is 0 will execute the init function (if needed) and the exec function; 
                        if is 1 will execute the deinit function.
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_query_generic_double(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               A function that handles the invocation of primitives that return long
 * \param args          An array of variable length containing the arguments of the funtion
 * \param num_args      Number of arguments
 * \param name          The name of the primitive that needs to be invocated
 * \param descriptor    A struct containing the allocated space for the structure used for by the primitive
 * \param destroy       Determines the behavior of the generic function. If is 0 will execute the init function (if needed) and the exec function; 
                        if is 1 will execute the deinit function.
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_query_generic_long(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               A function that handles the invocation of primitives that return string
 * \param args          An array of variable length containing the arguments of the funtion
 * \param num_args      Number of arguments
 * \param name          The name of the primitive that needs to be invocated
 * \param descriptor    A struct containing the allocated space for the structure used for by the primitive
 * \param destroy       Determines the behavior of the generic function. If is 0 will execute the init function (if needed) and the exec function; 
                        if is 1 will execute the deinit function.
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_query_generic_string(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);

/**
 * \brief               A function that handles the invocation of primitives that return binary
 * \param args          An array of variable length containing the arguments of the funtion
 * \param num_args      Number of arguments
 * \param name          The name of the primitive that needs to be invocated
 * \param descriptor    A struct containing the allocated space for the structure used for by the primitive
 * \param destroy       Determines the behavior of the generic function. If is 0 will execute the init function (if needed) and the exec function; 
                        if is 1 will execute the deinit function.
 * \param er            A flag to be changed in case of error
 */
oph_query_expr_value oph_query_generic_binary(oph_query_expr_value* args, int num_args, char* name, oph_query_expr_udf_descriptor* descriptor, int destroy, int *er);


#endif // __OPH_QUERY_EXPRESSION_FUNCTIONS_H__
