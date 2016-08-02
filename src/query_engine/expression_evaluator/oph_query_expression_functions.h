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
 */
oph_query_expr_value oph_id(oph_query_expr_value* args, int num_args, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 3 element array (args[0]=id, args[1]=size, args[2]=block_size)
 * \param num_args      Number of arguments
 */
oph_query_expr_value oph_id2(oph_query_expr_value* args, int num_args, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          An array of at least 2 elements (args[0]=id,args[1]=size,...)
 * \param num_args      Number of arguments
 */
oph_query_expr_value oph_id_to_index (oph_query_expr_value* args, int num_args, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 3 element array (args[0]=id, args[1]=block_size, args[2]=size)
 * \param num_args      Number of arguments
 */
oph_query_expr_value oph_id_to_index2(oph_query_expr_value* args, int num_args, int *er);

/**
 * \brief               One of the functions that can apper in the parsed query
 * \param args          A 4 element array (args[0]=id, args[1]=start, args[2]=step, args[3]=size)
 * \param num_args      Number of arguments
 */
oph_query_expr_value oph_is_in_subset(oph_query_expr_value* args, int num_args, int *er);

#endif // __OPH_QUERY_EXPRESSION_FUNCTIONS_H__

//id3
//id_of_subset