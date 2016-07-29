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

#include "oph_query_expression_evaluator.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <debug.h>

#define _GNU_SOURCE

int main(void)
{   
	set_debug_level(LOG_DEBUG);
	set_log_prefix(OPH_IO_SERVER_PREFIX);

    char test_syntax_error[] = "mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,3) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),6,1,8)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),2,2,6) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),9,1,9))";
    char test_eval_error1[] = "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10,2),1,1,3) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),6,1,8)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),2,2,6) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),9,1,9))";
    char test_eval_error2[] = "oph_id_to_index(1)";
    char test_right1[] = "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,3) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),6,1,8)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),2,2,6) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),9,1,9))";
    char test_right2[] = "oph_id_to_index(2,1,1,3)";

    oph_query_expr_node *e; 

	//Test 1
    oph_query_expr_get_ast(test_syntax_error, &e);

    oph_query_expr_symtable *table;
    oph_query_expr_create_symtable(&table, 1);
    double res;

    //Test 2
    oph_query_expr_get_ast(test_eval_error1, &e);

	oph_query_expr_add_variable("id_dim",1,table);    
	if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%f\n",res);

    oph_query_expr_delete_node(e);
	e = NULL;

    //Test 3
    oph_query_expr_get_ast(test_eval_error2, &e);

    oph_query_expr_add_variable("id_dim",1,table);    
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%f\n",res);

    oph_query_expr_delete_node(e);
    e = NULL;

	//Test 4
    oph_query_expr_get_ast(test_right1, &e);

	oph_query_expr_add_variable("id_dim",2,table);    
	if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%f\n",res);
    oph_query_expr_delete_node(e);
    e = NULL;

    //Test 5
    oph_query_expr_get_ast(test_right2, &e);

    oph_query_expr_add_variable("id_dim",2,table);    
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%f\n",res);

    oph_query_expr_delete_node(e);
    oph_query_expr_destroy_symtable(table);
    
	return 1;
}
