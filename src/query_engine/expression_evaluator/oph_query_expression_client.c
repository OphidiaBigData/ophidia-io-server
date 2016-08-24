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
#include "oph_query_parser.h"    
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
    char test_right1[] = "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,1)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),1,1,1))";
    char test_right2[] = "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,1) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),2,1,2)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),1,1,1) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),7,1,8))";
    oph_query_expr_node *e;
    e = NULL;

	//Test 1
    printf("\nTest 1\n");
    printf("Expected result: syntax error.\n");
    oph_query_expr_get_ast(test_syntax_error, &e);

    oph_query_expr_symtable *table;
    oph_query_expr_create_symtable(&table, 1);
    oph_query_expr_value *res;
    res = NULL;


    //Test 2
    printf("\nTest 2\n");
    oph_query_expr_get_ast(test_eval_error1, &e);

	oph_query_expr_add_long("id_dim",1,table);    
    printf("Expected result: eval error.\n");
	if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%lld\n",res->data.long_value);
    oph_query_expr_delete_node(e, table);
	e = NULL;

    //Test 3
    printf("\nTest 3\n");
    oph_query_expr_get_ast(test_eval_error2, &e);

    oph_query_expr_add_long("id_dim",1,table);
    printf("Expected result: eval error.\n");
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("%lld\n",res->data.long_value);
    oph_query_expr_delete_node(e, table);
    e = NULL;

	//Test 4
    printf("\nTest 4\n");
    oph_query_expr_get_ast(test_right1, &e);
    oph_query_expr_add_long("id_dim",1,table);    
	if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("Expected result: 1. Actual result: %lld\n",res->data.long_value);
    if(res != NULL) free(res);
    oph_query_expr_add_long("id_dim",2,table);    
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("Expected result: 0. Actual result: %lld\n",res->data.long_value);
    if(res != NULL) free(res);
    oph_query_expr_delete_node(e, table);
    e = NULL;

    //Test 5
    printf("\nTest 5\n");
    oph_query_expr_get_ast(test_right2, &e);
    oph_query_expr_add_long("id_dim",1,table);    
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("Expected result: 1. Actual result: %lld\n",res->data.long_value);
    if(res != NULL) free(res);
    oph_query_expr_add_long("id_dim",11,table);    
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("Expected result: 1. Actual result: %lld\n",res->data.long_value);
    if(res != NULL) free(res);
    oph_query_expr_add_long("id_dim",2,table);
    if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) printf("Expected result: 0. Actual result: %lld\n",res->data.long_value);

    if(res != NULL) free(res);
    oph_query_expr_delete_node(e, table);
    e = NULL;
    oph_query_expr_destroy_symtable(table);


    //binary tests
    printf("\nTest 6\n");
    char* test_right3 = "oph_id3(id_dim,binary,1)";
    oph_query_expr_node *e1;
    e1 = NULL;
    oph_query_expr_symtable *table1;
    oph_query_expr_create_symtable(&table1, 2);
    oph_query_expr_value *res1;
    res1 = NULL;

    //creation of binary
    long long a[3] = {7,8,5};
    char *v = (char*)malloc(3*sizeof(long long));
    memcpy((void*) v, (long long*) &a, 3*sizeof(long long));
    oph_query_arg b;
    b.arg_length = 3*sizeof(long long);
    b.arg = v;
    oph_query_expr_add_binary("binary", &b, table1);

    oph_query_expr_get_ast(test_right3, &e1);

    oph_query_expr_add_long("id_dim",2, table1);
    if(e1 != NULL && !oph_query_expr_eval_expression(e1,&res1,table1)) printf("Expected result: 1. Actual result: %lld\n",res1->data.long_value);
    if(res1 != NULL) free(res1);
    oph_query_expr_add_long("id_dim",8, table1);
    if(e1 != NULL && !oph_query_expr_eval_expression(e1,&res1,table1)) printf("Expected result: 2. Actual result: %lld\n",res1->data.long_value);
    if(res1 != NULL) free(res1);
    oph_query_expr_add_long("id_dim",16, table1);
    if(e1 != NULL && !oph_query_expr_eval_expression(e1,&res1,table1)) printf("Expected result: 3. Actual result: %lld\n",res1->data.long_value);
    if(res1 != NULL) free(res1);


    oph_query_expr_delete_node(e1, table1);
    oph_query_expr_destroy_symtable(table1);
    free(v);

    //generic tests
    printf("\nTest 7\n");
    char* test_right4 = "one(one(1,1),1)";
    oph_query_expr_node *e2;
    e2 = NULL;
    oph_query_expr_symtable *table2;
    oph_query_expr_create_symtable(&table2, 1);
    oph_query_expr_value *res2;
    res2 = NULL;

    oph_query_expr_get_ast(test_right4, &e2);

    int i = 0; 
    for(;i < 3; i++)
    {
        if(e2 != NULL && !oph_query_expr_eval_expression(e2,&res2,table2)) printf("Expected result: 1. Actual result: %f\n",res2->data.double_value);
        if(res2 != NULL) free(res2);
    }
    
    oph_query_expr_delete_node(e2, table2);
    oph_query_expr_destroy_symtable(table2);
    return 1;
}
