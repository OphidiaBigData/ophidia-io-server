/*
    Ophidia IO Server
    Copyright (C) 2014-2018 CMCC Foundation

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
#include "oph_query_expression_functions.h"
#include "oph_query_parser.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <debug.h>
#include <pthread.h>
#include <signal.h>

#include "oph_query_plugin_loader.h"
#include "oph_network.h"

#define _GNU_SOURCE

HASHTBL *plugin_table = NULL;
oph_query_expr_symtable *oph_function_table = NULL;
pthread_mutex_t libtool_lock = PTHREAD_MUTEX_INITIALIZER;
unsigned short disable_mem_check = 0;

int main(void)
{
	set_debug_level(LOG_DEBUG);
	set_log_prefix(OPH_IO_SERVER_PREFIX);
	char test_syntax_error[] =
	    "mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,3) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),6,1,8)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),2,2,6) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),9,1,9))";
	char test_eval_error1[] =
	    "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10,2),1,1,3) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),6,1,8)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),2,2,6) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),9,1,9))";
	char test_eval_error2[] = "oph_id_to_index(1)";
	char test_right1[] = "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,1)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),1,1,1))";
	char test_right2[] =
	    "(mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),1,1,1) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,10,10),2,1,2)) AND (mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),1,1,1) OR mysql.oph_is_in_subset(mysql.oph_id_to_index2(id_dim,1,10),7,1,8))";
	oph_query_expr_node *e;
	e = NULL;

	oph_query_expr_create_function_symtable(1);

	//Test 1
	printf("\nTest 1\n");
	printf("Expected result: syntax error.\n");
	_oph_query_parser_remove_query_tokens(test_syntax_error);
	oph_query_expr_get_ast(test_syntax_error, &e);

	oph_query_expr_symtable *table;
	oph_query_expr_create_symtable(&table, 1);
	oph_query_expr_value *res;
	res = NULL;


	//Test 2
	printf("\nTest 2\n");
	_oph_query_parser_remove_query_tokens(test_eval_error1);
	oph_query_expr_get_ast(test_eval_error1, &e);

	oph_query_expr_add_long("id_dim", 1, table);
	printf("Expected result: eval error.\n");
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("%lld\n", res->data.long_value);
	free(res);
	oph_query_expr_delete_node(e, table);
	e = NULL;

	//Test 3
	printf("\nTest 3\n");
	_oph_query_parser_remove_query_tokens(test_eval_error2);
	oph_query_expr_get_ast(test_eval_error2, &e);

	oph_query_expr_add_long("id_dim", 1, table);
	printf("Expected result: eval error.\n");
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("%lld\n", res->data.long_value);
	oph_query_expr_delete_node(e, table);
	e = NULL;

	//Test 4
	printf("\nTest 4\n");
	_oph_query_parser_remove_query_tokens(test_right1);
	oph_query_expr_get_ast(test_right1, &e);
	oph_query_expr_add_long("id_dim", 1, table);
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("Expected result: 1. Actual result: %lld\n", res->data.long_value);
	if (res != NULL)
		free(res);
	oph_query_expr_add_long("id_dim", 2, table);
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("Expected result: 0. Actual result: %lld\n", res->data.long_value);
	if (res != NULL)
		free(res);
	oph_query_expr_delete_node(e, table);
	e = NULL;

	//Test 5
	printf("\nTest 5\n");
	_oph_query_parser_remove_query_tokens(test_right2);
	oph_query_expr_get_ast(test_right2, &e);
	oph_query_expr_add_long("id_dim", 1, table);
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("Expected result: 1. Actual result: %lld\n", res->data.long_value);
	if (res != NULL)
		free(res);
	oph_query_expr_add_long("id_dim", 11, table);
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("Expected result: 1. Actual result: %lld\n", res->data.long_value);
	if (res != NULL)
		free(res);
	oph_query_expr_add_long("id_dim", 2, table);
	if (e != NULL && !oph_query_expr_eval_expression(e, &res, table))
		printf("Expected result: 0. Actual result: %lld\n", res->data.long_value);

	if (res != NULL)
		free(res);
	oph_query_expr_delete_node(e, table);
	e = NULL;
	oph_query_expr_destroy_symtable(table);


	//binary tests
	printf("\nTest 6\n");
	char *test_right3 = "oph_id3(id_dim,binary,1)";
	oph_query_expr_node *e1;
	e1 = NULL;
	oph_query_expr_symtable *table1;
	oph_query_expr_create_symtable(&table1, 2);
	oph_query_expr_value *res1;
	res1 = NULL;

	//creation of binary
	long long a[3] = { 7, 8, 5 };
	char *v = (char *) malloc(3 * sizeof(long long));
	memcpy((void *) v, (long long *) &a, 3 * sizeof(long long));
	oph_query_arg b;
	b.arg_length = 3 * sizeof(long long);
	b.arg = v;
	oph_query_expr_add_binary("binary", &b, table1);

	oph_query_expr_get_ast(test_right3, &e1);

	oph_query_expr_add_long("id_dim", 2, table1);
	if (e1 != NULL && !oph_query_expr_eval_expression(e1, &res1, table1))
		printf("Expected result: 1. Actual result: %lld\n", res1->data.long_value);
	if (res1 != NULL)
		free(res1);
	oph_query_expr_add_long("id_dim", 8, table1);
	if (e1 != NULL && !oph_query_expr_eval_expression(e1, &res1, table1))
		printf("Expected result: 2. Actual result: %lld\n", res1->data.long_value);
	if (res1 != NULL)
		free(res1);
	oph_query_expr_add_long("id_dim", 16, table1);
	if (e1 != NULL && !oph_query_expr_eval_expression(e1, &res1, table1))
		printf("Expected result: 3. Actual result: %lld\n", res1->data.long_value);
	if (res1 != NULL)
		free(res1);


	oph_query_expr_delete_node(e1, table1);
	oph_query_expr_destroy_symtable(table1);
	free(v);

	//generic tests
	printf("\nTest 7\n");
	char *test_right6 = "b + a + f(?2) + ?1 + ?2";
	oph_query_expr_node *e4;
	e4 = NULL;
	oph_query_expr_symtable *table4;
	oph_query_expr_create_symtable(&table4, 1);
	oph_query_expr_get_ast(test_right6, &e4);
	char **variables = NULL;
	int var_count = 0;
	oph_query_expr_get_variables(e4, &variables, &var_count);
	int n = 0;
	printf("Query = %s\n", test_right6);
	printf("Variables = ");
	for (n = 0; n < var_count; n++) {
		printf("%s ", variables[n]);
	}
	printf("\n");
	oph_query_expr_delete_node(e4, table4);
	oph_query_expr_destroy_symtable(table4);
	free(variables);

	oph_query_expr_destroy_symtable(oph_function_table);

	//Stress test
	printf("\nTest 8\n");
	if (oph_load_plugins(&plugin_table, &oph_function_table)) {
		oph_unload_plugins(&plugin_table, &oph_function_table);
	} else {
		void *exp_thread(void *arg);
		void release(int signo);

		//Signal(SIGPIPE, SIG_IGN);
		oph_net_signal(SIGINT, release);
		oph_net_signal(SIGABRT, release);
		oph_net_signal(SIGQUIT, release);

		int id;
		int *k;
		pthread_t tid;
		for (id = 0; id < 100; id++) {
			k = (int *) malloc(sizeof(int));
			*k = id;
			if (pthread_create(&tid, NULL, &exp_thread, k) != 0) {
				continue;
			}
		}

		while (1);
	}

	return 1;
}

//Garbage collecition function
void release(int signo)
{
	//Cleanup procedures
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Catched signal %d\n", signo);
	oph_unload_plugins(&plugin_table, &oph_function_table);

	exit(0);
}

void *exp_thread(void *arg)
{
	if (pthread_detach(pthread_self()) != 0)
		return (NULL);

	printf("Running thread %d\n", *((int *) arg));

	//creation of binary variable
	double bin[10] = { 10.1, 3.45, 2.6, 100.234, 23.46, 67.98, 13.91, 8.546, 923.45, -10.9456 };
	char *vbin = (char *) malloc(10 * sizeof(double));
	memcpy((void *) vbin, (long long *) &bin, 10 * sizeof(double));

	oph_query_arg val_b;
	val_b.arg_length = 10 * sizeof(double);
	val_b.arg = vbin;

	char *stress_test = NULL;
	switch (*((int *) arg) % 3) {
		case 0:
			stress_test = "oph_get_subarray('OPH_DOUBLE','OPH_DOUBLE',measure,3,4)";
			break;
		case 1:
			stress_test = "oph_reduce('OPH_DOUBLE','OPH_DOUBLE',measure,'OPH_AVG',0,2)";
			break;
		case 2:
			stress_test = "oph_gsl_sort('OPH_DOUBLE','OPH_DOUBLE',measure)";
			break;
	}

	oph_query_expr_node *e = NULL;
	oph_query_expr_symtable *table;
	oph_query_expr_create_symtable(&table, 10);
	oph_query_expr_value *res = NULL;

	oph_query_expr_get_ast(stress_test, &e);
	oph_query_expr_add_binary("measure", &val_b, table);

	int j;
	for (j = 0; j < 1; j++) {
		if (e != NULL && !oph_query_expr_eval_expression(e, &res, table)) {
			printf("Correct execution of thread %d on row %d\n", *((int *) arg), j);
			free(res->data.binary_value->arg);
			free(res->data.binary_value);
			free(res);
		} else {
			printf("Incorrect execution of thread %d on row %d\n", *((int *) arg), j);
			free(res->data.binary_value->arg);
			free(res->data.binary_value);
			free(res);
			break;
		}
	}

	printf("Ending thread %d\n", *((int *) arg));

	oph_query_expr_delete_node(e, table);
	oph_query_expr_destroy_symtable(table);
	free(vbin);
	free(arg);

	return (NULL);
}
