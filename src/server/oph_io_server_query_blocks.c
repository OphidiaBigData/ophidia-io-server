/*
    Ophidia IO Server
    Copyright (C) 2014-2019 CMCC Foundation

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

#define _GNU_SOURCE

#include "oph_io_server_query_manager.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <debug.h>
#include <errno.h>
#include <pthread.h>

#include "oph_server_utility.h"
#include "oph_query_engine_language.h"

#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_functions.h"
#include "oph_query_plugin_loader.h"

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern pthread_rwlock_t rwlock;
extern HASHTBL *plugin_table;

//Internal structures used to manage group of rows
typedef struct oph_ioserver_group_elem {
	long long elem_index;
	struct oph_ioserver_group_elem *next;
} oph_ioserver_group_elem;

typedef struct oph_ioserver_group_elem_list {
	struct oph_ioserver_group_elem *last;
	struct oph_ioserver_group_elem *first;
	long long length;
} oph_ioserver_group_elem_list;

typedef struct oph_ioserver_group {
	oph_ioserver_group_elem_list *group_list;
	struct oph_ioserver_group *next;
} oph_ioserver_group;

int _oph_ioserver_query_add_group_elem(oph_ioserver_group_elem_list * list, long long index)
{
	if (!list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_ioserver_group_elem *tmp = (oph_ioserver_group_elem *) malloc(sizeof(oph_ioserver_group_elem));
	if (!tmp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	tmp->next = NULL;
	tmp->elem_index = index;

	if (list->first == NULL) {
		list->first = tmp;
	} else {
		list->last->next = tmp;
	}
	list->last = tmp;
	list->length++;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_add_group(oph_ioserver_group ** current, oph_ioserver_group_elem_list * index)
{
	if (!current || !index) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_ioserver_group *tmp = (oph_ioserver_group *) malloc(sizeof(oph_ioserver_group));
	if (!tmp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	tmp->next = NULL;
	tmp->group_list = index;

	if (*current != NULL) {
		(*current)->next = tmp;
	}
	*current = tmp;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_delete_group_elem_list(oph_ioserver_group_elem_list * list)
{
	if (!list) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_ioserver_group_elem *tmp = NULL;
	while (list->first) {
		tmp = list->first;
		list->first = list->first->next;
		free(tmp);
	}
	free(list);

	return OPH_IO_SERVER_SUCCESS;
}


int _oph_ioserver_query_build_groups(HASHTBL * query_groups, char *group, long long row, oph_ioserver_group ** head_group, oph_ioserver_group ** current_group, long long *group_num)
{
	if (!query_groups || !group || !head_group || !current_group || !group_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	if (!(*head_group)) {
		//Create first group list
		oph_ioserver_group_elem_list *list = (oph_ioserver_group_elem_list *) malloc(sizeof(oph_ioserver_group_elem_list));
		if (!list) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		list->first = list->last = NULL;
		list->length = 0;
		if (_oph_ioserver_query_add_group_elem(list, row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(list);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		if (_oph_ioserver_query_add_group(head_group, list)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(list);
			_oph_ioserver_query_delete_group_elem_list(list);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		*current_group = *head_group;
		hashtbl_insert(query_groups, (char *) group, (void *) (*current_group));
		(*group_num)++;
	} else {
		if (!(*current_group)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
			return OPH_IO_SERVER_NULL_PARAM;
		}
		//Check if group is already available
		oph_ioserver_group *group_index = (oph_ioserver_group *) hashtbl_get(query_groups, group);
		if (!group_index) {
			//Add group to hash table
			oph_ioserver_group_elem_list *list = (oph_ioserver_group_elem_list *) malloc(sizeof(oph_ioserver_group_elem_list));
			if (!list) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
			list->first = list->last = NULL;
			list->length = 0;
			if (_oph_ioserver_query_add_group_elem(list, row)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				free(list);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
			if (_oph_ioserver_query_add_group(current_group, list)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				free(list);
				_oph_ioserver_query_delete_group_elem_list(list);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
			hashtbl_insert(query_groups, (char *) group, (void *) (*current_group));
			(*group_num)++;
		} else {
			//Simply extend the group list
			if (_oph_ioserver_query_add_group_elem(group_index->group_list, row)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}

		}
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_get_groups(HASHTBL * query_args, long long total_row_number, oph_query_arg ** args, oph_iostore_frag_record_set ** inputs, int table_num, long long *output_row_num,
				   oph_ioserver_group_elem_list *** group_lists)
{
	if (!query_args || !total_row_number || !table_num || !inputs || !output_row_num || !group_lists) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int k, l, i;
	long long j;

	*output_row_num = 0;
	*group_lists = NULL;

	// Check group by clause
	char *group_by = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_GROUP);
	if (group_by) {
		//Extract groups
		char **group_list = NULL;
		int group_list_num = 0;

		//Check binary fields if available
		unsigned int arg_count = 0;
		i = 0;
		if (args != NULL) {
			while (args[i++])
				arg_count++;
		}

		if (oph_query_parse_multivalue_arg(group_by, &group_list, &group_list_num) || group_list_num != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_TOO_MANY_GROUPS, group_by);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_TOO_MANY_GROUPS, group_by);
			if (group_list)
				free(group_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if (group_list)
			free(group_list);

		//Find id columns in each table
		short int id_indexes[table_num];
		for (l = 0; l < table_num; l++) {
			for (i = 0; i < inputs[l]->field_num; i++) {
				if (!STRCMP(inputs[l]->field_name[i], OPH_NAME_ID)) {
					id_indexes[l] = i;
					break;
				}
			}
			//Id not found  
			if (i == inputs[l]->field_num) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, OPH_NAME_ID);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, OPH_NAME_ID);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		}

		oph_query_expr_node *e = NULL;

		if (oph_query_expr_get_ast(group_by, &e) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		oph_query_expr_symtable *table;
		if (oph_query_expr_create_symtable(&table, OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			oph_query_expr_delete_node(e, table);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		int var_count = 0;
		char **var_list = NULL;

		//Read all variables and link them to input record set fields
		if (oph_query_expr_get_variables(e, &var_list, &var_count)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, group_by);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, group_by);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		unsigned int field_indexes[var_count];
		int frag_indexes[var_count];
		char field_binary[var_count];

		if (var_count > 0) {
			if (_oph_ioserver_query_get_variable_indexes(arg_count, var_list, var_count, inputs, table_num, field_indexes, frag_indexes, field_binary, 1, id_indexes)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, group_by);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, group_by);
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NO_VARIABLE_FOR_GROUP, group_by);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NO_VARIABLE_FOR_GROUP, group_by);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		oph_query_expr_value *res = NULL;

		//TODO Count actual number of string/binary variables
		oph_query_arg val_b[var_count];

		//Create group hash table
		HASHTBL *query_groups = hashtbl_create(2 * total_row_number, NULL);
		if (!query_groups) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		oph_ioserver_group *head = NULL, *current = NULL;
		long long group_number = 0;

		char result[OPH_IO_SERVER_BUFFER] = { '\0' };

		for (j = 0; j < total_row_number; j++) {
			if (_oph_ioserver_query_set_parser_variables(args, var_list, var_count, inputs, table, field_indexes, frag_indexes, field_binary, val_b, group_by, j, NULL)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				for (current = head; current; current = current->next)
					if (current->group_list)
						_oph_ioserver_query_delete_group_elem_list(current->group_list);
				hashtbl_destroy(query_groups);
				return OPH_IO_SERVER_PARSE_ERROR;
			}

			if (e != NULL && !oph_query_expr_eval_expression(e, &res, table)) {
				//Create index record
				switch (res->type) {
					case OPH_QUERY_EXPR_TYPE_DOUBLE:
						{
							snprintf(result, OPH_IO_SERVER_BUFFER - 1, "%f", res->data.double_value);
							free(res);
							break;
						}
					case OPH_QUERY_EXPR_TYPE_LONG:
						{
							snprintf(result, OPH_IO_SERVER_BUFFER - 1, "%lld", res->data.long_value);
							free(res);
							break;
						}
					default:
						{
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
							free(res);
							oph_query_expr_delete_node(e, table);
							oph_query_expr_destroy_symtable(table);
							free(var_list);
							for (current = head; current; current = current->next)
								if (current->group_list)
									_oph_ioserver_query_delete_group_elem_list(current->group_list);
							hashtbl_destroy(query_groups);
							return OPH_IO_SERVER_PARSE_ERROR;
						}
				}

				//Add row to correct group
				if (_oph_ioserver_query_build_groups(query_groups, result, j, &head, &current, &group_number)) {

					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					free(var_list);
					for (current = head; current; current = current->next)
						if (current->group_list)
							_oph_ioserver_query_delete_group_elem_list(current->group_list);
					hashtbl_destroy(query_groups);
					return OPH_IO_SERVER_MEMORY_ERROR;
				}
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, group_by);
				free(var_list);
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				for (current = head; current; current = current->next)
					if (current->group_list)
						_oph_ioserver_query_delete_group_elem_list(current->group_list);
				hashtbl_destroy(query_groups);
				return OPH_IO_SERVER_PARSE_ERROR;
			}
		}
		free(var_list);
		oph_query_expr_delete_node(e, table);
		oph_query_expr_destroy_symtable(table);

		oph_ioserver_group_elem_list **groups = (oph_ioserver_group_elem_list **) malloc(group_number * sizeof(oph_ioserver_group_elem_list *));
		if (!groups) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			for (current = head; current; current = current->next)
				if (current->group_list)
					_oph_ioserver_query_delete_group_elem_list(current->group_list);
			hashtbl_destroy(query_groups);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		for (k = 0; k < group_number; k++) {
			groups[k] = head->group_list;
			head = head->next;
		}
		hashtbl_destroy(query_groups);

		//Return groups
		*output_row_num = group_number;
		*group_lists = groups;

	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_set_parser_variables(oph_query_arg ** args, char **var_list, unsigned int var_count, oph_iostore_frag_record_set ** inputs, oph_query_expr_symtable * table,
					     unsigned int *field_indexes, int *frag_indexes, char *field_binary, oph_query_arg * binary_var, char *field, long long row, long long *where_start_id)
{
	if (!var_list || !var_count || !inputs || !table || !field_indexes || !frag_indexes || !field_binary || !binary_var || !field) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	unsigned int k;

	for (k = 0; k < var_count; k++) {
		if (field_binary[k]) {
			if (args) {
				if (oph_query_expr_add_binary(var_list[k], args[field_indexes[k]], table)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			} else {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		} else {
			switch (inputs[frag_indexes[k]]->field_type[field_indexes[k]]) {
				case OPH_IOSTORE_LONG_TYPE:
					{
						if (oph_query_expr_add_long
						    (var_list[k],
						     *((long long *) inputs[frag_indexes[k]]->record_set[(where_start_id ? where_start_id[frag_indexes[k]] + row : row)]->field[field_indexes[k]]),
						     table)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							return OPH_IO_SERVER_EXEC_ERROR;
						}
						break;
					}
				case OPH_IOSTORE_REAL_TYPE:
					{
						if (oph_query_expr_add_double
						    (var_list[k],
						     *((double *) inputs[frag_indexes[k]]->record_set[(where_start_id ? where_start_id[frag_indexes[k]] + row : row)]->field[field_indexes[k]]),
						     table)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							return OPH_IO_SERVER_EXEC_ERROR;
						}
						break;
					}
					//TODO Check if string and binary can be treated separately
				case OPH_IOSTORE_STRING_TYPE:
					{
						binary_var[k].arg = inputs[frag_indexes[k]]->record_set[(where_start_id ? where_start_id[frag_indexes[k]] + row : row)]->field[field_indexes[k]];
						binary_var[k].arg_length =
						    inputs[frag_indexes[k]]->record_set[(where_start_id ? where_start_id[frag_indexes[k]] + row : row)]->field_length[field_indexes[k]];
						if (oph_query_expr_add_binary(var_list[k], &(binary_var[k]), table)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field);
							return OPH_IO_SERVER_EXEC_ERROR;
						}
						break;
					}
			}
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_get_variable_indexes(unsigned int arg_count, char **var_list, unsigned int var_count, oph_iostore_frag_record_set ** inputs, unsigned int table_num,
					     unsigned int *field_indexes, int *frag_indexes, char *field_binary, char only_id, short int *id_indexes)
{
	if (!var_list || !var_count || !inputs || !table_num || !field_indexes || !frag_indexes || !field_binary || (only_id && !id_indexes)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	unsigned int k, l;
	long long j;
	char *tmp_var_list[var_count];
	for (k = 0; k < var_count; k++) {
		tmp_var_list[k] = (char *) strdup(var_list[k]);
		if (tmp_var_list[k] == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			for (k = 0; k < var_count; k++)
				if (tmp_var_list[k])
					free(tmp_var_list[k]);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	unsigned int binary_index = 0;

	for (k = 0; k < var_count; k++) {
		//Match binary values
		if (tmp_var_list[k][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE) {
			binary_index = strtoll((char *) (tmp_var_list[k] + 1), NULL, 10);
			field_indexes[k] = (binary_index - 1);
			frag_indexes[k] = -1;
			field_binary[k] = 1;

			if (field_indexes[k] >= arg_count) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
				for (k = 0; k < var_count; k++)
					if (tmp_var_list[k])
						free(tmp_var_list[k]);
				return OPH_IO_SERVER_PARSE_ERROR;
			}
		}
		//Match field names
		else {
			if (table_num > 1) {
				//Set frag and field index
				//Split frag name from field name
				char **field_components = NULL;
				int field_components_num = 0;

				if (oph_query_parse_hierarchical_args(tmp_var_list[k], &field_components, &field_components_num)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					for (k = 0; k < var_count; k++)
						if (tmp_var_list[k])
							free(tmp_var_list[k]);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				if (field_components_num != 2) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					for (k = 0; k < var_count; k++)
						if (tmp_var_list[k])
							free(tmp_var_list[k]);
					free(field_components);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				if (only_id) {
					//Check if we only have ids, any other field is not allowed
					if (STRCMP(field_components[1], OPH_NAME_ID)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						for (k = 0; k < var_count; k++)
							if (tmp_var_list[k])
								free(tmp_var_list[k]);
						free(field_components);
						return OPH_IO_SERVER_EXEC_ERROR;
					}
				}
				//Match table
				for (l = 0; l < table_num; l++) {
					if (!STRCMP(field_components[0], inputs[l]->frag_name)) {
						frag_indexes[k] = l;
						if (!only_id) {
							for (j = 0; j < inputs[l]->field_num; j++) {
								if (!STRCMP(field_components[1], inputs[l]->field_name[j])) {
									field_indexes[k] = j;
									field_binary[k] = 0;
									break;
								}
							}
							if (j == inputs[l]->field_num) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
								for (k = 0; k < var_count; k++)
									if (tmp_var_list[k])
										free(tmp_var_list[k]);
								free(field_components);
								return OPH_IO_SERVER_PARSE_ERROR;
							}
						} else {
							field_indexes[k] = id_indexes[l];
							field_binary[k] = 0;
						}
						break;
					}
				}
				if (l == table_num) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
					for (k = 0; k < var_count; k++)
						if (tmp_var_list[k])
							free(tmp_var_list[k]);
					free(field_components);
					return OPH_IO_SERVER_PARSE_ERROR;
				}
				free(field_components);
			} else {
				char **field_components = NULL;
				int field_components_num = 0;

				if (oph_query_parse_hierarchical_args(tmp_var_list[k], &field_components, &field_components_num)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					for (k = 0; k < var_count; k++)
						if (tmp_var_list[k])
							free(tmp_var_list[k]);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				if (field_components_num > 2 || field_components_num < 1) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
					for (k = 0; k < var_count; k++)
						if (tmp_var_list[k])
							free(tmp_var_list[k]);
					free(field_components);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				if (!only_id) {
					for (j = 0; j < inputs[0]->field_num; j++) {
						if (!STRCMP((field_components_num == 1 ? var_list[k] : field_components[1]), inputs[0]->field_name[j])) {
							field_indexes[k] = j;
							field_binary[k] = 0;
							break;
						}
					}
					if (j == inputs[0]->field_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
						for (k = 0; k < var_count; k++)
							if (tmp_var_list[k])
								free(tmp_var_list[k]);
						free(field_components);
						return OPH_IO_SERVER_PARSE_ERROR;
					}
				} else {
					//Check if we only have ids, any other field is not allowed
					if (STRCMP((field_components_num == 1 ? tmp_var_list[k] : field_components[1]), OPH_NAME_ID)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						for (k = 0; k < var_count; k++)
							if (tmp_var_list[k])
								free(tmp_var_list[k]);
						free(field_components);
						return OPH_IO_SERVER_EXEC_ERROR;
					}
					//Match field
					field_indexes[k] = id_indexes[0];
					field_binary[k] = 0;
				}
				frag_indexes[k] = 0;
				free(field_components);
			}
		}
	}

	for (k = 0; k < var_count; k++)
		if (tmp_var_list[k])
			free(tmp_var_list[k]);

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_io_server_query_compute_limits(HASHTBL * query_args, long long *offset, long long *limit)
{
	if (!query_args || !offset || !limit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*limit = 0;
	*offset = 0;

	char *limits = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
	if (limits) {
		char **limit_list = NULL;
		int limit_list_num = 0;
		if (oph_query_parse_multivalue_arg(limits, &limit_list, &limit_list_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
			if (limit_list)
				free(limit_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if (limit_list) {
			switch (limit_list_num) {
				case 1:
					*limit = strtoll(limit_list[0], NULL, 10);
					break;
				case 2:
					*offset = strtoll(limit_list[0], NULL, 10);
					*limit = strtoll(limit_list[1], NULL, 10);
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
					free(limit_list);
					return OPH_IO_SERVER_EXEC_ERROR;
			}
			free(limit_list);
			if ((*limit) < 0)
				*limit = 0;
			if ((*offset) < 0)
				*offset = 0;
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_io_server_query_order_output(HASHTBL * query_args, oph_iostore_frag_record_set * rs)
{
	if (!query_args || !rs || !rs->record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	char *order = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_ORDER);
	if (order) {
		int i = 0;
		long long j = 1, l = 0;
		oph_iostore_frag_record *tmp = NULL;
		for (i = 0; i < rs->field_num; i++) {
			if (!STRCMP(order, rs->field_name[i])) {
				//If single row, then no order required
				if (!rs->record_set[j])
					break;

				switch (rs->field_type[i]) {
					case OPH_IOSTORE_REAL_TYPE:
						{
							//Run insertion sort routine
							while (rs->record_set[j]) {
								tmp = rs->record_set[j];
								for (l = j - 1; l >= 0; l--) {
									if (*((double *) tmp->field[i]) >= *((double *) rs->record_set[l]->field[i]))
										break;
									else
										rs->record_set[l + 1] = rs->record_set[l];
								}
								rs->record_set[l + 1] = tmp;
								j++;
							}
							break;
						}
					case OPH_IOSTORE_LONG_TYPE:
						{
							//Run insertion sort routine
							while (rs->record_set[j]) {
								tmp = rs->record_set[j];
								for (l = j - 1; l >= 0; l--) {
									if (*((long long *) tmp->field[i]) >= *((long long *) rs->record_set[l]->field[i]))
										break;
									else
										rs->record_set[l + 1] = rs->record_set[l];
								}
								rs->record_set[l + 1] = tmp;
								j++;
							}
							break;
						}
					default:
						{
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ORDER_TYPE_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ORDER_TYPE_ERROR);
							return OPH_IO_SERVER_EXEC_ERROR;
						}
				}
				break;
			}
		}
		if (i == rs->field_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, order);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, order);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}


int _oph_ioserver_query_release_input_record_set(oph_iostore_handler * dev_handle, oph_iostore_frag_record_set ** stored_rs, oph_iostore_frag_record_set ** input_rs)
{
	if (!dev_handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int l = 0;
	if (stored_rs) {
		for (l = 0; stored_rs[l]; l++) {
			if (dev_handle->is_persistent || stored_rs[l]->tmp_flag != 0)
				oph_iostore_destroy_frag_recordset(&(stored_rs[l]));
		}
		free(stored_rs);
	}
	if (input_rs) {
		for (l = 0; input_rs[l]; l++) {
			oph_iostore_destroy_frag_recordset_only(&(input_rs[l]));
		}
		free(input_rs);
	}
	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_multi_table_where_assert(int table_num, short int *id_indexes, long long *start_row_indexes, long long *input_row_num, oph_iostore_frag_record_set ** in_record_set)
{
	if (!table_num || !id_indexes || !start_row_indexes || !input_row_num || !in_record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Assume to have only inner join where clauses with unique and sorted ids 
	char id_error_flag = 0;
	long long a, b, j;
	long long table_min[table_num];
	long long table_max[table_num];
	int l;

	for (l = 0; l < table_num; l++) {
		table_min[l] = *((long long *) in_record_set[l]->record_set[0]->field[id_indexes[l]]);
		for (j = 1; in_record_set[l]->record_set[j]; j++) {
			//Verify order, uniqueness and no values missing
			b = *((long long *) in_record_set[l]->record_set[j]->field[id_indexes[l]]);
			a = *((long long *) in_record_set[l]->record_set[j - 1]->field[id_indexes[l]]);
			if ((b <= a) || (b - a) != 1) {
				id_error_flag = 1;
				break;
			}
		}
		if (id_error_flag == 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, in_record_set[l]->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, in_record_set[l]->frag_name);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		table_max[l] = *((long long *) in_record_set[l]->record_set[j - 1]->field[id_indexes[l]]);
	}

	//Get min and max idvalues
	long long tmp_min = table_min[l - 1], tmp_max = table_max[l - 1];
	for (l = 1; l < table_num; l++) {
		if (table_min[l] < table_min[l - 1])
			tmp_min = table_min[l];
		if (table_max[l] > table_max[l - 1])
			tmp_max = table_max[l];
	}

	//Check table overlap and return empty set in case
	for (l = 0; l < table_num; l++) {
		if ((table_min[l] > tmp_max) || (table_max[l] < tmp_min)) {
			//Update output argument with actual value
			*input_row_num = 0;
			return OPH_IO_SERVER_SUCCESS;
		}
	}

	//Find index of minimum value in each table
	for (l = 0; l < table_num; l++) {
		for (j = 0; in_record_set[l]->record_set[j]; j++) {
			a = *((long long *) in_record_set[l]->record_set[j]->field[id_indexes[l]]);
			if (a == tmp_min) {
				start_row_indexes[l] = j;
				break;
			}
		}
	}

	*input_row_num = tmp_max - tmp_min + 1;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_run_where_clause(char *where_string, oph_query_arg ** args, int table_num, oph_iostore_frag_record_set ** stored_rs, long long *input_row_num,
					 oph_iostore_frag_record_set ** input_rs)
{
	if (!where_string || !table_num || !stored_rs || !input_row_num || !input_rs) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int l, i;
	long long j;

	//Check binary fields if available
	unsigned int arg_count = 0;
	i = 0;
	if (args != NULL) {
		while (args[i++])
			arg_count++;
	}
	//Find id columns in each table
	short int id_indexes[table_num];
	for (l = 0; l < table_num; l++) {
		for (i = 0; i < stored_rs[l]->field_num; i++) {
			if (!STRCMP(stored_rs[l]->field_name[i], OPH_NAME_ID)) {
				id_indexes[l] = i;
				break;
			}
		}
		//Id not found  
		if (i == stored_rs[l]->field_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, OPH_NAME_ID);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, OPH_NAME_ID);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	long long start_row_indexes[table_num];

	if (table_num > 1) {
		if (_oph_ioserver_query_multi_table_where_assert(table_num, id_indexes, start_row_indexes, input_row_num, stored_rs)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, stored_rs[l]->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, stored_rs[l]->frag_name);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	} else {
		start_row_indexes[0] = 0;
	}
	//No rows found simply return empty set
	if ((*input_row_num) == 0)
		return OPH_IO_SERVER_SUCCESS;

	oph_query_expr_node *e = NULL;

	if (oph_query_expr_get_ast(where_string, &e) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	oph_query_expr_symtable *table;
	if (oph_query_expr_create_symtable(&table, OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_query_expr_delete_node(e, table);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	int var_count = 0;
	char **var_list = NULL;

	//Read all variables and link them to input record set fields
	if (oph_query_expr_get_variables(e, &var_list, &var_count)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where_string);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where_string);
		oph_query_expr_delete_node(e, table);
		oph_query_expr_destroy_symtable(table);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	unsigned int field_indexes[var_count];
	int frag_indexes[var_count];
	char field_binary[var_count];

	if (var_count > 0) {
		if (_oph_ioserver_query_get_variable_indexes(arg_count, var_list, var_count, input_rs, table_num, field_indexes, frag_indexes, field_binary, 1, id_indexes)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, where_string);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, where_string);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	} else {
		if (table_num > 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			return OPH_IO_SERVER_PARSE_ERROR;
		}
	}

	oph_query_expr_value *res = NULL;

	//TODO Count actual number of string/binary variables
	oph_query_arg val_b[var_count];
	long long curr_row = 0;

	for (j = 0; j < (*input_row_num); j++) {

		if (_oph_ioserver_query_set_parser_variables(args, var_list, var_count, stored_rs, table, field_indexes, frag_indexes, field_binary, val_b, where_string, j, start_row_indexes)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		if (e != NULL && !oph_query_expr_eval_expression(e, &res, table)) {
			//Create index record
			long long result = 0;
			switch (res->type) {
				case OPH_QUERY_EXPR_TYPE_DOUBLE:
					{
						result = (long long) res->data.double_value;
						free(res);
						break;
					}
				case OPH_QUERY_EXPR_TYPE_LONG:
					{
						result = res->data.long_value;
						free(res);
						break;
					}
				default:
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						free(res);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						return OPH_IO_SERVER_PARSE_ERROR;
					}
			}

			//Add result to each index table
			if (result) {
				for (l = 0; l < table_num; l++) {
					input_rs[l]->record_set[curr_row] = stored_rs[l]->record_set[start_row_indexes[l] + j];
				}
				curr_row++;
			}
		} else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			free(var_list);
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			return OPH_IO_SERVER_PARSE_ERROR;
		}
	}
	free(var_list);
	oph_query_expr_delete_node(e, table);
	oph_query_expr_destroy_symtable(table);
	*input_row_num = curr_row;

	return OPH_IO_SERVER_SUCCESS;
}

#ifdef OPH_IO_SERVER_NETCDF
int _oph_io_server_query_load_from_file(oph_metadb_db_row ** meta_db, oph_iostore_handler * dev_handle, char *current_db, HASHTBL * query_args, oph_iostore_frag_record_set ** loaded_record_sets,
					unsigned long long *loaded_frag_size)
{
	if (!query_args || !dev_handle || !current_db || !meta_db || !query_args) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_iostore_frag_record_set *record_sets = NULL;

	if (oph_io_server_run_create_empty_frag(meta_db, dev_handle, current_db, query_args, &record_sets)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create Empty Frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create Empty Frag");
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	char **dim_type_list = NULL, **dim_index_list = NULL, **dim_start_list = NULL, **dim_end_list = NULL;
	int dim_list_num = 0, tmpdim_list_num = 0;
	int i;

	//Get import specific arguments
	char *src_path = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_PATH);
	if (!src_path) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_PATH);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_PATH);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	char *measure = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_MEASURE);
	if (!measure) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_MEASURE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_MEASURE);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	unsigned long long row_num = 0;
	char *nrows = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_NROW);
	if (!nrows) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_NROW);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_NROW);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	} else {
		row_num = strtoull(nrows, NULL, 10);
		if (row_num <= 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_NROW);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_NROW);
			oph_iostore_destroy_frag_recordset(&record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	long long frag_start = 0;
	char *row_start = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_ROW_START);
	if (!row_start) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_ROW_START);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_ROW_START);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	} else {
		frag_start = strtoll(row_start, NULL, 10);
		if (frag_start <= 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_ROW_START);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_ROW_START);
			oph_iostore_destroy_frag_recordset(&record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	char *compression = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_COMPRESSED);
	if (compression == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COMPRESSED);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COMPRESSED);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//If final statement is set, then activate flag
	char compressed_flag = (STRCMP(compression, OPH_QUERY_ENGINE_LANG_VAL_YES) == 0);


	char *dim_type = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
	if (!dim_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
		oph_iostore_destroy_frag_recordset(&record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if (oph_query_parse_multivalue_arg(dim_type, &dim_type_list, &dim_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
		oph_iostore_destroy_frag_recordset(&record_sets);
		if (dim_type_list)
			free(dim_type_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Convert to correct type
	short int *dims_type = NULL;
	if (!(dims_type = (short int *) calloc(dim_list_num, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dim_type_list);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	for (i = 0; i < dim_list_num; i++) {
		dims_type[i] = (short int) strtol(dim_type_list[i], NULL, 10);
		if (dims_type[i] < 0 || dims_type[i] > 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_TYPE);
			oph_iostore_destroy_frag_recordset(&record_sets);
			free(dim_type_list);
			free(dims_type);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	free(dim_type_list);

	char *dim_index = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
	if (!dim_index) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
		free(dims_type);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if (oph_query_parse_multivalue_arg(dim_index, &dim_index_list, &tmpdim_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
		free(dims_type);
		oph_iostore_destroy_frag_recordset(&record_sets);
		if (dim_index_list)
			free(dim_index_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Check if number of dimension values are compliant
	if (tmpdim_list_num != dim_list_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dim_index_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Convert to correct type
	short int *dims_index = NULL;
	if (!(dims_index = (short int *) calloc(dim_list_num, sizeof(short int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dim_index_list);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	for (i = 0; i < dim_list_num; i++) {
		dims_index[i] = (short int) strtol(dim_index_list[i], NULL, 10);
		if (dims_index[i] < 0 || dims_index[i] > (dim_list_num - 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_INDEX);
			oph_iostore_destroy_frag_recordset(&record_sets);
			free(dims_type);
			free(dim_index_list);
			free(dims_index);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	free(dim_index_list);


	char *dim_start = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
	if (!dim_start) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if (oph_query_parse_multivalue_arg(dim_start, &dim_start_list, &tmpdim_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		if (dim_start_list)
			free(dim_start_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Check if number of dimension values are compliant
	if (tmpdim_list_num != dim_list_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dim_start_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Convert to correct type
	int *dims_start = NULL;
	if (!(dims_start = (int *) calloc(dim_list_num, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dim_start_list);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	for (i = 0; i < dim_list_num; i++) {
		dims_start[i] = (int) strtol(dim_start_list[i], NULL, 10);
		if (dims_start[i] < 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_START);
			oph_iostore_destroy_frag_recordset(&record_sets);
			free(dims_type);
			free(dims_index);
			free(dim_start_list);
			free(dims_start);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	free(dim_start_list);


	char *dim_end = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
	if (!dim_end) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if (oph_query_parse_multivalue_arg(dim_end, &dim_end_list, &tmpdim_list_num)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		if (dim_end_list)
			free(dim_end_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Check if number of dimension values are compliant
	if (tmpdim_list_num != dim_list_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_FILE_IMPORT);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		free(dim_end_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Convert to correct type
	int *dims_end = NULL;
	if (!(dims_end = (int *) calloc(dim_list_num, sizeof(int)))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		free(dim_end_list);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	for (i = 0; i < dim_list_num; i++) {
		dims_end[i] = (int) strtol(dim_end_list[i], NULL, 10);
		if (dims_end[i] < 0 || dims_end[i] < dims_start[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_DIM_END);
			oph_iostore_destroy_frag_recordset(&record_sets);
			free(dims_type);
			free(dims_index);
			free(dims_start);
			free(dim_end_list);
			free(dims_end);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	free(dim_end_list);

	record_sets->record_set = (oph_iostore_frag_record **) calloc(1 + row_num, sizeof(oph_iostore_frag_record *));
	if (record_sets->record_set == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		free(dims_end);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Define record struct
	unsigned long long frag_size = 0;

	if (_oph_ioserver_nc_read(src_path, measure, row_num, frag_start, compressed_flag, dim_list_num, dims_type, dims_index, dims_start, dims_end, record_sets, &frag_size)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data from NetCDF file\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read data from NetCDF file\n");
		oph_iostore_destroy_frag_recordset(&record_sets);
		free(dims_type);
		free(dims_index);
		free(dims_start);
		free(dims_end);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	free(dims_type);
	free(dims_index);
	free(dims_start);
	free(dims_end);

	*loaded_frag_size = frag_size;
	*loaded_record_sets = record_sets;

	return OPH_IO_SERVER_SUCCESS;
}
#endif


int _oph_ioserver_query_build_input_record_set(HASHTBL * query_args, oph_query_arg ** args, oph_metadb_db_row ** meta_db, oph_iostore_handler * dev_handle, char *current_db,
					       oph_iostore_frag_record_set *** stored_rs, long long *input_row_num, oph_iostore_frag_record_set *** input_rs, char *out_db_name, char *out_frag_name,
					       char file_load_flag)
{
	if (!dev_handle || !query_args || !stored_rs || !input_row_num || !input_rs || !meta_db || !current_db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*stored_rs = NULL;
	*input_row_num = 0;
	*input_rs = NULL;

	char create_flag = (out_db_name != NULL && out_frag_name != NULL);

	//Extract frag_name arg from query args
	char *from_frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM);
	if (from_frag_name == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	char **table_list = NULL;
	int table_list_num = 0;
	if (oph_query_parse_multivalue_arg(from_frag_name, &table_list, &table_list_num) || !table_list_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		if (table_list)
			free(table_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Build list of input db and frag names 
	char **in_frag_names = NULL;
	char **in_db_names = NULL;
	in_frag_names = (char **) malloc(table_list_num * sizeof(char *));
	in_db_names = (char **) malloc(table_list_num * sizeof(char *));
	if (!in_frag_names || !in_db_names) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (table_list)
			free(table_list);
		if (in_frag_names)
			free(in_frag_names);
		if (in_db_names)
			free(in_db_names);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	int l = 0;
	char **from_components = NULL;
	int from_components_num = 0;
	char *tmp_file_kw = NULL;
	short int file_pos = -1;
	//From multiple table
	for (l = 0; l < table_list_num; l++) {
		if (oph_query_parse_hierarchical_args(table_list[l], &from_components, &from_components_num)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			free(table_list);
			free(in_frag_names);
			free(in_db_names);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		if (file_load_flag) {
			//If data can be loaded from file, check for proper keyword
			tmp_file_kw = ((from_components_num == 1) ? from_components[0] : from_components[1]);
			if (!STRCMP(tmp_file_kw, OPH_QUERY_ENGINE_LANG_KW_FILE)) {
				//Only a single file can be provided in combination with at least another table
				if ((table_list_num == 1) || (from_components_num == 2) || (file_pos != -1)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
					free(table_list);
					free(in_frag_names);
					free(in_db_names);
					free(from_components);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				in_frag_names[l] = from_frag_name;
				in_db_names[l] = current_db;
				file_pos = l;
				free(from_components);
				continue;
			}
		}
		//If DB is setted in frag name
		if ((table_list_num == 1 && (from_components_num > 2 || from_components_num < 1)) || (table_list_num > 1 && from_components_num != 2)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			free(table_list);
			free(in_frag_names);
			free(in_db_names);
			free(from_components);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		if (from_components_num == 1) {
			in_frag_names[l] = from_components[0];
			in_db_names[l] = current_db;
		} else if (from_components_num == 2) {
			//If DB is setted in frag name
			in_frag_names[l] = from_components[1];
			in_db_names[l] = from_components[0];
		}
		free(from_components);
	}
	free(table_list);

	if (file_load_flag) {
		//If no file key word is provided, then query is not correct
		if (file_pos == -1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
			free(in_frag_names);
			free(in_db_names);
			return OPH_IO_SERVER_PARSE_ERROR;
		}
	}

	oph_iostore_frag_record_set **record_sets = NULL;
	oph_iostore_frag_record_set **orig_record_sets = NULL;
	orig_record_sets = (oph_iostore_frag_record_set **) calloc((table_list_num + 1), sizeof(oph_iostore_frag_record_set *));
	if (!orig_record_sets) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(in_frag_names);
		free(in_db_names);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	oph_metadb_frag_row *frag = NULL;
	oph_metadb_db_row *db_row = NULL;

	//LOCK FROM HERE
	if (pthread_rwlock_rdlock(&rwlock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_LOCK_ERROR);
		free(in_frag_names);
		free(in_db_names);
		free(orig_record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if (create_flag == 1) {
		//Retrieve current db
		if (oph_metadb_find_db(*meta_db, out_db_name, dev_handle->device, &db_row) || db_row == NULL) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_METADB_ERROR;
		}
		//Check if Frag already exists
		if (oph_metadb_find_frag(db_row, out_frag_name, &frag)) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_METADB_ERROR;
		}
		if (frag != NULL) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	for (l = 0; l < table_list_num; l++) {

		frag = NULL;
		db_row = NULL;

		if (file_load_flag) {
			//If file keyword is found then skip the table load procedure
			if (file_pos == l)
				continue;
		}
		//Retrieve current db
		if (oph_metadb_find_db(*meta_db, in_db_names[l], dev_handle->device, &db_row) || db_row == NULL) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_METADB_ERROR;
		}
		//Check if Frag exists
		if (oph_metadb_find_frag(db_row, in_frag_names[l], &frag)) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_METADB_ERROR;
		}
		//TODO Lock table while working with it

		if (frag == NULL) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//Call API to read Frag
		if (oph_iostore_get_frag(dev_handle, &(frag->frag_id), &(orig_record_sets[l])) != 0) {
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_API_ERROR;
		}
	}
	free(in_db_names);
	free(in_frag_names);

	//UNLOCK FROM HERE
	if (pthread_rwlock_unlock(&rwlock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if (file_load_flag) {
		unsigned long long frag_size = 0;
		//If file keyword is provided, load data from file
		if (_oph_io_server_query_load_from_file(meta_db, dev_handle, current_db, query_args, &(orig_record_sets[file_pos]), &frag_size)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read data from NetCDF file\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read data from NetCDF file\n");
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//TODO create specific functions to better manage temporary tables
		//Make the table temporary
		orig_record_sets[file_pos]->tmp_flag = 1;
	}
	//Build portion of fragments used in selection

	//Count number of rows to compute
	long long j = 0, total_row_number = 0;

	//Prepare input record set
	record_sets = (oph_iostore_frag_record_set **) calloc((table_list_num + 1), sizeof(oph_iostore_frag_record_set *));
	if (!record_sets) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	char **alias_list = NULL;
	int alias_num = 0;
	char *from_aliases = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
	if (from_aliases == NULL && table_list_num > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if (from_aliases != NULL) {
		if (oph_query_parse_multivalue_arg(from_aliases, &alias_list, &alias_num) || !alias_num || alias_num != table_list_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	long long partial_tot_row_number = 0;
	for (l = 0; l < table_list_num; l++) {

		//Take the biggest row number as reference 
		partial_tot_row_number = 0;
		while (orig_record_sets[l]->record_set[partial_tot_row_number])
			partial_tot_row_number++;
		if (partial_tot_row_number > total_row_number)
			total_row_number = partial_tot_row_number;

		if ((oph_iostore_copy_frag_record_set_only(orig_record_sets[l], &(record_sets[l]), 0, 0) != 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if (alias_list)
				free(alias_list);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		if (!alias_list)
			record_sets[l]->frag_name = (char *) strndup(orig_record_sets[l]->frag_name, strlen(orig_record_sets[l]->frag_name));
		else
			record_sets[l]->frag_name = (char *) strndup(alias_list[l], strlen(alias_list[l]));
		if (record_sets[l]->frag_name == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if (alias_list)
				free(alias_list);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}
	if (alias_list)
		free(alias_list);

	// Check where clause
	char *where = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_WHERE);
	if (table_list_num == 1 || file_load_flag != 0) {
		if (where) {
			//Apply where condition
			if (_oph_ioserver_query_run_where_clause(where, args, table_list_num, orig_record_sets, &total_row_number, record_sets)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		} else {
			//Get all rows
			for (j = 0; j < total_row_number; j++) {
				record_sets[0]->record_set[j] = orig_record_sets[0]->record_set[j];
			}
		}
	} else {
		if (where) {
			//Apply where condition
			if (_oph_ioserver_query_run_where_clause(where, args, table_list_num, orig_record_sets, &total_row_number, record_sets)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		} else {
			//There should be a where clause in case of multitable query
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	//Update output argument with actual value
	*stored_rs = orig_record_sets;
	*input_row_num = total_row_number;
	*input_rs = record_sets;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_build_input_record_set_create(HASHTBL * query_args, oph_query_arg ** args, oph_metadb_db_row ** meta_db, oph_iostore_handler * dev_handle, char *out_db_name,
						      char *out_frag_name, char *current_db, oph_iostore_frag_record_set *** stored_rs, long long *input_row_num,
						      oph_iostore_frag_record_set *** input_rs, char file_load_flag)
{
	return _oph_ioserver_query_build_input_record_set(query_args, args, meta_db, dev_handle, current_db, stored_rs, input_row_num, input_rs, out_db_name, out_frag_name, file_load_flag);
}

int _oph_ioserver_query_build_input_record_set_select(HASHTBL * query_args, oph_query_arg ** args, oph_metadb_db_row ** meta_db, oph_iostore_handler * dev_handle, char *current_db,
						      oph_iostore_frag_record_set *** stored_rs, long long *input_row_num, oph_iostore_frag_record_set *** input_rs)
{
	return _oph_ioserver_query_build_input_record_set(query_args, args, meta_db, dev_handle, current_db, stored_rs, input_row_num, input_rs, NULL, NULL, 0);
}

int _oph_ioserver_query_build_select_columns(HASHTBL * query_args, char **field_list, int field_list_num, long long offset, long long total_row_number, oph_query_arg ** args,
					     oph_iostore_frag_record_set ** inputs, oph_iostore_frag_record_set * output)
{
	if (!query_args || !field_list || !field_list_num || !total_row_number || !inputs || !output) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int i = 0, k = 0;
	long long j = 0, l = 0;
	unsigned long long id = 0;
	int var_count = 0;
	char **var_list = NULL;

	oph_query_field_types field_type[field_list_num];

	//Check binary fields if available
	unsigned int arg_count = 0;
	if (args != NULL) {
		while (args[i++])
			arg_count++;
	}
	//Temp variables used to assign values to result set
	double val_d = 0;
	unsigned long long val_l = 0;

	//Used for internal parser
	oph_query_expr_node *e = NULL;
	oph_query_expr_symtable *table = NULL;
	oph_query_expr_value *res = NULL;
	unsigned int binary_index = 0;

	long long actual_rows = 0, rows = 0;

	//Count number of tables
	i = 0;
	int table_num = 0;
	while (inputs[i++])
		table_num++;

	//Check if sequential ID are used
	char sequential_id = 0;
	long long start_id = 0;
	char *sid = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_SEQUENTIAL);
	if (sid != NULL) {
		sequential_id = 1;
		char *end = NULL;
		start_id = strtoll(sid, &end, 10);
		if ((errno != 0) || (end == sid) || (*end != 0)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_SEQUENTIAL);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, OPH_QUERY_ENGINE_LANG_ARG_SEQUENTIAL);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	//Check column type for each selection field
	for (i = 0; i < field_list_num; ++i) {
		//Check for field type
		if (oph_query_field_type(field_list[i], &(field_type[i]))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, field_list[i]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, field_list[i]);
			return OPH_IO_SERVER_PARSE_ERROR;
		}
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Column %s is of type %d\n", field_list[i], field_type[i]);
	}

	//Check group by
	oph_ioserver_group_elem_list **group_lists = NULL;
	if (_oph_ioserver_query_get_groups(query_args, total_row_number, args, inputs, table_num, &actual_rows, &group_lists)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_GROUP_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_GROUP_ERROR);
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	for (i = 0; i < field_list_num; ++i) {
		switch (field_type[i]) {
			case OPH_QUERY_FIELD_TYPE_UNKNOWN:
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
					if (group_lists) {
						for (k = 0; k < actual_rows; k++)
							if (group_lists[k])
								_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
						free(group_lists);
					}
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			case OPH_QUERY_FIELD_TYPE_DOUBLE:
				{
					val_d = strtod((char *) (field_list[i]), NULL);
					//Simply copy the value on each row
					rows = (actual_rows ? actual_rows : total_row_number);
					for (j = 0; j < rows; j++) {
						if (memory_check()) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							if (group_lists) {
								for (k = 0; k < actual_rows; k++)
									if (group_lists[k])
										_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
								free(group_lists);
							}
							return OPH_IO_SERVER_MEMORY_ERROR;
						}

						output->record_set[j]->field[i] = (void *) memdup((const void *) &val_d, sizeof(double));
						output->record_set[j]->field_length[i] = sizeof(double);
					}
					output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
					break;
				}
			case OPH_QUERY_FIELD_TYPE_LONG:
				{
					val_l = strtoll((char *) (field_list[i]), NULL, 10);
					//Simply copy the value on each row
					rows = (actual_rows ? actual_rows : total_row_number);
					for (j = 0; j < rows; j++) {
						if (memory_check()) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							if (group_lists) {
								for (k = 0; k < actual_rows; k++)
									if (group_lists[k])
										_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
								free(group_lists);
							}
							return OPH_IO_SERVER_MEMORY_ERROR;
						}

						output->record_set[j]->field[i] = (void *) memdup((const void *) &val_l, sizeof(unsigned long long));
						output->record_set[j]->field_length[i] = sizeof(unsigned long long);
					}
					output->field_type[i] = OPH_IOSTORE_LONG_TYPE;
					break;
				}
			case OPH_QUERY_FIELD_TYPE_STRING:
				{
					//Simply copy the value on each row
					rows = (actual_rows ? actual_rows : total_row_number);
					for (j = 0; j < rows; j++) {
						if (memory_check()) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							if (group_lists) {
								for (k = 0; k < actual_rows; k++)
									if (group_lists[k])
										_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
								free(group_lists);
							}
							return OPH_IO_SERVER_MEMORY_ERROR;
						}

						output->record_set[j]->field[i] = (void *) memdup((const void *) (field_list[i]), strlen(field_list[i]) + 1);
						output->record_set[j]->field_length[i] = strlen(field_list[i]) + 1;
					}
					output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
					break;
				}
			case OPH_QUERY_FIELD_TYPE_BINARY:
				{
					binary_index = strtoll((char *) (field_list[i] + 1), NULL, 10) - 1;
					if (binary_index >= arg_count) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_PARSE_ERROR;
					}
					//Simply copy the value on each row
					rows = (actual_rows ? actual_rows : total_row_number);
					for (j = 0; j < rows; j++) {
						if (memory_check()) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
							if (group_lists) {
								for (k = 0; k < actual_rows; k++)
									if (group_lists[k])
										_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
								free(group_lists);
							}
							return OPH_IO_SERVER_MEMORY_ERROR;
						}

						output->record_set[j]->field[i] = (void *) memdup((const void *) (args[binary_index]->arg), args[binary_index]->arg_length);
						output->record_set[j]->field_length[i] = args[binary_index]->arg_length;
					}
					switch (args[binary_index]->arg_type) {
						case OPH_QUERY_TYPE_LONG:
							output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
							break;
						case OPH_QUERY_TYPE_DOUBLE:
							output->field_type[i] = OPH_IOSTORE_LONG_TYPE;
							break;
						case OPH_QUERY_TYPE_NULL:
						case OPH_QUERY_TYPE_VARCHAR:
						case OPH_QUERY_TYPE_BLOB:
							output->field_type[i] = OPH_IOSTORE_STRING_TYPE;
							break;
					}
					break;
				}
			case OPH_QUERY_FIELD_TYPE_VARIABLE:
				{
					//Get var from input table
					int field_index = 0;
					int frag_index = 0;
					char use_seq_id = 0;

					//Split frag name from field name
					char **field_components = NULL;
					int field_components_num = 0;

					if (oph_query_parse_hierarchical_args(field_list[i], &field_components, &field_components_num)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_PARSE_ERROR;
					}

					if (((field_components_num > 2 || field_components_num < 1) && table_num == 1) || (field_components_num != 2 && table_num > 1)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						free(field_components);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_PARSE_ERROR;
					}
					//Match table
					for (l = 0; l < table_num; l++) {
						if (field_components_num == 1 || !STRCMP(field_components[0], inputs[l]->frag_name)) {
							frag_index = l;
							for (j = 0; j < inputs[l]->field_num; j++) {
								if (!STRCMP((field_components_num == 1 ? field_list[i] : field_components[1]), inputs[l]->field_name[j])) {
									field_index = j;
									if (sequential_id && !STRCMP(OPH_NAME_ID, inputs[l]->field_name[j])) {
										//If sequential id  flag is set check if field is called id_dim
										use_seq_id = 1;
									}
									break;
								}
							}
							if (j == inputs[l]->field_num) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
								free(field_components);
								if (group_lists) {
									for (k = 0; k < actual_rows; k++)
										if (group_lists[k])
											_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
									free(group_lists);
								}
								return OPH_IO_SERVER_PARSE_ERROR;
							}
							break;
						}
					}
					if (l == table_num) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						free(field_components);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_PARSE_ERROR;
					}
					free(field_components);

					rows = (actual_rows ? actual_rows : total_row_number);
					if (!use_seq_id) {
						if (!group_lists) {
							id = offset;
							for (j = 0; j < rows; j++, id++) {
								if (memory_check()) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									if (group_lists) {
										for (k = 0; k < actual_rows; k++)
											if (group_lists[k])
												_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
										free(group_lists);
									}
									return OPH_IO_SERVER_MEMORY_ERROR;
								}
								output->record_set[j]->field[i] =
								    inputs[frag_index]->record_set[id]->field_length[field_index] ?
								    memdup(inputs[frag_index]->record_set[id]->field[field_index],
									   inputs[frag_index]->record_set[id]->field_length[field_index]) : NULL;
								output->record_set[j]->field_length[i] = inputs[frag_index]->record_set[id]->field_length[field_index];
							}
						} else {
							//Aggregation is used, no offset allowed
							for (j = 0; j < rows; j++) {
								if (memory_check()) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									if (group_lists) {
										for (k = 0; k < actual_rows; k++)
											if (group_lists[k])
												_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
										free(group_lists);
									}
									return OPH_IO_SERVER_MEMORY_ERROR;
								}
								output->record_set[j]->field[i] =
								    inputs[frag_index]->record_set[group_lists[j]->first->elem_index]->field_length[field_index] ?
								    memdup(inputs[frag_index]->record_set[group_lists[j]->first->elem_index]->field[field_index],
									   inputs[frag_index]->record_set[group_lists[j]->first->elem_index]->field_length[field_index]) : NULL;
								output->record_set[j]->field_length[i] = inputs[frag_index]->record_set[group_lists[j]->first->elem_index]->field_length[field_index];
							}
						}
					} else {
						//Use sequential IDs instead
						for (j = 0; j < rows; j++) {
							if (memory_check()) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
								if (group_lists) {
									for (k = 0; k < actual_rows; k++)
										if (group_lists[k])
											_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
									free(group_lists);
								}
								return OPH_IO_SERVER_MEMORY_ERROR;
							}
							val_l = start_id + j;
							output->record_set[j]->field[i] = memdup(&val_l, sizeof(unsigned long long));
							output->record_set[j]->field_length[i] = sizeof(unsigned long long);
						}
					}
					output->field_type[i] = inputs[frag_index]->field_type[field_index];
					break;
				}
			case OPH_QUERY_FIELD_TYPE_FUNCTION:
				{
					//Reset values
					var_list = NULL;
					var_count = 0;
					e = NULL;
					table = NULL;
					res = NULL;

					if (oph_query_expr_create_symtable(&table, OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					if (oph_query_expr_get_ast(field_list[i], &e) != 0) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						oph_query_expr_destroy_symtable(table);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_EXEC_ERROR;
					}
					//Read all variables and link them to input record set fields
					if (oph_query_expr_get_variables(e, &var_list, &var_count)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					unsigned int field_indexes[var_count];
					int frag_indexes[var_count];
					//Match binary with 1
					char field_binary[var_count];
					binary_index = 0;

					//TODO Count actual number of string/binary variables
					oph_query_arg val_b[var_count];


					if (var_count > 0) {
						if (_oph_ioserver_query_get_variable_indexes(arg_count, var_list, var_count, inputs, table_num, field_indexes, frag_indexes, field_binary, 0, NULL)) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, field_list[i]);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_VARIABLE_MATCH_ERROR, field_list[i]);
							oph_query_expr_delete_node(e, table);
							oph_query_expr_destroy_symtable(table);
							free(var_list);
							if (group_lists) {
								for (k = 0; k < actual_rows; k++)
									if (group_lists[k])
										_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
								free(group_lists);
							}
							return OPH_IO_SERVER_EXEC_ERROR;
						}
					}

					long long function_row_number = 0;
					if (!group_lists) {
						//No group by provided  
						char is_aggregate = 0;
						id = offset;

						for (j = 0; j < total_row_number; j++, id++) {
							if (memory_check()) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
								oph_query_expr_delete_node(e, table);
								oph_query_expr_destroy_symtable(table);
								free(var_list);
								return OPH_IO_SERVER_MEMORY_ERROR;
							}

							if (var_count > 0) {
								if (_oph_ioserver_query_set_parser_variables
								    (args, var_list, var_count, inputs, table, field_indexes, frag_indexes, field_binary, val_b, field_list[i], id, NULL)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									return OPH_IO_SERVER_PARSE_ERROR;
								}
							}
							//IF last row of aggregating function   
							if ((total_row_number == 1) || ((j == (total_row_number - 1)) && (is_aggregate == 1))) {
								if (oph_query_expr_change_group(e)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									return OPH_IO_SERVER_PARSE_ERROR;
								}
							}

							if (e != NULL && !oph_query_expr_eval_expression(e, &res, table)) {
								if (res->jump_flag == 0) {
									switch (res->type) {
										case OPH_QUERY_EXPR_TYPE_DOUBLE:
											{
												if (!function_row_number)
													output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
												output->record_set[function_row_number]->field[i] =
												    (void *) memdup((const void *) &(res->data.double_value), sizeof(double));
												output->record_set[function_row_number]->field_length[i] = sizeof(double);
												free(res);
												break;
											}
										case OPH_QUERY_EXPR_TYPE_LONG:
											{
												if (!function_row_number)
													output->field_type[i] = OPH_IOSTORE_LONG_TYPE;
												output->record_set[function_row_number]->field[i] =
												    (void *) memdup((const void *) &(res->data.long_value), sizeof(unsigned long long));
												output->record_set[function_row_number]->field_length[i] = sizeof(unsigned long long);
												free(res);
												break;
											}
										case OPH_QUERY_EXPR_TYPE_STRING:
											{
												if (!function_row_number)
													output->field_type[i] = OPH_IOSTORE_STRING_TYPE;
#ifdef PLUGIN_RES_COPY
												output->record_set[function_row_number]->field[i] = (void *) res->data.string_value;
#else
												output->record_set[function_row_number]->field[i] =
												    (void *) memdup((const void *) res->data.string_value, strlen(res->data.string_value) + 1);
#endif
												output->record_set[function_row_number]->field_length[i] = strlen(res->data.string_value) + 1;
												free(res);
												break;
											}
										case OPH_QUERY_EXPR_TYPE_BINARY:
											{
												if (!function_row_number)
													output->field_type[i] = OPH_IOSTORE_STRING_TYPE;
#ifdef PLUGIN_RES_COPY
												output->record_set[function_row_number]->field[i] = (void *) res->data.binary_value->arg;
#else
												output->record_set[function_row_number]->field[i] =
												    (void *) memdup((const void *) res->data.binary_value->arg, res->data.binary_value->arg_length);
#endif
												output->record_set[function_row_number]->field_length[i] = res->data.binary_value->arg_length;
												free(res->data.binary_value);
												free(res);
												break;
											}
										default:
											{
												pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
												logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
												free(res);
												oph_query_expr_delete_node(e, table);
												oph_query_expr_destroy_symtable(table);
												free(var_list);
												return OPH_IO_SERVER_EXEC_ERROR;
											}
									}
									function_row_number++;
								} else {
									free(res);
									is_aggregate = 1;
								}
							} else {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
								oph_query_expr_delete_node(e, table);
								oph_query_expr_destroy_symtable(table);
								free(var_list);
								return OPH_IO_SERVER_PARSE_ERROR;
							}
						}

					} else {
						//Group by is provided, no offset allowed 
						oph_ioserver_group_elem *tmp = NULL;
						char jump_flag = 1;

						for (k = 0; k < actual_rows; k++) {
							//Loop on groups
							for (tmp = group_lists[k]->first, j = 0; tmp; tmp = tmp->next, j++) {
								jump_flag = 1;

								//Loop on rows                                          
								if (memory_check()) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for (k = 0; k < actual_rows; k++)
										if (group_lists[k])
											_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
									free(group_lists);
									return OPH_IO_SERVER_MEMORY_ERROR;
								}

								if (var_count > 0) {
									if (_oph_ioserver_query_set_parser_variables
									    (args, var_list, var_count, inputs, table, field_indexes, frag_indexes, field_binary, val_b, field_list[i], tmp->elem_index,
									     NULL)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
										oph_query_expr_delete_node(e, table);
										oph_query_expr_destroy_symtable(table);
										free(var_list);
										for (k = 0; k < actual_rows; k++)
											if (group_lists[k])
												_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
										free(group_lists);
										return OPH_IO_SERVER_PARSE_ERROR;
									}
								}
								//IF last row of group  
								if (tmp->next == NULL) {
									if (oph_query_expr_change_group(e)) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
										logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
										oph_query_expr_delete_node(e, table);
										oph_query_expr_destroy_symtable(table);
										free(var_list);
										for (k = 0; k < actual_rows; k++)
											if (group_lists[k])
												_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
										free(group_lists);
										return OPH_IO_SERVER_PARSE_ERROR;
									}
									//Unset internal jump flag for non-aggregating functions
									jump_flag = 0;
								}

								if (e != NULL && !oph_query_expr_eval_expression(e, &res, table)) {
									if (res->jump_flag == 0 && jump_flag == 0) {
										switch (res->type) {
											case OPH_QUERY_EXPR_TYPE_DOUBLE:
												{
													if (!function_row_number)
														output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
													output->record_set[function_row_number]->field[i] =
													    (void *) memdup((const void *) &(res->data.double_value), sizeof(double));
													output->record_set[function_row_number]->field_length[i] = sizeof(double);
													free(res);
													break;
												}
											case OPH_QUERY_EXPR_TYPE_LONG:
												{
													if (!function_row_number)
														output->field_type[i] = OPH_IOSTORE_LONG_TYPE;
													output->record_set[function_row_number]->field[i] =
													    (void *) memdup((const void *) &(res->data.long_value), sizeof(unsigned long long));
													output->record_set[function_row_number]->field_length[i] = sizeof(unsigned long long);
													free(res);
													break;
												}
											case OPH_QUERY_EXPR_TYPE_STRING:
												{
													if (!function_row_number)
														output->field_type[i] = OPH_IOSTORE_STRING_TYPE;
#ifdef PLUGIN_RES_COPY
													output->record_set[function_row_number]->field[i] = (void *) res->data.string_value;
#else
													output->record_set[function_row_number]->field[i] =
													    (void *) memdup((const void *) res->data.string_value, strlen(res->data.string_value) + 1);
#endif
													output->record_set[function_row_number]->field_length[i] = strlen(res->data.string_value) + 1;
													free(res);
													break;
												}
											case OPH_QUERY_EXPR_TYPE_BINARY:
												{
													if (!function_row_number)
														output->field_type[i] = OPH_IOSTORE_STRING_TYPE;
#ifdef PLUGIN_RES_COPY
													output->record_set[function_row_number]->field[i] = (void *) res->data.binary_value->arg;
#else
													output->record_set[function_row_number]->field[i] =
													    (void *) memdup((const void *) res->data.binary_value->arg,
															    res->data.binary_value->arg_length);
#endif
													output->record_set[function_row_number]->field_length[i] = res->data.binary_value->arg_length;
													free(res->data.binary_value);
													free(res);
													break;
												}
											default:
												{
													pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
													logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
													free(res);
													oph_query_expr_delete_node(e, table);
													oph_query_expr_destroy_symtable(table);
													free(var_list);
													for (k = 0; k < actual_rows; k++)
														if (group_lists[k])
															_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
													free(group_lists);
													return OPH_IO_SERVER_EXEC_ERROR;
												}
										}
										function_row_number++;
									} else {
										free(res);
									}
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for (k = 0; k < actual_rows; k++)
										if (group_lists[k])
											_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
									free(group_lists);
									return OPH_IO_SERVER_PARSE_ERROR;
								}
							}
						}
					}
					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					free(var_list);

					//Check row number
					if (function_row_number == 0 || (function_row_number != actual_rows && actual_rows != 0)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						if (group_lists) {
							for (k = 0; k < actual_rows; k++)
								if (group_lists[k])
									_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
							free(group_lists);
						}
						return OPH_IO_SERVER_PARSE_ERROR;
					}
					actual_rows = function_row_number;
				}
		}
	}

	if (group_lists) {
		for (k = 0; k < actual_rows; k++)
			if (group_lists[k])
				_oph_ioserver_query_delete_group_elem_list(group_lists[k]);
		free(group_lists);
	}

	actual_rows = (actual_rows ? actual_rows : total_row_number);
	if (actual_rows != total_row_number) {
		//Remove unnecessary rows
		for (j = actual_rows; j < total_row_number; j++) {
			oph_iostore_destroy_frag_record(&(output->record_set[j]), output->field_num);
		}
		//Realloc record set array
		oph_iostore_frag_record **tmp = (oph_iostore_frag_record **) realloc(output->record_set, (actual_rows + 1) * sizeof(oph_iostore_frag_record *));
		if (tmp == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Set last element of record to NULL
		tmp[actual_rows] = NULL;
		output->record_set = tmp;
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_set_column_info(HASHTBL * query_args, char **field_list, int field_list_num, oph_iostore_frag_record_set * rs)
{
	if (!query_args || !field_list || !field_list_num || !rs) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Get select alias, if available
	int i = 0;
	char **field_alias_list = NULL;
	int field_alias_list_num = 0;
	//Fields section
	char *fields_alias = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);
	if (fields_alias != NULL) {
		if (oph_query_parse_multivalue_arg(fields_alias, &field_alias_list, &field_alias_list_num) || !field_alias_list_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);
			if (field_alias_list)
				free(field_alias_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		if (field_alias_list_num != field_list_num) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_ALIAS_NOT_MATCH);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_ALIAS_NOT_MATCH);
			if (field_alias_list)
				free(field_alias_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	//Set alias or input table names
	if (field_alias_list != NULL) {
		for (i = 0; i < field_list_num; i++) {
			rs->field_name[i] = (strlen(field_alias_list[i]) == 0 ? strdup(field_list[i]) : strdup(field_alias_list[i]));
		}
	} else {
		for (i = 0; i < field_list_num; i++) {
			rs->field_name[i] = strdup(field_list[i]);
		}
	}
	free(field_alias_list);

	//Set default column types (will be updated later to correct value)
	for (i = 0; i < field_list_num - 1; i++) {
		rs->field_type[i] = OPH_IOSTORE_LONG_TYPE;
	}
	rs->field_type[i] = OPH_IOSTORE_STRING_TYPE;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_store_fragment(oph_metadb_db_row ** meta_db, oph_iostore_handler * dev_handle, char *current_db, unsigned long long frag_size, oph_iostore_frag_record_set ** final_result_set)
{
	if (!meta_db || !dev_handle || !current_db || !(*final_result_set)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Check current db
	oph_metadb_db_row *db_row = NULL;

	//LOCK FROM HERE
	if (pthread_rwlock_wrlock(&rwlock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_LOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Retrieve current db
	if (oph_metadb_find_db(*meta_db, current_db, dev_handle->device, &db_row) || db_row == NULL) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
		return OPH_IO_SERVER_METADB_ERROR;
	}
	//Call API to insert Frag
	oph_iostore_resource_id *frag_id = NULL;
	if (oph_iostore_put_frag(dev_handle, *final_result_set, &frag_id) != 0) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");
		return OPH_IO_SERVER_API_ERROR;
	}

	oph_metadb_frag_row *frag = NULL;

	//Add Frag to MetaDB
	if (oph_metadb_setup_frag_struct((*final_result_set)->frag_name, dev_handle->device, dev_handle->is_persistent, &(db_row->db_id), frag_id, frag_size, &frag)) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
		free(frag_id->id);
		free(frag);
		return OPH_IO_SERVER_METADB_ERROR;
	}

	free(frag_id->id);
	free(frag_id);
	frag_id = NULL;

	oph_metadb_db_row *tmp_db_row = NULL;
	if (oph_metadb_setup_db_struct(db_row->db_name, db_row->device, dev_handle->is_persistent, &(db_row->db_id), db_row->frag_number, &tmp_db_row)) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		oph_metadb_cleanup_frag_struct(frag);
		return OPH_IO_SERVER_METADB_ERROR;
	}
	if (oph_metadb_add_frag(db_row, frag)) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");
		oph_metadb_cleanup_frag_struct(frag);
		oph_metadb_cleanup_db_struct(tmp_db_row);
		return OPH_IO_SERVER_METADB_ERROR;
	}

	oph_metadb_cleanup_frag_struct(frag);

	tmp_db_row->frag_number++;
	if (oph_metadb_update_db(*meta_db, tmp_db_row)) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");
		oph_metadb_cleanup_db_struct(tmp_db_row);
		return OPH_IO_SERVER_METADB_ERROR;
	}
	//If device is transient then block record from being deleted
	if (!dev_handle->is_persistent)
		*final_result_set = NULL;

	//UNLOCK FROM HERE
	if (pthread_rwlock_unlock(&rwlock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		oph_metadb_cleanup_db_struct(tmp_db_row);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	oph_metadb_cleanup_db_struct(tmp_db_row);

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_build_row(unsigned int arg_count, unsigned long long *row_size, oph_iostore_frag_record_set * partial_result_set, char **field_list, char **value_list, oph_query_arg ** args,
				  oph_iostore_frag_record ** new_record)
{
	if (!arg_count || !row_size || !partial_result_set || !field_list || !value_list || !new_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Created record struct
	*new_record = NULL;
	if (oph_iostore_create_frag_record(new_record, partial_result_set->field_num) == 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	long long tmpL = 0;
	double tmpD = 0;
	int i = 0, k = 0;
	(*row_size) = sizeof(oph_iostore_frag_record);
	oph_query_field_types field_type = OPH_QUERY_FIELD_TYPE_UNKNOWN;

	int var_count = 0;
	char **var_list = NULL;

	//Used for internal parser
	oph_query_expr_node *e = NULL;
	oph_query_expr_symtable *table = NULL;
	oph_query_expr_value *res = NULL;
	unsigned int binary_index = 0;


	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	for (i = 0; i < partial_result_set->field_num; i++) {
		//For each field check column name correspondence
		if (STRCMP(field_list[i], partial_result_set->field_name[i]) == 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR);
			oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//Check for field type
		if (oph_query_field_type(value_list[i], &field_type)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, value_list[i]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, value_list[i]);
			oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		switch (field_type) {
			case OPH_QUERY_FIELD_TYPE_BINARY:
				{
					//For each value check if argument contains ? and substitute with arg[i]
					if (!args) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						return OPH_IO_SERVER_NULL_PARAM;
					}

					binary_index = strtoll((char *) (value_list[i] + 1), NULL, 10) - 1;
					if (binary_index >= arg_count) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, value_list[i]);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						return OPH_IO_SERVER_PARSE_ERROR;
					}

					(*new_record)->field_length[i] = args[binary_index]->arg_length;
					(*new_record)->field[i] = (void *) memdup(args[binary_index]->arg, (*new_record)->field_length[i]);
					break;
				}
				//No substitution occurs, use directly strings
			case OPH_QUERY_FIELD_TYPE_STRING:
				{
					(*new_record)->field_length[i] = strlen(value_list[i]) + 1;
					(*new_record)->field[i] = (char *) strndup(value_list[i], (*new_record)->field_length[i]);
					break;
				}
			case OPH_QUERY_FIELD_TYPE_DOUBLE:
				{
					tmpD = (double) strtod(value_list[i], NULL);
					(*new_record)->field_length[i] = sizeof(double);
					(*new_record)->field[i] = (void *) memdup((const void *) &tmpD, (*new_record)->field_length[i]);
					break;
				}
			case OPH_QUERY_FIELD_TYPE_LONG:
				{
					tmpL = (long long) strtoll(value_list[i], NULL, 10);
					(*new_record)->field_length[i] = sizeof(long long);
					(*new_record)->field[i] = (void *) memdup((const void *) &tmpL, (*new_record)->field_length[i]);
					break;
				}
			case OPH_QUERY_FIELD_TYPE_FUNCTION:
				{
					//Reset values
					var_list = NULL;
					var_count = 0;
					e = NULL;
					table = NULL;
					k = 0;
					res = NULL;

					if (oph_query_expr_create_symtable(&table, OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					if (oph_query_expr_get_ast(value_list[i], &e) != 0) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						oph_query_expr_destroy_symtable(table);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						return OPH_IO_SERVER_EXEC_ERROR;
					}
					//Read all variables and link them to input record set fields
					if (oph_query_expr_get_variables(e, &var_list, &var_count)) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
						oph_query_expr_delete_node(e, table);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						oph_query_expr_destroy_symtable(table);
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					binary_index = 0;

					if (var_count > 0) {
						for (k = 0; k < var_count; k++) {
							//Match binary values
							if (var_list[k][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE) {
								binary_index = strtoll((char *) (var_list[k] + 1), NULL, 10) - 1;
								if (binary_index >= arg_count || oph_query_expr_add_binary(var_list[k], args[binary_index], table)) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
									return OPH_IO_SERVER_EXEC_ERROR;
								}
							}
							//No field names can be used
							else {
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);
								oph_query_expr_delete_node(e, table);
								oph_query_expr_destroy_symtable(table);
								free(var_list);
								oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
								return OPH_IO_SERVER_PARSE_ERROR;
							}
						}
					}

					if (e != NULL && !oph_query_expr_eval_expression(e, &res, table) && (res->jump_flag == 0)) {
						switch (res->type) {
							case OPH_QUERY_EXPR_TYPE_DOUBLE:
								{
									(*new_record)->field_length[i] = sizeof(double);
									(*new_record)->field[i] = (void *) memdup((const void *) &(res->data.double_value), sizeof(double));
									free(res);
									break;
								}
							case OPH_QUERY_EXPR_TYPE_LONG:
								{
									(*new_record)->field_length[i] = sizeof(unsigned long long);
									(*new_record)->field[i] = (void *) memdup((const void *) &(res->data.long_value), sizeof(unsigned long long));
									free(res);
									break;
								}
							case OPH_QUERY_EXPR_TYPE_STRING:
								{
									(*new_record)->field_length[i] = strlen(res->data.string_value) + 1;
#ifdef PLUGIN_RES_COPY
									(*new_record)->field[i] = (void *) res->data.string_value;
#else
									(*new_record)->field[i] = (void *) memdup((const void *) res->data.string_value, strlen(res->data.string_value) + 1);
#endif
									free(res);
									break;
								}
							case OPH_QUERY_EXPR_TYPE_BINARY:
								{
									(*new_record)->field_length[i] = res->data.binary_value->arg_length;
#ifdef PLUGIN_RES_COPY
									(*new_record)->field[i] = (void *) res->data.binary_value->arg;
#else
									(*new_record)->field[i] = (void *) memdup((const void *) res->data.binary_value->arg, res->data.binary_value->arg_length);
#endif
									free(res->data.binary_value);
									free(res);
									break;
								}
							default:
								{
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
									free(res);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
									free(var_list);
									return OPH_IO_SERVER_EXEC_ERROR;
								}
						}
					} else {
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						return OPH_IO_SERVER_PARSE_ERROR;
					}

					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					free(var_list);
					break;
				}
			case OPH_QUERY_FIELD_TYPE_VARIABLE:
			case OPH_QUERY_FIELD_TYPE_UNKNOWN:
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR, value_list[i]);
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					return OPH_IO_SERVER_PARSE_ERROR;
				}
		}

		(*row_size) += ((*new_record)->field_length[i] + sizeof((*new_record)->field_length[i]) + sizeof((*new_record)->field[i]));
		if ((*new_record)->field[i] == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}
