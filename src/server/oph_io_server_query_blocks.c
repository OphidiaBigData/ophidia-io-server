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

#define _GNU_SOURCE

#include "oph_io_server_interface.h"
#include "oph_query_engine_language.h"
#include "oph_query_engine.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>

#include <ctype.h>
#include <unistd.h>

#include <debug.h>

#include <errno.h>
#include <pthread.h>

#include "oph_server_utility.h"

#include "oph_iostorage_interface.h"
#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_functions.h"

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern pthread_rwlock_t rwlock;
extern HASHTBL *plugin_table;

int _oph_io_server_query_compute_limits(HASHTBL *query_args, long long *offset, long long *limit)
{
	if (!query_args || !offset || !limit){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*limit=0;
	*offset=0;

	char *limits = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
	if(limits)
  {
		char **limit_list = NULL;
		int limit_list_num = 0;
		if(oph_query_parse_multivalue_arg (limits, &limit_list, &limit_list_num))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT); 
			if(limit_list) free(limit_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if(limit_list)
		{
			switch (limit_list_num)
			{
				case 1:
					*limit = strtoll(limit_list[0],NULL,10);
					break;
				case 2:
					*offset = strtoll(limit_list[0],NULL,10);
					*limit = strtoll(limit_list[1],NULL,10);
					break;
				default:
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT); 
					free(limit_list);
					return OPH_IO_SERVER_EXEC_ERROR;
			}
			free(limit_list);
			if ((*limit) < 0) *limit = 0;
			if ((*offset)< 0) *offset = 0;
		}
	}

  return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_release_input_record_set(oph_iostore_handler* dev_handle, oph_iostore_frag_record_set **stored_rs, oph_iostore_frag_record_set **input_rs){
	if (!dev_handle){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int l = 0;
	if(stored_rs){
		for(l = 0; stored_rs[l]; l++){
			if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&(stored_rs[l]));
		}
		free(stored_rs);
	}
	if(input_rs){	
		for(l = 0; input_rs[l]; l++){
			oph_iostore_destroy_frag_recordset_only(&(input_rs[l]));
		}
		free(input_rs);
	}
	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_multi_table_where_assert(int table_num, short int *id_indexes, long long *start_row_indexes, long long *input_row_num, oph_iostore_frag_record_set **in_record_set)
{
	if (!table_num || !id_indexes || !start_row_indexes || !input_row_num || !in_record_set){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Assume to have only inner join where clauses with unique and sorted ids 
	short int id_error_flag = 0;
	long long a,b,j;
	long long table_min[table_num];
	long long table_max[table_num];
	int l;

	for(l = 0; l < table_num; l++){
		table_min[l] = *((long long *)in_record_set[l]->record_set[0]->field[id_indexes[l]]);
		for(j = 1; in_record_set[l]->record_set[j]; j++){
			//Verify order, uniqueness and no values missing
			b = *((long long *)in_record_set[l]->record_set[j]->field[id_indexes[l]]);
			a = *((long long *)in_record_set[l]->record_set[j-1]->field[id_indexes[l]]);
			if( (b <= a) || (b-a) != 1){
				id_error_flag = 1;
				break;
			}
		}	
		if(id_error_flag == 1){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, in_record_set[l]->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, in_record_set[l]->frag_name);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		table_max[l] = *((long long *)in_record_set[l]->record_set[j-1]->field[id_indexes[l]]);
	}

	//Get min and max idvalues
	long long tmp_min = table_min[l-1], tmp_max = table_max[l-1];
	for(l = 1; l < table_num; l++){
		if(table_min[l] < table_min[l-1]) tmp_min = table_min[l];
		if(table_max[l] > table_max[l-1]) tmp_max = table_max[l];
	}

	//Check table overlap and return empty set in case
	for(l = 0; l < table_num; l++){
		if((table_min[l] > tmp_max) || (table_max[l] < tmp_min)){
			//Update output argument with actual value
			*input_row_num = 0;
			return OPH_IO_SERVER_SUCCESS;
		}
	}

	//Find index of minimum value in each table
	for(l = 0; l < table_num; l++){
		for(j = 0; in_record_set[l]->record_set[j]; j++){
			a = *((long long *)in_record_set[l]->record_set[j]->field[id_indexes[l]]);
			if(a == tmp_min){
				start_row_indexes[l] = j;
				break;
			}
		}	
	}

	*input_row_num = tmp_max - tmp_min + 1;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_run_where_clause(char *where_string, int table_num, oph_iostore_frag_record_set **stored_rs, long long *input_row_num, oph_iostore_frag_record_set **input_rs)
{
	if (!where_string || !table_num || !stored_rs || !input_row_num || !input_rs ){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int k, l, i;
	long long j;

	//Find id columns in each table
	short int id_indexes[table_num];
	for(l = 0; l < table_num; l++){
		for(i = 0; i < stored_rs[l]->field_num; i++){
			//TODO Check what fields are selected
			if (!STRCMP(stored_rs[l]->field_name[i],OPH_NAME_ID)){
				id_indexes[l] = i;
				break;
			}
		}
		//Id not found	
		if(i == stored_rs[l]->field_num){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN,OPH_NAME_ID);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN,OPH_NAME_ID);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	long long start_row_indexes[table_num];

	if(table_num > 1){
		if(_oph_ioserver_query_multi_table_where_assert(table_num, id_indexes, start_row_indexes, input_row_num, stored_rs))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, stored_rs[l]->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ID_MULTITABLE_CONSTRAINT_ERROR, stored_rs[l]->frag_name);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}
	else{
		start_row_indexes[0] = 0;
	}
	//No rows found	simply return empty set
	if((*input_row_num) == 0) 	return OPH_IO_SERVER_SUCCESS;

	oph_query_expr_node *e = NULL; 

	if(oph_query_expr_get_ast(where_string, &e) != 0){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	oph_query_expr_symtable *table;
	if(oph_query_expr_create_symtable(&table, OPH_IO_SERVER_MAX_PLUGIN_NUMBER)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		oph_query_expr_delete_node(e, table);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	int var_count = 0;
	char **var_list = NULL;

	//Read all variables and link them to input record set fields
	if(oph_query_expr_get_variables(e, &var_list, &var_count)){
		oph_query_expr_delete_node(e, table);
		oph_query_expr_destroy_symtable(table);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where_string);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where_string);	
		return OPH_IO_SERVER_EXEC_ERROR;     
	}

	int field_indexes[var_count];
	int frag_indexes[var_count];
	char **field_components = NULL;
	int field_components_num = 0;

	char *tmp_var_list[var_count];
	for(k = 0; k < var_count; k++){
		tmp_var_list[k] = (char *)strndup(var_list[k], strlen(var_list[k])+1);
		if(tmp_var_list[k] == NULL)
		{
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	if(var_count > 0){
		for(k = 0; k < var_count; k++){
			if(tmp_var_list[k][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE){
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);	
				return OPH_IO_SERVER_PARSE_ERROR;     
			}
			//Match field names
			else{

				if(table_num > 1){
					//Split frag name from field name
					field_components = NULL;
					field_components_num = 0;

					if(oph_query_parse_hierarchical_args (tmp_var_list[k], &field_components, &field_components_num)){
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);	
						return OPH_IO_SERVER_PARSE_ERROR;        
					}

					if(field_components_num != 2){
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						free(field_components);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);	
						return OPH_IO_SERVER_PARSE_ERROR;
					}

					//Check if we only have ids, any other field is not allowed
					if (STRCMP(field_components[1],OPH_NAME_ID)){
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						free(field_components);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					//Match table
					for(l = 0; l < table_num; l++){
						if(!STRCMP(field_components[0], input_rs[l]->frag_name)){
							field_indexes[k] = id_indexes[l];
							frag_indexes[k] = l;
							break;
						}
					}	
					if(l == table_num){				
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						free(field_components);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_components[0]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_components[0]);	
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
					free(field_components);
				}
				else{
					//Check if we only have ids, any other field is not allowed
					if (STRCMP(tmp_var_list[k],OPH_NAME_ID)){
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ONLY_ID_ERROR);
						return OPH_IO_SERVER_EXEC_ERROR;
					}

					//Match field
					field_indexes[k] = id_indexes[0];
					frag_indexes[k] = 0;
				}
			}
		}
	}
	else {				
		if(table_num > 1){
			oph_query_expr_delete_node(e, table);
			oph_query_expr_destroy_symtable(table);
			free(var_list);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_WHERE_MULTITABLE);
			return OPH_IO_SERVER_PARSE_ERROR; 
		}    
	}
	for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);

	oph_query_expr_value* res = NULL;

	//Temp variables used to assign values to result set
	double val_d = 0;
	unsigned long long val_l = 0;
	//TODO Count actual number of string/binary variables
	oph_query_arg val_b[var_count];
	long long curr_row = 0;

	for (j = 0; j < (*input_row_num); j++)
	{
		for(k = 0; k < var_count; k++){
			switch(input_rs[frag_indexes[k]]->field_type[field_indexes[k]]){
				case OPH_IOSTORE_LONG_TYPE:
				{
					val_l = *((long long *)stored_rs[frag_indexes[k]]->record_set[start_row_indexes[frag_indexes[k]] + j]->field[field_indexes[k]]);
					if(oph_query_expr_add_long(var_list[k],val_l,table)){ 
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						return OPH_IO_SERVER_EXEC_ERROR;     
					} 
					break;
				}
				case OPH_IOSTORE_REAL_TYPE:
				{
					val_d = *((double *)stored_rs[frag_indexes[k]]->record_set[start_row_indexes[frag_indexes[k]] + j]->field[field_indexes[k]]);
					if(oph_query_expr_add_double(var_list[k],val_d,table)){ 
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						return OPH_IO_SERVER_EXEC_ERROR;     
					}  
					break;
				}
				//TODO Check if string and binary can be treated separately
				case OPH_IOSTORE_STRING_TYPE:
				{
					val_b[k].arg = stored_rs[frag_indexes[k]]->record_set[start_row_indexes[frag_indexes[k]] + j]->field[field_indexes[k]];
					val_b[k].arg_length = stored_rs[frag_indexes[k]]->record_set[start_row_indexes[frag_indexes[k]] + j]->field_length[field_indexes[k]];								
					if(oph_query_expr_add_binary(var_list[k],&(val_b[k]),table)){ 
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
						return OPH_IO_SERVER_EXEC_ERROR;     
					}  
					break;
				}
			}	
		}

		if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) {
			//Create index record
			long long result = 0;
			if(res->type == OPH_QUERY_EXPR_TYPE_DOUBLE){
				result = (long long) res->data.double_value;
				free(res);
			}else if(res->type == OPH_QUERY_EXPR_TYPE_LONG){
			  result = res->data.long_value;
			  free(res);
			}else {
			  free(res);
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
			  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where_string);
			  return OPH_IO_SERVER_PARSE_ERROR;
			}

			//Add result to each index table
			if(result){
				for(l = 0; l < table_num; l++){
					input_rs[l]->record_set[curr_row] = stored_rs[l]->record_set[start_row_indexes[l] + j];  
				}
				curr_row++;
			}
		}
		else
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where_string);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where_string);
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

int _oph_ioserver_query_build_input_record_set(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set ***stored_rs, long long *input_row_num, oph_iostore_frag_record_set ***input_rs, char *out_db_name, char *out_frag_name)
{
	if (!dev_handle || !query_args || !stored_rs || !input_row_num || !input_rs || !meta_db || !thread_status){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*stored_rs = NULL;
	*input_row_num = 0;
	*input_rs = NULL;

	short int create_flag = 0;
	if(out_db_name != NULL && out_frag_name != NULL) create_flag = 1;

	//Extract frag_name arg from query args
	char *from_frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM);
	if(from_frag_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	char **table_list = NULL;
	int table_list_num = 0;    
	if(oph_query_parse_multivalue_arg (from_frag_name, &table_list, &table_list_num) || !table_list_num){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		if(table_list) free(table_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Build list of input db and frag names 
	char **in_frag_names = NULL;
	char **in_db_names = NULL;
	in_frag_names = (char**)malloc(table_list_num*sizeof(char*));
	in_db_names = (char**)malloc(table_list_num*sizeof(char*));
	if(!in_frag_names || !in_db_names)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if(table_list) free(table_list);
		if(in_frag_names) free(in_frag_names);
		if(in_db_names) free(in_db_names);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	
	int l = 0;
	char **from_components = NULL;
	int from_components_num = 0;
	if(table_list_num == 1){
		in_frag_names[0] = from_frag_name;
		in_db_names[0] = thread_status->current_db;
	}
	else{
		//From multiple table
		for(l = 0; l < table_list_num; l++){

			if(oph_query_parse_hierarchical_args (table_list[l], &from_components, &from_components_num)){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);	
				free(table_list);
				free(in_frag_names);
				free(in_db_names);
				return OPH_IO_SERVER_PARSE_ERROR;        
			}
			//If DB is setted in frag name
			if(from_components_num != 2){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, table_list[l]);	
				free(table_list);
				free(in_frag_names);
				free(in_db_names);
				free(from_components);
				return OPH_IO_SERVER_PARSE_ERROR;        
			}

			in_frag_names[l] = from_components[1];
			in_db_names[l] = from_components[0];
			free(from_components);	
		}
	}
	free(table_list);

	oph_iostore_frag_record_set **record_sets = NULL;
	oph_iostore_frag_record_set **orig_record_sets = NULL;
	orig_record_sets = (oph_iostore_frag_record_set**)calloc((table_list_num+1),sizeof(oph_iostore_frag_record_set*));
	if(!orig_record_sets)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(in_frag_names);
		free(in_db_names);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	oph_metadb_frag_row *frag = NULL;
	oph_metadb_db_row *db_row = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_rdlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		free(in_frag_names);
		free(in_db_names);
		free(orig_record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	if(create_flag == 1)
	{
		//Retrieve current db
		if(oph_metadb_find_db (*meta_db, out_db_name, thread_status->device, &db_row) ||  db_row == NULL){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		//Check if Frag already exists
		if(oph_metadb_find_frag (db_row, out_frag_name, &frag)){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		if(frag != NULL){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);	
			free(in_frag_names);
			free(in_db_names);
			free(orig_record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}
	}

	for(l = 0; l < table_list_num; l++){

		frag = NULL;
		db_row = NULL;

		//Retrieve current db
		if(oph_metadb_find_db (*meta_db, in_db_names[l], thread_status->device, &db_row) ||  db_row == NULL){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_METADB_ERROR;             
		}

		//Check if Frag exists
		if(oph_metadb_find_frag (db_row, in_frag_names[l], &frag)){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		//TODO Lock table while working with it
		
		if(frag == NULL){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);	
			free(in_frag_names);
			free(in_db_names);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

		//Call API to read Frag
		if(oph_iostore_get_frag(dev_handle, &(frag->frag_id), &(orig_record_sets[l])) != 0){
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
	if(pthread_rwlock_unlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
    
	//Build portion of fragments used in selection
	

	//Count number of rows to compute
	long long j = 0, total_row_number = 0;

	//Prepare input record set
	record_sets = (oph_iostore_frag_record_set**)calloc((table_list_num+1),sizeof(oph_iostore_frag_record_set*));
	if(!record_sets)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if(table_list_num == 1){
		while(orig_record_sets[0]->record_set[total_row_number]) total_row_number++;
		if( (oph_iostore_copy_frag_record_set_only(orig_record_sets[0], &(record_sets[0]), 0, 0) != 0))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		record_sets[0]->frag_name = (char *)strndup(orig_record_sets[0]->frag_name, strlen(orig_record_sets[0]->frag_name));
		if(record_sets[0]->frag_name == NULL)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}
	else{
		//From alias must be provided in case of multiple tables	
		char *from_aliases = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
		if(from_aliases == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);	
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

		char **alias_list = NULL;
		int alias_num = 0;    
		if(oph_query_parse_multivalue_arg (from_aliases, &alias_list, &alias_num) || !alias_num || alias_num != table_list_num){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS);	
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

		long long partial_tot_row_number = 0;
		for(l = 0; l < table_list_num; l++){

			//Take the biggest row number as reference 
			partial_tot_row_number = 0;
			while(orig_record_sets[l]->record_set[partial_tot_row_number]) partial_tot_row_number++;
			if(partial_tot_row_number > total_row_number) total_row_number = partial_tot_row_number; 


			if( (oph_iostore_copy_frag_record_set_only(orig_record_sets[l], &(record_sets[l]), 0, 0) != 0))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}

			record_sets[l]->frag_name = (char *)strndup(alias_list[l], strlen(alias_list[l]));
			if(record_sets[l]->frag_name == NULL)
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				if(alias_list) free(alias_list);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
		}
		if(alias_list) free(alias_list);
	}

	// Check where clause
	char *where = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_WHERE);
	if(table_list_num == 1){
		if(where)
		{
			//Apply where condition
			if(_oph_ioserver_query_run_where_clause(where, table_list_num, orig_record_sets, &total_row_number, record_sets)){
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);	
				return OPH_IO_SERVER_EXEC_ERROR;     
			}
		}
		else{
			//Get all rows
			for (j = 0; j < total_row_number; j++)
			{
				record_sets[0]->record_set[j] = orig_record_sets[0]->record_set[j];  
			}
		}
	}
	else {
		if(where){
			//Apply where condition
			if(_oph_ioserver_query_run_where_clause(where, table_list_num, orig_record_sets, &total_row_number, record_sets)){
				_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, where);	
				return OPH_IO_SERVER_EXEC_ERROR;     
			}
		}
		else{
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

int _oph_ioserver_query_build_input_record_set_create(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, char *out_db_name, char *out_frag_name, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set ***stored_rs, long long *input_row_num, oph_iostore_frag_record_set ***input_rs)
{
	return _oph_ioserver_query_build_input_record_set(query_args, meta_db, dev_handle, thread_status, stored_rs, input_row_num, input_rs, out_db_name, out_frag_name);
}

int _oph_ioserver_query_build_input_record_set_select(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set ***stored_rs, long long *input_row_num, oph_iostore_frag_record_set ***input_rs)
{
	return _oph_ioserver_query_build_input_record_set(query_args, meta_db, dev_handle, thread_status, stored_rs, input_row_num, input_rs, NULL, NULL);
}

int _oph_ioserver_query_build_select_columns(char **field_list, int field_list_num, long long offset, long long total_row_number, oph_query_arg **args, oph_iostore_frag_record_set **inputs, oph_iostore_frag_record_set *output)
{
	if (!field_list || !field_list_num || !total_row_number || !inputs || !output){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int i = 0, k = 0;
	long long j = 0, l = 0;
	unsigned long long id = 0;
	char *updated_query = NULL;
	int var_count = 0;
	char **var_list = NULL;

	oph_query_field_types field_type[field_list_num];

	//Check binary fields if available
	int arg_count = 0; 
	if (args != NULL){
		while(args[i++]) arg_count++;	
	}

	//Track last binary argument used
	int curr_arg = 0;


	//Temp variables used to assign values to result set
	double val_d = 0;
	unsigned long long val_l = 0;

	//Used for internal parser
	oph_query_expr_node *e = NULL; 
	oph_query_expr_symtable *table = NULL;
	oph_query_expr_value* res = NULL;

	//Count number of tables
	i = 0;
	int table_num = 0;
	while(inputs[i++]) table_num++;	

	//Check column type for each selection field
	for (i=0; i<field_list_num; ++i)
	{
		//Check for field type
		if(oph_query_field_type(field_list[i], &(field_type[i]))){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,field_list[i]);
			logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,field_list[i]);    
			return OPH_IO_SERVER_PARSE_ERROR;
		}
		pmesg(LOG_DEBUG,__FILE__,__LINE__,"Column %s is of type %d\n", field_list[i], field_type[i]);
	}

	for (i=0; i<field_list_num; ++i)
	{
		switch(field_type[i]){
			case OPH_QUERY_FIELD_TYPE_UNKNOWN:
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);	
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			case OPH_QUERY_FIELD_TYPE_DOUBLE:
			{
				val_d = strtod ((char *)(field_list[i]), NULL);
				//Simply copy the value on each row
				for (j = 0; j < total_row_number; j++)
				{
					if (memory_check())
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					output->record_set[j]->field[i] = (void *)memdup((const void *)&val_d,sizeof(double));
					output->record_set[j]->field_length[i] = sizeof(double);
				}
				output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
				break;
			}
			case OPH_QUERY_FIELD_TYPE_LONG:
			{
				val_l = strtoll ((char *)(field_list[i]), NULL, 10);
				//Simply copy the value on each row
				for (j = 0; j < total_row_number; j++)
				{
					if (memory_check())
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					output->record_set[j]->field[i] = (void *)memdup((const void *)&val_l,sizeof(unsigned long long));
					output->record_set[j]->field_length[i] = sizeof(unsigned long long);
				}
				output->field_type[i] = OPH_IOSTORE_LONG_TYPE;
				break;
			}
			case OPH_QUERY_FIELD_TYPE_STRING:
			{
				//Simply copy the value on each row
				for (j = 0; j < total_row_number; j++)
				{
					if (memory_check())
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					output->record_set[j]->field[i] = (void *)memdup((const void *)(field_list[i]),strlen(field_list[i])+1);
					output->record_set[j]->field_length[i] = strlen(field_list[i])+1;
				}
				output->field_type[i] = OPH_IOSTORE_REAL_TYPE;
				break;
			}
			case OPH_QUERY_FIELD_TYPE_BINARY:
			{
				//Simply copy the value on each row
				for (j = 0; j < total_row_number; j++)
				{
					if (memory_check())
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					output->record_set[j]->field[i] = (void *)memdup((const void *)(args[curr_arg]->arg),args[curr_arg]->arg_length);
					output->record_set[j]->field_length[i] = args[curr_arg]->arg_length;
				}
				switch(args[curr_arg]->arg_type){
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
				curr_arg++;
				break;
			}
			case OPH_QUERY_FIELD_TYPE_VARIABLE:
			{
				//Get var from input table
				int field_index;
				int frag_index;
				if(table_num > 1){
					//Split frag name from field name
					char **field_components = NULL;
					int field_components_num = 0;

					if(oph_query_parse_hierarchical_args (field_list[i], &field_components, &field_components_num)){
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);	
						return OPH_IO_SERVER_PARSE_ERROR;        
					}

					if(field_components_num != 2){
						free(field_components);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, field_list[i]);	
						return OPH_IO_SERVER_PARSE_ERROR;
					}

					//Match table
					for(l = 0; l < table_num; l++){
						if(!STRCMP(field_components[0], inputs[l]->frag_name)){
							frag_index = l;
							for(j = 0; j < inputs[l]->field_num; j++){
								if(!STRCMP(field_components[1],inputs[l]->field_name[j])){
									field_index = j;
									break;
								}
							}	
							if(j == inputs[l]->field_num){				
								free(field_components);
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);	
								return OPH_IO_SERVER_PARSE_ERROR;     
							}
							break;
						}
					}	
					if(l == table_num){				
						free(field_components);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);	
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
					free(field_components);
				}
				else{
					for(j = 0; j < inputs[0]->field_num; j++){
						if(!STRCMP(field_list[i],inputs[0]->field_name[j])){
							field_index = j;
							break;
						}
					}	
					if(j == inputs[0]->field_num){				
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, field_list[i]);	
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
					//Take first frag
					frag_index = 0;
				}

				id = offset;
				for (j = 0; j < total_row_number; j++, id++)
				{
					if (memory_check())
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					output->record_set[j]->field[i] = inputs[frag_index]->record_set[id]->field_length[field_index] ? memdup(inputs[frag_index]->record_set[id]->field[field_index], inputs[frag_index]->record_set[id]->field_length[field_index]) : NULL;
					output->record_set[j]->field_length[i] = inputs[frag_index]->record_set[id]->field_length[field_index];
				}
				output->field_type[i] = inputs[frag_index]->field_type[field_index];
				break;
			}
			case OPH_QUERY_FIELD_TYPE_FUNCTION:
			{
				//Reset values
				updated_query = NULL;
				var_list = NULL;
				var_count = 0;
				e = NULL; 
				table = NULL;
				res = NULL;

				//First update string ? to ?#
				if(oph_query_expr_update_binary_args(field_list[i], &updated_query)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				if(oph_query_expr_create_symtable(&table, OPH_IO_SERVER_MAX_PLUGIN_NUMBER)){
					free(updated_query);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				if(oph_query_expr_get_ast(updated_query, &e) != 0){
					free(updated_query);
					oph_query_expr_destroy_symtable(table);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				//Read all variables and link them to input record set fields
				if(oph_query_expr_get_variables(e, &var_list, &var_count)){
					free(updated_query);
					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				int field_indexes[var_count];
				int frag_indexes[var_count];
				//Match binary with 1
				short int field_binary[var_count];
				unsigned long long binary_index = 0;

				//TODO Count actual number of string/binary variables
				oph_query_arg val_b[var_count];

				char *tmp_var_list[var_count];
				for(k = 0; k < var_count; k++){
					tmp_var_list[k] = (char *)strndup(var_list[k], strlen(var_list[k])+1);
					if(tmp_var_list[k] == NULL)
					{
						free(updated_query);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						return OPH_IO_SERVER_MEMORY_ERROR;
					}
				}

				if(var_count > 0){
					for(k = 0; k < var_count; k++){
						//Match binary values
						if(tmp_var_list[k][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE){
							binary_index = strtoll ((char *)(tmp_var_list[k]+1), NULL, 10);
							field_indexes[k] = (binary_index - 1 + curr_arg);
							frag_indexes[k] = -1;
							field_binary[k] = 1;

							if(field_indexes[k] >= arg_count){				
								free(updated_query);
								oph_query_expr_delete_node(e, table);
								oph_query_expr_destroy_symtable(table);
								free(var_list);
								for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);	
								return OPH_IO_SERVER_PARSE_ERROR;     
							}
						}
						//Match field names
						else{
							if(table_num > 1){
								//Set frag and field index
								//Split frag name from field name
								char **field_components = NULL;
								int field_components_num = 0;

								if(oph_query_parse_hierarchical_args (tmp_var_list[k], &field_components, &field_components_num)){
									free(updated_query);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);	
									return OPH_IO_SERVER_PARSE_ERROR;        
								}

								if(field_components_num != 2){
									free(field_components);
									free(updated_query);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, tmp_var_list[k]);	
									return OPH_IO_SERVER_PARSE_ERROR;
								}

								//Match table
								for(l = 0; l < table_num; l++){
									if(!STRCMP(field_components[0], inputs[l]->frag_name)){
										frag_indexes[k] = l;
										for(j = 0; j < inputs[l]->field_num; j++){
											if(!STRCMP(field_components[1],inputs[l]->field_name[j])){
												field_indexes[k] = j;
												field_binary[k] = 0;
												break;
											}
										}	
										if(j == inputs[l]->field_num){				
											free(field_components);
											free(updated_query);
											oph_query_expr_delete_node(e, table);
											oph_query_expr_destroy_symtable(table);
											free(var_list);
											for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
											pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);	
											return OPH_IO_SERVER_PARSE_ERROR;     
										}
										break;
									}
								}	
								if(l == table_num){				
									free(updated_query);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
									free(field_components);
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);	
									return OPH_IO_SERVER_PARSE_ERROR;     
								}
								free(field_components);
							}	
							else{
								for(j = 0; j < inputs[0]->field_num; j++){
									if(!STRCMP(var_list[k],inputs[0]->field_name[j])){
										field_indexes[k] = j;
										field_binary[k] = 0;
										break;
									}
								}	
								if(j == inputs[0]->field_num){				
									free(updated_query);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, tmp_var_list[k]);	
									return OPH_IO_SERVER_PARSE_ERROR;     
								}
								frag_indexes[k] = 0;
							}
						}
					}
				}

				for(k = 0; k < var_count; k++) if(tmp_var_list[k]) free(tmp_var_list[k]);

				id = offset;
				for (j = 0; j < total_row_number; j++, id++)
				{
					if (memory_check())
					{
						free(updated_query);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
						return OPH_IO_SERVER_MEMORY_ERROR;
					}

					if(var_count > 0){
						for(k = 0; k < var_count; k++){
							if(field_binary[k]){
								if(oph_query_expr_add_binary(var_list[k],args[field_indexes[k]],table)){ 
									free(updated_query);
									oph_query_expr_delete_node(e, table);
									oph_query_expr_destroy_symtable(table);
									free(var_list);
									pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
									return OPH_IO_SERVER_EXEC_ERROR;     
								}
							}
							else{
								switch(inputs[frag_indexes[k]]->field_type[field_indexes[k]]){
									case OPH_IOSTORE_LONG_TYPE:
									{
										val_l = *((long long *)inputs[frag_indexes[k]]->record_set[id]->field[field_indexes[k]]);
										if(oph_query_expr_add_long(var_list[k],val_l,table)){ 
											free(updated_query);
											oph_query_expr_delete_node(e, table);
											oph_query_expr_destroy_symtable(table);
											free(var_list);
											pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											return OPH_IO_SERVER_EXEC_ERROR;     
										} 
										break;
									}
									case OPH_IOSTORE_REAL_TYPE:
									{
										val_d = *((double *)inputs[frag_indexes[k]]->record_set[id]->field[field_indexes[k]]);
										if(oph_query_expr_add_double(var_list[k],val_d,table)){ 
											free(updated_query);
											oph_query_expr_delete_node(e, table);
											oph_query_expr_destroy_symtable(table);
											free(var_list);
											pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											return OPH_IO_SERVER_EXEC_ERROR;     
										}  
										break;
									}
									//TODO Check if string and binary can be treated separately
									case OPH_IOSTORE_STRING_TYPE:
									{
										val_b[k].arg = inputs[frag_indexes[k]]->record_set[id]->field[field_indexes[k]];
										val_b[k].arg_length = inputs[frag_indexes[k]]->record_set[id]->field_length[field_indexes[k]];								
										if(oph_query_expr_add_binary(var_list[k],&(val_b[k]),table)){ 
											free(updated_query);
											oph_query_expr_delete_node(e, table);
											oph_query_expr_destroy_symtable(table);
											free(var_list);
											pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
											return OPH_IO_SERVER_EXEC_ERROR;     
										}  
										break;
									}
								}
							}	
						}
					}
					if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) {
						if(res->type == OPH_QUERY_EXPR_TYPE_DOUBLE){
							if(!j) output->field_type[i] = OPH_IOSTORE_REAL_TYPE;	
							output->record_set[j]->field[i] = (void *)memdup((const void *)&(res->data.double_value),sizeof(double));
							output->record_set[j]->field_length[i] = sizeof(double);
							free(res);
						}else if(res->type == OPH_QUERY_EXPR_TYPE_LONG){
							if(!j) output->field_type[i] = OPH_IOSTORE_LONG_TYPE;	
							output->record_set[j]->field[i] = (void *)memdup((const void *)&(res->data.long_value),sizeof(unsigned long long));
							output->record_set[j]->field_length[i] = sizeof(unsigned long long);
							free(res);
						}else if(res->type == OPH_QUERY_EXPR_TYPE_STRING){
							if(!j) output->field_type[i] = OPH_IOSTORE_STRING_TYPE;	
							output->record_set[j]->field[i] = (void *)memdup((const void *)res->data.string_value,strlen(res->data.string_value) +1);
							output->record_set[j]->field_length[i] = strlen(res->data.string_value) + 1;
							free(res->data.string_value);
							free(res);
						}else if(res->type == OPH_QUERY_EXPR_TYPE_BINARY){
							if(!j) output->field_type[i] = OPH_IOSTORE_STRING_TYPE;	
							output->record_set[j]->field[i] = (void *)memdup((const void *)res->data.binary_value->arg,res->data.binary_value->arg_length);
							output->record_set[j]->field_length[i] = res->data.binary_value->arg_length;
							free(res->data.binary_value->arg);
							free(res->data.binary_value);
							free(res);
						}else {
							free(res);
							free(updated_query);
							oph_query_expr_delete_node(e, table);
							oph_query_expr_destroy_symtable(table);
							free(var_list);
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
							return OPH_IO_SERVER_EXEC_ERROR;     
						}
					}
					else
					{
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						free(updated_query);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
				}

				//Update current binary args used
				for(k = 0; k < var_count; k++){
					if(field_binary[k]) curr_arg++;
				}	
				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				free(updated_query);

			}
		}
	}

  	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_set_column_info(HASHTBL *query_args, char **field_list, int field_list_num, oph_iostore_frag_record_set *rs)
{
	if (!query_args || !field_list || !field_list_num || !rs){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Get select alias, if available
	int i = 0;
	char **field_alias_list = NULL;
	int field_alias_list_num = 0;    
	//Fields section
	char *fields_alias = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);
	if (fields_alias != NULL)
	{
		if(oph_query_parse_multivalue_arg (fields_alias, &field_alias_list, &field_alias_list_num) || !field_alias_list_num){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS);	
			if(field_alias_list) free(field_alias_list);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

		if(field_alias_list_num != field_list_num){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_ALIAS_NOT_MATCH);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_ALIAS_NOT_MATCH);	
			if(field_alias_list) free(field_alias_list);
			return OPH_IO_SERVER_EXEC_ERROR;        
		}
	}

	//Set alias or input table names
	if(field_alias_list != NULL){
		for (i=0; i<field_list_num; i++)
		{
			rs->field_name[i] = (strlen(field_alias_list[i]) == 0 ? strdup(field_list[i]) : strdup(field_alias_list[i]));
		}
	}
	else{
		for (i=0; i<field_list_num; i++)
		{
			rs->field_name[i] = strdup(field_list[i]);
		}
	}
	free(field_alias_list);

	//Set default column types (will be updated later to correct value)
	for (i=0; i<field_list_num-1; i++)
	{
		rs->field_type[i] = OPH_IOSTORE_LONG_TYPE;
	}
	rs->field_type[i] = OPH_IOSTORE_STRING_TYPE;

  	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_store_fragment(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, char *frag_name, unsigned long long frag_size, oph_iostore_frag_record_set **final_result_set)
{
	if (!meta_db || !dev_handle || !thread_status || !(*final_result_set)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Check current db
	oph_metadb_db_row *db_row = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_wrlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Retrieve current db
	if(oph_metadb_find_db (*meta_db, thread_status->current_db, thread_status->device, &db_row) ||  db_row == NULL){
		  pthread_rwlock_unlock(&rwlock);
		    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
		  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
		  return OPH_IO_SERVER_METADB_ERROR;             
	}

	//Call API to insert Frag
	oph_iostore_resource_id *frag_id = NULL;
	if(oph_iostore_put_frag(dev_handle, *final_result_set, &frag_id) != 0){
		  pthread_rwlock_unlock(&rwlock);
		  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");
		  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");	
		  return OPH_IO_SERVER_API_ERROR;             
	} 

	oph_metadb_frag_row *frag = NULL;

	//Add Frag to MetaDB
	if(oph_metadb_setup_frag_struct (frag_name, thread_status->device, dev_handle->is_persistent, &(db_row->db_id), frag_id, frag_size, &frag)) {
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
	if(oph_metadb_setup_db_struct (db_row->db_name, db_row->device, dev_handle->is_persistent, &(db_row->db_id), db_row->frag_number, &tmp_db_row)) {
		  pthread_rwlock_unlock(&rwlock);
		  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		  oph_metadb_cleanup_frag_struct (frag);
		  return OPH_IO_SERVER_METADB_ERROR;             
	}
	if(oph_metadb_add_frag (db_row, frag)){
		  pthread_rwlock_unlock(&rwlock);
		  oph_metadb_cleanup_frag_struct (frag);
		  oph_metadb_cleanup_db_struct (tmp_db_row);
		  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");
		  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");	
		  return OPH_IO_SERVER_METADB_ERROR;             
	}

	oph_metadb_cleanup_frag_struct (frag);

	tmp_db_row->frag_number++;
	if(oph_metadb_update_db (*meta_db, tmp_db_row)){
		  pthread_rwlock_unlock(&rwlock);
		oph_metadb_cleanup_db_struct (tmp_db_row);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	//If device is transient then block record from being deleted
	if(!dev_handle->is_persistent) *final_result_set = NULL;

	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		oph_metadb_cleanup_db_struct (tmp_db_row);
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_cleanup_db_struct (tmp_db_row);

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_build_row(int *arg_count, unsigned long long *row_size, oph_iostore_frag_record_set *partial_result_set, char **field_list, char **value_list, oph_query_arg **args, oph_iostore_frag_record **new_record)
{
	if (!arg_count || !row_size || !partial_result_set || !field_list || !value_list || !new_record){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//TODO Extend management of ?  in order to evaluate also ? inside more complex constructs

	//Created record struct
	*new_record = NULL;     
	if(oph_iostore_create_frag_record(new_record, partial_result_set->field_num) == 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
		return OPH_IO_SERVER_MEMORY_ERROR;             
	}

	long long tmpL = 0;
	double tmpD = 0;
	int i = 0, k = 0;
	(*row_size) = sizeof(oph_iostore_frag_record);
	oph_query_field_types field_type = OPH_QUERY_FIELD_TYPE_UNKNOWN;

	char *updated_query = NULL;
	int var_count = 0;
	char **var_list = NULL;

	//Used for internal parser
	oph_query_expr_node *e = NULL; 
	oph_query_expr_symtable *table = NULL;
	oph_query_expr_value* res = NULL;

	if (memory_check())
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
		oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	for(i = 0; i < partial_result_set->field_num; i++){
		//For each field check column name correspondence
		if(STRCMP(field_list[i], partial_result_set->field_name[i]) == 1){
		  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR);
		  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR); 
		  oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
		  return OPH_IO_SERVER_EXEC_ERROR;        
		}

		//Check for field type
		if(oph_query_field_type(value_list[i], &field_type)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,value_list[i]);
			logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,value_list[i]);    
      oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
			return OPH_IO_SERVER_PARSE_ERROR;
		}

		switch(field_type){
			case OPH_QUERY_FIELD_TYPE_VARIABLE:
			case OPH_QUERY_FIELD_TYPE_UNKNOWN:
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,value_list[i]);
				logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELD_TYPE_ERROR,value_list[i]);    
	      oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
				return OPH_IO_SERVER_PARSE_ERROR;
			}
			case OPH_QUERY_FIELD_TYPE_FUNCTION:
			{
				//Reset values
				updated_query = NULL;
				var_list = NULL;
				var_count = 0;
				e = NULL; 
				table = NULL;
				k = 0;
				res = NULL;

				//First update string ? to ?#
				if(oph_query_expr_update_binary_args(value_list[i], &updated_query)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);	
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				if(oph_query_expr_create_symtable(&table, OPH_IO_SERVER_MAX_PLUGIN_NUMBER)){
					free(updated_query);
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				if(oph_query_expr_get_ast(updated_query, &e) != 0){
					free(updated_query);
					oph_query_expr_destroy_symtable(table);
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				//Read all variables and link them to input record set fields
				if(oph_query_expr_get_variables(e, &var_list, &var_count)){
					free(updated_query);
					oph_query_expr_delete_node(e, table);
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					oph_query_expr_destroy_symtable(table);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, value_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				unsigned long long binary_index = 0;

				if(var_count > 0){
					for(k = 0; k < var_count; k++){
						//Match binary values
						if(var_list[k][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE){
							binary_index = strtoll ((char *)(var_list[k]+1), NULL, 10) - 1 + *(arg_count);
							if(oph_query_expr_add_binary(var_list[k],args[binary_index],table)){ 
								free(updated_query);
								oph_query_expr_delete_node(e, table);
								oph_query_expr_destroy_symtable(table);
								free(var_list);
								oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
								pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
								logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
								return OPH_IO_SERVER_EXEC_ERROR;     
							}
						}
						//No field names can be used
						else{
							free(updated_query);
							oph_query_expr_delete_node(e, table);
							oph_query_expr_destroy_symtable(table);
							free(var_list);
							oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);	
							return OPH_IO_SERVER_PARSE_ERROR;     
						}
					}
					*(arg_count) += var_count;
				}

				if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) {
					if(res->type == OPH_QUERY_EXPR_TYPE_DOUBLE){
						(*new_record)->field_length[i] = sizeof(double);
						(*new_record)->field[i] = (void *)memdup((const void *)&(res->data.double_value),sizeof(double));
						free(res);
					}else if(res->type == OPH_QUERY_EXPR_TYPE_LONG){
						(*new_record)->field_length[i] =  sizeof(unsigned long long);
						(*new_record)->field[i] = (void *)memdup((const void *)&(res->data.long_value),sizeof(unsigned long long));
						free(res);
					}else if(res->type == OPH_QUERY_EXPR_TYPE_STRING){
						(*new_record)->field_length[i] = strlen(res->data.string_value) + 1;
						(*new_record)->field[i] = (void *)memdup((const void *)res->data.string_value,strlen(res->data.string_value) +1);
						free(res->data.string_value);
						free(res);
					}else if(res->type == OPH_QUERY_EXPR_TYPE_BINARY){
						(*new_record)->field_length[i] = res->data.binary_value->arg_length;
						(*new_record)->field[i] = (void *)memdup((const void *)res->data.binary_value->arg,res->data.binary_value->arg_length);
						free(res->data.binary_value->arg);
						free(res->data.binary_value);
						free(res);
					}else {
						free(res);
						free(updated_query);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
						return OPH_IO_SERVER_EXEC_ERROR;     
					}
				}
				else
				{
					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					free(var_list);
					free(updated_query);
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
					return OPH_IO_SERVER_PARSE_ERROR;     
				}

				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				free(updated_query);
				break;
			}
			case OPH_QUERY_FIELD_TYPE_BINARY:
			{
				//For each value check if argument contains ? and substitute with arg[i]
				//Check column type with arg_type ...  
				//if(args[*arg_count]->arg_type != partial_result_set->field_type)
				//EXIT
				if (!args){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
					logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					return OPH_IO_SERVER_NULL_PARAM;
				}

				(*new_record)->field_length[i] = args[*arg_count]->arg_length;
				(*new_record)->field[i] = (void *)memdup(args[*arg_count]->arg,(*new_record)->field_length[i]);
				(*arg_count)++;
				break;
			}
			//No substitution occurs, use directly strings
			case OPH_QUERY_FIELD_TYPE_STRING:
			{
				(*new_record)->field_length[i] = strlen(value_list[i]) + 1;
				(*new_record)->field[i] = (char *)strndup(value_list[i],(*new_record)->field_length[i]);
				break;
			}
			case OPH_QUERY_FIELD_TYPE_DOUBLE:
			{
				tmpD = (double)strtod(value_list[i], NULL);
				(*new_record)->field_length[i] = sizeof(double);
				(*new_record)->field[i] = (void *)memdup((const void *)&tmpD,(*new_record)->field_length[i]);
				break;
			}
			case OPH_QUERY_FIELD_TYPE_LONG:
			{
				tmpL = (long long)strtoll(value_list[i], NULL, 10);
				(*new_record)->field_length[i] = sizeof(long long);
				(*new_record)->field[i] = (void *)memdup((const void *)&tmpL,(*new_record)->field_length[i]);
				break;
			}
		}

		(*row_size) += ((*new_record)->field_length[i] + sizeof((*new_record)->field_length[i]) + sizeof((*new_record)->field[i]));
		if((*new_record)->field[i] == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
			oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
			return OPH_IO_SERVER_MEMORY_ERROR;             
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}

