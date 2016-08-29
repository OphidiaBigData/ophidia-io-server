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

int _oph_ioserver_query_build_input_record_set(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set **stored_rs, long long *input_row_num, oph_iostore_frag_record_set **input_rs, short int create_flag)
{
	if (!query_args || !stored_rs || !input_row_num || !input_rs || (create_flag != 0 && create_flag != 1)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*stored_rs = NULL;
	*input_row_num = 0;
	*input_rs = NULL;

	//Extract frag_name arg from query args
	//TODO extend mechanism to allow multiple table selections
	char *from_frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM);
	if(from_frag_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	char *frag_name = NULL;
	if(create_flag == 1)
	{
		//Extract new frag_name arg from query args
		frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
		if(frag_name == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
			return OPH_IO_SERVER_EXEC_ERROR;        
		}
	}

	oph_metadb_frag_row *frag = NULL;
	oph_metadb_db_row *db_row = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_rdlock(&rwlock) != 0){
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

	if(create_flag == 1)
	{
		//Check if Frag already exists
		if(oph_metadb_find_frag (db_row, frag_name, &frag)){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		if(frag != NULL){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);	
			return OPH_IO_SERVER_EXEC_ERROR;        
		}
	}

	//Check if Frag exists
	if(oph_metadb_find_frag (db_row, from_frag_name, &frag)){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}
	//TODO Lock table while working with it
    
	if(frag == NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//TODO read other clauses

	oph_iostore_frag_record_set *orig_record_set = NULL;

	//Call API to read Frag
	if(oph_iostore_get_frag(dev_handle, &(frag->frag_id), &orig_record_set) != 0){
		pthread_rwlock_unlock(&rwlock);        
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");	
		return OPH_IO_SERVER_API_ERROR;             
	} 

	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
    
	//Count number of rows to compute
	long long i = 0, j = 0, k = 0, total_row_number = 0;
	while(orig_record_set->record_set[total_row_number]) total_row_number++;

	//Prepare input record set
	oph_iostore_frag_record_set *record_set = NULL;
	if( (oph_iostore_copy_frag_record_set_only(orig_record_set, &record_set, 0, 0) != 0))
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	record_set->frag_name = (char *)strndup(orig_record_set->frag_name, strlen(orig_record_set->frag_name));
	if(record_set->frag_name == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
		oph_iostore_destroy_frag_recordset_only(&record_set);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	// Check where clause
	unsigned long long id;
	char *where = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_WHERE);
	if(where)
	{
		for(i = 0; i < record_set->field_num; i++){
			//TODO Check what fields are selected
			if (!STRCMP(record_set->field_name[i],OPH_NAME_ID))
			{
				oph_query_expr_node *e = NULL; 

				if(oph_query_expr_get_ast(where, &e) != 0){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
					if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
					oph_iostore_destroy_frag_recordset_only(&record_set);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				oph_query_expr_symtable *table;
				if(oph_query_expr_create_symtable(&table, 1)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					oph_query_expr_delete_node(e, table);
					if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
					oph_iostore_destroy_frag_recordset_only(&record_set);
					return OPH_IO_SERVER_MEMORY_ERROR;
				}

				oph_query_expr_value* res = NULL;
				k = 0;
				for (j = 0; j < total_row_number; j++)
				{
					id = *((long long *)orig_record_set->record_set[j]->field[i]);
					oph_query_expr_add_long("id_dim",id,table);    
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
						  //this error message is not that accurate change it. means that the result of the evaluation was not a number
						  free(res);
						  oph_query_expr_delete_node(e, table);
						  oph_query_expr_destroy_symtable(table);
							if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
							oph_iostore_destroy_frag_recordset_only(&record_set);
						  pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
						  logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
						  return OPH_IO_SERVER_PARSE_ERROR;
						}

						if(result){
							record_set->record_set[k++] = orig_record_set->record_set[j];  
							pmesg(LOG_DEBUG, __FILE__, __LINE__, "id_dim = %d has been considered in where condition\n", id);
							logging(LOG_DEBUG, __FILE__, __LINE__, "id_dim = %d has been considered in where condition\n", id);
						}
					}
					else
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
						oph_iostore_destroy_frag_recordset_only(&record_set);
						return OPH_IO_SERVER_PARSE_ERROR;
					}
				}

				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				break;
			}
		}
	}
	else{
		for (j = 0; j < total_row_number; j++)
		{
			record_set->record_set[k++] = orig_record_set->record_set[j];  
		}
	}

	//Update output argument with actual value
	*stored_rs = orig_record_set;
	*input_row_num = k;
	*input_rs = record_set;

  return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_query_build_input_record_set_create(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set **stored_rs, long long *input_row_num, oph_iostore_frag_record_set **input_rs)
{
	return _oph_ioserver_query_build_input_record_set(query_args, meta_db, dev_handle, thread_status, stored_rs, input_row_num, input_rs, 1);
}

int _oph_ioserver_query_build_input_record_set_select(HASHTBL *query_args, oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_iostore_frag_record_set **stored_rs, long long *input_row_num, oph_iostore_frag_record_set **input_rs)
{
	return _oph_ioserver_query_build_input_record_set(query_args, meta_db, dev_handle, thread_status, stored_rs, input_row_num, input_rs, 0);
}


int _oph_ioserver_query_build_select_columns(char **field_list, int field_list_num, long long offset, long long total_row_number, oph_iostore_frag_record_set *input, oph_iostore_frag_record_set *output)
{
	if (!field_list || !field_list_num || !input || !output){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	int i, k;
	long long j;
	unsigned long long id;
	char *updated_query = NULL;
	int var_count = 0;
	char **var_list = NULL;

	oph_query_field_types field_type[field_list_num];

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
			case OPH_QUERY_FIELD_TYPE_DOUBLE:
			case OPH_QUERY_FIELD_TYPE_LONG:
			case OPH_QUERY_FIELD_TYPE_STRING:
			case OPH_QUERY_FIELD_TYPE_BINARY:
			case OPH_QUERY_FIELD_TYPE_UNKNOWN:
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);	
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			case OPH_QUERY_FIELD_TYPE_VARIABLE:
			{
				//TODO Read var from input table
				if (!STRCMP(field_list[i],OPH_NAME_ID))
				{
					id = offset + 1;
					for (j = 0; j < total_row_number; j++, id++)
					{
						if (i==1) id=1;
						output->record_set[j]->field[i] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
						output->record_set[j]->field_length[i] = sizeof(unsigned long long);
					}
				}
				else if (!STRCMP(field_list[i],OPH_NAME_MEASURE))
				{
					id = offset;
					for (j = 0; j < total_row_number; j++, id++)
					{
						output->record_set[j]->field[i] = input->record_set[id]->field_length[i] ? memdup(input->record_set[id]->field[i], input->record_set[id]->field_length[i]) : NULL;
						output->record_set[j]->field_length[i] = input->record_set[id]->field_length[i];
					}
				}
				else // Copy data
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;
				}

				//TODO Get output type
				output->field_type[i] = input->field_type[i];
	
				break;
			}
			case OPH_QUERY_FIELD_TYPE_FUNCTION:
			{
				//First update string ? to ?#
				if(oph_query_expr_update_binary_args(field_list[i], &updated_query)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				oph_query_expr_node *e = NULL; 

				if(oph_query_expr_get_ast(field_list[i], &e) != 0){
					free(updated_query);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, updated_query);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, updated_query);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				oph_query_expr_symtable *table;

				if(oph_query_expr_create_symtable(&table, 1)){
					free(updated_query);
					oph_query_expr_delete_node(e, table);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				//Read all variables and associate them to input record set fields
				if(oph_query_expr_get_variables(e, &var_list, &var_count)){
					free(updated_query);
					oph_query_expr_delete_node(e, table);
					oph_query_expr_destroy_symtable(table);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
					return OPH_IO_SERVER_EXEC_ERROR;     
				}

				k = 0;
				int field_indexes[var_count];
				while(var_list[k] != 0){
					for(j = 0; j < input->field_num; j++){
						//TODO Match binary values
						if(!STRCMP(var_list[k],input->field_name[j])){
							field_indexes[k] = j;
							break;
						}
					}	
					if(j == input->field_num){				
						free(updated_query);
						oph_query_expr_delete_node(e, table);
						oph_query_expr_destroy_symtable(table);
						free(var_list);
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELD_NAME_UNKNOWN, var_list[k]);	
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
					k++;
				}

				oph_query_expr_value* res = NULL;
				oph_query_arg value_b;
				long long value_l;
				double value_d;

				for (j = 0; j < total_row_number; j++)
				{
					//TODO Extract all variables
					k = 0;
					while(var_list[k] != 0){
						switch(input->field_type[field_indexes[k]]){
							case OPH_IOSTORE_LONG_TYPE:
							{
								value_l = *((long long *)input->record_set[j]->field[field_indexes[k]]);
								oph_query_expr_add_long(var_list[k],value_l,table); 
								break;
							}
							case OPH_IOSTORE_REAL_TYPE:
							{
								value_d = *((double *)input->record_set[j]->field[field_indexes[k]]);
								oph_query_expr_add_double(var_list[k],value_d,table); 
								break;
							}
							//TODO Check if binary can be added
							case OPH_IOSTORE_STRING_TYPE:
							{
								value_b.arg = input->record_set[j]->field[field_indexes[k]];
								value_b.arg_length = input->record_set[j]->field_length[field_indexes[k]];								
								oph_query_expr_add_binary(var_list[k],&value_b,table); 
								break;
							}
						}
						k++;	   
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
							output->record_set[j]->field_length[i] = strlen(res->data.string_value);
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
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
						return OPH_IO_SERVER_PARSE_ERROR;     
					}
				}

				oph_query_expr_delete_node(e, table);
				oph_query_expr_destroy_symtable(table);
				free(var_list);
				free(updated_query);
				updated_query = NULL;
				var_list = NULL;
				var_count = 0;
			}
		}
	}

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
	int i = 0;
	(*row_size) = sizeof(oph_iostore_frag_record);
	oph_query_field_types field_type = OPH_QUERY_FIELD_TYPE_UNKNOWN;

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
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, value_list[i]);	
	      oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
				return OPH_IO_SERVER_PARSE_ERROR;
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

				if (memory_check())
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
					oph_iostore_destroy_frag_record(new_record, partial_result_set->field_num);
					return OPH_IO_SERVER_MEMORY_ERROR;
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

