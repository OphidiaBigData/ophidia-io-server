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

#include "oph_io_server_query_manager.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <debug.h>
#include <pthread.h>

#include "oph_server_utility.h"
#include "oph_query_engine_language.h"

extern int msglevel;
extern pthread_rwlock_t rwlock;

int oph_io_server_run_create_as_select(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, char *current_db, oph_query_arg **args, HASHTBL *query_args)
{
	if (!query_args || !dev_handle || !current_db || !meta_db){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_iostore_frag_record_set **orig_record_sets = NULL;
	oph_iostore_frag_record_set **record_sets = NULL;
	long long row_number = 0;

	//First check if other mandatory fields are set
	char *out_frag_name = NULL;
	char *out_db_name = NULL;

	//Extract new frag_name arg from query args
	out_frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
	if(out_frag_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	char **frag_components = NULL;
	int frag_components_num = 0;
	if(oph_query_parse_hierarchical_args (out_frag_name, &frag_components, &frag_components_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, out_frag_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, out_frag_name);	
		return OPH_IO_SERVER_PARSE_ERROR;        
	}

	//If DB is setted in frag name
	if(frag_components_num > 1){
		//Check if db is the one used by the query
		if(STRCMP(current_db,frag_components[0]) != 0){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);	
			free(frag_components);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		out_db_name = frag_components[0];
		out_frag_name = frag_components[1];
	}
	else{
		out_db_name = current_db;

	}

	if(_oph_ioserver_query_build_input_record_set_create(query_args, args, meta_db, dev_handle, out_db_name, out_frag_name, current_db, &orig_record_sets, &row_number, &record_sets)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_SELECTION_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_SELECTION_ERROR);	
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
	if(fields == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	char **field_list = NULL;
	int field_list_num = 0;
	if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD); 
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Check field list number it can only be 2
	if( field_list_num != 2){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR); 
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	// Check limit clauses
	long long limit=0, offset=0;
	if(_oph_io_server_query_compute_limits(query_args, &offset, &limit)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_LIMIT_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_LIMIT_ERROR);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Prepare output record set
	oph_iostore_frag_record_set *rs = NULL;
	int i = 0;
	long long j = 0, total_row_number = 0;

	//If recordset is not empty proceed
	if(record_sets[0]->record_set[0] != NULL){
		//Count number of rows to compute
		if (!offset || (offset<row_number))
		{
			j = offset;
			while(record_sets[0]->record_set[j] && (!limit || (total_row_number<limit))) { j++; total_row_number++; }
		}

		//Create output record set
		if(oph_iostore_create_frag_recordset(&rs, total_row_number, field_list_num))	
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if(field_list) free(field_list);
			free(frag_components);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		rs->field_num = field_list_num;

		//Set column names and types
		if(_oph_ioserver_query_set_column_info(query_args, field_list, field_list_num, rs)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);    
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if(field_list) free(field_list);
			if (rs) oph_iostore_destroy_frag_recordset(&rs);
			free(frag_components);
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		//Process each column
		if(_oph_ioserver_query_build_select_columns(query_args, field_list, field_list_num, offset, total_row_number, args, record_sets, rs)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);    
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if(field_list) free(field_list);
			if (rs) oph_iostore_destroy_frag_recordset(&rs);
			free(frag_components);
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		//Order rows
		if(_oph_io_server_query_order_output(query_args, rs)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ORDER_EXEC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_ORDER_EXEC_ERROR);    
			_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
			if(field_list) free(field_list);
			if (rs) oph_iostore_destroy_frag_recordset(&rs);
			free(frag_components);
			return OPH_IO_SERVER_EXEC_ERROR;
		}		
	}
	else{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_EMPTY_SELECTION);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_EMPTY_SELECTION);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		if (rs) oph_iostore_destroy_frag_recordset(&rs);
		free(frag_components);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if(field_list) free(field_list);
    
	_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);

	rs->frag_name = strndup(out_frag_name, strlen(out_frag_name));
	free(frag_components);

	//TODO manage fragment struct creation
	//Compute size of record_set variable
	unsigned long long tot_size = sizeof(oph_iostore_frag_record *);
	j = 0;
	while(rs->record_set[j]){
		tot_size += sizeof(oph_iostore_frag_record *) + sizeof(oph_iostore_frag_record);
		for(i = 0; i < rs->field_num; i++){
			tot_size += rs->record_set[j]->field_length[i] + sizeof(rs->record_set[j]->field_length[i]) + sizeof(rs->record_set[j]->field[i]);
		}
		j++;
	}
	int ret = _oph_ioserver_query_store_fragment(meta_db, dev_handle, current_db, tot_size, &rs);

	//Destroy tmp recordset 
	if (rs) oph_iostore_destroy_frag_recordset(&rs);

	if(ret){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	return OPH_IO_SERVER_SUCCESS;
}


int oph_io_server_run_select(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, char *current_db, oph_query_arg **args, HASHTBL *query_args, oph_iostore_frag_record_set **output_rs)
{
	if (!query_args || !dev_handle || !current_db || !meta_db || !output_rs){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	oph_iostore_frag_record_set **orig_record_sets = NULL;
	oph_iostore_frag_record_set **record_sets = NULL;
	*output_rs = NULL;
	long long row_number = 0;

	if(_oph_ioserver_query_build_input_record_set_select(query_args, args, meta_db, dev_handle, current_db, &orig_record_sets, &row_number, &record_sets)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_SELECTION_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_SELECTION_ERROR);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	char **field_list = NULL;
	int field_list_num = 0;    
	//Fields section
	char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
	if (fields == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num) || !field_list_num){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	// Check limit clauses
	long long limit=0, offset=0;
	if(_oph_io_server_query_compute_limits(query_args, &offset, &limit)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_LIMIT_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_LIMIT_ERROR);	
		_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Prepare output record set
	oph_iostore_frag_record_set *rs = NULL;
	long long j = 0, total_row_number = 0;
	int error = 0;

	//If recordset is not empty proceed
	if(record_sets[0]->record_set[0] != NULL){

		//Count number of rows to compute
		if (!offset || (offset<row_number))
		{
			j = offset;
			while(record_sets[0]->record_set[j] && (!limit || (total_row_number<limit))) { j++; total_row_number++; }
		}

		//Create output record set
		if(oph_iostore_create_frag_recordset(&rs, total_row_number, field_list_num))	
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
			error = OPH_IO_SERVER_MEMORY_ERROR;
		}

		if(!error){
			rs->field_num = field_list_num;

			//Set column names and types
			if(_oph_ioserver_query_set_column_info(query_args, field_list, field_list_num, rs)){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);    
				error = OPH_IO_SERVER_EXEC_ERROR;
			}
			else{
				//Process each column
				if(_oph_ioserver_query_build_select_columns(query_args, field_list, field_list_num, offset, total_row_number, args, record_sets, rs)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);    
					error = OPH_IO_SERVER_EXEC_ERROR;
				}
				else{
					//Order rows
					if(_oph_io_server_query_order_output(query_args, rs)){
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ORDER_EXEC_ERROR);
						logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_ORDER_EXEC_ERROR);    
						error = OPH_IO_SERVER_EXEC_ERROR;
					}		
				}
			}
		}
	}
	else{
		//Set empty recordset
		if(oph_iostore_create_frag_recordset(&rs, 0, field_list_num)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
			error = OPH_IO_SERVER_MEMORY_ERROR;
		}

		if(!error){
			rs->field_num = field_list_num;

			//Set column names and types
			if(_oph_ioserver_query_set_column_info(query_args, field_list, field_list_num, rs)){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_FIELDS_EXEC_ERROR);    
				error = OPH_IO_SERVER_EXEC_ERROR;
			}
		}
	}

	_oph_ioserver_query_release_input_record_set(dev_handle, orig_record_sets, record_sets);
	if(field_list) free(field_list);

	if (error)
	{
		if (rs) oph_iostore_destroy_frag_recordset(&rs);
		return error;
	}

	*output_rs = rs;

	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_insert(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_iostore_frag_record_set *rs, unsigned long long rs_index, oph_query_arg **args, HASHTBL *query_args, unsigned long long *size)
{
	if (!query_args || !dev_handle || !rs || !meta_db || !size){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*size = 0;
	char **field_list = NULL, **value_list = NULL;
	int field_list_num = 0, value_list_num = 0;    
	//Fields section
	char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
	if (fields == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD); 
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
      
	//Values section
	char *values = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
	if (!values)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_VALUE);	
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (values, &value_list, &value_list_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_VALUE); 
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	if(value_list_num != field_list_num || value_list_num != rs->field_num){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT); 
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Define record struct
	oph_iostore_frag_record *new_record = NULL;     
	unsigned int arg_count = 0;
	unsigned long long row_size = 0;
	unsigned int l = 0;

	if (args != NULL){
		while(args[l++]) arg_count++;
	}

	if(_oph_ioserver_query_build_row(arg_count, &row_size, rs, field_list, value_list, args, &new_record)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);	
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		return OPH_IO_SERVER_MEMORY_ERROR;             
	}

	//Add record to partial record set
	rs->record_set[rs_index] = new_record;
	//Update current record size
	*size = row_size;

	if(field_list) free(field_list);
	if(value_list) free(value_list);

	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_multi_insert(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_query_arg **args, HASHTBL *query_args, unsigned int *num_insert, unsigned long long *size)
{
	if (!query_args || !dev_handle || !thread_status || !meta_db || !num_insert || !size){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*num_insert = 0;
	*size = 0;

	oph_iostore_frag_record_set *tmp = thread_status->curr_stmt->partial_result_set;

	char **field_list = NULL, **value_list = NULL;
	int field_list_num = 0, value_list_num = 0;    
	//Fields section
	char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
	if (fields == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD); 
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Values section
	char *values = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
	if (!values)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_VALUE);	
		if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (values, &value_list, &value_list_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_VALUE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_VALUE); 
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Check if values number is a multiple of field number
	if(value_list_num < field_list_num || (value_list_num % field_list_num != 0) || field_list_num != tmp->field_num){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT); 
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	unsigned int insert_num = (int)value_list_num / field_list_num;

	if(thread_status->curr_stmt->curr_run == 1 || (thread_status->curr_stmt->curr_run == 0 && thread_status->curr_stmt->tot_run == 0)){
		//0- For first time: create record_set array - Also executed when it is a single run

		unsigned long long progressive_mi_rows = 0;
		if(thread_status->curr_stmt->mi_prev_rows == 0){		
			//In case first time or no remainder, create the recordset
			progressive_mi_rows = (thread_status->curr_stmt->tot_run ? thread_status->curr_stmt->tot_run : 1)*insert_num;
			tmp->record_set = (oph_iostore_frag_record **)calloc(1 + progressive_mi_rows, sizeof(oph_iostore_frag_record *));
			if(tmp->record_set == NULL){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
				if(field_list) free(field_list);
				if(value_list) free(value_list);
				return OPH_IO_SERVER_MEMORY_ERROR;             
			}
		}
		else{
			//Cope with second query for uneven multi-insert
			progressive_mi_rows = thread_status->curr_stmt->mi_prev_rows + (thread_status->curr_stmt->tot_run ? thread_status->curr_stmt->tot_run : 1)*insert_num;
			//Realloc struct
			oph_iostore_frag_record **tmp_record_set = (oph_iostore_frag_record **)realloc(tmp->record_set, (1 + progressive_mi_rows) * sizeof(oph_iostore_frag_record *));
			if(tmp_record_set == NULL){
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
				if(field_list) free(field_list);
				if(value_list) free(value_list);
				return OPH_IO_SERVER_MEMORY_ERROR;             
			}
			else{
				unsigned long long k = 0;
				for(k = thread_status->curr_stmt->mi_prev_rows; k < (1 + progressive_mi_rows); k++) tmp_record_set[k] = NULL; 
				tmp->record_set = tmp_record_set;
			}
		}
	}

	//Define record struct
	oph_iostore_frag_record *new_record = NULL;     
	unsigned int arg_count = 0;
	unsigned long long row_size = 0;
	unsigned long long cumulative_size = 0;

	unsigned int l = 0;

	if (args != NULL){
		while(args[l++]) arg_count++;
	}

	unsigned long long curr_start_row = thread_status->curr_stmt->mi_prev_rows + ((thread_status->curr_stmt->curr_run ? thread_status->curr_stmt->curr_run : 1) - 1) * insert_num;
	for(l = 0; l < insert_num; l++){

		if(_oph_ioserver_query_build_row(arg_count, &row_size, tmp, field_list, ((char **)value_list+(2*l)), args, &new_record)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);	
			if(field_list) free(field_list);
			if(value_list) free(value_list);
			return OPH_IO_SERVER_MEMORY_ERROR;             
		}

		//Add record to partial record set
		tmp->record_set[curr_start_row + l] = new_record;
		//Update current record size
		cumulative_size += row_size;

		new_record = NULL;
	}

	if(field_list) free(field_list);
	if(value_list) free(value_list);

	*num_insert = insert_num;
	*size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_create_empty_frag(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, char *current_db, HASHTBL *query_args, oph_iostore_frag_record_set **output_rs)
{
	if (!query_args || !dev_handle || !current_db || !meta_db || !output_rs){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	*output_rs = NULL;

	//Extract frag_name arg from query args
	char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
	if(frag_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
		return OPH_IO_SERVER_EXEC_ERROR;        
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
	if(oph_metadb_find_db (*meta_db, current_db, dev_handle->device, &db_row) ||  db_row == NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	//Check if Frag already exists
	if(oph_metadb_find_frag (db_row, frag_name, &frag)){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
    
	if(frag != NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_EXIST_ERROR);	
		return OPH_IO_SERVER_EXEC_ERROR;             
	}

	if (memory_check())
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	//Extract frag column name from query args
	char *frag_column_names = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME);
	if(frag_column_names == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	char **column_name_list = NULL;
	int column_name_num = 0;
	if(oph_query_parse_multivalue_arg (frag_column_names, &column_name_list, &column_name_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME); 
		if(column_name_list) free(column_name_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Check column list number; it can only be 2
	if( column_name_num != 2){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR); 
		if(column_name_list) free(column_name_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Extract frag column types from query args
	char *frag_column_types = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE);
	char **column_type_list = NULL;
	int column_type_num = 0;
	if(frag_column_types == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	if(oph_query_parse_multivalue_arg (frag_column_types, &column_type_list, &column_type_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE); 
		if(column_name_list) free(column_name_list);
		if(column_type_list) free(column_type_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	//If column type is defined then check coerence with column name
	if(column_type_num != column_name_num){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG); 
		if(column_name_list) free(column_name_list);
		if(column_type_list) free(column_type_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
    
	//Create fragment record_set skeleton
	oph_iostore_frag_record_set *new_record_set = NULL;
	if(oph_iostore_create_frag_recordset(&new_record_set, 0, column_name_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
		if(column_name_list) free(column_name_list);
		if(column_type_list) free(column_type_list);
		return OPH_IO_SERVER_MEMORY_ERROR;             
	}

	int i = 0;
	for(i = 0; i < column_name_num; i++){
		//Copy column name
		new_record_set->field_name[i] = (char *)strndup(column_name_list[i],(strlen(column_name_list[i]) +1)*sizeof(char));
		if(new_record_set->field_name[i] == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
			if(column_name_list) free(column_name_list);
			if(column_type_list) free(column_type_list);
			oph_iostore_destroy_frag_recordset(&new_record_set);
			return OPH_IO_SERVER_MEMORY_ERROR;             
		}  
		//Copy column type
		if(STRCMP(column_type_list[i], "int") == 0 || STRCMP(column_type_list[i], "integer") == 0 || STRCMP(column_type_list[i], "long") == 0 ){
			new_record_set->field_type[i] = OPH_IOSTORE_LONG_TYPE;
		}
		else if(STRCMP(column_type_list[i], "double") == 0 || STRCMP(column_type_list[i], "float") == 0 || STRCMP(column_type_list[i], "real") == 0 ){
			new_record_set->field_type[i] = OPH_IOSTORE_REAL_TYPE;
		}
		else if(STRCMP(column_type_list[i], "char") == 0 || STRCMP(column_type_list[i], "varchar") == 0 || STRCMP(column_type_list[i], "string") == 0 || STRCMP(column_type_list[i], "blob") == 0 ){
			new_record_set->field_type[i] = OPH_IOSTORE_STRING_TYPE;
		}
		else{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_TYPE_ERROR, column_type_list[i]);
			logging(LOG_ERROR, __FILE__, __LINE__,  OPH_IO_SERVER_LOG_QUERY_TYPE_ERROR, column_type_list[i]);
			if(column_name_list) free(column_name_list);
			if(column_type_list) free(column_type_list);
			oph_iostore_destroy_frag_recordset(&new_record_set);
			return OPH_IO_SERVER_EXEC_ERROR;             
		}
	} 
	new_record_set->frag_name = strndup(frag_name,strlen(frag_name)); 

	if(column_name_list) free(column_name_list);
	if(column_type_list) free(column_type_list);

	*output_rs = new_record_set;

	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_drop_frag(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, char *current_db, HASHTBL *query_args)
{
	if (!query_args || !dev_handle || !current_db || !meta_db){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Extract frag_name arg from query args
	char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
	if(frag_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_db_row *db_row = NULL;
	oph_iostore_resource_id frag_id;
    frag_id.id = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_wrlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Retrieve current db
	if(oph_metadb_find_db (*meta_db, current_db, dev_handle->device, &db_row) ||  db_row == NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}
    
	//Remove Frag from MetaDB
	if(oph_metadb_remove_frag (db_row, frag_name, &frag_id)){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}
    
	if(frag_id.id == NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
		return OPH_IO_SERVER_SUCCESS;             
	}

	oph_metadb_db_row *tmp_db_row = NULL; 
	if(oph_metadb_setup_db_struct (db_row->db_name, db_row->device, dev_handle->is_persistent, &(db_row->db_id), db_row->frag_number, &tmp_db_row)) {
		pthread_rwlock_unlock(&rwlock);
        free(frag_id.id);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	tmp_db_row->frag_number--;
	if(oph_metadb_update_db (*meta_db, tmp_db_row)){
		pthread_rwlock_unlock(&rwlock);
        free(frag_id.id);
		oph_metadb_cleanup_db_struct (tmp_db_row);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		oph_metadb_cleanup_db_struct (tmp_db_row);
        free(frag_id.id);
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_cleanup_db_struct (tmp_db_row);

	//Call API to delete Frag
	if(oph_iostore_delete_frag(dev_handle, &(frag_id)) != 0){
        free(frag_id.id);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");	
		return OPH_IO_SERVER_API_ERROR;             
	} 
    free(frag_id.id);
    
	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_create_db(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, HASHTBL *query_args)
{
	if (!query_args || !dev_handle || !meta_db){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Extract db_name arg from query args
	char *db_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DB);
	if(db_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DB);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DB);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_db_row *db = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_wrlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Check if DB already exists
	if(*meta_db != NULL){
		if(oph_metadb_find_db (*meta_db, db_name, dev_handle->device, &db)){
			pthread_rwlock_unlock(&rwlock);          
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
			return OPH_IO_SERVER_METADB_ERROR;             
		}
	}
    
	if(db != NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DB_EXIST_ERROR);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DB_EXIST_ERROR);	
		return OPH_IO_SERVER_SUCCESS;             
	}

	//Setup record to be inserted
	oph_iostore_db_record_set db_record;

	//TODO Change the way DB name is managed;
	db_record.db_name = db_name; 

	//Call API to insert DB
	oph_iostore_resource_id *db_id = NULL;
	if(oph_iostore_put_db(dev_handle, &db_record, &db_id) != 0){
		pthread_rwlock_unlock(&rwlock);        
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_db");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_db");	
		return OPH_IO_SERVER_API_ERROR;             
	} 
    
	//Add DB to MetaDB
	if(oph_metadb_setup_db_struct (db_name, dev_handle->device, dev_handle->is_persistent, db_id, 0, &db)) {
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "DB");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "DB");
		free(db_id->id);
		free(db_id);
		return OPH_IO_SERVER_METADB_ERROR;             
	}
	free(db_id->id);
	free(db_id);
	db_id = NULL;

	if(oph_metadb_add_db (meta_db, db)){
		pthread_rwlock_unlock(&rwlock);
		oph_metadb_cleanup_db_struct (db);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB add");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB add");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}

	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		oph_metadb_cleanup_db_struct (db);
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_cleanup_db_struct (db);

	return OPH_IO_SERVER_SUCCESS;
}

int oph_io_server_run_drop_db(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, HASHTBL *query_args, char **deleted_db)
{
	if (!query_args || !dev_handle || !meta_db || !deleted_db){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Extract DB name from query args
	char *db_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_DB);
	if(db_name == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DB);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_DB);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	oph_metadb_db_row *db = NULL;

	//LOCK FROM HERE
	if(pthread_rwlock_wrlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Check if DB exists
	if(*meta_db != NULL){
		if(oph_metadb_find_db (*meta_db, db_name, dev_handle->device, &db)){
			pthread_rwlock_unlock(&rwlock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
			return OPH_IO_SERVER_METADB_ERROR;             
		}
	}
    
	if(db == NULL){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DB_NOT_EXIST_ERROR);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DB_NOT_EXIST_ERROR);	
		return OPH_IO_SERVER_SUCCESS;             
	}

	//Check if DB is empty; otherwise delete all fragments
	if(db->frag_number != 0 || db->first_frag != NULL){
		while(db->first_frag){
			oph_metadb_frag_row *curr_frag = (oph_metadb_frag_row *)db->first_frag;

			//Call API to delete Frag
			if(oph_iostore_delete_frag(dev_handle, &(curr_frag->frag_id)) != 0){
				pthread_rwlock_unlock(&rwlock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");	
				return OPH_IO_SERVER_API_ERROR;             
			} 

			//Remove Frag from MetaDB
			if(oph_metadb_remove_frag (db, curr_frag->frag_name, NULL)){
				pthread_rwlock_unlock(&rwlock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");	
				return OPH_IO_SERVER_METADB_ERROR;             
			}
		}      
	}

	//Call API to delete DB
	if(oph_iostore_delete_db(dev_handle, &(db->db_id)) != 0){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_db");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_db");	
		return OPH_IO_SERVER_API_ERROR;             
	} 
    
	//Remove DB from MetaDB
	if(oph_metadb_remove_db (meta_db, db_name, dev_handle->device)){
		pthread_rwlock_unlock(&rwlock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB remove");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB remove");	
		return OPH_IO_SERVER_METADB_ERROR;             
	}
    
	//UNLOCK FROM HERE
	if(pthread_rwlock_unlock(&rwlock) != 0){
		pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	*deleted_db = db_name;

	return OPH_IO_SERVER_SUCCESS;
}
