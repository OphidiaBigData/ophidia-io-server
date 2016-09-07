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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <debug.h>

#include <pthread.h>

#include "oph_iostorage_interface.h"
#include "oph_server_utility.h"
#include "oph_query_engine_language.h"

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern pthread_rwlock_t rwlock;

//Procedure OPH_IO_SERVER_PROCEDURE_SUBSET
int oph_io_server_run_subset_procedure(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_query_arg **args, HASHTBL *query_args)
{
	if (!query_args || !dev_handle || !thread_status || !meta_db){	
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
		return OPH_IO_SERVER_NULL_PARAM;
	}

	//Fetch function arguments
	char *function_args = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_ARG);
	if (function_args == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_ARG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_ARG);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	char **func_args_list = NULL;
	int func_args_num = 0;
	if(oph_query_parse_multivalue_arg (function_args, &func_args_list, &func_args_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_ARG);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_ARG); 
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	
	//Check number of arguments
	if(func_args_num < 4 || func_args_num > 5){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_WRONG_PROCEDURE_ARG, OPH_IO_SERVER_PROCEDURE_SUBSET);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_WRONG_PROCEDURE_ARG, OPH_IO_SERVER_PROCEDURE_SUBSET); 
		free(func_args_list);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Argument 0 should be a string with optionally a hierarchical name (a.b)

	//Remove leading/trailing spaces and match string
	if(oph_query_check_procedure_string(&(func_args_list[0]))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[0]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[0]);	
		free(func_args_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	//Extract hierachical info
	char **in_frag_components = NULL;
	int in_frag_components_num = 0;
	if(oph_query_parse_hierarchical_args (func_args_list[0], &in_frag_components, &in_frag_components_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, func_args_list[0]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, func_args_list[0]);	
		free(func_args_list);
		return OPH_IO_SERVER_PARSE_ERROR;        
	}

	char *in_frag_name = NULL;
	//If DB is setted in frag name
	if(in_frag_components_num > 1){
		//Check if db is the one used by the query
		if(STRCMP(thread_status->current_db,in_frag_components[0]) != 0){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);	
			free(in_frag_components);
			free(func_args_list);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		in_frag_name = in_frag_components[1];
	}
	else{
		in_frag_name = in_frag_components[0];
	}

	//Argument 1 should be a number
	long long id_start = 0;

	//Test number presence
    char* end = NULL;
    errno = 0;
    id_start = strtoll((char *)func_args_list[1], &end, 10);
    if ((errno != 0) || (end == (char *)func_args_list[1]) || (*end != 0)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, func_args_list[1]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_LONG, func_args_list[1]);	
		free(in_frag_components);
		free(func_args_list);
		return OPH_IO_SERVER_EXEC_ERROR;		
    }

	//Argument 2 should be a string

	//Remove leading/trailing spaces and match string
	if(oph_query_check_procedure_string(&(func_args_list[2]))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[2]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[2]);	
		free(in_frag_components);
		free(func_args_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	//Argument 3 should be a string with optionally a hierarchical name (a.b)

	//Remove leading/trailing spaces and match string
	if(oph_query_check_procedure_string(&(func_args_list[3]))){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[3]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[3]);	
		free(in_frag_components);
		free(func_args_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	//Extract hierachical info
	char **out_frag_components = NULL;
	int out_frag_components_num = 0;
	if(oph_query_parse_hierarchical_args (func_args_list[3], &out_frag_components, &out_frag_components_num)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, func_args_list[3]);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_HIERARCHY_PARSE_ERROR, func_args_list[3]);	
		free(in_frag_components);
		free(func_args_list);
		return OPH_IO_SERVER_PARSE_ERROR;        
	}

	char *out_frag_name = NULL;
	//If DB is setted in frag name
	if(out_frag_components_num > 1){
		//Check if db is the one used by the query
		if(STRCMP(thread_status->current_db,out_frag_components[0]) != 0){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_WRONG_DB_SELECTED);	
			free(in_frag_components);
			free(out_frag_components);
			free(func_args_list);
			return OPH_IO_SERVER_METADB_ERROR;             
		}
		out_frag_name = out_frag_components[1];
	}
	else{
		out_frag_name = out_frag_components[0];
	}

	//Argument 4, if available, should be a string
	short int where_flag = 0;
	if(func_args_num == 5){
		//Remove leading/trailing spaces and match string
		if(oph_query_check_procedure_string(&(func_args_list[4]))){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[4]);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_ARG_NO_STRING, func_args_list[4]);	
			free(in_frag_components);
			free(out_frag_components);
			free(func_args_list);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if((func_args_list[4])[0] != 0) where_flag = 1;
	}

	char new_query[OPH_IO_SERVER_BUFFER] = {'\0'};

	//Check if where is enabled
	if(where_flag){
		snprintf(new_query,OPH_IO_SERVER_BUFFER-1,"operation=create_frag_select;frag_name=%s;field=id_dim|%s;select_alias=|measure;from=%s;where=%s;sequential_id=%lld;", out_frag_name, func_args_list[2], in_frag_name, func_args_list[4], id_start);
	}
	else{
		snprintf(new_query,OPH_IO_SERVER_BUFFER-1,"operation=create_frag_select;frag_name=%s;field=id_dim|%s;select_alias=|measure;from=%s;", out_frag_name, func_args_list[2], in_frag_name);
	}

	free(in_frag_components);
	free(out_frag_components);
	free(func_args_list);

	//Parse internal query
	HASHTBL *procedure_query_args = NULL;
	if(oph_query_parser(new_query, &procedure_query_args)){
		pmesg(LOG_WARNING,__FILE__,__LINE__,"Unable to run query\n");
		logging(LOG_WARNING,__FILE__,__LINE__,"Unable to run query\n");
	}

	//Run create as select block
	if(oph_io_server_run_create_as_select(meta_db, dev_handle, thread_status->current_db, args, procedure_query_args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create as Select");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create as Select");	
		hashtbl_destroy(procedure_query_args);
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	hashtbl_destroy(procedure_query_args);

	return OPH_IO_SERVER_SUCCESS;
}
