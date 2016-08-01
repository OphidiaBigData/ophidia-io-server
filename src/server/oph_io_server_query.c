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

#include "oph_query_engine.h"
#include "oph_query_plugin_loader.h"
#include "oph_query_plugin_executor.h"
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

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern unsigned short omp_threads;
extern pthread_rwlock_t rwlock;

int oph_io_server_dispatcher(oph_metadb_db_row **meta_db, oph_iostore_handler* dev_handle, oph_io_server_thread_status *thread_status, oph_query_arg **args, HASHTBL *query_args, HASHTBL *plugin_table){
	if (!query_args || !plugin_table || !thread_status || !meta_db){	
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
  	logging(LOG_ERROR, __FILE__, __LINE__,OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);    
    return OPH_IO_SERVER_NULL_PARAM;
	}

	//Retrieve operation type
	char* query_oper = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_OPERATION);
	if (!query_oper)
	{
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, "OPERATION");
    logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, "OPERATION");	
    return OPH_IO_SERVER_EXEC_ERROR;        
	}
  //TODO manage exclusive execution of blocks (delete or read/insert)

  //SWITCH on operation
  if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG_SELECT)==0){
    //Execute create + select fragment query  

    //Extract new frag_name arg from query args
    char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_frag_row *frag = NULL;

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

    //Extract frag_name arg from query args
    //TODO extend mechanism to allow multiple table selections
    char *from_frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }
    char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
    if(fields == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    char **field_list = NULL;
    int field_list_num = 0;
    if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num)){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD); 
        if(field_list) free(field_list);
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    int i = 0, id_col_index = 0, id_col = 0;

    //TODO Improve field selection for other fields
    char *id_query = NULL, *plugin_query = NULL;
    for(i = 0; i < field_list_num; i++){
      if(strstr(field_list[i],OPH_NAME_ID) != NULL){
        id_query = field_list[i];
		id_col = i;
      }
      if(strstr(field_list[i],OPH_NAME_MEASURE) != NULL){
        plugin_query = field_list[i];
      }
    }
    if(!id_query || !plugin_query){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_FIELD); 
        if(field_list) free(field_list);
        return OPH_IO_SERVER_EXEC_ERROR;        
    }
    if(field_list) free(field_list);

    // Check limit clauses
    long long limit=0, offset=0;
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
				limit=strtoll(limit_list[0],NULL,10);
				break;
			case 2:
				offset=strtoll(limit_list[0],NULL,10);
				limit=strtoll(limit_list[1],NULL,10);
				break;
			default:
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT); 
				free(limit_list);
				return OPH_IO_SERVER_EXEC_ERROR;
		}
		free(limit_list);
		if (limit<0) limit=0;
		if (offset<0) offset=0;
	}
    }

    //TODO read other clauses

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

    //Check if Frag exists
     if(oph_metadb_find_frag (db_row, from_frag_name, &frag)){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

    if(frag == NULL){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_iostore_frag_record_set *record_set = NULL;

    //Call API to read Frag
    if(oph_iostore_get_frag(dev_handle, &(frag->frag_id), &record_set) != 0){
        pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");	
        return OPH_IO_SERVER_API_ERROR;             
    } 
    
    //UNLOCK FROM HERE
    if(pthread_rwlock_unlock(&rwlock) != 0){
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    //Simulate read fragment
/*    //Build sample fragment with double type
    oph_iostore_frag_record_set *record_set = NULL;
    if(oph_iostore_create_sample_frag(10, 10, &record_set)){
      pmesg(LOG_DEBUG,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "GET FRAG");
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "GET FRAG");
      oph_iostore_destroy_frag_recordset(&record_set);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }			

    long long r, a;
    //Show fragment data
    for(r=0; r < 10; r++){
      //Fill array				
      for(a = 0; a < 10; a++)
	      printf("%f - ", *((double*)((char*)record_set->record_set[r]->field[1] + a*sizeof(double))));
      printf("\n");
    }
*/
    
	  //Define global variables
    oph_udf_arg *oph_args = NULL;	
	  oph_plugin *plugin = NULL;
    unsigned int arg_count = 0;
    unsigned long l = 0;

	  //Select plugin and set arguments
    //TODO improve this section

    oph_iostore_frag_record_set *rs = NULL;
    if(!STRCMP(plugin_query,OPH_NAME_MEASURE)){
      //Just pass fragment
      if(oph_iostore_copy_frag_record_set_limit(record_set, &rs, limit, offset)){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        if(field_list) free(field_list);
	      return OPH_IO_SERVER_PARSE_ERROR;
      }
    }
    else
    {
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"EXECUTING %s function\n", plugin_query);

	if (!STRCMP(id_query,OPH_NAME_ID)) id_col_index = id_col;
	// else // TODO: consider a query applied to OPH_NAME_ID
	
	if(oph_parse_plugin(plugin_query, plugin_table, record_set, &plugin, &oph_args, &arg_count)){
		//pthread_rwlock_unlock(&rwlock);
		for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
		free(oph_args);
		if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&record_set);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, plugin_query);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, plugin_query);	
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	if(oph_execute_plugin(plugin, oph_args, arg_count, &rs, field_list_num, id_col_index, 1, limit, offset, omp_threads)){
		//pthread_rwlock_unlock(&rwlock);
		for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
		free(oph_args);
		if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&record_set);
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, plugin_query);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, plugin_query);	
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
	free(oph_args);
    }
    
    rs->frag_name = strndup(frag_name,strlen(frag_name));
    if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&record_set);

    //TODO manage fragment struct creation
    //LOCK FROM HERE
    if(pthread_rwlock_wrlock(&rwlock) != 0){
      oph_iostore_destroy_frag_recordset(&rs);
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    //Call API to insert Frag
    oph_iostore_resource_id *frag_id = NULL;
    if(oph_iostore_put_frag(dev_handle, rs, &frag_id) != 0){
          pthread_rwlock_unlock(&rwlock);        
        oph_iostore_destroy_frag_recordset(&rs);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");	
        if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&rs);
        return OPH_IO_SERVER_API_ERROR;             
    } 
    
    if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&rs);

    frag = NULL;

    //Add Frag to MetaDB
    //TODO compute frag size
    if(oph_metadb_setup_frag_struct (frag_name, thread_status->device, dev_handle->is_persistent, &(db_row->db_id), frag_id, 0, &frag)) {
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
        free(frag_id->id);
        free(frag_id);
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

    //UNLOCK FROM HERE
    if(pthread_rwlock_unlock(&rwlock) != 0){
      oph_metadb_cleanup_db_struct (tmp_db_row);
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_cleanup_db_struct (tmp_db_row);

  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_SELECT) ==0){
    //Execute select fragment query  

    //First delete last result set
    if(thread_status->last_result_set != NULL)	oph_iostore_destroy_frag_recordset(&(thread_status->last_result_set));
    thread_status->last_result_set = NULL;

    //Extract frag_name arg from query args
    //TODO extend mechanism to allow multiple table selections
    char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FROM);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FROM);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_frag_row *frag = NULL;

    //Check if current DB is setted
	//TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }


    char **field_list = NULL;
    int field_list_num = 0;    
    //Fields section
    char *fields = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
    if (fields == NULL)
    {
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }
    if(oph_query_parse_multivalue_arg (fields, &field_list, &field_list_num) || !field_list_num){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FIELD);	
        if(field_list) free(field_list);
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_db_row *db_row = NULL;

    //LOCK FROM HERE
    if(pthread_rwlock_rdlock(&rwlock) != 0){
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_LOCK_ERROR);
      if(field_list) free(field_list);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    //Retrieve current db
    if(oph_metadb_find_db (*meta_db, thread_status->current_db, thread_status->device, &db_row) ||  db_row == NULL){
          pthread_rwlock_unlock(&rwlock);
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "DB find");	
        if(field_list) free(field_list);
        return OPH_IO_SERVER_METADB_ERROR;             
    }

    //Check if Frag exists
    if(oph_metadb_find_frag (db_row, frag_name, &frag)){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
        if(field_list) free(field_list);
        return OPH_IO_SERVER_METADB_ERROR;             
    }
    
    if(frag == NULL){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR);	
        if(field_list) free(field_list);
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    // Chek limit clause
    long long limit=0, offset=0;
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
        if(field_list) free(field_list);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if(limit_list)
	{
		switch (limit_list_num)
		{
			case 1:
				limit=strtoll(limit_list[0],NULL,10);
				break;
			case 2:
				offset=strtoll(limit_list[0],NULL,10);
				limit=strtoll(limit_list[1],NULL,10);
				break;
			default:
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_PARSE_ERROR, OPH_QUERY_ENGINE_LANG_ARG_LIMIT); 
				free(limit_list);
				free(field_list);
				return OPH_IO_SERVER_EXEC_ERROR;
		}
		free(limit_list);
		if (limit<0) limit=0;
		if (offset<0) offset=0;
	}
    }

    //TODO read other clauses

    oph_iostore_frag_record_set *orig_record_set = NULL;

    //Call API to read Frag
    if(oph_iostore_get_frag(dev_handle, &(frag->frag_id), &orig_record_set) != 0){
          pthread_rwlock_unlock(&rwlock);        
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "get_frag");	
        if(field_list) free(field_list);
         return OPH_IO_SERVER_API_ERROR;             
    } 
    
    //UNLOCK FROM HERE
    if(pthread_rwlock_unlock(&rwlock) != 0){
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
        if(field_list) free(field_list);
      return OPH_IO_SERVER_EXEC_ERROR;
    }
    
	//Define global variables
	oph_udf_arg *oph_args[field_list_num];	
	oph_plugin *plugin[field_list_num];
	unsigned int arg_count[field_list_num];
	unsigned long l = 0;

	//Select plugin and set arguments

	oph_iostore_frag_record_set *rs = NULL;
	//TODO Check what fields are selected

	int i;
	for (i=0; i<field_list_num; ++i)
	{
		oph_args[i] = NULL;
		plugin[i] = NULL;
		arg_count[i] = 0;
	}

	//Prepare input record set

	//Count number of rows to compute
	long long j, total_row_number = 0, row_number = 0;
	while(orig_record_set->record_set[total_row_number]) total_row_number++;
	if (!offset || (offset<total_row_number))
	{
		j=offset;
		while(orig_record_set->record_set[j] && (!limit || (row_number<limit))) { j++; row_number++; }
	}

	oph_iostore_frag_record_set *record_set = NULL;

    // Check where clause
	unsigned long long id;
    char *where = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_WHERE);
    if(where)
    {
		if( (oph_iostore_copy_frag_record_set_only(orig_record_set, &record_set, 0, 0) != 0))
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			if(field_list) free(field_list);
			if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		record_set->frag_name = (char *)strndup(orig_record_set->frag_name, strlen(orig_record_set->frag_name));
		if(record_set->frag_name == NULL)
		{
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			if(field_list) free(field_list);
			if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
			oph_iostore_destroy_frag_recordset_only(&record_set);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}


		for(i = 0; i < record_set->field_num; i++){
			if (!STRCMP(record_set->field_name[i],OPH_NAME_ID))
			{
				id = offset + 1;
				oph_query_expr_node *e; 
				if(oph_query_expr_get_ast(where, &e) != 0){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, where);
					if(field_list) free(field_list);
					if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
					oph_iostore_destroy_frag_recordset_only(&record_set);
					return OPH_IO_SERVER_PARSE_ERROR;
				}

				oph_query_expr_symtable *table;
				if(oph_query_expr_create_symtable(&table, 1)){
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
					oph_query_expr_delete_node(e);
					if(field_list) free(field_list);
					if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
					oph_iostore_destroy_frag_recordset_only(&record_set);
					return OPH_IO_SERVER_MEMORY_ERROR;
				}

				double res;
				long long k = 0;
				for (j = 0; j < total_row_number; j++, id++)
				{
					oph_query_expr_add_variable("id_dim",id,table);    
					if(e != NULL && !oph_query_expr_eval_expression(e,&res,table)) {
						//Create index record
						if(res){
							record_set->record_set[k++] = orig_record_set->record_set[j];  
							pmesg(LOG_DEBUG, __FILE__, __LINE__, "id_dim = %d has been considered in where condition\n", id);
							logging(LOG_DEBUG, __FILE__, __LINE__, "id_dim = %d has been considered in where condition\n", id);
							//In case of limit break before the total number of rows
							if( k == row_number ) break;
						}
					}
					else
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR,where);
						oph_query_expr_delete_node(e);
						oph_query_expr_destroy_symtable(table);
						if(field_list) free(field_list);
						if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
						oph_iostore_destroy_frag_recordset_only(&record_set);
						return OPH_IO_SERVER_PARSE_ERROR;
					}
				}
				//Update row number with actual value
				row_number = k;			

				oph_query_expr_delete_node(e);
				oph_query_expr_destroy_symtable(table);
				break;
			}
		}
    }
	else{
		record_set = orig_record_set; 
	}

	int error = 0, aggregation = 0;

	//If recordset is not empty proceed
	if(record_set->record_set[0] != NULL){

		//Prepare output record set
		for (i=0; i<field_list_num; ++i)
		{
			pmesg(LOG_DEBUG,__FILE__,__LINE__,"EXECUTING function %s\n", field_list[i]);
			if(oph_parse_plugin(field_list[i], plugin_table, record_set, &plugin[i], &oph_args[i], &arg_count[i]))
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, field_list[i]);	
				error = OPH_IO_SERVER_PARSE_ERROR;
				break;
			}
			if (plugin[i] && (plugin[i]->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE)) aggregation = 1;
		}
		if (!error)
		{
			//Create output record set
			if (aggregation) oph_iostore_create_frag_recordset(&rs, 1, field_list_num);
			else oph_iostore_create_frag_recordset(&rs, row_number, field_list_num);

			rs->field_num = field_list_num;
			for (i=0; i<field_list_num-1; ++i)
			{
				rs->field_name[i] = strdup(OPH_NAME_ID);
				rs->field_type[i] = OPH_IOSTORE_LONG_TYPE;
			}
			rs->field_name[i] = strdup(OPH_NAME_MEASURE);
			rs->field_type[i] = plugin[i] ? plugin[i]->plugin_return : OPH_IOSTORE_STRING_TYPE;

			oph_iostore_frag_record **input = record_set->record_set, **output = rs->record_set;

			for (i=0; i<field_list_num; ++i)
			{
				if (plugin[i])
				{		
					if(oph_execute_plugin2(plugin[i], oph_args[i], arg_count[i], rs, field_list_num, i, row_number, offset, omp_threads))
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);
						logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ENGINE_ERROR, field_list[i]);	
						error = OPH_IO_SERVER_EXEC_ERROR;     
						break;   
					}
				}
				else if (!STRCMP(field_list[i],OPH_NAME_ID))
				{
					id = offset + 1;
					for (j = 0; j < row_number; j++, id++)
					{
						if (i==1) id=1;
						output[j]->field[i] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
						output[j]->field_length[i] = sizeof(unsigned long long);
					}
				}
				else if (!STRCMP(field_list[i],OPH_NAME_MEASURE))
				{
					id = offset;
					for (j = 0; j < row_number; j++, id++)
					{
						output[j]->field[i] = input[id]->field_length[i] ? memdup(input[id]->field[i], input[id]->field_length[i]) : NULL;
						output[j]->field_length[i] = input[id]->field_length[i];
					}
				}
				else // Copy data
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);
					logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported execution of %s\n", field_list[i]);	
					error = OPH_IO_SERVER_EXEC_ERROR;
					break;
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

		for (i=0; i<field_list_num-1; ++i)
		{
			rs->field_name[i] = strdup(OPH_NAME_ID);
			rs->field_type[i] = OPH_IOSTORE_LONG_TYPE;
		}
		rs->field_name[i] = strdup(OPH_NAME_MEASURE);
		rs->field_type[i] = plugin[i] ? plugin[i]->plugin_return : OPH_IOSTORE_STRING_TYPE;

	}

	for (i=0; i<field_list_num; ++i)
	{
		for(l = 0; l < arg_count[i]; l++) oph_free_udf_arg(&oph_args[i][l]);
		free(oph_args[i]);
	}

	//If where clause was applied record set is not the same as original
	if(record_set != orig_record_set) oph_iostore_destroy_frag_recordset_only(&record_set);

	if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&orig_record_set);
	if(field_list) free(field_list);

	if (error)
	{
		if (rs) oph_iostore_destroy_frag_recordset(&rs);
		return error;
	}

	thread_status->last_result_set = rs;
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_INSERT) ==0){
    //Execute insert query 

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

    //First check partial result
    if(thread_status->curr_stmt == NULL || thread_status->curr_stmt->partial_result_set == NULL || thread_status->curr_stmt->frag == NULL || thread_status->curr_stmt->device == NULL){
      //Exit 
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);	
      return OPH_IO_SERVER_EXEC_ERROR;        
    }
    oph_iostore_frag_record_set *tmp = thread_status->curr_stmt->partial_result_set;

    //Read frag_name
    char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    //Check if fragment corresponds to the one created previously
    if(STRCMP(frag_name, thread_status->curr_stmt->frag) == 1 || STRCMP(thread_status->curr_stmt->device, thread_status->device) == 1){
      //Exit 
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);	
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    if(thread_status->curr_stmt->curr_run == 1 || (thread_status->curr_stmt->curr_run == 0 && thread_status->curr_stmt->tot_run == 0)){
       //0- For first time: create record_set array - Also executed when it is a single run
      tmp->record_set = (oph_iostore_frag_record **)calloc(1 + (thread_status->curr_stmt->tot_run ? thread_status->curr_stmt->tot_run : 1), sizeof(oph_iostore_frag_record *));
      if(tmp->record_set == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        return OPH_IO_SERVER_MEMORY_ERROR;             
      }

      //TODO Compute record set size
      //thread_status->curr_stmt->size
    }

    char **field_list = NULL, **value_list = NULL;
    int field_list_num = 0, value_list_num = 0;    
    int i = 0, j = 0;
    //Fields sectionn
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

    if(value_list_num != field_list_num || value_list_num != tmp->field_num){
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_MULTIVAL_ARGS_DIFFER, OPH_QUERY_ENGINE_LANG_OP_INSERT); 
      if(field_list) free(field_list);
      if(value_list) free(value_list);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }

    //TODO Extend management of ?  in order to evaluate also ? inside more complex constructs

    //Created record struct
    oph_iostore_frag_record *new_record = NULL;     
    if(oph_iostore_create_frag_record(&new_record, tmp->field_num) == 1){
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
      if(field_list) free(field_list);
      if(value_list) free(value_list);
      return OPH_IO_SERVER_MEMORY_ERROR;             
    }

    int arg_count = 0;
    unsigned long long row_size = sizeof(oph_iostore_frag_record);
    long long tmpL = 0;
    double tmpD = 0;

    for(i = 0; i < tmp->field_num; i++){
      //For each field check column name correspondence
      if(STRCMP(field_list[i], tmp->field_name[i]) == 1){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_COLUMN_ERROR); 
        if(field_list) free(field_list);
        if(value_list) free(value_list);
        oph_iostore_destroy_frag_record(&new_record, tmp->field_num);
        return OPH_IO_SERVER_EXEC_ERROR;        
      }

      //For each value check if argument contains ? and substitute with arg[i]
	//TODO make this a function
      if(value_list[i][0] == OPH_QUERY_ENGINE_LANG_ARG_REPLACE){
        //Check column type with arg_type ...  
        /*if(args[arg_count]->arg_type != tmp->field_type)
          //EXIT
        }*/

	if (memory_check())
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
		if(field_list) free(field_list);
		if(value_list) free(value_list);
		oph_iostore_destroy_frag_record(&new_record, tmp->field_num);
		return OPH_IO_SERVER_MEMORY_ERROR;
        }

        new_record->field_length[i] = args[arg_count]->arg_length;
        row_size += (new_record->field_length[i] + sizeof(new_record->field_length[i]) + sizeof(new_record->field[i]));
        new_record->field[i] = (void *)memdup(args[arg_count]->arg,new_record->field_length[i]);
        if(new_record->field[i] == NULL){
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
          logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
          if(field_list) free(field_list);
          if(value_list) free(value_list);
          oph_iostore_destroy_frag_record(&new_record, tmp->field_num);
          return OPH_IO_SERVER_MEMORY_ERROR;             
        }
        arg_count++;
      }
      else{
        //No substitution occurs, use directly strings

        //Check for string type	OPH_IOSTORE_STRING_TYPE
        if(value_list[i][0] == 	OPH_QUERY_ENGINE_LANG_STRING_DELIMITER)
        {
          new_record->field_length[i] = strlen(value_list[i]) + 1;
          new_record->field[i] = (char *)strndup(value_list[i],new_record->field_length[i]);
        }
        else
        {
          for(j = 0; j < (int)strlen(value_list[i]); j++ )
          {
            //Check for string type	OPH_IOSTORE_REAL_TYPE
            if(value_list[i][j] == OPH_QUERY_ENGINE_LANG_REAL_NUMBER_POINT)
            {
              tmpD = (double)strtod(value_list[i], NULL);
              new_record->field_length[i] = sizeof(double);
              new_record->field[i] = (void *)memdup((const void *)&tmpD,new_record->field_length[i]);
              break;
            }
          }
          //Check for string type	OPH_IOSTORE_LONG_TYPE
          if(j == (int)strlen(value_list[i]))
          {
            tmpL = (long long)strtoll(value_list[i], NULL, 10);
            new_record->field_length[i] = sizeof(long long);
            new_record->field[i] = (void *)memdup((const void *)&tmpL,new_record->field_length[i]);
          }
        }
        row_size += (new_record->field_length[i] + sizeof(new_record->field_length[i]) + sizeof(new_record->field[i]));
	      if(new_record->field[i] == NULL){
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
          logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
          if(field_list) free(field_list);
          if(value_list) free(value_list);
          oph_iostore_destroy_frag_record(&new_record, tmp->field_num);
          return OPH_IO_SERVER_MEMORY_ERROR;             
        }
      }
    }
    if(field_list) free(field_list);
    if(value_list) free(value_list);

    //Add record to partial record set
    tmp->record_set[(thread_status->curr_stmt->curr_run ? thread_status->curr_stmt->curr_run : 1) - 1] = new_record;
    //Update current record size
    thread_status->curr_stmt->size += row_size;
     
    if(thread_status->curr_stmt->curr_run == thread_status->curr_stmt->tot_run){
      //THIS BLOCK IS PERFORMED AT THE VERY LAST INSERT

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
      if(oph_iostore_put_frag(dev_handle, tmp, &frag_id) != 0){
          pthread_rwlock_unlock(&rwlock);
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");
          logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "put_frag");	
          return OPH_IO_SERVER_API_ERROR;             
      } 
      
      oph_metadb_frag_row *frag = NULL;

      //Add Frag to MetaDB
      if(oph_metadb_setup_frag_struct (thread_status->curr_stmt->frag, thread_status->device, dev_handle->is_persistent, &(db_row->db_id), frag_id, thread_status->curr_stmt->size, &frag)) {
          pthread_rwlock_unlock(&rwlock);
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
          logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
          free(frag_id->id);
          free(frag);
          return OPH_IO_SERVER_METADB_ERROR;             
      }
      //If device is transient then block record from being deleted
      if(!dev_handle->is_persistent) tmp = thread_status->curr_stmt->partial_result_set = NULL;

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
      //UNLOCK FROM HERE
      if(pthread_rwlock_unlock(&rwlock) != 0){
        oph_metadb_cleanup_db_struct (tmp_db_row);
        pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
        logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
        return OPH_IO_SERVER_EXEC_ERROR;        
      }

      oph_metadb_cleanup_db_struct (tmp_db_row);

      //Clean global status 
      if(dev_handle->is_persistent) oph_iostore_destroy_frag_recordset(&(thread_status->curr_stmt->partial_result_set));
      free(thread_status->curr_stmt->device);
      free(thread_status->curr_stmt->frag);
      free(thread_status->curr_stmt);
      thread_status->curr_stmt = NULL;
    }
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG) ==0){
    //Execute create fragment query  

    //Extract frag_name arg from query args
    char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_frag_row *frag = NULL;

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

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
    thread_status->curr_stmt= (oph_io_server_running_stmt *)malloc(1*sizeof(oph_io_server_running_stmt));  
    if(thread_status->curr_stmt == NULL)        
    {
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        if(column_name_list) free(column_name_list);
        if(column_type_list) free(column_type_list);
        oph_iostore_destroy_frag_recordset(&new_record_set);        
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }          
    thread_status->curr_stmt->tot_run = 0; 
    thread_status->curr_stmt->curr_run = 0; 
    thread_status->curr_stmt->partial_result_set = new_record_set;
    thread_status->curr_stmt->device = NULL;
    thread_status->curr_stmt->frag = NULL;
    thread_status->curr_stmt->size = 0;
    
    oph_iostore_frag_record_set *tmp = thread_status->curr_stmt->partial_result_set;

    thread_status->curr_stmt->frag =  (char *)strndup(frag_name,(strlen(frag_name) +1)*sizeof(char));
    if(thread_status->curr_stmt->frag == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        if(column_name_list) free(column_name_list);
        if(column_type_list) free(column_type_list);
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }  
    thread_status->curr_stmt->device =  (char *)strndup(thread_status->device,(strlen(thread_status->device) +1)*sizeof(char));
    if(thread_status->curr_stmt->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        if(column_name_list) free(column_name_list);
        if(column_type_list) free(column_type_list);
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }  
    
    int i = 0;
    for(i = 0; i < column_name_num; i++){
      //Copy column name
      tmp->field_name[i] = (char *)strndup(column_name_list[i],(strlen(column_name_list[i]) +1)*sizeof(char));
      if(tmp->field_name[i] == NULL){
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
          logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
          if(column_name_list) free(column_name_list);
          if(column_type_list) free(column_type_list);
          return OPH_IO_SERVER_MEMORY_ERROR;             
      }  
      //Copy column type
      if(STRCMP(column_type_list[i], "int") == 0 || STRCMP(column_type_list[i], "integer") == 0 || STRCMP(column_type_list[i], "long") == 0 ){
        tmp->field_type[i] = OPH_IOSTORE_LONG_TYPE;
      }
      else if(STRCMP(column_type_list[i], "double") == 0 || STRCMP(column_type_list[i], "float") == 0 || STRCMP(column_type_list[i], "real") == 0 ){
        tmp->field_type[i] = OPH_IOSTORE_REAL_TYPE;
      }
      else if(STRCMP(column_type_list[i], "char") == 0 || STRCMP(column_type_list[i], "varchar") == 0 || STRCMP(column_type_list[i], "string") == 0 || STRCMP(column_type_list[i], "blob") == 0 ){
        tmp->field_type[i] = OPH_IOSTORE_STRING_TYPE;
      }
      else{
          pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_TYPE_ERROR, column_type_list[i]);
          logging(LOG_ERROR, __FILE__, __LINE__,  OPH_IO_SERVER_LOG_QUERY_TYPE_ERROR, column_type_list[i]);
          if(column_name_list) free(column_name_list);
          if(column_type_list) free(column_type_list);
          return OPH_IO_SERVER_EXEC_ERROR;             
      }
    } 
    tmp->frag_name = strndup(frag_name,strlen(frag_name)); 

    if(column_name_list) free(column_name_list);
    if(column_type_list) free(column_type_list);

/* //Call API to insert Frag
    //oph_iostore_add_frag

    oph_iostore_resource_id frag_id;
    frag_id.id_length = 0;
    frag_id.id = NULL; 

    //Add Frag to MetaDB
    if(oph_metadb_setup_frag_struct (frag_name, thread_status->device, thread_status->current_db->db_id, frag_id, 0, &frag)) {
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "frag");
        return OPH_IO_SERVER_METADB_ERROR;             
    }
        if(oph_metadb_add_frag (thread_status->current_db, frag)){
        
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "frag add");	
        return OPH_IO_SERVER_METADB_ERROR;             
    }
    */
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_DROP_FRAG) ==0){
    //Execute drop frag 

    //Extract frag_name arg from query args
    char *frag_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
    if(frag_name == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FRAG);	
        return OPH_IO_SERVER_EXEC_ERROR;        
    }

    oph_metadb_frag_row *frag = NULL;

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

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

    //Check if Frag exists
    if(oph_metadb_find_frag (db_row, frag_name, &frag)){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR, "Frag find");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_FRAG_NOT_EXIST_ERROR, "Frag find");	
        return OPH_IO_SERVER_METADB_ERROR;             
    }
    
    if(frag == NULL){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");
        logging(LOG_DEBUG, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag find");	
        return OPH_IO_SERVER_SUCCESS;             
    }

    //Call API to delete Frag
    if(oph_iostore_delete_frag(dev_handle, &(frag->frag_id)) != 0){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_IO_API_ERROR, "delete_frag");	
        return OPH_IO_SERVER_API_ERROR;             
    } 
    
    //Remove Frag from MetaDB
    if(oph_metadb_remove_frag (db_row, frag_name)){
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "Frag remove");	
        return OPH_IO_SERVER_METADB_ERROR;             
    }
    
    oph_metadb_db_row *tmp_db_row = NULL; 
    if(oph_metadb_setup_db_struct (db_row->db_name, db_row->device, dev_handle->is_persistent, &(db_row->db_id), db_row->frag_number, &tmp_db_row)) {
          pthread_rwlock_unlock(&rwlock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ALLOC_ERROR, "db");
        return OPH_IO_SERVER_METADB_ERROR;             
    }

    tmp_db_row->frag_number--;
    if(oph_metadb_update_db (*meta_db, tmp_db_row)){
          pthread_rwlock_unlock(&rwlock);
      oph_metadb_cleanup_db_struct (tmp_db_row);
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_METADB_ERROR, "db update");	
      return OPH_IO_SERVER_METADB_ERROR;             
    }

    //UNLOCK FROM HERE
    if(pthread_rwlock_unlock(&rwlock) != 0){
      oph_metadb_cleanup_db_struct (tmp_db_row);
      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_UNLOCK_ERROR);
      return OPH_IO_SERVER_EXEC_ERROR;        
    }
    
    oph_metadb_cleanup_db_struct (tmp_db_row);
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_CREATE_DB) ==0){
    //Execute create database query  

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
      if(oph_metadb_find_db (*meta_db, db_name, thread_status->device, &db)){
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
    if(oph_metadb_setup_db_struct (db_name, thread_status->device, dev_handle->is_persistent, db_id, 0, &db)) {
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
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_DROP_DB) ==0){
    //Execute drop DB 

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
      if(oph_metadb_find_db (*meta_db, db_name, thread_status->device, &db)){
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
        if(oph_metadb_remove_frag (db, curr_frag->frag_name)){
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
    
    //If DB to delete is default DB, then reset default
    if(thread_status->current_db != NULL && STRCMP(db->db_name,thread_status->current_db) == 0)  thread_status->current_db = NULL;

    //Remove DB from MetaDB
    if(oph_metadb_remove_db (meta_db, db_name, thread_status->device)){
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
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_FUNCTION) ==0){
    //Compose query by selecting fields in the right order 

    //TODO Implement function management

  }
  else{
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_OPERATION_UNKNOWN, query_oper);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_OPERATION_UNKNOWN, query_oper);	
     return OPH_IO_SERVER_EXEC_ERROR;        
  }

  return OPH_IO_SERVER_SUCCESS;
}
