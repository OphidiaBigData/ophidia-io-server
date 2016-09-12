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
//extern pthread_mutex_t metadb_mutex;
extern unsigned short omp_threads;
extern pthread_rwlock_t rwlock;
extern HASHTBL *plugin_table;

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

		//Check if current DB is setted
		//TODO Improve how current DB is found
		if(thread_status->current_db == NULL || thread_status->device == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
			return OPH_IO_SERVER_METADB_ERROR;             
		}

		if(oph_io_server_run_create_as_select(meta_db, dev_handle, thread_status->current_db, args, query_args)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create as Select");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create as Select");	
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

  }
	else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_SELECT) ==0){
		//Execute select fragment query  

		//First delete last result set
		if(thread_status->last_result_set != NULL)	oph_iostore_destroy_frag_recordset(&(thread_status->last_result_set));
		thread_status->last_result_set = NULL;

		//Check if current DB is setted
		//TODO Improve how current DB is found
		if(thread_status->current_db == NULL || thread_status->device == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
			return OPH_IO_SERVER_METADB_ERROR;             
		}

		oph_iostore_frag_record_set *rs = NULL;
		if(oph_io_server_run_select(meta_db, dev_handle, thread_status->current_db, args, query_args, &rs)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Select");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Select");	
			return OPH_IO_SERVER_EXEC_ERROR;        
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

			//Compute size of record_set variable
      thread_status->curr_stmt->size = sizeof(oph_iostore_frag_record *)*(1 + (thread_status->curr_stmt->tot_run ? thread_status->curr_stmt->tot_run : 1));
    }

	unsigned long long row_size = 0;
	if(oph_io_server_run_insert(meta_db, dev_handle, thread_status->curr_stmt->partial_result_set, ((thread_status->curr_stmt->curr_run ? thread_status->curr_stmt->curr_run : 1) - 1), args, query_args, &row_size)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Insert Row");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Insert Row");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	thread_status->curr_stmt->size += row_size;


    if(thread_status->curr_stmt->curr_run == thread_status->curr_stmt->tot_run){
      //THIS BLOCK IS PERFORMED AT THE VERY LAST INSERT

			int ret = _oph_ioserver_query_store_fragment(meta_db, dev_handle, thread_status->current_db, thread_status->curr_stmt->size, &(thread_status->curr_stmt->partial_result_set));

      //Clean global status 
	   if(thread_status->curr_stmt->partial_result_set != NULL) oph_iostore_destroy_frag_recordset(&(thread_status->curr_stmt->partial_result_set));
      free(thread_status->curr_stmt->device);
      free(thread_status->curr_stmt->frag);
      free(thread_status->curr_stmt);
      thread_status->curr_stmt = NULL;

			if(ret){
        pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
        logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
        return OPH_IO_SERVER_EXEC_ERROR;        
      }
    }
  }
	else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_MULTI_INSERT) ==0){
		//Execute insert query 

		//Check if current DB is setted
		//TODO Improve how current DB is found
		if(thread_status->current_db == NULL || thread_status->device == NULL){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
			return OPH_IO_SERVER_METADB_ERROR;             
		}

		thread_status->curr_stmt->size = 0;

		//First check partial result
		if(thread_status->curr_stmt == NULL || thread_status->curr_stmt->partial_result_set == NULL || thread_status->curr_stmt->frag == NULL || thread_status->curr_stmt->device == NULL){
			//Exit 
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_INSERT_STATUS_ERROR);	
			return OPH_IO_SERVER_EXEC_ERROR;        
		}

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
	
	unsigned int insert_num = 0;
	unsigned long long row_size = 0;

	if(oph_io_server_run_multi_insert(meta_db, dev_handle, thread_status, args, query_args, &insert_num, &row_size)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Multi-insert Row");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Multi-insert Row");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	thread_status->curr_stmt->size += row_size;

		if((thread_status->curr_stmt->curr_run == thread_status->curr_stmt->tot_run)){
			//THIS BLOCK IS PERFORMED AT THE VERY LAST INSERT 

			//Check if last statement
			char *final_stmt = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FINAL_STATEMENT);
			if (final_stmt == NULL)
			{
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FINAL_STATEMENT);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FINAL_STATEMENT);	
				return OPH_IO_SERVER_EXEC_ERROR;        
			}
			//If final statement is set, then activate flag
			short int final_stmt_flag = (STRCMP(final_stmt, OPH_QUERY_ENGINE_LANG_VAL_YES) == 0);

			//Add rows inserted by current statement
			thread_status->curr_stmt->mi_prev_rows += (thread_status->curr_stmt->tot_run ? thread_status->curr_stmt->tot_run : 1)*insert_num;				

			//if remainder multi-insert flag is set add fragment
			if(final_stmt_flag == 1){

				//Compute size of record_set variable
      	thread_status->curr_stmt->size += (thread_status->curr_stmt->mi_prev_rows +  1)*sizeof(oph_iostore_frag_record *);

				int ret = _oph_ioserver_query_store_fragment(meta_db, dev_handle, thread_status->current_db, thread_status->curr_stmt->size, &(thread_status->curr_stmt->partial_result_set));

		    //Clean global status 
				thread_status->curr_stmt->mi_prev_rows = 0;
		    if(thread_status->curr_stmt->partial_result_set != NULL) oph_iostore_destroy_frag_recordset(&(thread_status->curr_stmt->partial_result_set));
		    free(thread_status->curr_stmt->device);
		    free(thread_status->curr_stmt->frag);
		    free(thread_status->curr_stmt);
		    thread_status->curr_stmt = NULL;

				if(ret){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
		      logging(LOG_ERROR,__FILE__,__LINE__,OPH_IO_SERVER_LOG_QUERY_FRAG_STORE_ERROR);
		      return OPH_IO_SERVER_EXEC_ERROR;        
		    }
			}
		}
	}
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG) ==0){
    //Execute create fragment query  

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

	//Delete previous statement	
	if(thread_status->curr_stmt != NULL){
		pmesg(LOG_WARNING, __FILE__, __LINE__, OPH_IO_SERVER_LOG_DELETE_OLD_STMT);
		logging(LOG_WARNING, __FILE__, __LINE__, OPH_IO_SERVER_LOG_DELETE_OLD_STMT);	
		if(thread_status->curr_stmt->partial_result_set != NULL) oph_iostore_destroy_frag_recordset(&thread_status->curr_stmt->partial_result_set);
		if(thread_status->curr_stmt->device != NULL) free(thread_status->curr_stmt->device);
		if(thread_status->curr_stmt->frag != NULL) free(thread_status->curr_stmt->frag);
		free(thread_status->curr_stmt);
	}	

    thread_status->curr_stmt= (oph_io_server_running_stmt *)malloc(1*sizeof(oph_io_server_running_stmt));  
    if(thread_status->curr_stmt == NULL)        
    {
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }          
    thread_status->curr_stmt->tot_run = 0; 
    thread_status->curr_stmt->curr_run = 0; 
    thread_status->curr_stmt->partial_result_set = NULL;
    thread_status->curr_stmt->device = NULL;
    thread_status->curr_stmt->frag = NULL;
    thread_status->curr_stmt->size = 0;
    thread_status->curr_stmt->mi_prev_rows = 0;
    
	if(oph_io_server_run_create_empty_frag(meta_db, dev_handle, thread_status->current_db, query_args, &thread_status->curr_stmt->partial_result_set)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create Empty Frag");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create Empty Frag");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

    thread_status->curr_stmt->frag =  (char *)strndup(thread_status->curr_stmt->partial_result_set->frag_name,(strlen(thread_status->curr_stmt->partial_result_set->frag_name) +1)*sizeof(char));
    if(thread_status->curr_stmt->frag == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }  
    thread_status->curr_stmt->device =  (char *)strndup(thread_status->device,(strlen(thread_status->device) +1)*sizeof(char));
    if(thread_status->curr_stmt->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);	
        return OPH_IO_SERVER_MEMORY_ERROR;             
    }  
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_DROP_FRAG) ==0){
    //Execute drop frag 

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

	if(oph_io_server_run_drop_frag(meta_db, dev_handle, thread_status->current_db, query_args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Drop Fragment");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Drop Fragment");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_CREATE_DB) ==0){
    //Execute create database query  
	if(oph_io_server_run_create_db(meta_db, dev_handle, query_args)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create DB");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Create DB");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_DROP_DB) ==0){
    //Execute drop DB 
	char *db_name = NULL;
	if(oph_io_server_run_drop_db(meta_db, dev_handle, query_args, &db_name)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Drop DB");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Drop DB");	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//If DB to delete is default DB, then reset default
	if(thread_status->current_db != NULL && STRCMP(db_name,thread_status->current_db) == 0)  thread_status->current_db = NULL;
  }
  else if(STRCMP(query_oper,OPH_QUERY_ENGINE_LANG_OP_FUNCTION) ==0){
    //Compose query by selecting fields in the right order 

    //Check if current DB is setted
    //TODO Improve how current DB is found
    if(thread_status->current_db == NULL || thread_status->device == NULL){
        pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);
        logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_NO_DB_SELECTED);	
        return OPH_IO_SERVER_METADB_ERROR;             
    }

	//Fetch procedure name
	char *function_name = hashtbl_get(query_args, OPH_QUERY_ENGINE_LANG_ARG_FUNC);
	if (function_name == NULL)
	{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FUNC);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MISSING_QUERY_ARGUMENT, OPH_QUERY_ENGINE_LANG_ARG_FUNC);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}

	//Remove unwanted tokens
	if(_oph_query_parser_remove_query_tokens(function_name)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, function_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_PARSING_ERROR, function_name);   
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	//Switch on procedure
	if(STRCMP(function_name, OPH_IO_SERVER_PROCEDURE_SUBSET) ==0){
		//Call Subset internal procedure
		if(oph_io_server_run_subset_procedure(meta_db, dev_handle, thread_status, args, query_args)){
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Subset Procedure");
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, "Subset Procedure");	
			return OPH_IO_SERVER_EXEC_ERROR;        
		}
	}	
	else{
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, function_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_DISPATCH_ERROR, function_name);	
		return OPH_IO_SERVER_EXEC_ERROR;        
	}
	//Other procedures
	
  }
  else{
      pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_OPERATION_UNKNOWN, query_oper);
      logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_OPERATION_UNKNOWN, query_oper);	
     return OPH_IO_SERVER_EXEC_ERROR;        
  }

  return OPH_IO_SERVER_SUCCESS;
}
