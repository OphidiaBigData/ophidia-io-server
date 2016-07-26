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

#include "oph_query_engine.h"
#include "oph_query_plugin_loader.h"
#include "oph_query_plugin_executor.h"
#include "oph_query_engine_log_error_codes.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>

#include <ctype.h>
#include <unistd.h>

#include <debug.h>

extern int msglevel;

int oph_query_engine_start(HASHTBL **plugin_table){
	if(!plugin_table){
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
  	logging(LOG_ERROR, __FILE__, __LINE__,OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);    
    return OPH_QUERY_ENGINE_NULL_PARAM;
  }

	*plugin_table = NULL;
	if(oph_load_plugins (plugin_table)){
		oph_unload_plugins (*plugin_table);
    *plugin_table = NULL;
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_LOAD_ERROR);
  	logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_LOAD_ERROR);    
    return OPH_QUERY_ENGINE_ERROR;
	}

  return OPH_QUERY_ENGINE_SUCCESS;
}

int oph_query_engine_run(HASHTBL *plugin_table, const char *plugin_string, oph_iostore_frag_record_set *record_set, oph_iostore_frag_record_set **result_set){
	if(!plugin_table || !plugin_string || !record_set || !result_set){
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
  	logging(LOG_ERROR, __FILE__, __LINE__,OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);    
    return OPH_QUERY_ENGINE_NULL_PARAM;
  }

  //Define global variables
  oph_udf_arg *oph_args = NULL;	
  oph_plugin *plugin = NULL;
  unsigned int arg_count = 0;
  unsigned long l  = 0;

  //Select plugin and set arguments	
  if(oph_parse_plugin(plugin_string, plugin_table, record_set, &plugin, &oph_args, &arg_count)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_QUERY_PARSING_ERROR, plugin_string);
  	logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_QUERY_PARSING_ERROR, plugin_string);    
    for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
    free(oph_args);
	  return OPH_QUERY_ENGINE_PARSE_ERROR;
  }		

  *result_set = NULL;
  //TODO update field_list_num, limit, offset and thread number
  if(oph_execute_plugin(plugin, oph_args, arg_count, result_set, 2, 0, 1, 0, 0, 4)){
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_EXEC_ERROR, plugin_string);
  	logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_EXEC_ERROR, plugin_string);    
    for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
    free(oph_args);
	  return 1;
  }

  for(l = 0; l < arg_count; l++) oph_free_udf_arg(&oph_args[l]);
  free(oph_args);

	return OPH_QUERY_ENGINE_SUCCESS;
}

int oph_query_engine_end(HASHTBL **plugin_table){
	if(!plugin_table){
    pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
  	logging(LOG_ERROR, __FILE__, __LINE__,OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);    
    return OPH_QUERY_ENGINE_NULL_PARAM;
  }

	oph_unload_plugins (*plugin_table);
  *plugin_table = NULL;
  
	return OPH_QUERY_ENGINE_SUCCESS;
}
