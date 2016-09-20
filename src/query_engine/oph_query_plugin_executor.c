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

#include "oph_query_plugin_executor.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>

#include <ctype.h>
#include <unistd.h>

#include <debug.h>

#include <errno.h>

#include "oph_server_utility.h"

#include <pthread.h>
#include <omp.h>

extern int msglevel;
extern unsigned long long omp_threads;
HASHTBL *plugin_table = NULL;  
pthread_mutex_t libtool_lock;

//TODO - Add debug mesg and logging
//TODO - Define specific return codes

int free_udf_arg(UDF_ARGS *args){
	if( !args )
		return -1;	

	unsigned int i = 0;
	for(i=0; i < args->arg_count; i++){
		if(args->args[i]) free(args->args[i]);
		args->args[i] = NULL;
	}
	free(args->arg_type);
	free(args->args);
	free(args->lengths);

	return 0;
}

int _oph_execute_plugin(const oph_plugin *plugin, UDF_ARGS *args, UDF_INIT *initid, void **res, unsigned long long *res_length, char *is_null, char *error, char *result, oph_plugin_api *functions){
	if( !plugin || !args || !res || !res_length || !functions)
		return -1;

	unsigned long len = 0;

	if (memory_check()) return -1;

	//Execute main function
	switch (plugin->plugin_return)
	{
		case OPH_IOSTORE_LONG_TYPE:
		{
			if (!(_oph_plugin1 = (long long (*)(UDF_INIT*, UDF_ARGS *, char *, char*)) functions->exec_api)){
				return -1;
			}
			long long tmp_res = (long long)_oph_plugin1 (initid, args, is_null, error);
			if(*error == 1){
				return -1;
			}
			if(!*res && !*res_length)			
				*res = (void*)malloc(1*sizeof(long long));
			memcpy(*res, (void *)&tmp_res, sizeof(long long));
			*res_length = sizeof(tmp_res);
			break;
		}
		case OPH_IOSTORE_REAL_TYPE:
		{
			if (!(_oph_plugin2 = (double (*)(UDF_INIT*, UDF_ARGS *, char *, char*)) functions->exec_api)){
				return -1;
			}
			double tmp_res = (double)_oph_plugin2 (initid, args, is_null, error);
			if(*error == 1){
				return -1;
			}
			if(!*res && !*res_length)			
				*res = (double*)malloc(1*sizeof(double));
			memcpy(*res, (void *)&tmp_res, sizeof(double));
			*res_length = sizeof(tmp_res);
			break;
		}
		case OPH_IOSTORE_STRING_TYPE:
		{
			if (!(_oph_plugin3 = (char* (*)(UDF_INIT*, UDF_ARGS*,  char*, unsigned long*, char*, char*)) functions->exec_api)){
				return -1;
			}

			char *tmp_res = _oph_plugin3 (initid, args, result, &len, is_null, error);
			if(*error == 1){
				return -1;
			}
			if(!*res && !*res_length)
				*res = (char*)malloc(sizeof(char)*(len));
			memcpy(*res, (void *)tmp_res, len);
			*res_length = len;
			break;
		}
		default:
			return -1;
	}
	//End cycle	
	return 0;
}

int oph_execute_plugin(const oph_plugin *plugin, oph_udf_arg *args, unsigned int arg_count, oph_iostore_frag_record_set **result_set, int field_list_num, int id_col_index, int col_index, long long limit, long long offset, unsigned short omp_thread_num){
	if( !plugin || !args || !result_set || (limit<0) || (offset<0))
		return -1;

#ifndef OPH_OMP
UNUSED(omp_thread_num)
#endif

  //Load all functions
	char plugin_init_name[BUFLEN], plugin_deinit_name[BUFLEN], plugin_clear_name[BUFLEN], plugin_add_name[BUFLEN], plugin_reset_name[BUFLEN];
	snprintf(plugin_init_name, BUFLEN, "%s_init", plugin->plugin_name);
	snprintf(plugin_deinit_name, BUFLEN, "%s_deinit", plugin->plugin_name);
	snprintf(plugin_clear_name, BUFLEN, "%s_clear", plugin->plugin_name);
	snprintf(plugin_add_name, BUFLEN, "%s_add", plugin->plugin_name);
	snprintf(plugin_reset_name, BUFLEN, "%s_reset", plugin->plugin_name);
	void	*dlh = NULL;

	//Initialize libltdl
pthread_mutex_lock(&libtool_lock);
	lt_dlinit ();
pthread_mutex_unlock(&libtool_lock);

pthread_mutex_lock(&libtool_lock);
	//Load library
	if (!(dlh = (lt_dlhandle) lt_dlopen (plugin->plugin_library)))
	{
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading plugin dynamic library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);

	//Load all library symbols
	oph_plugin_api function;
pthread_mutex_lock(&libtool_lock);
	function.init_api = lt_dlsym (dlh, plugin_init_name);
	function.clear_api = NULL;
	function.reset_api = NULL;
	function.add_api = NULL;
	function.exec_api = lt_dlsym (dlh, plugin->plugin_name);
	function.deinit_api = lt_dlsym (dlh, plugin_deinit_name);
	if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
		function.clear_api = lt_dlsym (dlh, plugin_clear_name);
		function.reset_api = lt_dlsym (dlh, plugin_reset_name);
		function.add_api = lt_dlsym (dlh, plugin_add_name);
  }
pthread_mutex_unlock(&libtool_lock);

  //Prepare dynamic functions pointers

	//Initialize function
	if (!(_oph_plugin_init = (my_bool (*)(UDF_INIT*, UDF_ARGS *, char *)) function.init_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function\n");
		return -1;
	}
  //Deinitialize function
	if (!(_oph_plugin_deinit = (void (*)(UDF_INIT*)) function.deinit_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin DEINIT function\n");
		return -1;
	}		
	if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
    //Clear function
    if (!(_oph_plugin_clear = (void (*)(UDF_INIT*, char *, char *)) function.clear_api)){
  pthread_mutex_lock(&libtool_lock);
      lt_dlclose(dlh);
      lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
      return -1;
    }
    //Add function
    if (!(_oph_plugin_add = (void (*)(UDF_INIT*,  UDF_ARGS*, char*, char*)) function.add_api)){
  pthread_mutex_lock(&libtool_lock);
      lt_dlclose(dlh);
      lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
      return -1;
    }
  }

  //SETUP UDF_ARG
	UDF_ARGS tmp_args;

  //Create new UDF fixed fields
  long long i = 0, j = 0;
  oph_selection *tmp_sel = NULL;
  for(i = 0; i < arg_count ; i++){
    if( args[i].arg_pointer && !args[i].arg_value ){
      tmp_sel = ((oph_selection *)args[i].arg_pointer);
      break;
    }
  }

	//Count number of rows to compute
	long long total_row_number = 0, row_number = 0;
	if (offset) while(tmp_sel->groups[0][total_row_number]) total_row_number++;
	if (!offset || (offset<total_row_number))
	{
		long long jj=offset;
		while(tmp_sel->groups[0][jj] && (!limit || (row_number<limit))) { jj++; row_number++; }
	}

  //Create output record set
  if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ) oph_iostore_create_frag_recordset(result_set, 1, field_list_num);
  else oph_iostore_create_frag_recordset(result_set, row_number, field_list_num);

  (*result_set)->field_num = field_list_num;
  switch (field_list_num)
  {
	case 1:
		(*result_set)->field_name[0] = strndup(OPH_NAME_MEASURE,strlen(OPH_NAME_MEASURE));
		(*result_set)->field_type[0] = plugin->plugin_return;
		break;
	case 2:
		(*result_set)->field_name[0] = strndup(OPH_NAME_ID,strlen(OPH_NAME_ID));
		(*result_set)->field_name[1] = strndup(OPH_NAME_MEASURE,strlen(OPH_NAME_MEASURE));
		(*result_set)->field_type[0] = OPH_IOSTORE_LONG_TYPE;
		(*result_set)->field_type[1] = plugin->plugin_return;
		break;
	default:
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while processing the number of fields\n");
		pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
		pthread_mutex_unlock(&libtool_lock);
		oph_iostore_destroy_frag_recordset(result_set);
		return -1;
  }

  oph_iostore_frag_record **rs = (*result_set)->record_set;

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Executing %s primitive returning %d field%s\n", plugin->plugin_name, field_list_num, field_list_num==1?"":"s");

  //If function type is aggregate then
  if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
    UDF_INIT initid;

    //Both simple and aggregate functions need initialization
	  char is_null = 0, error = 0, result = 0;
	  char *message = (char*)malloc(BUFLEN*sizeof(char));

    //Create new UDF fixed fields
    tmp_args.arg_count = arg_count;
    tmp_args.arg_type = (enum Item_result*)calloc(arg_count,sizeof(enum Item_result));
    tmp_args.args = (char**)calloc(arg_count,sizeof(char*));
    tmp_args.lengths = (unsigned long *)calloc(arg_count,sizeof(unsigned long));

    unsigned int l = 0;
    oph_selection *tmp_sel = NULL;
    for(l = 0; l < arg_count ; l++){
      tmp_args.arg_type[l] = args[l].arg_type;
      if( args[l].arg_pointer && !args[l].arg_value ){
          tmp_sel = ((oph_selection *)args[l].arg_pointer);
          tmp_args.args[l] = ((tmp_sel->groups[0][0])->field_length[col_index] ? memdup((tmp_sel->groups[0][0])->field[col_index], (tmp_sel->groups[0][0])->field_length[col_index]) : NULL);
          tmp_args.lengths[l] = (tmp_sel->groups[0][0])->field_length[col_index];
      }
      else{
        tmp_args.args[l] = (args[l].arg_length ? memdup(args[l].arg_value, args[l].arg_length) : NULL);
        tmp_args.lengths[l] = args[l].arg_length;
      }
    }

	*message=0;
	if(_oph_plugin_init (&initid, &tmp_args, message)){
		pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
		pthread_mutex_unlock(&libtool_lock);
		free_udf_arg(&tmp_args);
		oph_iostore_destroy_frag_recordset(result_set);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
		free(message);
		return -1;
	}
    free(message);

    unsigned long long id = 0;

    if(0){
	    //If aggregate function cycle on rows
	    //NOTE: the aggregation is performed on all rows without GROUP BY clause
	    if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
		    //Execute _clear function
		    _oph_plugin_clear (&initid, &is_null, &error);        
			if(error == 1){
				_oph_plugin_deinit (&initid);
				free_udf_arg(&tmp_args);
				pthread_mutex_lock(&libtool_lock);
				lt_dlclose(dlh);
				lt_dlexit();
				pthread_mutex_unlock(&libtool_lock);
				oph_iostore_destroy_frag_recordset(result_set);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
				return -1;
			}

		    /*void _oph_plugin_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error )*/
	    }
    }

	id = offset + 1;
	rs[0]->field[0] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
	rs[0]->field_length[0] = sizeof(unsigned long long);

	for(j = 0; j < row_number; j++, id++)
	{
		_oph_plugin_add (&initid, &tmp_args, &is_null, &error);
		if(error == 1){
			_oph_plugin_deinit (&initid);
			free_udf_arg(&tmp_args);
			pthread_mutex_lock(&libtool_lock);
			lt_dlclose(dlh);
			lt_dlexit();
			pthread_mutex_unlock(&libtool_lock);
			oph_iostore_destroy_frag_recordset(result_set);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
			return -1;
		}

	    if(_oph_execute_plugin(plugin, &tmp_args, &initid, &rs[0]->field[col_index], &rs[0]->field_length[col_index], &is_null, &error, &result, &function) || error == 1){
		_oph_plugin_deinit (&initid);
		pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
		pthread_mutex_unlock(&libtool_lock);
		free_udf_arg(&tmp_args);
		oph_iostore_destroy_frag_recordset(result_set);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
		return -1;
	    }

  	  //TODO - Check error parameter of execute call

	      //TODO get rows
	      for(l = 0; l < arg_count; l++){
		if( args[l].arg_pointer && !args[l].arg_value ){
		  tmp_sel = ((oph_selection *)args[l].arg_pointer);
		  if ((tmp_sel->groups[0][id]) != NULL ){
		    if(tmp_args.args[l]) free(tmp_args.args[l]);
		    tmp_args.args[l] = ((tmp_sel->groups[0][id])->field_length[col_index] ? memdup((tmp_sel->groups[0][id])->field[col_index], (tmp_sel->groups[0][id])->field_length[col_index]) : NULL);
		    tmp_args.lengths[l] = (tmp_sel->groups[0][id])->field_length[col_index];
		  }
		}
	      }

	}      
	//TODO manage row groups
	free_udf_arg(&tmp_args);
    	_oph_plugin_deinit (&initid);
  
  } 
  else
  {
    //Execute simple plugin
    short unsigned int error_flag = 0;
    unsigned long long id = 0, key_start = 1;

#ifdef OPH_OMP
omp_set_num_threads(omp_thread_num);
#pragma omp parallel shared(rs, args, error_flag, i) private(tmp_args, tmp_sel, id, key_start)
#endif
{
    UDF_INIT initid;
    unsigned int l = 0;

    //Both simple and aggregate functions need initialization
	  char is_null = 0, error = 0, result = 0;
	  char *message = (char*)malloc(BUFLEN*sizeof(char));

#ifdef OPH_OMP
    //int tid = omp_get_thread_num();
#endif

    //Create new UDF fixed fields
    tmp_args.arg_count = arg_count;
    tmp_args.arg_type = (enum Item_result*)calloc(arg_count,sizeof(enum Item_result));
    tmp_args.args = (char**)calloc(arg_count,sizeof(char*));
    tmp_args.lengths = (unsigned long *)calloc(arg_count,sizeof(unsigned long));

    for(l = 0; l < arg_count ; l++){
      tmp_args.arg_type[l] = args[l].arg_type;
      if( args[l].arg_pointer && !args[l].arg_value ){
        tmp_sel = ((oph_selection *)args[l].arg_pointer);
        tmp_args.args[l] = ((tmp_sel->groups[0][0])->field_length[col_index] ? memdup((tmp_sel->groups[0][0])->field[col_index], (tmp_sel->groups[0][0])->field_length[col_index]) : NULL);
        tmp_args.lengths[l] = (tmp_sel->groups[0][0])->field_length[col_index];
      }
      else{
        tmp_args.args[l] = (args[l].arg_length ? memdup(args[l].arg_value, args[l].arg_length) : NULL);
        tmp_args.lengths[l] = args[l].arg_length;
      }
    }

	*message=0;
	if(_oph_plugin_init (&initid, &tmp_args, message)){
		free_udf_arg(&tmp_args);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
		free(message);
		error_flag = 1;
	}
    else{

    free(message);

    if ((tmp_sel->groups[0][offset])->field[id_col_index]) key_start = *((unsigned long long*)( (tmp_sel->groups[0][offset])->field[id_col_index] ));

#ifdef OPH_OMP
#pragma omp for
#endif
    //TODO manage group selection
	 for(i = 0; i < row_number ; i++){

      if(error_flag == 1) continue;
      
      for(l = 0; l < arg_count; l++){
        if( args[l].arg_pointer && !args[l].arg_value ){
          tmp_sel = ((oph_selection *)args[l].arg_pointer);
          if ((tmp_sel->groups[0][i+offset]) != NULL ){
            if(tmp_args.args[l]) free(tmp_args.args[l]);
            tmp_args.args[l] = ((tmp_sel->groups[0][i+offset])->field_length[col_index] ? memdup((tmp_sel->groups[0][i+offset])->field[col_index], (tmp_sel->groups[0][i+offset])->field_length[col_index]) : NULL);
            tmp_args.lengths[l] = (tmp_sel->groups[0][i+offset])->field_length[col_index];
          }
          else{
            pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while getting next row\n");
            error_flag = 1;
            continue;
          }
        }
      }

	id = i + offset + key_start;
	rs[i]->field[0] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
	rs[i]->field_length[0] = sizeof(unsigned long long);
	if(_oph_execute_plugin(plugin, &tmp_args, &initid, &rs[i]->field[col_index], &rs[i]->field_length[col_index], &is_null, &error, &result, &function )  || error == 1){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
		error_flag = 1;
		continue;
	}

  	  //TODO - Check error parameter of execute call

    }
#ifdef OPH_OMP
#pragma omp barrier
#endif
  free_udf_arg(&tmp_args);
  _oph_plugin_deinit (&initid);
  }
}

    if(error_flag){
  pthread_mutex_lock(&libtool_lock);
	  lt_dlclose(dlh);
	  lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
        oph_iostore_destroy_frag_recordset(result_set);
        return -1;
    }
  }


  //Release functions
#ifndef OPH_WITH_VALGRIND
	//Close dynamic loaded library
pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(dlh)))
	{
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
    oph_iostore_destroy_frag_recordset(result_set);
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
	dlh = NULL;

pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()){
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
    oph_iostore_destroy_frag_recordset(result_set);
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
#endif
	return 0;
}

int oph_execute_plugin2(const oph_plugin *plugin, oph_udf_arg *args, unsigned int arg_count, oph_iostore_frag_record_set *result_set, int field_list_num, int col_index, long long row_number, long long offset, unsigned short omp_thread_num){
	if( !plugin || !args || (row_number<0) || (offset<0))
		return -1;

#ifndef OPH_OMP
UNUSED(omp_thread_num)
#endif

  //Load all functions
	char plugin_init_name[BUFLEN], plugin_deinit_name[BUFLEN], plugin_clear_name[BUFLEN], plugin_add_name[BUFLEN], plugin_reset_name[BUFLEN];
	snprintf(plugin_init_name, BUFLEN, "%s_init", plugin->plugin_name);
	snprintf(plugin_deinit_name, BUFLEN, "%s_deinit", plugin->plugin_name);
	snprintf(plugin_clear_name, BUFLEN, "%s_clear", plugin->plugin_name);
	snprintf(plugin_add_name, BUFLEN, "%s_add", plugin->plugin_name);
	snprintf(plugin_reset_name, BUFLEN, "%s_reset", plugin->plugin_name);
	void	*dlh = NULL;

	//Initialize libltdl
pthread_mutex_lock(&libtool_lock);
	lt_dlinit ();
pthread_mutex_unlock(&libtool_lock);

pthread_mutex_lock(&libtool_lock);
	//Load library
	if (!(dlh = (lt_dlhandle) lt_dlopen (plugin->plugin_library)))
	{
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading plugin dynamic library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);

	//Load all library symbols
	oph_plugin_api function;
pthread_mutex_lock(&libtool_lock);
	function.init_api = lt_dlsym (dlh, plugin_init_name);
	function.clear_api = NULL;
	function.reset_api = NULL;
	function.add_api = NULL;
	function.exec_api = lt_dlsym (dlh, plugin->plugin_name);
	function.deinit_api = lt_dlsym (dlh, plugin_deinit_name);
	if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
		function.clear_api = lt_dlsym (dlh, plugin_clear_name);
		function.reset_api = lt_dlsym (dlh, plugin_reset_name);
		function.add_api = lt_dlsym (dlh, plugin_add_name);
  }
pthread_mutex_unlock(&libtool_lock);

  //Prepare dynamic functions pointers

	//Initialize function
	if (!(_oph_plugin_init = (my_bool (*)(UDF_INIT*, UDF_ARGS *, char *)) function.init_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function\n");
		return -1;
	}
  //Deinitialize function
	if (!(_oph_plugin_deinit = (void (*)(UDF_INIT*)) function.deinit_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin DEINIT function\n");
		return -1;
	}		
	if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
    //Clear function
    if (!(_oph_plugin_clear = (void (*)(UDF_INIT*, char *, char *)) function.clear_api)){
  pthread_mutex_lock(&libtool_lock);
      lt_dlclose(dlh);
      lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
      return -1;
    }
    //Add function
    if (!(_oph_plugin_add = (void (*)(UDF_INIT*,  UDF_ARGS*, char*, char*)) function.add_api)){
  pthread_mutex_lock(&libtool_lock);
      lt_dlclose(dlh);
      lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
      return -1;
    }
  }

  //SETUP UDF_ARG
	UDF_ARGS tmp_args;

  long long i = 0, j = 0;
  oph_selection *tmp_sel = NULL;
  oph_iostore_frag_record **rs = result_set->record_set;

  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Executing %s\n", plugin->plugin_name);

  //If function type is aggregate then
  if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) )
  {
	    UDF_INIT initid;

	    //Both simple and aggregate functions need initialization
		  char is_null = 0, error = 0, result = 0;
		  char *message = (char*)malloc(BUFLEN*sizeof(char));

	    //Create new UDF fixed fields
	    tmp_args.arg_count = arg_count;
	    tmp_args.arg_type = (enum Item_result*)calloc(arg_count,sizeof(enum Item_result));
	    tmp_args.args = (char**)calloc(arg_count,sizeof(char*));
	    tmp_args.lengths = (unsigned long *)calloc(arg_count,sizeof(unsigned long));

	    unsigned int l = 0;
	    oph_selection *tmp_sel = NULL;
	    for(l = 0; l < arg_count ; l++){
	      tmp_args.arg_type[l] = args[l].arg_type;
	      if( args[l].arg_pointer && !args[l].arg_value ){
		  tmp_sel = ((oph_selection *)args[l].arg_pointer);
		  tmp_args.args[l] = ((tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1] ? memdup((tmp_sel->groups[0][0])->field[col_index==field_list_num-1], (tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1]) : NULL);
		  tmp_args.lengths[l] = (tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1];
		  if (col_index<field_list_num-1) tmp_args.arg_type[l] = INT_RESULT;
	      }
	      else{
		tmp_args.args[l] = (args[l].arg_length ? memdup(args[l].arg_value, args[l].arg_length) : NULL);
		tmp_args.lengths[l] = args[l].arg_length;
	      }
	    }

	    *message = 0;
		if(_oph_plugin_init (&initid, &tmp_args, message)){
			pthread_mutex_lock(&libtool_lock);
			lt_dlclose(dlh);
			lt_dlexit();
			pthread_mutex_unlock(&libtool_lock);
			free_udf_arg(&tmp_args);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
			free(message);
			return -1;
		}
	    free(message);

	    unsigned long long id = 0;

	    if(0){
		    //If aggregate function cycle on rows
		    //NOTE: the aggregation is performed on all rows without GROUP BY clause
		    if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
			    //Execute _clear function
			    _oph_plugin_clear (&initid, &is_null, &error);        
				if(error == 1){
				_oph_plugin_deinit (&initid);
				free_udf_arg(&tmp_args);
				pthread_mutex_lock(&libtool_lock);
				lt_dlclose(dlh);
				lt_dlexit();
				pthread_mutex_unlock(&libtool_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
				    return -1;
				}

			    /*void _oph_plugin_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* error )*/
		    }
	    }

		id = offset + 1;
	     /* //  id_dim is set as normal column
		rs[0]->field[0] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
		rs[0]->field_length[0] = sizeof(unsigned long long);
	    */

	    for(j = 0; j < row_number; j++, id++)
	    {
		    _oph_plugin_add (&initid, &tmp_args, &is_null, &error);
			if(error == 1){
				_oph_plugin_deinit (&initid);
				free_udf_arg(&tmp_args);
				pthread_mutex_lock(&libtool_lock);
				lt_dlclose(dlh);
				lt_dlexit();
				pthread_mutex_unlock(&libtool_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
				return -1;
			}

		    if(_oph_execute_plugin(plugin, &tmp_args, &initid, &rs[0]->field[col_index], &rs[0]->field_length[col_index], &is_null, &error, &result, &function) || error == 1){
			_oph_plugin_deinit (&initid);
			pthread_mutex_lock(&libtool_lock);
			lt_dlclose(dlh);
			lt_dlexit();
			pthread_mutex_unlock(&libtool_lock);
			free_udf_arg(&tmp_args);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
			return -1;
		    }

  	  //TODO - Check error parameter of execute call

		      //TODO get rows
		      for(l = 0; l < arg_count; l++){
			if( args[l].arg_pointer && !args[l].arg_value ){
			  tmp_sel = ((oph_selection *)args[l].arg_pointer);
			  if ((tmp_sel->groups[0][id]) != NULL ){
			    if(tmp_args.args[l]) free(tmp_args.args[l]);
			    tmp_args.args[l] = ((tmp_sel->groups[0][id])->field_length[col_index==field_list_num-1] ? memdup((tmp_sel->groups[0][id])->field[col_index==field_list_num-1], (tmp_sel->groups[0][id])->field_length[col_index==field_list_num-1]) : NULL);
			    tmp_args.lengths[l] = (tmp_sel->groups[0][id])->field_length[col_index==field_list_num-1];
		            if (col_index<field_list_num-1) tmp_args.arg_type[l] = INT_RESULT;
			  }
			}
		      }

	    }      
	    //TODO manage row groups
	    free_udf_arg(&tmp_args);
	    	_oph_plugin_deinit (&initid);
	  
  } 
  else
  {
    //Execute simple plugin
    short unsigned int error_flag = 0;

#ifdef OPH_OMP
omp_set_num_threads(omp_thread_num);
#pragma omp parallel shared(rs, args, error_flag, i) private(tmp_args, tmp_sel)
#endif
{
    UDF_INIT initid;
    unsigned int l = 0;

    //Both simple and aggregate functions need initialization
	  char is_null = 0, error = 0, result = 0;
	  char *message = (char*)malloc(BUFLEN*sizeof(char));

#ifdef OPH_OMP
    //int tid = omp_get_thread_num();
#endif

    //Create new UDF fixed fields
    tmp_args.arg_count = arg_count;
    tmp_args.arg_type = (enum Item_result*)calloc(arg_count,sizeof(enum Item_result));
    tmp_args.args = (char**)calloc(arg_count,sizeof(char*));
    tmp_args.lengths = (unsigned long *)calloc(arg_count,sizeof(unsigned long));

    for(l = 0; l < arg_count ; l++){
      tmp_args.arg_type[l] = args[l].arg_type;
      if( args[l].arg_pointer && !args[l].arg_value ){
        tmp_sel = ((oph_selection *)args[l].arg_pointer);
        tmp_args.args[l] = ((tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1] ? memdup((tmp_sel->groups[0][0])->field[col_index==field_list_num-1], (tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1]) : NULL);
        tmp_args.lengths[l] = (tmp_sel->groups[0][0])->field_length[col_index==field_list_num-1];
	if (col_index<field_list_num-1) tmp_args.arg_type[l] = INT_RESULT;
      }
      else{
        tmp_args.args[l] = (args[l].arg_length ? memdup(args[l].arg_value, args[l].arg_length) : NULL);
        tmp_args.lengths[l] = args[l].arg_length;
      }
    }

	*message=0;
	if(_oph_plugin_init (&initid, &tmp_args, message)){
		free_udf_arg(&tmp_args);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
		free(message);
		error_flag = 1;
	}
	else
	{
		free(message);

#ifdef OPH_OMP
#pragma omp for
#endif
    //TODO manage group selection
	 	for(i = 0; i < row_number ; i++){

			if(error_flag == 1) continue;

			for(l = 0; l < arg_count; l++){
				if( args[l].arg_pointer && !args[l].arg_value ){
					tmp_sel = ((oph_selection *)args[l].arg_pointer);
					if ((tmp_sel->groups[0][i+offset]) != NULL ){
						if(tmp_args.args[l]) free(tmp_args.args[l]);
						tmp_args.args[l] = ((tmp_sel->groups[0][i+offset])->field_length[col_index==field_list_num-1] ? memdup((tmp_sel->groups[0][i+offset])->field[col_index==field_list_num-1], (tmp_sel->groups[0][i+offset])->field_length[col_index==field_list_num-1]) : NULL);
						tmp_args.lengths[l] = (tmp_sel->groups[0][i+offset])->field_length[col_index==field_list_num-1];
						if (col_index<field_list_num-1) tmp_args.arg_type[l] = INT_RESULT;
					}
					else
					{
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while getting next row\n");
						error_flag = 1;
						continue;
					}
				}
			}

			/* //  id_dim is set as normal column
			id = i + offset + 1;
			rs[i]->field[0] = (void *)memdup((const void *)&id,sizeof(unsigned long long));
			rs[i]->field_length[0] = sizeof(unsigned long long);
			*/

			if(_oph_execute_plugin(plugin, &tmp_args, &initid, &rs[i]->field[col_index], &rs[i]->field_length[col_index], &is_null, &error, &result, &function )  || error == 1){
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
				error_flag = 1;
				continue;
			}
  	  //TODO - Check error parameter of execute call

		}
#ifdef OPH_OMP
#pragma omp barrier
#endif
  free_udf_arg(&tmp_args);
  _oph_plugin_deinit (&initid);
  }
}

    if(error_flag){
  pthread_mutex_lock(&libtool_lock);
	  lt_dlclose(dlh);
	  lt_dlexit();
  pthread_mutex_unlock(&libtool_lock);
        return -1;
    }
  }

  //Release functions
#ifndef OPH_WITH_VALGRIND
	//Close dynamic loaded library
pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(dlh)))
	{
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
	dlh = NULL;

pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()){
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
#endif

	return 0;
}

int oph_free_udf_arg(oph_udf_arg *arg){
  if(!arg)
    return 0;

  if(arg->arg_value){
    free(arg->arg_value);
    arg->arg_value = NULL;
  } 
  if(arg->arg_pointer){
    oph_free_select_data(arg->arg_pointer);
    arg->arg_pointer = NULL;
  }
  return 0;
}

//Function to set up UDF arg from the query parameter
int oph_set_udf_arg(const char *param, oph_udf_arg *arg)
{
    if (param == NULL || !arg)
      return -1;

    //If string is empty
    if(*param == '\0' || isspace(*param))
    {
      arg->arg_pointer = NULL;
      arg->arg_value = NULL;
      arg->arg_length = 0;
      arg->arg_type = STRING_RESULT;
      return 0;
    } 

    //Check argument type
    char* end = NULL;
    errno = 0;

    double val_d = 0;
    unsigned long val_l = 0;
    arg->arg_pointer = NULL;
    arg->arg_value = NULL;

    //Test INT_RESULT (long long)
    val_l = strtoll((char *)param, &end, 10);
    if ((errno != 0) || (end == (char *)param) || (*end != 0)){
        errno = 0;
        //Test DECIMAL_RESULT (double)
        val_d = strtod ((char *)param, &end);
        if ((errno != 0) || (end == (char *)param) || (*end != 0)){
          arg->arg_type = STRING_RESULT;
        }
        else{
          arg->arg_type = DECIMAL_RESULT;
        }
    }
    else{
      arg->arg_type = INT_RESULT;
    }
        
    switch(arg->arg_type) {
      case STRING_RESULT:
        //TODO check string length parameter
        arg->arg_length = (unsigned long)strlen(param) +1;
        arg->arg_value = (char *)malloc(arg->arg_length*sizeof(char));
	memcpy ((char *)arg->arg_value, param, arg->arg_length*sizeof(char)); 
        break;
      case DECIMAL_RESULT:
        arg->arg_length = (unsigned long)sizeof(double);
        arg->arg_value = (char *)malloc(arg->arg_length*sizeof(char));
        memcpy ((char *)arg->arg_value, &val_d, arg->arg_length*sizeof(char)); 
        break;
      case INT_RESULT:
        arg->arg_length = (unsigned long)sizeof(long long);
        arg->arg_value = (char *)malloc(arg->arg_length*sizeof(char));
        memcpy ((char *)arg->arg_value, &val_l, arg->arg_length*sizeof(char)); 
        break;
      default:
        return -1;
    } 
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"UDF Argument parsed: %s, Type: %s\n", param, (arg->arg_type == STRING_RESULT ? "string" : (arg->arg_type == INT_RESULT ? "int" : "double")));

    return 0;
}

int oph_select_data(oph_selection **select, oph_iostore_frag_record **recordset){
  *select = (oph_selection *)malloc(sizeof(oph_selection));
  (*select)->group_number = 1;
  (*select)->groups = (oph_iostore_frag_record ***)malloc((*select)->group_number*sizeof(oph_iostore_frag_record **));
  (*select)->groups[0] = recordset;
  return 0;
}

int oph_free_select_data(oph_selection *select){
  if(select->groups) free(select->groups);
  if(select) free(select);
  return 0;
}

//Plugin should be in format: plugin(arg1,arg2,arg3)
int oph_parse_plugin(const char* query_string, HASHTBL *plugin_table, oph_iostore_frag_record_set *record_set, oph_plugin **plugin, oph_query_arg **stmt_args, oph_udf_arg **args, unsigned int *arg_count){
	if(!query_string || !plugin || !args || !record_set || !record_set->record_set)
		return -1;

	*plugin = NULL;
	*args = NULL;
	*arg_count = 0;

	//Count row number
	long long rows = 0;
	while(record_set->record_set[rows]) rows++;

//START SIMPLE PLUGIN PARSER
  char *ptr_begin, *ptr_separ, *ptr_start, *ptr_end, *ptr_delim, *ptr_binary;
  //Count plugin arguments
  int count_args = 1;
  int count_plugins = 0;
  ptr_begin = (char *)query_string;

  //First validate string: at least 1 char; then (; then one or more ,; and finaly )
  ptr_separ = strchr(query_string, ',');
  ptr_start = strchr(query_string, '(');
  ptr_end = strchr(query_string, ')');
	ptr_delim = strchr(query_string, '\'');	
	ptr_binary = strchr(query_string, '?');	

  if ((ptr_start == NULL) && (ptr_end == NULL))
  {
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Query valid: no function found\n");
    return 0;
  }

  //Test special char presence
  if(ptr_start == NULL || ptr_end == NULL || ptr_begin == ptr_start || ptr_start >= ptr_end ){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: wrong syntax\n");
    return -1;
  }
  //Test separator char position
  if(ptr_separ != NULL && (ptr_separ <= ptr_start || ptr_separ >= ptr_end) ){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: bracket mismatch.\n");
    return -1;
  }
	//Test string delimiter position
  if(ptr_delim != NULL && (ptr_delim <= ptr_start || ptr_delim >= ptr_end) ){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: bracket mismatch.\n");
    return -1;
  }
	//Test binary placeholder position
  if(ptr_binary != NULL && (ptr_binary <= ptr_start || ptr_binary >= ptr_end) ){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: bracket mismatch.\n");
    return -1;
  }

	int count_strings = 0;
	short int string_flag = 0;
	int count_binary = 0;
  while(*ptr_begin){
		if(*ptr_begin == '\'') {
			count_strings++;
			if(count_strings % 2 == 1) string_flag = 1;
			else string_flag = 0;
		}
		else if(string_flag == 0){
			switch(*ptr_begin){
				case ',': 
				{
					count_args++;
					//Check that after commas there are all characters except commas 
					while(*ptr_begin){
						ptr_begin++;
						if(*ptr_begin != ' '){
							if(*ptr_begin != ','){
								//Recover normal checks
								ptr_begin--;
								break;
							}
							else{
								pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: empty argument cannot be used!\n");
								return -1;
							}
						}
					}
					break;
				}
				case '(': 
				{
					count_plugins++;
					//TODO extend support for nested plugins 
					if(count_plugins > 1){
						pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: nested plugins not supported!\n");
						return -1;
					}
					break;
				}
				case ')':
				{
					count_plugins--; 
					if(count_plugins < 0){
						pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: bracket mismatch.\n");
						return -1;
					}
					break;
				}
				case '?': 
				{
					//Support for binary arguments 
					count_binary++;
					//Check that after ? there are only spaces, commas or brakets
					while(*ptr_begin){
						ptr_begin++;
						if(*ptr_begin != ' '){
							if(*ptr_begin == ',' || *ptr_begin == ')'){
								//Step one character back to recover normal checks
								ptr_begin--;
								break;
							}
							else{
								pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: binary arguments used improperly!\n");
								return -1;
							}
						}
					}
					break;
				}
			}
		}
  	ptr_begin++;
  } 

	if(count_binary > 0 && stmt_args == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Query not valid: missing arguments for statement variables");
		return -1;
	}

  if(count_strings % 2 != 0){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: string delimiter mismatch.\n");
    return -1;
  }

  if(count_plugins != 0){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: bracket mismatch.\n");
    return -1;
  }

   //If at least one argument is specified, then go further
  if(count_args < 1){ // < 3 in case of data primitives
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Query not valid: missing argument in plugin\n");
    return -1;
  }

  //Retrieve plugin name
  ptr_begin = (char *)query_string;
  ptr_separ = strchr(query_string, ',');
  ptr_delim = strchr(query_string, '\'');
  ptr_start = strchr(query_string, '(');
  ptr_end = strchr(query_string, ')');

  char plugin_name[BUFLEN];
  strncpy(plugin_name, ptr_begin, strlen(ptr_begin) - strlen(ptr_start));
  plugin_name[strlen(ptr_begin) - strlen(ptr_start)] = 0;	

	//Load plugin shared library
	*plugin = (oph_plugin*) hashtbl_get(plugin_table, plugin_name);
	if(!*plugin){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Plugin not allowed\n");
		return -1;
	}

  ptr_begin = ptr_start + 1;
  ptr_separ = strchr(ptr_begin + 1, ',');
  ptr_delim = strchr(ptr_begin, '\'');

  char *real_start = NULL, *real_end = NULL;

  //Retrieve plugin argument values
  oph_udf_arg *parsed_args = (oph_udf_arg*)malloc(count_args*sizeof(oph_udf_arg));

  int i = 0, j;
  char parsed_arg[BUFLEN];
	int count_stmt_args = 0;
  while(*ptr_begin)
  {
		string_flag = 0;

    if(!ptr_begin) {
        free(parsed_args);
  		  return -1;
    } 
 	
		//If separator is within delimiter
		if((ptr_delim != NULL) && (ptr_separ != NULL) && (ptr_delim < ptr_separ)){
			ptr_delim = strchr(ptr_delim + 1, '\'');
			if(ptr_delim > ptr_separ){
				//In this case ignore all separators till second delimiter
				ptr_separ = strchr(ptr_delim + 1, ',');
			}
		}

    //Remove starting quotes and spaces
    real_start = ptr_begin;
    while(*real_start == ' ' || *real_start == '\'' ){
			if(*real_start == '\'' ) string_flag = 1;
      real_start++;
    }
    //Remove trailing quotes and spaces
    if(ptr_separ != NULL){
      real_end = ptr_separ - 1;
    }
    else{
      real_end = ptr_end - 1;
    }
    while(*real_end == ' ' || *real_end == '\'' ){
      real_end--;
    }

    //Check if end < start it means the the string is empty
    if(real_end < real_start){
      parsed_arg[0] = 0;	      
    }
    else
    {
      strncpy(parsed_arg, real_start, strlen(real_start) - strlen(real_end) +1);
      parsed_arg[strlen(real_start) - strlen(real_end) +1] = 0;	
    }


    //For each argument find type, value and length
		//Check if ?, in this case set directly udf_arg with values from stmt_args 
		if(string_flag == 0 && parsed_arg[0] == '?'){

			//Check if number of args is compliant with those available in stmt_args
			if(stmt_args[count_stmt_args] != NULL){
				parsed_args[i].arg_pointer = NULL;
				parsed_args[i].arg_type = STRING_RESULT;
				parsed_args[i].arg_length = (unsigned long)stmt_args[count_stmt_args]->arg_length;
				parsed_args[i].arg_value = (char *)malloc(parsed_args[i].arg_length*sizeof(char));
				memcpy ((char *)parsed_args[i].arg_value, stmt_args[count_stmt_args]->arg, parsed_args[i].arg_length*sizeof(char)); 
				count_stmt_args++;
			}
			else{
				pmesg(LOG_ERROR,__FILE__,__LINE__,"Input query args mismatch\n");
				return -1;
			}
	}
	else{
		if( oph_set_udf_arg(parsed_arg, &parsed_args[i])){
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to parse argument %s\n", parsed_arg);
		  for(j = 0; j <= i; j++) oph_free_udf_arg(&parsed_args[j]);
		  free(parsed_args);
		  return -1;
		}
	}

    //If first or last argument (without trailing ,) exit while
    if(ptr_separ == NULL) break;

    //Get next arg and next arg ,
    ptr_begin = ptr_separ + 1;
    ptr_separ = strchr(ptr_begin + 1, ',');
    ptr_delim = strchr(ptr_begin, '\'');
    
    //If next , is outside ) then discard ,
    if(ptr_separ >= ptr_end ) ptr_separ = NULL;

    if(ptr_begin >= ptr_end) break;
    i++;
  }
  oph_selection *select = NULL;
//END SIMPLE PLUGIN PARSER

	//Set up UDF plugin measures
	for(i = 0; i < count_args ; i++){
	    if(parsed_args[i].arg_value && (!STRCMP(parsed_args[i].arg_value,OPH_NAME_ID) || !STRCMP(parsed_args[i].arg_value,OPH_NAME_MEASURE)) )
	    {
	      //Do rows selection
	      oph_select_data(&select, record_set->record_set);
	      parsed_args[i].arg_pointer = select;
	      parsed_args[i].arg_length = 0;
	      free(parsed_args[i].arg_value);
	      parsed_args[i].arg_value = NULL;
	    }
	}
  
	*arg_count = count_args;
	*args = parsed_args;

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Query valid: found function %s\n", plugin_name);
	return 0;
}


int oph_query_plugin_clear(oph_plugin_api *function, void *dlh, UDF_INIT *initid){
	if(!function || !dlh || !initid)
		return -1;

	char is_null = 0, error = 0;


	//Clear function
	if (!(_oph_plugin_clear = (void (*)(UDF_INIT*, char *, char *)) function->clear_api)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
		return -1;
	}

	_oph_plugin_clear (initid, &is_null, &error);    
	if(error){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
		return -1;
	}

	return 0;
}


int oph_query_plugin_deinit(oph_plugin_api *function, void *dlh, UDF_INIT *initid, UDF_ARGS *internal_args){
	if(!function || !dlh || !initid || !internal_args)
		return -1;

  //Deinitialize function
	if (!(_oph_plugin_deinit = (void (*)(UDF_INIT*)) function->deinit_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin DEINIT function\n");
		return -1;
	}		

  _oph_plugin_deinit (initid);

	free(initid);
	free_udf_arg(internal_args);
	free(internal_args);

  //Release functions
#ifndef OPH_WITH_VALGRIND
	//Close dynamic loaded library
pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(dlh)))
	{
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
	dlh = NULL;

pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()){
pthread_mutex_unlock(&libtool_lock);
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);
#endif

	return 0;
}

int oph_query_plugin_init(oph_plugin_api *function, void **dlh, UDF_INIT **initid, UDF_ARGS **internal_args, char *plugin_name, int arg_count, oph_query_expr_value* args, char *is_aggregate){
	if(!function || !dlh || !initid || !internal_args || !plugin_name || !arg_count || !args || !plugin_table || !is_aggregate)
		return -1;

	//Load plugin shared library
	oph_plugin *plugin = (oph_plugin*) hashtbl_get(plugin_table, plugin_name);
	if(!plugin){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Plugin not allowed\n");
		return -1;
	}

	//Load all functions
	char plugin_init_name[BUFLEN], plugin_deinit_name[BUFLEN], plugin_clear_name[BUFLEN], plugin_add_name[BUFLEN], plugin_reset_name[BUFLEN];
	snprintf(plugin_init_name, BUFLEN, "%s_init", plugin->plugin_name);
	snprintf(plugin_deinit_name, BUFLEN, "%s_deinit", plugin->plugin_name);
	snprintf(plugin_clear_name, BUFLEN, "%s_clear", plugin->plugin_name);
	snprintf(plugin_add_name, BUFLEN, "%s_add", plugin->plugin_name);
	snprintf(plugin_reset_name, BUFLEN, "%s_reset", plugin->plugin_name);
	*dlh = NULL;

	//Initialize libltdl
pthread_mutex_lock(&libtool_lock);
	lt_dlinit ();
pthread_mutex_unlock(&libtool_lock);

pthread_mutex_lock(&libtool_lock);
	//Load library
	if (!(*dlh = (lt_dlhandle) lt_dlopen (plugin->plugin_library)))
	{
		lt_dlclose(*dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
	pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading plugin dynamic library\n");
		return -1;
	}
pthread_mutex_unlock(&libtool_lock);

	//Load all library symbols
	pthread_mutex_lock(&libtool_lock);
	function->init_api = lt_dlsym (*dlh, plugin_init_name);
	function->clear_api = NULL;
	function->reset_api = NULL;
	function->add_api = NULL;
	function->exec_api = lt_dlsym (*dlh, plugin->plugin_name);
	function->deinit_api = lt_dlsym (*dlh, plugin_deinit_name);
	if( (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE) ){
		function->clear_api = lt_dlsym (*dlh, plugin_clear_name);
		function->reset_api = lt_dlsym (*dlh, plugin_reset_name);
		function->add_api = lt_dlsym (*dlh, plugin_add_name);
	}
	pthread_mutex_unlock(&libtool_lock);

	*is_aggregate = (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE); 
  

	//Initialize function
	if (!(_oph_plugin_init = (my_bool (*)(UDF_INIT*, UDF_ARGS *, char *)) function->init_api)){
pthread_mutex_lock(&libtool_lock);
		lt_dlclose(*dlh);
		lt_dlexit();
pthread_mutex_unlock(&libtool_lock);
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function\n");
		return -1;
	}

	char *message = (char*)malloc(BUFLEN*sizeof(char));
	if(message == NULL){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		return -1;
	}
	//SETUP UDF_ARG
	UDF_ARGS *tmp_args = (UDF_ARGS*)malloc(sizeof(UDF_ARGS));
	if(tmp_args == NULL){
		free(message);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		return -1;
	}

  	int l = 0;

    //Create new UDF fixed fields
    tmp_args->arg_count = arg_count;
    tmp_args->arg_type = (enum Item_result*)calloc(arg_count,sizeof(enum Item_result));
    tmp_args->args = (char**)calloc(arg_count,sizeof(char*));
    tmp_args->lengths = (unsigned long *)calloc(arg_count,sizeof(unsigned long));

	if(!tmp_args->arg_type || !tmp_args->args || !tmp_args->lengths)	
	{
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		return -1;
	}

	//First time setup all the values
    for(l = 0; l < arg_count ; l++){
		switch(args[l].type){
			case OPH_QUERY_EXPR_TYPE_STRING:
				tmp_args->arg_type[l] = STRING_RESULT;
				tmp_args->lengths[l] =  (unsigned long)(strlen(args[l].data.string_value) +1);
				tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
				if(!tmp_args->args[l])	
				{
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					return -1;
				}
		        memcpy ((char *)tmp_args->args[l], args[l].data.string_value, tmp_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				tmp_args->arg_type[l] = STRING_RESULT;
				tmp_args->lengths[l] =  (unsigned long)args[l].data.binary_value->arg_length;
				tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
				if(!tmp_args->args[l])	
				{
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					return -1;
				}
		        memcpy ((char *)tmp_args->args[l], args[l].data.binary_value->arg, tmp_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				tmp_args->arg_type[l] = DECIMAL_RESULT;
				tmp_args->lengths[l] =  (unsigned long)sizeof(double);
				tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
				if(!tmp_args->args[l])	
				{
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					return -1;
				}
		        memcpy ((char *)tmp_args->args[l], &(args[l].data.double_value), tmp_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				tmp_args->arg_type[l] = INT_RESULT;
				tmp_args->lengths[l] =  (unsigned long)sizeof(long long);
				tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
				if(!tmp_args->args[l])	
				{
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					return -1;
				}
		        memcpy ((char *)tmp_args->args[l], &(args[l].data.long_value), tmp_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_NULL:
				tmp_args->arg_type[l] = INT_RESULT;
				tmp_args->lengths[l] =  0;
				tmp_args->args[l] = NULL;
				break;
			default:
				free(message);
				free_udf_arg(tmp_args);
				free(tmp_args);
				return -1;
		}
	}

	*message=0;

	*initid = (UDF_INIT *)malloc(sizeof(UDF_INIT));
	if(*initid == NULL){
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		*initid = NULL;
		return -1;
	}

	if(_oph_plugin_init (*initid, tmp_args, message)){
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
		return -1;
	}
	free(message);

	//Check if some field type has been updated by init
    for(l = 0; l < arg_count ; l++){
		switch(args[l].type){
			case OPH_QUERY_EXPR_TYPE_NULL:
			case OPH_QUERY_EXPR_TYPE_STRING:
			case OPH_QUERY_EXPR_TYPE_BINARY:
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				if ((tmp_args->arg_type[l] != DECIMAL_RESULT) && (tmp_args->arg_type[l] != REAL_RESULT)){
					//Double should be considered long long
					tmp_args->lengths[l] =  (unsigned long)sizeof(long long);
					free(tmp_args->args[l]);
					tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
					if(!tmp_args->args[l])	
					{
						free_udf_arg(tmp_args);
						free(tmp_args);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error after calling plugin INIT function\n");
						return -1;
					}
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				if(tmp_args->arg_type[l] != INT_RESULT){
					//Int should be considered double
					tmp_args->lengths[l] =  (unsigned long)sizeof(double);
					free(tmp_args->args[l]);
					tmp_args->args[l] = (char *)malloc(tmp_args->lengths[l]*sizeof(char));
					if(!tmp_args->args[l])	
					{
						free_udf_arg(tmp_args);
						free(tmp_args);
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error after calling plugin INIT function\n");
						return -1;
					}
				}
				break;
			default:
				free_udf_arg(tmp_args);
				free(tmp_args);
				return -1;
		}
	}


	*internal_args = tmp_args;

	return 0;
}

int oph_query_plugin_add(oph_plugin_api *function, void **dlh, UDF_INIT *initid, UDF_ARGS *internal_args, int arg_count, oph_query_expr_value* args){
	if(!function || !dlh || !initid || !internal_args || !arg_count || !args)
		return -1;

	char is_null = 0, error = 0;

    //Add function
	if (!(_oph_plugin_add = (void (*)(UDF_INIT*,  UDF_ARGS*, char*, char*)) function->add_api)){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
		return -1;
	}

  	int l = 0;

    //Update UDF fields
    for(l = 0; l < arg_count ; l++){
		switch(args[l].type){
			case OPH_QUERY_EXPR_TYPE_STRING:
				internal_args->lengths[l] =  (unsigned long)(strlen(args[l].data.string_value) +1);
				if(internal_args->args[l]) free(internal_args->args[l]);
				internal_args->args[l] = (char *)malloc(internal_args->lengths[l]*sizeof(char));
				if(!internal_args->args[l])	
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin ADD function\n");
					return -1;
				}
		        memcpy ((char *)internal_args->args[l], args[l].data.string_value, internal_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				internal_args->lengths[l] =  (unsigned long)args[l].data.binary_value->arg_length;
				if(internal_args->args[l]) free(internal_args->args[l]);
				internal_args->args[l] = (char *)malloc(internal_args->lengths[l]*sizeof(char));
				if(!internal_args->args[l])	
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin ADD function\n");
					return -1;
				}
		        memcpy ((char *)internal_args->args[l], args[l].data.binary_value->arg, internal_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				//Check if type should be casted
				if ((internal_args->arg_type[l] != DECIMAL_RESULT) && (internal_args->arg_type[l] != REAL_RESULT)){
					long long val_l = (long long)args[l].data.double_value;
		        	memcpy ((char *)internal_args->args[l], &(val_l), internal_args->lengths[l]*sizeof(char)); 
				}
				else{
		        	memcpy ((char *)internal_args->args[l], &(args[l].data.double_value), internal_args->lengths[l]*sizeof(char)); 
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				//Check if type should be casted
				if (internal_args->arg_type[l] != INT_RESULT){
					double val_d = (double)args[l].data.long_value;
		        	memcpy ((char *)internal_args->args[l], &(val_d), internal_args->lengths[l]*sizeof(char)); 
				}
				else{
		        	memcpy ((char *)internal_args->args[l], &(args[l].data.long_value), internal_args->lengths[l]*sizeof(char)); 
				}
				break;
			case OPH_QUERY_EXPR_TYPE_NULL:
				break;	
			default:
				return -1;
		}
	}	

	//Run add function
	_oph_plugin_add (initid, internal_args, &is_null, &error);
	if(error){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
		return -1;
	}

	return 0;
}


int oph_query_plugin_exec(oph_plugin_api *function, void **dlh, UDF_INIT *initid, UDF_ARGS *internal_args, char *plugin_name, int arg_count, oph_query_expr_value* args, oph_query_expr_value *res){
	if(!function || !dlh || !initid || !internal_args || !plugin_name || !arg_count || !args || !plugin_table)
		return -1;

	char is_null = 0, error = 0, result = 0;

	//Load plugin shared library
	oph_plugin *plugin = (oph_plugin*) hashtbl_get(plugin_table, plugin_name);
	if(!plugin){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Plugin not allowed\n");
		return -1;
	}

  	int l = 0;

    //Update UDF fields
    for(l = 0; l < arg_count ; l++){
		switch(args[l].type){
			case OPH_QUERY_EXPR_TYPE_STRING:
				internal_args->lengths[l] =  (unsigned long)(strlen(args[l].data.string_value) +1);
				if(internal_args->args[l]) free(internal_args->args[l]);
				internal_args->args[l] = (char *)malloc((internal_args->lengths[l]+1)*sizeof(char));
				if(!internal_args->args[l])	
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin EXEC function\n");
					return -1;
				}
		        memcpy ((char *)internal_args->args[l], args[l].data.string_value, internal_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				internal_args->lengths[l] =  (unsigned long)args[l].data.binary_value->arg_length;
				if(internal_args->args[l]) free(internal_args->args[l]);
				internal_args->args[l] = (char *)malloc(internal_args->lengths[l]*sizeof(char));
				if(!internal_args->args[l])	
				{
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin EXEC function\n");
					return -1;
				}
		        memcpy ((char *)internal_args->args[l], args[l].data.binary_value->arg, internal_args->lengths[l]*sizeof(char)); 
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				//Check if type should be casted
				if ((internal_args->arg_type[l] != DECIMAL_RESULT) && (internal_args->arg_type[l] != REAL_RESULT)){
					long long val_l = (long long)args[l].data.double_value;
		        	memcpy ((char *)internal_args->args[l], &(val_l), internal_args->lengths[l]*sizeof(char)); 
				}
				else{
		        	memcpy ((char *)internal_args->args[l], &(args[l].data.double_value), internal_args->lengths[l]*sizeof(char)); 
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				//Check if type should be casted
				if (internal_args->arg_type[l] != INT_RESULT){
					double val_d = (double)args[l].data.long_value;
		        	memcpy ((char *)internal_args->args[l], &(val_d), internal_args->lengths[l]*sizeof(char)); 
				}
				else{
		        	memcpy ((char *)internal_args->args[l], &(args[l].data.long_value), internal_args->lengths[l]*sizeof(char)); 
				}
				break;
			case OPH_QUERY_EXPR_TYPE_NULL:
				break;	
			default:
				return -1;
		}
	}

	void *rs = NULL;
	unsigned long long rs_length = 0;

	if(_oph_execute_plugin(plugin, internal_args, initid, &rs, &rs_length, &is_null, &error, &result, function )){
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
		return -1;
	}

	switch(plugin->plugin_return){
		case OPH_IOSTORE_STRING_TYPE:
		{

			oph_query_arg *temp = (oph_query_arg *)malloc(sizeof(oph_query_arg));
			if(temp == NULL){
				free(rs);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error after calling plugin EXEC function\n");
				return -1;
			}
			res->data.binary_value = temp;	

			res->data.binary_value->arg = rs;	
			//TODO Set right type
			res->data.binary_value->arg_type = OPH_QUERY_TYPE_BLOB;	
			res->data.binary_value->arg_length = rs_length;	
			res->data.binary_value->arg_is_null = is_null;	
			break;
		}
		case OPH_IOSTORE_LONG_TYPE:
		{
			res->data.long_value = *(long long *)rs;	
			free(rs);		
			break;
		}

		case OPH_IOSTORE_REAL_TYPE:
		{
			res->data.double_value = *(double *)rs;
			free(rs);		
			break;
		}
		default:
			return -1;
	}

	return 0;
}

