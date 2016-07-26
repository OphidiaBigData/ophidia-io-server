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

#include "oph_query_parser.h"

#include "oph_iostorage_data.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <ltdl.h>
#include <string.h>

#include <debug.h>

#include <ctype.h>
#include <unistd.h>
#include "taketime.h"

#include "oph_server_utility.h"

#define CLIENT_HELP "This client is used to run Ophidia plugin directly on memory data. \n\
Random data is created for the test.\n\
\n\
Usage example: ./plugin_test -p oph_dump -r 10 -a 10 \n\
Explanation:  oph_dump plugin is executed on a table of 10 rows each\n\
containing an array of 10 random values \n \
\n \
Allowed arguments are: \n\
-h \t\t show this message \n\
-p PLUGIN \t specify the plugin to run. Allowed plugins are: \n\
\t\t oph_dump, oph_count_array, oph_convert_d, oph_reduce3,\n\
\t\t oph_aggregate_operator and oph_get_subarray \n\
-r ROW NUM \t specifiy the number of rows used for the test \n\
-a ARRAY LENGTH  specifiy the number of elements in each array \n"

int exec(const char *query_string, long long rows, long long arrayl, char **result, long long *res_length){

	if(!rows || !arrayl || !query_string || !result || !res_length){
		return -1;
	}

	//Total Exec time evaluate
	struct timeval start_time, end_time, total_time;
	//Partial Exec time evaluate
	struct timeval s_time, e_time, t_time;

gettimeofday(&start_time, NULL);
gettimeofday(&s_time, NULL);		

	//Build sample fragment with double type
	oph_iostore_frag_record_set *record_set = NULL;
	if(oph_iostore_create_sample_frag(rows, arrayl, &record_set)){
		oph_iostore_destroy_frag_recordset(&record_set);
				return 1;
	}			

gettimeofday(&e_time, NULL);
timeval_subtract(&t_time, &e_time, &s_time);
  pmesg(LOG_INFO, __FILE__, __LINE__, "Populate fragment:\t Time %d,%06d sec\n", (int)t_time.tv_sec, (int)t_time.tv_usec);

  pmesg(LOG_INFO, __FILE__, __LINE__, "Input fragment size: %f\n", (double)((rows+1)*sizeof(oph_iostore_frag_record*) + (rows+1)*sizeof(oph_iostore_frag_record) + rows*arrayl*sizeof(double)*sizeof(char) )/(1024*1024));

#ifdef DEBUG_1
	long long r, a;
  unsigned long long *id = 0;
	//Show fragment data
	for(r=0; r < rows; r++){
    id = ((void *)record_set->record_set[r]->field[0]);
		printf("%llu - ", *id);
		//Fill array				
		for(a = 0; a < arrayl; a++)
			printf("%f - ", *((double*)((char*)record_set->record_set[r]->field[1] + a*sizeof(double))));
		printf("\n");
	}
#endif

	//Pre-load all plugin information
gettimeofday(&s_time, NULL);

	HASHTBL *plugin_table = NULL;
	if(oph_query_engine_start(&plugin_table)){
		oph_iostore_destroy_frag_recordset(&record_set);
		oph_query_engine_end(&plugin_table);
				return 1;
	}

gettimeofday(&e_time, NULL);
timeval_subtract(&t_time, &e_time, &s_time);
  pmesg(LOG_INFO, __FILE__, __LINE__, "Load all plugin structs:\t Time %d,%06d sec\n", (int)t_time.tv_sec, (int)t_time.tv_usec);

gettimeofday(&s_time, NULL);
  //Define global variables
       oph_iostore_frag_record_set *result_set = NULL;

	if(oph_query_engine_run(plugin_table, query_string, record_set, &result_set)){
		oph_iostore_destroy_frag_recordset(&record_set);
		oph_query_engine_end(&plugin_table);
		return 1;
	}
		
gettimeofday(&e_time, NULL);
timeval_subtract(&t_time, &e_time, &s_time);
  pmesg(LOG_INFO, __FILE__, __LINE__, "Execute plugin:\t\t Time %d,%06d sec\n", (int)t_time.tv_sec, (int)t_time.tv_usec);	

	oph_iostore_destroy_frag_recordset(&record_set);
	if(!result_set){
		oph_query_engine_end(&plugin_table);
		return 1;
	}

gettimeofday(&s_time, NULL);
	long long i = 0;
  long long n = 0;
  int is_string = 0;
  //Alloc result and compute result lenght
	while(result_set->record_set[i]){
	  if(result_set->field_type[1] == OPH_IOSTORE_LONG_TYPE || result_set->field_type[1]== OPH_IOSTORE_REAL_TYPE){
		  n += 24;
		  n += 20;
	  }
	  else if(result_set->field_type[1]== OPH_IOSTORE_STRING_TYPE){
      is_string = 0;
			is_numeric_string(result_set->record_set[i]->field_length[1], result_set->record_set[i]->field[1], &is_string);
		  if(is_string){
        n += result_set->record_set[i]->field_length[1];
		    n += 20;
      }
      else{
		    n += 26*(long long)(result_set->record_set[i]->field_length[1]/sizeof(double));
		    n += 20; 
      }     
	  }
    i++;
  }
  *res_length= n;
  *result = (char*)malloc(*res_length*sizeof(char));
  n = i = 0;

  long long len = 0;
  long long j = 0;
	double *k = NULL;
  char *out_string = NULL;
  long long *var_l = NULL;
  double *var_d = NULL;
  char *var_s = NULL;
				
	while(result_set->record_set[i]){
		if(result_set->field_type[1]== OPH_IOSTORE_LONG_TYPE){
			var_l = result_set->record_set[i]->field[1];
#ifdef DEBUG
			if(var_l){
				n += snprintf((*result)+n, BUFLEN, "Plugin result is: %lld\n", *var_l);
				printf("Plugin result is: %lld\n", *var_l);
      }
#endif
		}
		else if(result_set->field_type[1]== OPH_IOSTORE_REAL_TYPE){
			var_d = result_set->record_set[i]->field[1];
#ifdef DEBUG
			if(var_d){
				n += snprintf((*result)+n, BUFLEN, "Plugin result is: %f\n", *var_d);
				printf("Plugin result is: %f\n", *var_d);
      }
#endif
		}
		else if(result_set->field_type[1]== OPH_IOSTORE_STRING_TYPE){
			var_s = result_set->record_set[i]->field[1];
			len = result_set->record_set[i]->field_length[1];
#ifdef DEBUG
			if(var_s){
				is_string = 0;
				is_numeric_string(len, var_s, &is_string);

				if(is_string){
					out_string = (char *)malloc(len +1 * sizeof(char));
					if(out_string){
						memcpy(out_string, var_s, len);
						out_string[len] = 0;
						n += snprintf((*result)+n, BUFLEN, "Plugin result is: %s\n", out_string);
						printf("Plugin result is: %s\n", out_string);
						free(out_string);
					}
					else{
						oph_iostore_destroy_frag_recordset(&result_set);
						oph_query_engine_end(&plugin_table);
						return -1;
					}
				}
				else{
					j = 0;
					k = NULL;
          n += snprintf((*result)+n, BUFLEN, "Plugin result is: ");
          printf("Plugin result is: ");
					for(j = 0; j < (long long)(len/sizeof(double)); j++){
						k = (double*) (var_s + j* sizeof(double));
						n += snprintf((*result)+n, BUFLEN, "%f, ",*k);
            printf("%f, ",*k);
					}
					n += snprintf((*result)+n, BUFLEN, "\n");
          printf("\n");
				}
			}
#endif
		}
		i++;
	}
gettimeofday(&e_time, NULL);
timeval_subtract(&t_time, &e_time, &s_time);
  pmesg(LOG_INFO, __FILE__, __LINE__, "Show output result:\t Time %d,%06d sec\n", (int)t_time.tv_sec, (int)t_time.tv_usec);	

	oph_iostore_destroy_frag_recordset(&result_set);
	oph_query_engine_end(&plugin_table);
	
gettimeofday(&end_time, NULL);
timeval_subtract(&total_time, &end_time, &start_time);
  pmesg(LOG_INFO, __FILE__, __LINE__, "Total execution:\t Time %d,%06d sec\n", (int)total_time.tv_sec, (int)total_time.tv_usec);

	return 0;
}

int main(int argc, char **argv){

  set_debug_level(LOG_INFO);
  set_log_prefix(OPH_IO_SERVER_PREFIX);

	opterr = 0;
	int c;

	char *func_name = NULL;
	long long rows = 0;
	long long arrayl = 0;
	int row_flag = 0, array_flag = 0, plugin_flag = 0;

	while ((c = getopt (argc, argv, "hp:r:a:")) != -1){
		switch (c){
			case 'r':
				if (optarg){
					row_flag = 1;
					rows = strtoll(optarg,NULL,10);
				}
				break;
			case 'a':
				if (optarg){
					array_flag = 1;
					arrayl = strtoll(optarg,NULL,10);
				}
				break;
			case 'p':
				if (optarg){
					plugin_flag = 1;
					func_name = strdup(optarg);
				}
				break;
			case 'h':
			default:
				printf(CLIENT_HELP);
				return -1;
		}
	}

	if(!row_flag || !array_flag || !plugin_flag){
		printf(CLIENT_HELP);
		if(func_name) free(func_name);
		return -1;
	}

  char *result = NULL;
  long long len = 0;
  if(!exec(func_name, rows, arrayl, &result, &len))
    printf("%s\n", result);


  //Test simple submission string
  char test_string[] = "operation=test;db=Hello World!"; 
   HASHTBL *query_args = NULL;

  if(oph_query_parser(test_string, &query_args)){
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Unable to parse test query\n");
    hashtbl_destroy(query_args); 
    return -1;
  }
  char *test_arg = (char*) hashtbl_get(query_args, "operation");

  if(test_arg && STRCMP(test_arg, "test") == 0){
    printf("Test query parsed correctly\n");
  }
  else{
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Unable to parse test query\n");
  }

  hashtbl_destroy(query_args);  
 
  if(result) free(result);
	if(func_name) free(func_name);
  return 0;
}
