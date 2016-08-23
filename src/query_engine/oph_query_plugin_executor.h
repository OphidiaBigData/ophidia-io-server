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

#ifndef OPH_QUERY_PLUGIN_EXEC_H
#define OPH_QUERY_PLUGIN_EXEC_H

#include <mysql.h> // It contains UDF-related symbols and data structures
#include <mysql_com.h>
#include <ltdl.h>

#include "oph_query_plugin_loader.h"
#include "oph_query_parser.h"
#include "oph_iostorage_data.h"

#define BUFLEN 1024

//UDF interfaces. UDF_ARGS and UDF_INIT are defined in mysql_com.h

//UDF fixed interface
my_bool (*_oph_plugin_init)(UDF_INIT*, UDF_ARGS*, char*);
void (*_oph_plugin_deinit)(UDF_INIT*);
//UDF depending upon return type (long long, double or char)
long long (*_oph_plugin1)(UDF_INIT*, UDF_ARGS*, char*, char*);
double (*_oph_plugin2)(UDF_INIT*, UDF_ARGS*, char*, char*);
char* (*_oph_plugin3)(UDF_INIT*, UDF_ARGS*,  char*, unsigned long*, char*, char*);
//UDF aggregate functions interfaces
void (*_oph_plugin_add)(UDF_INIT*, UDF_ARGS*, char*, char*);
void (*_oph_plugin_reset)(UDF_INIT*, UDF_ARGS*, char*, char*);
void (*_oph_plugin_clear)(UDF_INIT*, char*, char*);

/**
 * \brief			          Structure to retrieve data where plugin is applied 
 * \param groups 	      Group of NULL-terminated record sets (record arrays)
 * \param group_number	Number of records per group
 */
typedef struct {
  oph_iostore_frag_record ***groups;  
  int group_number;
} oph_selection;

/**
 * \brief		            Structure to store description of a plugin argument
 * \param arg_type 	    Type of argument
 * \param arg_length    Argument length
 * \param arg_value     Argument value
 * \param arg_pointer   Pointer to selected data related to argument 
 */
typedef struct {
  enum Item_result arg_type;
  unsigned long arg_length; 
  char *arg_value;
  oph_selection *arg_pointer;
} oph_udf_arg;

/**
 * \brief			            Structure to contain UDF plugins function symbols
 * \param init_api   	    Pointer to init function in shared lib 
 * \param reset_api     	Pointer to reset function in shared lib (can be NULL)
 * \param clear_api       Pointer to clear function in shared lib (can be NULL)
 * \param add_api         Pointer to add function in shared lib (can be NULL)
 * \param exec_api        Pointer to exec function in shared lib
 * \param deinit_api      Pointer to deinit function in shared lib
 */
typedef struct{
	lt_ptr init_api;
	lt_ptr reset_api;
	lt_ptr clear_api;
	lt_ptr add_api;
	lt_ptr exec_api;
	lt_ptr deinit_api;
} plugin_api;

/**
 * \brief               Function used to assign a selects recordset to oph_selection structure
 * \param select        Pointer where oph_selection structure will be created
 * \param recordset     Pointer to recordset array to be assigned
 * \return              0 if successfull, non-0 otherwise
 */
int oph_select_data(oph_selection **select, oph_iostore_frag_record **recordset);

/**
 * \brief               Function used to free oph_select structure
 * \param select        Pointer to oph_selection structure to be freed
 * \return              0 if successfull, non-0 otherwise
 */
int oph_free_select_data(oph_selection *select);

/**
 * \brief               Function used to free UDF_ARG argument
 * \param arguments     Pointer to UDF_ARG structure to be freed
 * \return              0 if successfull, non-0 otherwise
 */
int free_udf_arg(UDF_ARGS *arguments);

/**
 * \brief             Internal function used by other oph_execute to run the exec_api of a plugin
 * \param plugin      Plugin to be executed
 * \param args        UDF Args used as plugin input
 * \param initid      UDF Init used by plugin
 * \param res         Pointer used to store plugin result value
 * \param res_length  Length of plugin result
 * \param is_null     Flag defining if plugin result is null
 * \param error       Possible plugin error message
 * \param functions   Structure containing pointer to plugin library symbols
 * \return            0 if successfull, non-0 otherwise
 */
int _oph_execute_plugin(const oph_plugin *plugin, UDF_ARGS *args, UDF_INIT *initid, void **res, unsigned long long *res_length, char *is_null, char *error, char *result,  plugin_api *functions);

/**
 * \brief                 Function used to run all the stesp of a UDF plugin (extended version)
 * \param plugin          Plugin to be executed
 * \param args            Arguments used as plugin input
 * \param arg_count       Number of arguments used as plugin input
 * \param result_set      Pointer used for output record set
 * \param field_list_num  Number of fields in input recordset
 * \param id_col_index    Index of column containing id values
 * \param col_index       Index of column containing measure values
 * \param limit           Number of rows to be considered by the plugin
 * \param offset          First row to be considered by the plugin
 * \param omp_thread_num  Number of OpenMP threads to be used
 * \return                0 if successfull, non-0 otherwise
 */
int oph_execute_plugin(const oph_plugin *plugin, oph_udf_arg *args, unsigned int arg_count, oph_iostore_frag_record_set **result_set, int field_list_num, int id_col_index, int col_index, long long limit, long long offset, unsigned short omp_thread_num);

/**
 * \brief                 Function used to run all the stesp of a UDF plugin
 * \param plugin          Plugin to be executed
 * \param args            Arguments used as plugin input
 * \param arg_count       Number of arguments used as plugin input
 * \param result_set      Pointer used for output record set
 * \param field_list_num  Number of fields in input recordset
 * \param col_index       Index of column containing measure values
 * \param row_number      Number of rows to be considered by the plugin
 * \param offset          First row to be considered by the plugin
 * \param omp_thread_num  Number of OpenMP threads to be used
 * \return                0 if successfull, non-0 otherwise
 */
int oph_execute_plugin2(const oph_plugin *plugin, oph_udf_arg *args, unsigned int arg_count, oph_iostore_frag_record_set *result_set, int field_list_num, int col_index, long long row_number, long long offset, unsigned short omp_thread_num);

/**
 * \brief       Function to set up oph_udf_arg from the query parameter
 * \param param Query parameter
 * \param args  Pointer to arg structure to set up
 * \return      0 if successfull, non-0 otherwise
 */
int oph_set_udf_arg(const char *param, oph_udf_arg *arg);

/**
 * \brief       Function to free oph_udf_arg
 * \param args  Pointer to arg structure to free
 * \return      0 if successfull, non-0 otherwise
 */
int oph_free_udf_arg(oph_udf_arg *arg);

/**
 * \brief               Function to get plugin library and arguments from query string 
 * \param query_string  Query string with plugin expression to be extracted
 * \param plugin_table  Hash table with plugin libraries available
 * \param record_set    Recordset where plugin should be applied
 * \param plugin        Pointer to plugin library selected
 * \param args          Array of plugin arguments
 * \param args_count    Number of plugin arguments 
 * \return              0 if successfull, non-0 otherwise
 */
int oph_parse_plugin(const char* query_string, HASHTBL *plugin_table, oph_iostore_frag_record_set *record_set, oph_plugin **plugin, oph_query_arg **stmt_args, oph_udf_arg **args, unsigned int *arg_count);

#endif /* OPH_QUERY_PLUGIN_EXEC_H */
