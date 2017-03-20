/*
    Ophidia IO Server
    Copyright (C) 2014-2017 CMCC Foundation

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
extern pthread_mutex_t libtool_lock;
//TODO Restore OpenMP code
extern unsigned long long omp_threads;
extern HASHTBL *plugin_table;

//TODO - Add debug mesg and logging
//TODO - Define specific return codes

int free_udf_arg(UDF_ARGS * args)
{
	if (!args)
		return -1;

	unsigned int i = 0;
	for (i = 0; i < args->arg_count; i++) {
		if (args->args[i])
			free(args->args[i]);
		args->args[i] = NULL;
	}
	free(args->arg_type);
	free(args->args);
	free(args->lengths);

	return 0;
}

int _oph_execute_plugin(const oph_plugin * plugin, UDF_ARGS * args, UDF_INIT * initid, void **res, unsigned long long *res_length, char *is_null, char *error, char *result, oph_plugin_api * functions)
{
	if (!plugin || !args || !res || !res_length || !functions)
		return -1;

	unsigned long len = 0;

	if (memory_check())
		return -1;

	//UDF depending upon return type (long long, double or char)
	long long (*_oph_plugin1) (UDF_INIT *, UDF_ARGS *, char *, char *);
	double (*_oph_plugin2) (UDF_INIT *, UDF_ARGS *, char *, char *);
	char *(*_oph_plugin3) (UDF_INIT *, UDF_ARGS *, char *, unsigned long *, char *, char *);

	//Execute main function
	switch (plugin->plugin_return) {
		case OPH_IOSTORE_LONG_TYPE:
			{
				if (!(_oph_plugin1 = (long long (*)(UDF_INIT *, UDF_ARGS *, char *, char *)) functions->exec_api)) {
					return -1;
				}
				long long tmp_res = (long long) _oph_plugin1(initid, args, is_null, error);
				if (*error == 1) {
					return -1;
				}
				if (!*res && !*res_length)
					*res = (void *) malloc(1 * sizeof(long long));
				memcpy(*res, (void *) &tmp_res, sizeof(long long));
				*res_length = sizeof(tmp_res);
				break;
			}
		case OPH_IOSTORE_REAL_TYPE:
			{
				if (!(_oph_plugin2 = (double (*)(UDF_INIT *, UDF_ARGS *, char *, char *)) functions->exec_api)) {
					return -1;
				}
				double tmp_res = (double) _oph_plugin2(initid, args, is_null, error);
				if (*error == 1) {
					return -1;
				}
				if (!*res && !*res_length)
					*res = (double *) malloc(1 * sizeof(double));
				memcpy(*res, (void *) &tmp_res, sizeof(double));
				*res_length = sizeof(tmp_res);
				break;
			}
		case OPH_IOSTORE_STRING_TYPE:
			{
				if (!(_oph_plugin3 = (char *(*)(UDF_INIT *, UDF_ARGS *, char *, unsigned long *, char *, char *)) functions->exec_api)) {
					return -1;
				}

				char *tmp_res = _oph_plugin3(initid, args, result, &len, is_null, error);
				if (*error == 1) {
					return -1;
				}
				if (!*res && !*res_length)
					*res = (char *) malloc(sizeof(char) * (len));
				memcpy(*res, (void *) tmp_res, len);
				*res_length = len;
				break;
			}
		default:
			return -1;
	}
	//End cycle     
	return 0;
}

int oph_query_plugin_clear(oph_plugin_api * function, void *dlh, UDF_INIT * initid)
{
	if (!function || !dlh || !initid)
		return -1;

	char is_null = 0, error = 0;


	//Clear function
	void (*_oph_plugin_clear) (UDF_INIT *, char *, char *);
	if (!(_oph_plugin_clear = (void (*)(UDF_INIT *, char *, char *)) function->clear_api)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
		return -1;
	}

	_oph_plugin_clear(initid, &is_null, &error);
	if (error) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin CLEAR function\n");
		return -1;
	}

	return 0;
}


int oph_query_plugin_deinit(oph_plugin_api * function, void *dlh, UDF_INIT * initid, UDF_ARGS * internal_args)
{
	if (!function || !dlh || !initid || !internal_args)
		return -1;

	//Deinitialize function
	void (*_oph_plugin_deinit) (UDF_INIT *);
	if (!(_oph_plugin_deinit = (void (*)(UDF_INIT *)) function->deinit_api)) {
		pthread_mutex_lock(&libtool_lock);
		lt_dlclose(dlh);
		lt_dlexit();
		pthread_mutex_unlock(&libtool_lock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin DEINIT function\n");
		return -1;
	}

	_oph_plugin_deinit(initid);

	free(initid);
	free_udf_arg(internal_args);
	free(internal_args);

	//Release functions
#ifndef OPH_WITH_VALGRIND
	//Close dynamic loaded library
	pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(dlh))) {
		lt_dlexit();
		pthread_mutex_unlock(&libtool_lock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
	pthread_mutex_unlock(&libtool_lock);
	dlh = NULL;

	pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()) {
		pthread_mutex_unlock(&libtool_lock);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing plugin library\n");
		return -1;
	}
	pthread_mutex_unlock(&libtool_lock);
#endif

	return 0;
}

int oph_query_plugin_init(oph_plugin_api * function, void **dlh, UDF_INIT ** initid, UDF_ARGS ** internal_args, char *plugin_name, int arg_count, oph_query_expr_value * args, char *is_aggregate)
{
	if (!function || !dlh || !initid || !internal_args || !plugin_name || !arg_count || !args || !plugin_table || !is_aggregate)
		return -1;

	//Load plugin shared library
	oph_plugin *plugin = (oph_plugin *) hashtbl_get(plugin_table, plugin_name);
	if (!plugin) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Plugin not allowed\n");
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
	lt_dlinit();
	pthread_mutex_unlock(&libtool_lock);

	pthread_mutex_lock(&libtool_lock);
	//Load library
	if (!(*dlh = (lt_dlhandle) lt_dlopen(plugin->plugin_library))) {
		lt_dlclose(*dlh);
		lt_dlexit();
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading plugin dynamic library: %s\n", lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return -1;
	}
	pthread_mutex_unlock(&libtool_lock);

	//Load all library symbols
	pthread_mutex_lock(&libtool_lock);
	function->init_api = lt_dlsym(*dlh, plugin_init_name);
	function->clear_api = NULL;
	function->reset_api = NULL;
	function->add_api = NULL;
	function->exec_api = lt_dlsym(*dlh, plugin->plugin_name);
	function->deinit_api = lt_dlsym(*dlh, plugin_deinit_name);
	if ((plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE)) {
		function->clear_api = lt_dlsym(*dlh, plugin_clear_name);
		function->reset_api = lt_dlsym(*dlh, plugin_reset_name);
		function->add_api = lt_dlsym(*dlh, plugin_add_name);
	}
	pthread_mutex_unlock(&libtool_lock);

	*is_aggregate = (plugin->plugin_type == OPH_AGGREGATE_PLUGIN_TYPE);


	//Initialize function
	my_bool(*_oph_plugin_init) (UDF_INIT *, UDF_ARGS *, char *);
	if (!(_oph_plugin_init = (my_bool(*)(UDF_INIT *, UDF_ARGS *, char *)) function->init_api)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function\n");
		return -1;
	}

	char *message = (char *) malloc(BUFLEN * sizeof(char));
	if (message == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		return -1;
	}
	//SETUP UDF_ARG
	UDF_ARGS *tmp_args = (UDF_ARGS *) malloc(sizeof(UDF_ARGS));
	if (tmp_args == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		free(message);
		return -1;
	}

	int l = 0;

	//Create new UDF fixed fields
	tmp_args->arg_count = arg_count;
	tmp_args->arg_type = (enum Item_result *) calloc(arg_count, sizeof(enum Item_result));
	tmp_args->args = (char **) calloc(arg_count, sizeof(char *));
	tmp_args->lengths = (unsigned long *) calloc(arg_count, sizeof(unsigned long));

	if (!tmp_args->arg_type || !tmp_args->args || !tmp_args->lengths) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		return -1;
	}
	//First time setup all the values
	for (l = 0; l < arg_count; l++) {
		switch (args[l].type) {
			case OPH_QUERY_EXPR_TYPE_STRING:
				tmp_args->arg_type[l] = STRING_RESULT;
				tmp_args->lengths[l] = (unsigned long) (strlen(args[l].data.string_value) + 1);
				tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
				if (!tmp_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					return -1;
				}
				memcpy((char *) tmp_args->args[l], args[l].data.string_value, tmp_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				tmp_args->arg_type[l] = STRING_RESULT;
				tmp_args->lengths[l] = (unsigned long) args[l].data.binary_value->arg_length;
				tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
				if (!tmp_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					return -1;
				}
				memcpy((char *) tmp_args->args[l], args[l].data.binary_value->arg, tmp_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				tmp_args->arg_type[l] = DECIMAL_RESULT;
				tmp_args->lengths[l] = (unsigned long) sizeof(double);
				tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
				if (!tmp_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					return -1;
				}
				memcpy((char *) tmp_args->args[l], &(args[l].data.double_value), tmp_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				tmp_args->arg_type[l] = INT_RESULT;
				tmp_args->lengths[l] = (unsigned long) sizeof(long long);
				tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
				if (!tmp_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
					free(message);
					free_udf_arg(tmp_args);
					free(tmp_args);
					return -1;
				}
				memcpy((char *) tmp_args->args[l], &(args[l].data.long_value), tmp_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_NULL:
				tmp_args->arg_type[l] = INT_RESULT;
				tmp_args->lengths[l] = 0;
				tmp_args->args[l] = NULL;
				break;
			default:
				free(message);
				free_udf_arg(tmp_args);
				free(tmp_args);
				return -1;
		}
	}

	*message = 0;

	*initid = (UDF_INIT *) malloc(sizeof(UDF_INIT));
	if (*initid == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin INIT function\n");
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		*initid = NULL;
		return -1;
	}

	if (_oph_plugin_init(*initid, tmp_args, message)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin INIT function: %s\n", message);
		free(message);
		free_udf_arg(tmp_args);
		free(tmp_args);
		return -1;
	}
	free(message);

	//Check if some field type has been updated by init
	for (l = 0; l < arg_count; l++) {
		switch (args[l].type) {
			case OPH_QUERY_EXPR_TYPE_NULL:
			case OPH_QUERY_EXPR_TYPE_STRING:
			case OPH_QUERY_EXPR_TYPE_BINARY:
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				if ((tmp_args->arg_type[l] != DECIMAL_RESULT) && (tmp_args->arg_type[l] != REAL_RESULT)) {
					//Double should be considered long long
					tmp_args->lengths[l] = (unsigned long) sizeof(long long);
					free(tmp_args->args[l]);
					tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
					if (!tmp_args->args[l]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error after calling plugin INIT function\n");
						free_udf_arg(tmp_args);
						free(tmp_args);
						return -1;
					}
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				if (tmp_args->arg_type[l] != INT_RESULT) {
					//Int should be considered double
					tmp_args->lengths[l] = (unsigned long) sizeof(double);
					free(tmp_args->args[l]);
					tmp_args->args[l] = (char *) malloc(tmp_args->lengths[l] * sizeof(char));
					if (!tmp_args->args[l]) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error after calling plugin INIT function\n");
						free_udf_arg(tmp_args);
						free(tmp_args);
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

int oph_query_plugin_add(oph_plugin_api * function, void **dlh, UDF_INIT * initid, UDF_ARGS * internal_args, int arg_count, oph_query_expr_value * args)
{
	if (!function || !dlh || !initid || !internal_args || !arg_count || !args)
		return -1;

	char is_null = 0, error = 0;

	//Add function
	void (*_oph_plugin_add) (UDF_INIT *, UDF_ARGS *, char *, char *);
	if (!(_oph_plugin_add = (void (*)(UDF_INIT *, UDF_ARGS *, char *, char *)) function->add_api)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
		return -1;
	}

	int l = 0;

	//Update UDF fields
	for (l = 0; l < arg_count; l++) {
		switch (args[l].type) {
			case OPH_QUERY_EXPR_TYPE_STRING:
				internal_args->lengths[l] = (unsigned long) (strlen(args[l].data.string_value) + 1);
				if (internal_args->args[l])
					free(internal_args->args[l]);
				internal_args->args[l] = (char *) malloc(internal_args->lengths[l] * sizeof(char));
				if (!internal_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin ADD function\n");
					return -1;
				}
				memcpy((char *) internal_args->args[l], args[l].data.string_value, internal_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				internal_args->lengths[l] = (unsigned long) args[l].data.binary_value->arg_length;
				if (internal_args->args[l])
					free(internal_args->args[l]);
				internal_args->args[l] = (char *) malloc(internal_args->lengths[l] * sizeof(char));
				if (!internal_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin ADD function\n");
					return -1;
				}
				memcpy((char *) internal_args->args[l], args[l].data.binary_value->arg, internal_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				//Check if type should be casted
				if ((internal_args->arg_type[l] != DECIMAL_RESULT) && (internal_args->arg_type[l] != REAL_RESULT)) {
					long long val_l = (long long) args[l].data.double_value;
					memcpy((char *) internal_args->args[l], &(val_l), internal_args->lengths[l] * sizeof(char));
				} else {
					memcpy((char *) internal_args->args[l], &(args[l].data.double_value), internal_args->lengths[l] * sizeof(char));
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				//Check if type should be casted
				if (internal_args->arg_type[l] != INT_RESULT) {
					double val_d = (double) args[l].data.long_value;
					memcpy((char *) internal_args->args[l], &(val_d), internal_args->lengths[l] * sizeof(char));
				} else {
					memcpy((char *) internal_args->args[l], &(args[l].data.long_value), internal_args->lengths[l] * sizeof(char));
				}
				break;
			case OPH_QUERY_EXPR_TYPE_NULL:
				break;
			default:
				return -1;
		}
	}

	//Run add function
	_oph_plugin_add(initid, internal_args, &is_null, &error);
	if (error) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin ADD function\n");
		return -1;
	}

	return 0;
}


int oph_query_plugin_exec(oph_plugin_api * function, void **dlh, UDF_INIT * initid, UDF_ARGS * internal_args, char *plugin_name, int arg_count, oph_query_expr_value * args, oph_query_expr_value * res)
{
	if (!function || !dlh || !initid || !internal_args || !plugin_name || !arg_count || !args || !plugin_table)
		return -1;

	char is_null = 0, error = 0, result = 0;

	//Load plugin shared library
	oph_plugin *plugin = (oph_plugin *) hashtbl_get(plugin_table, plugin_name);
	if (!plugin) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Plugin not allowed\n");
		return -1;
	}

	int l = 0;

	//Update UDF fields
	for (l = 0; l < arg_count; l++) {
		switch (args[l].type) {
			case OPH_QUERY_EXPR_TYPE_STRING:
				internal_args->lengths[l] = (unsigned long) (strlen(args[l].data.string_value) + 1);
				if (internal_args->args[l])
					free(internal_args->args[l]);
				internal_args->args[l] = (char *) malloc((internal_args->lengths[l] + 1) * sizeof(char));
				if (!internal_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin EXEC function\n");
					return -1;
				}
				memcpy((char *) internal_args->args[l], args[l].data.string_value, internal_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_BINARY:
				internal_args->lengths[l] = (unsigned long) args[l].data.binary_value->arg_length;
				if (internal_args->args[l])
					free(internal_args->args[l]);
				internal_args->args[l] = (char *) malloc(internal_args->lengths[l] * sizeof(char));
				if (!internal_args->args[l]) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Memory error before calling plugin EXEC function\n");
					return -1;
				}
				memcpy((char *) internal_args->args[l], args[l].data.binary_value->arg, internal_args->lengths[l] * sizeof(char));
				break;
			case OPH_QUERY_EXPR_TYPE_DOUBLE:
				//Check if type should be casted
				if ((internal_args->arg_type[l] != DECIMAL_RESULT) && (internal_args->arg_type[l] != REAL_RESULT)) {
					long long val_l = (long long) args[l].data.double_value;
					memcpy((char *) internal_args->args[l], &(val_l), internal_args->lengths[l] * sizeof(char));
				} else {
					memcpy((char *) internal_args->args[l], &(args[l].data.double_value), internal_args->lengths[l] * sizeof(char));
				}
				break;
			case OPH_QUERY_EXPR_TYPE_LONG:
				//Check if type should be casted
				if (internal_args->arg_type[l] != INT_RESULT) {
					double val_d = (double) args[l].data.long_value;
					memcpy((char *) internal_args->args[l], &(val_d), internal_args->lengths[l] * sizeof(char));
				} else {
					memcpy((char *) internal_args->args[l], &(args[l].data.long_value), internal_args->lengths[l] * sizeof(char));
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

	if (_oph_execute_plugin(plugin, internal_args, initid, &rs, &rs_length, &is_null, &error, &result, function)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while calling plugin execution function\n");
		return -1;
	}

	switch (plugin->plugin_return) {
		case OPH_IOSTORE_STRING_TYPE:
			{

				oph_query_arg *temp = (oph_query_arg *) malloc(sizeof(oph_query_arg));
				if (temp == NULL) {
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
				res->data.long_value = *(long long *) rs;
				free(rs);
				break;
			}

		case OPH_IOSTORE_REAL_TYPE:
			{
				res->data.double_value = *(double *) rs;
				free(rs);
				break;
			}
		default:
			return -1;
	}

	return 0;
}
