/*
    Ophidia IO Server
    Copyright (C) 2014-2019 CMCC Foundation

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

#include "oph_query_plugin_loader.h"
#include "oph_query_engine_log_error_codes.h"
#include "oph_query_expression_functions.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <debug.h>
#include <hashtbl.h>

#include <errno.h>

#include "oph_server_utility.h"
#include "oph_server_confs.h"

extern int msglevel;

//Setup oph_plugin with default values
int oph_init_plugin(oph_plugin * plugin)
{
	if (!plugin) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		return OPH_QUERY_ENGINE_NULL_PARAM;
	}

	plugin->plugin_name = NULL;
	plugin->plugin_library = NULL;
	plugin->plugin_type = OPH_SIMPLE_PLUGIN_TYPE;
	plugin->plugin_return = OPH_IOSTORE_STRING_TYPE;

	return OPH_QUERY_ENGINE_SUCCESS;
}

int oph_free_plugin(oph_plugin * plugin)
{
	if (!plugin) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		return OPH_QUERY_ENGINE_NULL_PARAM;
	}

	if (plugin->plugin_name)
		free(plugin->plugin_name);
	if (plugin->plugin_library)
		free(plugin->plugin_library);

	return OPH_QUERY_ENGINE_SUCCESS;
}

int oph_load_plugins(HASHTBL ** plugin_htable, oph_query_expr_symtable ** function_table)
{
	FILE *fp = NULL;
	char line[OPH_PLUGIN_FILE_LINE] = { 0 };
	char line_front[OPH_PLUGIN_FILE_LINE] = { 0 };
	char line_end[OPH_PLUGIN_FILE_LINE] = { 0 };
	char value[OPH_PLUGIN_FILE_LINE] = { 0 };
	char dyn_lib_str[OPH_PLUGIN_FILE_LINE] = { 0 };
	char *res_string = NULL;
	int i;


	if (!plugin_htable || !function_table) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		return OPH_QUERY_ENGINE_NULL_PARAM;
	}

	*plugin_htable = NULL;
	*function_table = NULL;

	//Load all functions in the global function symtable
	if (oph_query_expr_create_function_symtable(OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_LOAD_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_LOAD_ERROR);
		return OPH_QUERY_ENGINE_ERROR;
	}

	snprintf(dyn_lib_str, sizeof(dyn_lib_str), OPH_SERVER_PLUGIN_FILE_PATH);

	fp = fopen(dyn_lib_str, "r");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_OPEN_ERROR, errno, dyn_lib_str);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_OPEN_ERROR, errno, dyn_lib_str);
		oph_query_expr_destroy_symtable(*function_table);
		*function_table = NULL;
		return OPH_QUERY_ENGINE_ERROR;
	}

	oph_plugin *new = NULL;

	int lines = 0;

	while (!feof(fp)) {
		res_string = fgets(line, OPH_PLUGIN_FILE_LINE, fp);
		lines++;
	}
	int primitives_number = (int) lines / 4;

	rewind(fp);

	if (!(*plugin_htable = hashtbl_create(primitives_number, NULL))) {
		fclose(fp);
		oph_query_expr_destroy_symtable(*function_table);
		*function_table = NULL;
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_HASHTBL_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_HASHTBL_ERROR);
		return OPH_QUERY_ENGINE_ERROR;
	}

	while (!feof(fp)) {
		res_string = fgets(line, OPH_PLUGIN_FILE_LINE, fp);
		if (feof(fp)) {
			fclose(fp);
			return OPH_QUERY_ENGINE_SUCCESS;
		}

		if (!res_string) {
			fclose(fp);
			oph_unload_plugins(plugin_htable, function_table);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
			return OPH_QUERY_ENGINE_ERROR;
		}
		sscanf(line, "[%[^]]", value);

		new = (oph_plugin *) malloc(sizeof(oph_plugin));
		//Initilize plugin structure
		if (oph_init_plugin(new)) {
			fclose(fp);
			oph_unload_plugins(plugin_htable, function_table);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
			return OPH_QUERY_ENGINE_MEMORY_ERROR;
		}

		trim(value);
		if (!(new->plugin_name = (char *) strndup(value, OPH_PLUGIN_FILE_LINE))) {
			oph_unload_plugins(plugin_htable, function_table);
			free(new);
			fclose(fp);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
			return OPH_QUERY_ENGINE_MEMORY_ERROR;
		}

		for (i = 0; i < 3; i++) {
			res_string = NULL;
			res_string = fgets(line, OPH_PLUGIN_FILE_LINE, fp);
			if (!res_string) {
				oph_unload_plugins(plugin_htable, function_table);
				oph_free_plugin(new);
				free(new);
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
				return OPH_QUERY_ENGINE_ERROR;
			}
			if (sscanf(line, "%[^\n]", value) < 1) {
				oph_unload_plugins(plugin_htable, function_table);
				oph_free_plugin(new);
				free(new);
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR, dyn_lib_str);
				return OPH_QUERY_ENGINE_ERROR;
			}
			//TODO - Improve parsing mechanisms
			strncpy(line_front, value, strlen(OPH_PLUGIN_LIST_FUNCTION_DESC));
			memset(line_end, 0, OPH_PLUGIN_FILE_LINE);
			strncpy(line_end, value + strlen(OPH_PLUGIN_LIST_FUNCTION_DESC) + 1, OPH_PLUGIN_FILE_LINE - strlen(OPH_PLUGIN_LIST_FUNCTION_DESC) - 1);

			trim(line_end);
			if (!strcasecmp(line_front, OPH_PLUGIN_LIST_LIBRARY_DESC)) {
				if (!(new->plugin_library = (char *) strndup(line_end, OPH_PLUGIN_FILE_LINE))) {
					oph_unload_plugins(plugin_htable, function_table);
					oph_free_plugin(new);
					free(new);
					fclose(fp);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR);
					return OPH_QUERY_ENGINE_MEMORY_ERROR;
				}
			}
			if (!strcasecmp(line_front, OPH_PLUGIN_LIST_FUNCTION_DESC)) {
				if (!strcasecmp(line_end, OPH_PLUGIN_LIST_SIMPLE_FUNC))
					new->plugin_type = OPH_SIMPLE_PLUGIN_TYPE;
				else if (!strcasecmp(line_end, OPH_PLUGIN_LIST_AGGREGATE_FUNC))
					new->plugin_type = OPH_AGGREGATE_PLUGIN_TYPE;
				else {
					oph_unload_plugins(plugin_htable, function_table);
					oph_free_plugin(new);
					free(new);
					fclose(fp);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_FILE_CORRUPTED, line_front);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_FILE_CORRUPTED, line_front);
					return OPH_QUERY_ENGINE_ERROR;
				}
			}
			if (!strcasecmp(line_front, OPH_PLUGIN_LIST_RETURN_DESC)) {
				if (!strcasecmp(line_end, OPH_PLUGIN_LIST_LONG_TYPE))
					new->plugin_return = OPH_IOSTORE_LONG_TYPE;
				else if (!strcasecmp(line_end, OPH_PLUGIN_LIST_REAL_TYPE))
					new->plugin_return = OPH_IOSTORE_REAL_TYPE;
				else if (!strcasecmp(line_end, OPH_PLUGIN_LIST_STRING_TYPE))
					new->plugin_return = OPH_IOSTORE_STRING_TYPE;
				else {
					oph_unload_plugins(plugin_htable, function_table);
					oph_free_plugin(new);
					free(new);
					fclose(fp);
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_FILE_CORRUPTED, line_front);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_PLUGIN_FILE_CORRUPTED, line_front);
					return OPH_QUERY_ENGINE_ERROR;
				}
			}
		}
		hashtbl_insert(*plugin_htable, new->plugin_name, (oph_plugin *) new);
		//Load function is symtable     
		//TODO Set number of args in symtable and add string function
		switch (new->plugin_return) {
			case OPH_IOSTORE_LONG_TYPE:
				{
					oph_query_expr_add_function(new->plugin_name, 1, 1, oph_query_generic_long, *function_table);
					break;
				}
			case OPH_IOSTORE_REAL_TYPE:
				{
					oph_query_expr_add_function(new->plugin_name, 1, 1, oph_query_generic_double, *function_table);
					break;
				}
			case OPH_IOSTORE_STRING_TYPE:
				{
					oph_query_expr_add_function(new->plugin_name, 1, 1, oph_query_generic_binary, *function_table);
					break;
				}
		}

	}
	fclose(fp);
	return OPH_QUERY_ENGINE_SUCCESS;
}

int oph_unload_plugins(HASHTBL ** plugin_htable, oph_query_expr_symtable ** function_table)
{
	if (!plugin_htable || !function_table) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		return OPH_QUERY_ENGINE_NULL_PARAM;
	}

	hash_size n;
	oph_plugin *plugin_ptr;
	struct hashnode_s *node, *oldnode;
	for (n = 0; n < (*plugin_htable)->size; ++n) {
		node = (*plugin_htable)->nodes[n];
		while (node) {
			//plugin_ptr = (oph_plugin*)hashtbl_get(plugin_htable,node->key);
			plugin_ptr = (oph_plugin *) node->data;
			oph_free_plugin(plugin_ptr);
			free(plugin_ptr);
			node->data = NULL;
			free(node->key);
			free(node->data);
			oldnode = node;
			node = node->next;
			free(oldnode);
		}
	}
	free((*plugin_htable)->nodes);
	free(*plugin_htable);

	*plugin_htable = NULL;
	oph_query_expr_destroy_symtable(*function_table);
	*function_table = NULL;

	return OPH_QUERY_ENGINE_SUCCESS;
}
