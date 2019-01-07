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

#ifndef OPH_QUERY_PLUGIN_LOADER_H
#define OPH_QUERY_PLUGIN_LOADER_H

#include <hashtbl.h>

#include "oph_iostorage_interface.h"
#include "oph_query_expression_evaluator.h"

#define OPH_PLUGIN_FILE_LINE 1024
#define OPH_QUERY_ENGINE_MAX_PLUGIN_NUMBER 1000

//Macros used to access primitives list file
#define OPH_PLUGIN_LIST_FUNCTION_DESC 	"fun"
#define OPH_PLUGIN_LIST_RETURN_DESC 	"ret"
#define OPH_PLUGIN_LIST_LIBRARY_DESC 	"lib"
#define OPH_PLUGIN_LIST_REAL_TYPE	 	"real"
#define OPH_PLUGIN_LIST_LONG_TYPE 		"integer"
#define OPH_PLUGIN_LIST_STRING_TYPE 	"string"
#define	OPH_PLUGIN_LIST_SIMPLE_FUNC 	"simple"
#define	OPH_PLUGIN_LIST_AGGREGATE_FUNC 	"aggregate"

/**
 * \brief			          Enum with possible type of plugin
 */
typedef enum {
	OPH_SIMPLE_PLUGIN_TYPE,
	OPH_AGGREGATE_PLUGIN_TYPE
} oph_plugin_type;

/**
 * \brief			          Structure used to identify a plugin
 * \param plugin_name 	Name of plugin
 * \param plugin_library Filename with path of plugin
 * \param plugin_type		Type of plugin function (simple or aggragetion)
 * \param plugin_return	Return type of plugin
 */
typedef struct {
	char *plugin_name;
	char *plugin_library;
	oph_plugin_type plugin_type;
	oph_iostore_field_type plugin_return;
} oph_plugin;

/**
 * \brief			        Setup plugin with default values
 * \param plugin      Pointer to plugin to setup
 * \return            0 if successfull, non-0 otherwise
 */
int oph_init_plugin(oph_plugin * plugin);

/**
 * \brief			        Free resources allocated for plugin
 * \param plugin      Pointer to plugin to be freed
 * \return            0 if successfull, non-0 otherwise
 */
int oph_free_plugin(oph_plugin * plugin);

/**
 * \brief			        Load plugin list in plugin table
 * \param plugin_htable      Pointer to hash table used to store plugin list
 * \param function_htable      Pointer to symtable used to store plugin list
 * \return            0 if successfull, non-0 otherwise
 */
int oph_load_plugins(HASHTBL ** plugin_htable, oph_query_expr_symtable ** function_table);

/**
 * \brief			        Clean plugin list in plugin table
 * \param plugin_htable      Pointer to hash table to be freed
 * \param function_htable      Pointer to symtable to be freed
 * \return            0 if successfull, non-0 otherwise
 */
int oph_unload_plugins(HASHTBL ** plugin_htable, oph_query_expr_symtable ** function_table);

#endif				/* OPH_QUERY_PLUGIN_LOADER_H */
