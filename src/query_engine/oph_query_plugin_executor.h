/*
    Ophidia IO Server
    Copyright (C) 2014-2023 CMCC Foundation

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

#include <mysql.h>		// It contains UDF-related symbols and data structures
#include <mysql_com.h>
#include <ltdl.h>

#include "oph_query_parser.h"
#include "oph_iostorage_data.h"
#include "oph_query_expression_evaluator.h"
#include "oph_query_plugin_loader.h"

#define BUFLEN 1024

//UDF interfaces. UDF_ARGS and UDF_INIT are defined in mysql_com.h

//UDF fixed interface
extern void (*_oph_plugin_reset)(UDF_INIT *, UDF_ARGS *, char *, char *);

/**
 * \brief               Function used to free UDF_ARG argument
 * \param arguments     Pointer to UDF_ARG structure to be freed
 * \return              0 if successfull, non-0 otherwise
 */
int free_udf_arg(UDF_ARGS * arguments);

/**
 * \brief             Internal function used by other oph_execute to run the exec_api of a plugin
 * \param plugin      Plugin to be executed
 * \param args        UDF Args used as plugin input
 * \param initid      UDF Init used by plugin
 * \param res         Pointer used to store plugin result value
 * \param functions   Structure containing pointer to plugin library symbols
 * \return            0 if successfull, non-0 otherwise
 */
int _oph_execute_plugin(const oph_plugin * plugin, UDF_ARGS * args, UDF_INIT * initid, oph_query_expr_value * res, oph_plugin_api * functions);

/**
 * \brief               Function to run plugin CLEAR function 
 * \param function    Set of pointers to all plugins functions 
 * \param dlh       Pointer to plugin handler 
 * \param initid      Pointer to initid used by plugin functions 
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_plugin_clear(oph_plugin_api * function, void *dlh, UDF_INIT * initid);

/**
 * \brief               Function to run plugin DEINIT function 
 * \param function  	Set of pointers to all plugins functions 
 * \param dlh 			Pointer to plugin handler 
 * \param initid    	Pointer to initid used by plugin functions 
 * \param internal_args Pointer with internal argument structures used within plugin functions
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_plugin_deinit(oph_plugin_api * function, void *dlh, UDF_INIT * initid, UDF_ARGS * internal_args);

/**
 * \brief               Function to run plugin INIT function 
 * \param function  	Set of pointers to all plugins functions 
 * \param dlh 			Pointer to plugin handler 
 * \param initid    	Pointer to initid used by plugin functions 
 * \param internal_args Pointer with internal argument structures used within plugin functions
 * \param plugin_name   Name of plugin to be run
 * \param args_count    Number of query arguments 
 * \param args          Array of query arguments
 * \param is_aggregate  Flag set by init function if plugin is aggregating
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_plugin_init(oph_plugin_api * function, void **dlh, UDF_INIT ** initid, UDF_ARGS ** internal_args, char *plugin_name, int arg_count, oph_query_expr_value * args, char *is_aggregate);


/**
 * \brief               Function to run plugin ADD function 
 * \param function    Set of pointers to all plugins functions 
 * \param dlh       Pointer to plugin handler 
 * \param initid      Pointer to initid used by plugin functions 
 * \param internal_args Pointer with internal argument structures used within plugin functions
 * \param args_count    Number of query arguments 
 * \param args          Array of query arguments
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_plugin_add(oph_plugin_api * function, void **dlh, UDF_INIT * initid, UDF_ARGS * internal_args, int arg_count, oph_query_expr_value * args);

/**
 * \brief               Function to run plugin EXEC function 
 * \param function  	Set of pointers to all plugins functions 
 * \param dlh 			Pointer to plugin handler 
 * \param initid    	Pointer to initid used by plugin functions 
 * \param internal_args Pointer with internal argument structures used within plugin functions
 * \param plugin_name   Name of plugin to be run
 * \param args_count    Number of query arguments 
 * \param args          Array of query arguments
 * \param res          	Result of execution function
 * \return              0 if successfull, non-0 otherwise
 */
int oph_query_plugin_exec(oph_plugin_api * function, void **dlh, UDF_INIT * initid, UDF_ARGS * internal_args, char *plugin_name, int arg_count, oph_query_expr_value * args,
			  oph_query_expr_value * res);


#endif				/* OPH_QUERY_PLUGIN_EXEC_H */
