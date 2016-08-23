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

#ifndef __OPH_QUERY_ENGINE_LOG_ERROR_CODES_H
#define __OPH_QUERY_ENGINE_LOG_ERROR_CODES_H

/*QUERY ENGINE RETURN CODE*/
#define OPH_QUERY_ENGINE_SUCCESS                             0
#define OPH_QUERY_ENGINE_NULL_PARAM                          1
#define OPH_QUERY_ENGINE_MEMORY_ERROR                        2
#define OPH_QUERY_ENGINE_ERROR                               3
#define OPH_QUERY_ENGINE_PARSE_ERROR                         4
#define OPH_QUERY_ENGINE_EXEC_ERROR                          5

/*QUERY ENGINE LOG ERRORS*/
#define OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM       "Missing input argument\n"
#define OPH_QUERY_ENGINE_LOG_MEMORY_ALLOC_ERROR     "Memory allocation error\n"
#define OPH_QUERY_ENGINE_LOG_FILE_OPEN_ERROR        "Error %d while opening file %s\n"
#define OPH_QUERY_ENGINE_LOG_HASHTBL_ERROR          "Error while creating hash table for plugins\n"
#define OPH_QUERY_ENGINE_LOG_FILE_READ_ERROR        "Unable to read line from %s\n"
#define OPH_QUERY_ENGINE_LOG_PLUGIN_FILE_CORRUPTED  "Unable to read plugin file line %s\n"
#define OPH_QUERY_ENGINE_LOG_QUERY_PARSING_ERROR    "Unable to parse query %s\n"
#define OPH_QUERY_ENGINE_LOG_PLUGIN_LOAD_ERROR      "Unable to load plugin table\n"
#define OPH_QUERY_ENGINE_LOG_HASHTBL_CREATE_ERROR   "Unable to create hash table\n"
#define OPH_QUERY_ENGINE_LOG_QUERY_ARG_LOAD_ERROR   "Unable to load query args in table\n"
#define OPH_QUERY_ENGINE_LOG_PLUGIN_EXEC_ERROR      "Error while executing %s\n"

/* QUERY EVALUATOR LOG ERRORS */
#define OPH_QUERY_ENGINE_LOG_LEXER_INIT_ERROR       "Error while initializing the lexer\n"
#define OPH_QUERY_ENGINE_LOG_EVAL_ERROR       		"Error while evaluating the expression\n"
#define OPH_QUERY_ENGINE_LOG_FULL_SYMTABLE       	"Symtable is full\n"
#define OPH_QUERY_ENGINE_LOG_ARG_NUM_ERROR 			"Wrong number of arguments for function '%s.' Expected %d args. \n"
#define OPH_QUERY_ENGINE_LOG_ARG_TYPE_ERROR         "Wrong argument type for function '%s.' Expected %s. \n"
#define OPH_QUERY_ENGINE_LOG_UNKNOWN_SYMBOL         "Unknown  symbol '%s'. \n"
#define OPH_QUERY_ENGINE_LOG_UNKNOWN_TYPE         	"Unknown variable type\n"



#endif  //__OPH_QUERY_ENGINE_LOG_ERROR_CODES_H
