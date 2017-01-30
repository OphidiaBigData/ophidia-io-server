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

#ifndef __OPH_QUERY_ENGINE_LANGUAGE_H
#define __OPH_QUERY_ENGINE_LANGUAGE_H

#define OPH_QUERY_ENGINE_LANG_LEN			1024
#define OPH_QUERY_ENGINE_QUERY_ARGS			7

//*****************Submission query syntax***************//

#define OPH_QUERY_ENGINE_LANG_VALUE_SEPARATOR       '='
#define OPH_QUERY_ENGINE_LANG_PARAM_SEPARATOR       ';'
#define OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR '|'
#define OPH_QUERY_ENGINE_LANG_ARG_REPLACE           '?'
#define OPH_QUERY_ENGINE_LANG_STRING_DELIMITER      '\''
#define OPH_QUERY_ENGINE_LANG_STRING_DELIMITER2     '"'
#define OPH_QUERY_ENGINE_LANG_REAL_NUMBER_POINT     '.'
#define OPH_QUERY_ENGINE_LANG_FUNCTION_START     '('
#define OPH_QUERY_ENGINE_LANG_FUNCTION_END     ')'
#define OPH_QUERY_ENGINE_LANG_HIERARCHY_SEPARATOR	'.'

//*****************Query operation***************//

#define OPH_QUERY_ENGINE_LANG_OPERATION             "operation"
#define OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG_SELECT "create_frag_select"
#define OPH_QUERY_ENGINE_LANG_OP_CREATE_FRAG        "create_frag"
#define OPH_QUERY_ENGINE_LANG_OP_DROP_FRAG          "drop_frag"
#define OPH_QUERY_ENGINE_LANG_OP_CREATE_DB          "create_database"
#define OPH_QUERY_ENGINE_LANG_OP_DROP_DB            "drop_database"
#define OPH_QUERY_ENGINE_LANG_OP_INSERT             "insert"
#define OPH_QUERY_ENGINE_LANG_OP_MULTI_INSERT 		"multi_insert"
#define OPH_QUERY_ENGINE_LANG_OP_SELECT             "select"
#define OPH_QUERY_ENGINE_LANG_OP_FUNCTION           "function"

//*****************Query arguments***************//

#define OPH_QUERY_ENGINE_LANG_ARG_FINAL_STATEMENT "final_statement"
#define OPH_QUERY_ENGINE_LANG_ARG_FRAG        "frag_name"
#define OPH_QUERY_ENGINE_LANG_ARG_COLUMN_NAME "column_name"
#define OPH_QUERY_ENGINE_LANG_ARG_COLUMN_TYPE "column_type"
#define OPH_QUERY_ENGINE_LANG_ARG_FIELD       "field"
#define OPH_QUERY_ENGINE_LANG_ARG_FIELD_ALIAS "select_alias"
#define OPH_QUERY_ENGINE_LANG_ARG_FROM        "from"
#define OPH_QUERY_ENGINE_LANG_ARG_FROM_ALIAS  "from_alias"
#define OPH_QUERY_ENGINE_LANG_ARG_DB          "db_name"
#define OPH_QUERY_ENGINE_LANG_ARG_GROUP       "group"
#define OPH_QUERY_ENGINE_LANG_ARG_WHEREL      "where_left"
#define OPH_QUERY_ENGINE_LANG_ARG_WHEREC      "where_cond"
#define OPH_QUERY_ENGINE_LANG_ARG_WHERER      "where_right"
#define OPH_QUERY_ENGINE_LANG_ARG_WHERE       "where"
#define OPH_QUERY_ENGINE_LANG_ARG_ORDER       "order"
#define OPH_QUERY_ENGINE_LANG_ARG_ORDER_DIR   "order_dir"
#define OPH_QUERY_ENGINE_LANG_ARG_LIMIT       "limit"
#define OPH_QUERY_ENGINE_LANG_ARG_VALUE       "value"
#define OPH_QUERY_ENGINE_LANG_ARG_FUNC        "func_name"
#define OPH_QUERY_ENGINE_LANG_ARG_ARG         "arg"
#define OPH_QUERY_ENGINE_LANG_ARG_SEQUENTIAL  "sequential_id"

//*****************Query values***************//

#define OPH_QUERY_ENGINE_LANG_VAL_YES "yes"
#define OPH_QUERY_ENGINE_LANG_VAL_NO 	"no"

//*****************Keywords***************//

#define OPH_QUERY_ENGINE_LANG_KW_TABLE_SIZE         "@tot_table_size"
#define OPH_QUERY_ENGINE_LANG_KW_INFO_SYSTEM        "@info_system"
#define OPH_QUERY_ENGINE_LANG_KW_INFO_SYSTEM_TABLE  "@info_system_table"
#define OPH_QUERY_ENGINE_LANG_KW_FUNCTION_FIELDS    "@function_fields"
#define OPH_QUERY_ENGINE_LANG_KW_FUNCTION_TABLE     "@function_table"

#endif //__OPH_QUERY_ENGINE_LANGUAGE_H
