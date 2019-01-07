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

#ifndef __OPH_IOSTORAGE_LOG_ERROR_CODES_H
#define __OPH_IOSTORAGE_LOG_ERROR_CODES_H

//*************Error codes***************//

#define OPH_IOSTORAGE_SUCCESS				0
#define OPH_IOSTORAGE_NULL_HANDLE			-1
#define OPH_IOSTORAGE_NOT_NULL_LIB			-2
#define OPH_IOSTORAGE_LIB_NOT_FOUND			-3
#define OPH_IOSTORAGE_DLINIT_ERR			-4
#define OPH_IOSTORAGE_DLEXIT_ERR			-5
#define OPH_IOSTORAGE_DLOPEN_ERR			-6
#define OPH_IOSTORAGE_DLCLOSE_ERR			-7
#define OPH_IOSTORAGE_DLSYM_ERR				-8
#define OPH_IOSTORAGE_INIT_HANDLE_ERR			-9
#define OPH_IOSTORAGE_MEMORY_ERR			-10
#define OPH_IOSTORAGE_NULL_PARAM			-11
#define OPH_IOSTORAGE_VALID_ERROR			-12

#define OPH_IOSTORAGE_NULL_HANDLE_FIELD			-101
#define OPH_IOSTORAGE_NOT_NULL_OPERATOR_HANDLE		-102
#define OPH_IOSTORAGE_NULL_OPERATOR_HANDLE		-103
#define OPH_IOSTORAGE_CONNECTION_ERROR			-104
#define OPH_IOSTORAGE_NULL_CONNECTION			-105
#define OPH_IOSTORAGE_INVALID_PARAM			-106
#define OPH_IOSTORAGE_NULL_RESULT_HANDLE		-107
#define OPH_IOSTORAGE_COMMAND_ERROR			-108

#define OPH_IOSTORAGE_NOT_IMPLEMENTED			-201
#define OPH_IOSTORAGE_UTILITY_ERROR			-301
#define OPH_IOSTORAGE_BAD_PARAMETER			-302

//****************Log errors***************//

#define OPH_IOSTORAGE_LOG_NULL_HANDLE       "Null Handle\n"
#define OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM  "Null input parameter\n"
#define OPH_IOSTORAGE_LOG_INVALID_SERVER    "IO Server name not valid\n"
#define OPH_IOSTORAGE_LOG_SERVER_PARSE_ERROR "IO Server name parsing error\n"
#define OPH_IOSTORAGE_LOG_DLINIT_ERROR       "lt_dlinit error: %s\n"
#define OPH_IOSTORAGE_LOG_LIB_NOT_FOUND     "IO server library not found\n"
#define OPH_IOSTORAGE_LOG_DLOPEN_ERROR      "lt_dlopen error: %s\n"
#define OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR   "Unable to load IO server function %s\n"
#define OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR "IO Server not properly loaded\n"
#define OPH_IOSTORAGE_LOG_DLCLOSE_ERROR     "lt_dlclose error: %s\n"
#define OPH_IOSTORAGE_LOG_RELEASE_RES_ERROR "Unable to release resources\n"
#define OPH_IOSTORAGE_LOG_DLEXIT_ERROR      "Error while executing lt_dlexit %s\n"
#define OPH_IOSTORAGE_LOG_FILE_NOT_FOUND    "IO server file not found %s\n"
#define OPH_IOSTORAGE_LOG_READ_LINE_ERROR   "Unable to read file line\n"
#define OPH_IOSTORAGE_LOG_MEMORY_ERROR      "Memory allocation error\n"

#endif				//__OPH_IOSTORAGE_LOG_ERROR_CODES_H
