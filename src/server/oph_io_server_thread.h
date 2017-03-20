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

#ifndef OPH_IO_SERVER_THREAD_H
#define OPH_IO_SERVER_THREAD_H

// Prototypes

#include "oph_iostorage_interface.h"
#include <pthread.h>

//Packet codes

#define OPH_IO_SERVER_MSG_TYPE_LEN 2
#define OPH_IO_SERVER_MSG_LONG_LEN sizeof(unsigned long long)
#define OPH_IO_SERVER_MSG_SHORT_LEN sizeof(unsigned int)

#define OPH_IO_SERVER_MSG_PING "PG"
#define OPH_IO_SERVER_MSG_RESULT "RS"
#define OPH_IO_SERVER_MSG_USE_DB "UD"
#define OPH_IO_SERVER_MSG_SET_QUERY "SQ"
#define OPH_IO_SERVER_MSG_EXEC_QUERY "EQ"

#define OPH_IO_SERVER_MSG_ARG_DATA_LONG "DL"
#define OPH_IO_SERVER_MSG_ARG_DATA_DOUBLE "DD"
#define OPH_IO_SERVER_MSG_ARG_DATA_NULL "DN"
#define OPH_IO_SERVER_MSG_ARG_DATA_VARCHAR "DV"
#define OPH_IO_SERVER_MSG_ARG_DATA_BLOB "DB"

#define OPH_IO_SERVER_REQ_ERROR   "ER"

// enum and struct
#define OPH_IO_SERVER_MAX_LONG_LEN 24
#define OPH_IO_SERVER_MAX_DOUBLE_LEN 32

/**
 * \brief			            Structure to contain info about a running statement (query executed in multiple runs)
 * \param tot_run         Total number of times the query should be executed
 * \param curr_run        Current value of execution counter
 * \param partial_result_set	Pointer to last result set retrieved by a selection query
 * \param device        	Device where result set belongs
 * \param frag       	    Frag name related to result set
 * \param mi_prev_rows     If multi-insert remainder rows are expected, it will contain the rows already inserted otherwise it will be zero
 */
typedef struct {
	unsigned long long tot_run;
	unsigned long long curr_run;
	oph_iostore_frag_record_set *partial_result_set;
	char *device;
	char *frag;
	unsigned long long size;
	unsigned long long mi_prev_rows;
} oph_io_server_running_stmt;

/**
 * \brief			            Structure to store thread status info
 * \param current_db 	    Pointer to current (default) database, if defined
 * \param last_result_set	Pointer to last result set retrieved by a selection query
 * \param delete_only_rs	Flag set to 1 if only record set structure should be deleted
 * \param device        	Device selected for operations
 * \param curr_stmt       Current statement being executed, if any
 */
typedef struct {
	//oph_metadb_db_row *current_db; 
	char *current_db;
	oph_iostore_frag_record_set *last_result_set;
	char delete_only_rs;
	char *device;
	oph_io_server_running_stmt *curr_stmt;
} oph_io_server_thread_status;

/**
 * \brief               Function used to release thread status resources
 * \param status        Thread status
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_server_free_status(oph_io_server_thread_status * status);

/**
 * \brief               Thread function used by IO server
 * \param sockfd        Socket descriptor related to thread
 * \param tid        Thread ID
 * \return              0 if successfull, non-0 otherwise
 */
void oph_io_server_thread(int sockfd, pthread_t tid);

#endif				/* OPH_IO_SERVER_THREAD_H */
