/*
    Ophidia IO Server
    Copyright (C) 2014-2018 CMCC Foundation

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

#ifndef __OPH_IO_CLIENT_INTERFACE_H
#define __OPH_IO_CLIENT_INTERFACE_H

// error codes
#define OPH_IO_CLIENT_INTERFACE_OK 0
#define OPH_IO_CLIENT_INTERFACE_DATA_ERR 1
#define OPH_IO_CLIENT_INTERFACE_QUERY_ERR 2
#define OPH_IO_CLIENT_INTERFACE_IO_ERR 3
#define OPH_IO_CLIENT_INTERFACE_MEMORY_ERR 4
#define OPH_IO_CLIENT_INTERFACE_CONN_ERR 5

#define OPH_IO_CLIENT_PORT_LEN 10
#define OPH_IO_CLIENT_HOST_LEN 512
#define OPH_IO_CLIENT_DB_LEN 1024

//Packet format
/*
--------------------------------------------------
| char type[2]| uint64 payload_len| char *payload|
--------------------------------------------------
*/


//Result set packet format
/*
----------------------------------------------------------------------------------------------------------
| uint64 nrows| uint32 nfields| uint64 field11_len| char *field11| uint64 field12_len| char *field12| ...|
----------------------------------------------------------------------------------------------------------
*/

//Header type messages
#define OPH_IO_CLIENT_MSG_TYPE_LEN 2
#define OPH_IO_CLIENT_MSG_LONG_LEN sizeof(unsigned long long)
#define OPH_IO_CLIENT_MSG_SHORT_LEN sizeof(unsigned int)

#define OPH_IO_CLIENT_MSG_PING "PG"
#define OPH_IO_CLIENT_MSG_RESULT "RS"
#define OPH_IO_CLIENT_MSG_USE_DB "UD"
#define OPH_IO_CLIENT_MSG_SET_QUERY "SQ"
#define OPH_IO_CLIENT_MSG_EXEC_QUERY "EQ"

#define OPH_IO_CLIENT_REQ_ERROR   "ER"

#define OPH_IO_CLIENT_MSG_ARG_DATA_LONG "DL"
#define OPH_IO_CLIENT_MSG_ARG_DATA_DOUBLE "DD"
#define OPH_IO_CLIENT_MSG_ARG_DATA_NULL "DN"
#define OPH_IO_CLIENT_MSG_ARG_DATA_VARCHAR "DV"
#define OPH_IO_CLIENT_MSG_ARG_DATA_BLOB "DB"

/**
 * \brief        Structure to contain reference to server connection
 * \param host   String with hostname or IP address of server
 * \param port   Port of the server
 * \param db   	DB to be used on the server
 * \param socket Id of file descriptor of socket associated to connection
 */
typedef struct {
	char host[OPH_IO_CLIENT_HOST_LEN];
	char port[OPH_IO_CLIENT_PORT_LEN];
	char db_name[OPH_IO_CLIENT_DB_LEN];
	int socket;
} oph_io_client_connection;

/**
 * \brief			          Structure for storing information about the current record
 * \param field_length 	Array containing the length for each cell in the record
 * \param field			    NULL terminated array containing the cell values
 */
typedef struct {
	unsigned long *field_length;
	char **field;
} oph_io_client_record;

/**
 * \brief			Structure for the result set to be retrieved
 * \param num_rows		Number of rows of the result set
 * \param num_fields		Number of fields (columns) of the result set
 * \param max_field_length 	Array containing the maximum width of the field
 * \param current_row		Index of current row
 * \param result_set		Pointer to NULL terminated result set
 */
typedef struct {
	unsigned long long num_rows;
	unsigned int num_fields;
	unsigned long long *max_field_length;
	unsigned long long current_row;
	oph_io_client_record **result_set;
} oph_io_client_result;

/**
 * \brief           Enum with admissible argument types
 */
typedef enum {
	OPH_IO_CLIENT_TYPE_DECIMAL = 0, OPH_IO_CLIENT_TYPE_LONG,
	OPH_IO_CLIENT_TYPE_FLOAT, OPH_IO_CLIENT_TYPE_DOUBLE,
	OPH_IO_CLIENT_TYPE_NULL, OPH_IO_CLIENT_TYPE_LONGLONG,
	OPH_IO_CLIENT_TYPE_VARCHAR, OPH_IO_CLIENT_TYPE_BIT,
	OPH_IO_CLIENT_TYPE_LONG_BLOB, OPH_IO_CLIENT_TYPE_BLOB
} oph_io_client_arg_types;


/**
 * \brief             Structure to contain a query argument
 * \param arg_type    Type of argument      
 * \param arg_length  Length of argument
 * \param arg_is_null If argument can be null
 * \param arg         Pointer to argument value
 */
typedef struct {
	oph_io_client_arg_types arg_type;
	unsigned long arg_length;
	short int arg_is_null;
	void *arg;
} oph_io_client_query_arg;

/**
 * \brief               Structure to contain a query statement
 * \param query         Pointer to a statement setted by set_query API      
 * \param args          Pointer to array of query arguments
 * \param args_count    Number of arguments passed to the query (it can be 0)
 * \param fixed_length  Used to store length of fixed part of message transmitted to server
 * \param args_length   Current length or variable message part
 * \param tot_run       Total number of times the query should be executed
 * \param curr_run      Current value of execution counter
 */
typedef struct {
	void *query;
	unsigned long long args_count;
	oph_io_client_query_arg **args;
	unsigned int fixed_length;
	unsigned int args_length;
	unsigned long long tot_run;
	unsigned long long curr_run;
} oph_io_client_query;

/**
 * \brief               Function to initialize IO server library.
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_setup();

/**
 * \brief               Function to connect or reconnect to IO server.
 * \param hostname      Hostname of server
 * \param port          Port of server
 * \param db_name       DB to be used (can be NULL)
 * \param device        Name of device where data is stored
 * \param connection    Pointer to IO server connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_connect(const char *hostname, const char *port, const char *db_name, const char *device, oph_io_client_connection ** connection);

/**
 * \brief               Function to set default database for specified server.
 * \param db_name       Name of database to be used
 * \param device        Name of device where data is stored
 * \param connection    Pointer to IO server connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_use_db(const char *db_name, const char *device, oph_io_client_connection * connection);

/**
 * \brief               Function to execute an operation on data stored into server.
 * \param connection    Pointer to server-specific connection structure
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_execute_query(oph_io_client_connection * connection, oph_io_client_query * query);

/**
 * \brief               Function to setup the query structure with given operation and array argument
 * \param connection    Pointer to server-specific connection structure
 * \param operation     String with operation to be performed
 * \param device        Name of device where data is stored
 * \param tot_run       Total number of runs of the given operation
 * \param args          Array of arguments to be binded to the query (can be NULL)
 * \param query         Pointer to query to be built
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_setup_query(oph_io_client_connection * connection, const char *operation, const char *device, unsigned long long tot_run, oph_io_client_query_arg ** args,
			      oph_io_client_query ** query);

/**
 * \brief               Function to release resources allocated for query
 * \param query         Pointer to query to be executed
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_free_query(oph_io_client_query * query);

/**
 * \brief               Function to close connection established towards an IO server.
 * \param connection    Pointer to server-specific connection structure
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_close(oph_io_client_connection * connection);

/**
 * \brief               Function to finalize library of IO server and release all dynamic loading resources.
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_cleanup();

/**
 * \brief               Function to get result set after executing a query.
 * \param connection    Pointer to server-specific connection structure
 * \param result_set    Pointer to the result set array to be created
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_get_result(oph_io_client_connection * connection, oph_io_client_result ** result_set);

/**
 * \brief               Function to fetch the next row in a result set.
 * \param result        Pointer to the result set structure to scan
 * \param current_row	  Pointer to the next row structure in the result set
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_fetch_row(oph_io_client_result * result_set, oph_io_client_record ** current_row);

/**
 * \brief               Function to free the allocated result set.
 * \param result        Pointer to the result set structure to free
 * \return              0 if successfull, non-0 otherwise
 */
int oph_io_client_free_result(oph_io_client_result * result);

#endif				//__OPH_IO_CLIENT_INTERFACE_H
