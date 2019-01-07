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

#include "oph_io_client_interface.h"
#include "debug.h"
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include "oph_server_utility.h"

#define OPH_DEFAULT_HOST	"127.0.0.1"
#define OPH_DEFAULT_PORT	"65000"
#define OPH_DEFAULT_QUERY	"OPH_NULL"

//TODO Make it more general

int main(int argc, char *argv[])
{
	int ch, msglevel = LOG_DEBUG, res;
	char *query = OPH_DEFAULT_QUERY;
	char *dbname = NULL;
	char request[1000];
	char *host = NULL, *port = NULL;
	char *type = NULL;

	while ((ch = getopt(argc, argv, "c:h:l:p:q:d:t:vw")) != -1) {
		switch (ch) {
			case 'h':
				host = optarg;
				break;
			case 'p':
				port = optarg;
				break;
			case 'q':
				query = optarg;
				break;
			case 'd':
				dbname = optarg;
				break;
			case 't':
				type = optarg;
				break;
			case 'v':
				msglevel = LOG_DEBUG;
				break;
			case 'w':
				if (msglevel < LOG_WARNING)
					msglevel = LOG_WARNING;
				break;
		}
	}
	set_debug_level(msglevel);

	if (!strcasecmp(query, OPH_DEFAULT_QUERY) || !dbname || !port || !host || !type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Specify a query (not using oph_dump) and database... use: oph-client -h <host> -p <port> -q <query> -d <db_name> -t <dumping_type>\n");
		exit(0);
	}

	snprintf(request, sizeof(request), "%s\n", query);

	oph_io_client_connection *connection = NULL;
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Connection to server...\n");
	if ((res = oph_io_client_connect(host, port, NULL, "memory", &connection)))
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in connection\n");
	else {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sending request...\n");
		if ((res = oph_io_client_use_db(dbname, "memory", connection))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %d in selecting database '%s'\n", res, dbname);
			res = oph_io_client_close(connection);
			exit(0);
		}
		oph_io_client_query *query = NULL;

		//set example arg buffer
/*          oph_io_client_query_arg *args[3]; 
          oph_io_client_query_arg arg1, arg2;
          
          args[0] = &arg1;
          args[1] = &arg2;
          args[2] = NULL;
          unsigned long long id = 1;
          double measure = 8.7;
          arg1.arg_type = OPH_IO_CLIENT_TYPE_BLOB;
          arg1.arg_length = 100;
          arg1.arg = (void *)malloc(sizeof(double));
          memcpy(arg1.arg, &measure, sizeof(double));
          arg2.arg_type = OPH_IO_CLIENT_TYPE_LONG;
          arg2.arg_length = 8;
          arg2.arg = &id;
  */
		//TODO SET arg buffer

		if ((res = oph_io_client_setup_query(connection, request, "Memory", 0, (oph_io_client_query_arg **) NULL, &query))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %d in setup query '%s'\n", res, query);
			oph_io_client_free_query(query);
			res = oph_io_client_close(connection);
			exit(0);
		}

		if ((res = oph_io_client_execute_query(connection, query))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %d in executing query '%s'\n", res, request);
			oph_io_client_free_query(query);
			res = oph_io_client_close(connection);
			exit(0);
		}

		printf("Query submitted correctly.\n");
		oph_io_client_free_query(query);

		oph_io_client_result *result_set = NULL;
		if ((res = oph_io_client_get_result(connection, &result_set))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error %d in retrieving result set\n", res);
			res = oph_io_client_close(connection);
			exit(0);
		}
		//Output the result set (assuming array of double is gathered)
		oph_io_client_record *current_row = NULL;
		oph_io_client_fetch_row(result_set, &current_row);
		int num_elems = 0, j;

		if (!STRCMP(type, "int")) {
			while (current_row) {
				num_elems = (int) current_row->field_length[1] / sizeof(int);
				printf("ID: %s ", current_row->field[0]);
				for (j = 0; j < num_elems; j++) {
					printf(" %d ", *((int *) (current_row->field[1] + j * sizeof(int))));
				}
				oph_io_client_fetch_row(result_set, &current_row);
			}
		} else if (!STRCMP(type, "float")) {
			while (current_row) {
				num_elems = (int) current_row->field_length[1] / sizeof(float);
				printf("ID: %s ", current_row->field[0]);
				for (j = 0; j < num_elems; j++) {
					printf(" %f ", *((float *) (current_row->field[1] + j * sizeof(float))));
				}
				oph_io_client_fetch_row(result_set, &current_row);
			}
		} else if (!STRCMP(type, "long")) {
			while (current_row) {
				num_elems = (int) current_row->field_length[1] / sizeof(long long);
				printf("ID: %s ", current_row->field[0]);
				for (j = 0; j < num_elems; j++) {
					printf(" %lld ", *((long long *) (current_row->field[1] + j * sizeof(long long))));
				}
				oph_io_client_fetch_row(result_set, &current_row);
			}
		} else if (!STRCMP(type, "double")) {
			while (current_row) {
				num_elems = (int) current_row->field_length[1] / sizeof(double);
				printf("ID: %s ", current_row->field[0]);
				for (j = 0; j < num_elems; j++) {
					printf(" %f ", *((double *) (current_row->field[1] + j * sizeof(double))));
				}
				oph_io_client_fetch_row(result_set, &current_row);
			}
		}

		oph_io_client_free_result(result_set);
	}
	res = oph_io_client_close(connection);
	exit(0);

}
