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

#include "oph_io_client_interface.h"
#include "debug.h"
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

int main(int argc, char *argv[])
{
	int ch, msglevel = LOG_ERROR, res;
  char *host = NULL, *port = NULL, *db = NULL, *frag = NULL, *type = NULL;
	
	while ((ch = getopt(argc, argv, "f:h:d:p:t:vw"))!=-1)
	{
		switch (ch)
		{
			case 'h':
				host = optarg;
			break;
			case 'p':
				port = optarg;
			break;
			case 'd':
				db = optarg;
			break;
			case 'f':
				frag = optarg;
			break;
			case 't':
				type = optarg;
			break;
			case 'v':
				msglevel = LOG_DEBUG;
			break;
			case 'w':
				if (msglevel<LOG_WARNING) msglevel = LOG_WARNING;
			break;
		}
	}
	set_debug_level(msglevel);

	if (!host || !port || !db || !frag || !type)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error executing test client... use: oph_io_show -h <host address> -p <port> -d <db_name> -f <frag_name> -t <data_type>\n \n");
		return 0;
	}

  oph_io_client_connection *connection = NULL;  
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection to server...\n");
	if ((res = oph_io_client_connect (host, port, NULL, "memory", &connection)))
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error in connection\n");
  else{
    oph_io_client_query *stmt = NULL;

    //Use DB
    if ((res = oph_io_client_use_db(db, "memory", connection))){
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in selecting database '%s'\n",res,db);
      res = oph_io_client_close (connection);
      return 0;
    }

    char query[1024];
    snprintf(query, 1024, "operation=select;field=id_dim|oph_dump('oph_%s','oph_%s', measure);from=%s;", type, type, frag);

    //Execute oph_dump plugin
    if((res = oph_io_client_setup_query (connection, query, "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
	    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,query);
      oph_io_client_free_query (stmt);
      res = oph_io_client_close (connection);
      return 0;          
    }
  
    if ((res = oph_io_client_execute_query(connection, stmt))){
	    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,query);
      oph_io_client_free_query (stmt);
      res = oph_io_client_close (connection);
      return 0;          
    }

    printf("Query submitted correctly.\n");
    oph_io_client_free_query (stmt);
  
    oph_io_client_result *result_set = NULL;
	  if ((res = oph_io_client_get_result(connection, &result_set))){
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in retrieving result set\n",res);
      res = oph_io_client_close (connection);
      return 0;
    }
    //Output the result set (assuming array of double is gathered)
    oph_io_client_record *current_row = NULL;
    oph_io_client_fetch_row(result_set, &current_row);
    
    char *value;
    while(current_row){
      //Work-around to avoid memory leak while reading a non null-terminated string (with oph_dump)
      value = (char *)calloc(current_row->field_length[1]+1, sizeof(char));
      memcpy(value, current_row->field[1], current_row->field_length[1]);
      printf("| ID: %s ", current_row->field[0]);
      printf("| %s  |", current_row->field[1]);
      printf("\n");
      free(value);
      oph_io_client_fetch_row(result_set, &current_row);
    }
    oph_io_client_free_result(result_set);

  }
  res = oph_io_client_close (connection);

	return 0;
}

