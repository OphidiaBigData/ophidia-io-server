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
#include <sys/types.h>
#include <sys/wait.h>

#define OPH_DEFAULT_NCHILDREN	2
#define OPH_DEFAULT_NLOOPS	1

int main(int argc, char *argv[])
{
	int ch, msglevel = LOG_DEBUG, res;
  char *host = NULL, *port = NULL;
	
	while ((ch = getopt(argc, argv, "c:h:l:p:vw"))!=-1)
	{
		switch (ch)
		{
			case 'h':
				host = optarg;
			break;
			case 'p':
				port = optarg;
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

	if (!host || !port )
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error executing test client... use: oph_io_test -h <host address> -p <port> -q <test_case>\n");
		return 0;
	}

  int ii;
  int nchildren = OPH_DEFAULT_NCHILDREN;
  pid_t pid;

	for (ii = 0; ii < nchildren; ii++)
	{
		if ( (pid = fork()) == 0)		/* child */
		{
      oph_io_client_connection *connection = NULL;  
	    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection to server...\n");
	    if ((res = oph_io_client_connect (host, port, &connection))){
        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error in connection\n");
        return 0;
      }
      else{
        //Create 2 db and insert some fragments
        oph_io_client_query *stmt = NULL;
        char query[1024];
  
        snprintf(query, 1024, "operation=create_database;db_name=test%d;", ii);

        //Create empty DB
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

        snprintf(query, 1024, "test%d;", ii);

        //Use new DB
	      if ((res = oph_io_client_use_db(query, "memory", connection))){
			    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in selecting database '%s'\n",res,query);
          res = oph_io_client_close (connection);
          return 0;
        }

        //Create empty fragment
        if((res = oph_io_client_setup_query (connection, "operation=create_frag;frag_name=trial1;column_name=id|measure;column_type=long|blob;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=create_frag;frag_name=trial1;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
	      if ((res = oph_io_client_execute_query(connection, stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=create_frag;frag_name=trial1;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

	      printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        int i = 0, r = 0;

    //Setup arg buffer
        oph_io_client_query_arg *args[3]; 
        oph_io_client_query_arg arg1, arg2;
        
        args[0] = &arg1;
        args[1] = &arg2;
        args[2] = NULL;
        int array_length = 100;
        unsigned long long id = 0;
        double measure[array_length];

	      struct timeval time;
	      gettimeofday(&time, NULL);
	      srand (time.tv_sec*1000000 + time.tv_usec);

        arg1.arg_type = OPH_IO_CLIENT_TYPE_LONG;
        arg1.arg_length = sizeof(unsigned long long);
        arg1.arg = &id;
        arg2.arg_type = OPH_IO_CLIENT_TYPE_BLOB;
        arg2.arg_length = sizeof(double)*array_length;
        arg2.arg = (void *)malloc(sizeof(double)*array_length);
        
        int tot_run = 100;
        if((res = oph_io_client_setup_query (connection, "operation=insert;frag_name=trial1;field=id|measure;value=?|?;", "memory", tot_run, (oph_io_client_query_arg **)args, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=?|?;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          free(arg2.arg);
          return 0;          
        }

        for(i = 0; i < tot_run; i++){
          //Execute insert step
       		for(r = 0; r < array_length; r++)
			      measure[r] = ((double)rand()/RAND_MAX)*1000.0;

          id++;
          memcpy(arg2.arg, measure, array_length*sizeof(double)*sizeof(char));

	        if ((res = oph_io_client_execute_query(connection, stmt))){
		        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=?|?;");
            oph_io_client_free_query (stmt);
            res = oph_io_client_close (connection);
            free(arg2.arg);
            return 0;          
          }
        }

        oph_io_client_free_query (stmt);
        free(arg2.arg);


        //Create empty fragment
        if((res = oph_io_client_setup_query (connection, "operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res, "operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
	      if ((res = oph_io_client_execute_query(connection, stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res, "operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

	      printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        //Create empty fragment
        if((res = oph_io_client_setup_query (connection, "operation=insert;frag_name=trial1;field=id|measure;value=1|test;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=1|test;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
	      if ((res = oph_io_client_execute_query(connection, stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=1|test;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

	      printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        snprintf(query, 1024, "operation=create_database;db_name=test%d;", ii+(nchildren));

        //Create empty DB
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

        snprintf(query, 1024, "test%d;", ii+(nchildren));


        //Use new DB
	      if ((res = oph_io_client_use_db(query, "memory", connection))){
			    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in selecting database '%s'\n",res,query);
          res = oph_io_client_close (connection);
          return 0;
        }

        //Create empty fragment
        if((res = oph_io_client_setup_query (connection, "operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
	      if ((res = oph_io_client_execute_query(connection, stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=create_frag;frag_name=trial2;column_name=id|measure;column_type=long|blob;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

	      printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        //Create empty fragment
        if((res = oph_io_client_setup_query (connection, "operation=insert;frag_name=trial1;field=id|measure;value=1|test;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=1|test;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
	      if ((res = oph_io_client_execute_query(connection, stmt))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=insert;frag_name=trial1;field=id|measure;value=1|test;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

	      printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        snprintf(query, 1024, "test%d;", ii);

        //Use new DB
	      if ((res = oph_io_client_use_db(query, "memory", connection))){
			    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in selecting database '%s'\n",res,query);
          res = oph_io_client_close (connection);
          return 0;
        }

        //Execute oph_dump plugin
        if((res = oph_io_client_setup_query (connection, "operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial1;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial1;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
        
        if ((res = oph_io_client_execute_query(connection, stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial1;");
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

        while(current_row){
          printf("ID: %s ", current_row->field[0]);
          printf(" %s ", current_row->field[1]);
          oph_io_client_fetch_row(result_set, &current_row);
        }
        oph_io_client_free_result(result_set);

        //Execute oph_reduce plugin
        if((res = oph_io_client_setup_query (connection, "operation=create_frag_select;frag_name=trial3;field=id|oph_reduce(measure);alias=id|measure;from=trial1;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=create_frag_select;frag_name=trial3;field=id|oph_reduce(measure);alias=id|measure;from=trial1;", "memory");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
        if ((res = oph_io_client_execute_query(connection, stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=create_frag_select;frag_name=trial3;field=id|oph_reduce(measure);alias=id|measure;from=trial1;", "memory");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

        printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);
      
        //Execute oph_dump plugin
        if((res = oph_io_client_setup_query (connection, "operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial3;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial3;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
        if ((res = oph_io_client_execute_query(connection, stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=select;field=oph_dump('oph_double','oph_double',measure);from=trial3;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

        printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);
      
        result_set = NULL;
	      if ((res = oph_io_client_get_result(connection, &result_set))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in retrieving result set\n",res);
          res = oph_io_client_close (connection);
          return 0;
        }

        //Output the result set (assuming array of double is gathered)
        current_row = NULL;
        oph_io_client_fetch_row(result_set, &current_row);

        while(current_row){
          printf("ID: %s ", current_row->field[0]);
          printf(" %s ", current_row->field[1]);
          oph_io_client_fetch_row(result_set, &current_row);
        }
        oph_io_client_free_result(result_set);

        snprintf(query, 1024, "operation=drop_database;db_name=test%d;", ii);

        //Delete full DB
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

        snprintf(query, 1024, "test%d;", ii+(nchildren));
        
        //Use new DB
        if ((res = oph_io_client_use_db(query, "memory", connection))){
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in selecting database '%s'\n",res,query);
          res = oph_io_client_close (connection);
          return 0;
        }

        //Delete only a fragment
        if((res = oph_io_client_setup_query (connection, "operation=drop_frag;frag_name=trial2;", "memory", 0, (oph_io_client_query_arg **)NULL, &stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in setup query '%s'\n",res,"operation=drop_frag;frag_name=trial2;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }
      
        if ((res = oph_io_client_execute_query(connection, stmt))){
	        pmesg(LOG_ERROR,__FILE__,__LINE__,"Error %d in executing query '%s'\n",res,"operation=drop_frag;frag_name=trial2;");
          oph_io_client_free_query (stmt);
          res = oph_io_client_close (connection);
          return 0;          
        }

        printf("Query submitted correctly.\n");
        oph_io_client_free_query (stmt);

        snprintf(query, 1024, "operation=drop_database;db_name=test%d;", ii+(nchildren));
        
        //Delete empty DB
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
      }
      res = oph_io_client_close (connection);
    }
		else
    {
			pmesg(LOG_ERROR,__FILE__,__LINE__,"Fatal error while creating child process\n");
      continue;
    }
	}

	while (wait(NULL) > 0);	/* parent waits for all children */

	if (errno != ECHILD) pmesg(LOG_ERROR,__FILE__,__LINE__,"wait error");

	return 0;
}
