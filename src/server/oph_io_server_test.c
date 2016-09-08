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

#include "debug.h"
#include "hashtbl.h"
#include "oph_server_utility.h"
#include "oph_server_confs.h"
#include "oph_metadb_interface.h"
#include "oph_query_plugin_loader.h"
#include <pthread.h>
#include <unistd.h>
#include <errno.h>

#include "oph_network.h"

//TODO put globals into global struct 
//Global server variables (read only)
unsigned long long max_packet_length = 0;
unsigned short client_ttl = 0;
unsigned short omp_threads = 0;
//pthread_mutex_t metadb_mutex  = PTHREAD_MUTEX_INITIALIZER;
pthread_rwlock_t       rwlock = PTHREAD_RWLOCK_INITIALIZER;
oph_metadb_db_row *db_table = NULL;
extern HASHTBL *plugin_table;
extern oph_query_expr_symtable *oph_function_table;

int main(int argc, char *argv[])
{
	UNUSED(argc)
	UNUSED(argv)

	int msglevel = LOG_DEBUG;

	int			listenfd, tmpconnfd, *connfd = NULL;
	void			sig_int(int);
	void			*run_server_thread(void *);
	socklen_t		clilen, addrlen;
	struct sockaddr	*cliaddr;
	
	set_debug_level(msglevel);
  set_log_prefix(OPH_IO_SERVER_PREFIX);

  //Setup section: 1 - load conf files; 2 - load metaDB

  //Load params from conf files
  HASHTBL *conf_db = NULL;
  
  if(oph_server_conf_load(0, &conf_db)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while loading configuration file\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Error while loading configuration file\n");
    return -1;
  }

	char* hostname = 0;
	char* port = 0;
  char* max_length = 0;
  char *ttl = 0;
  char *omp = 0;

  if(oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_HOSTNAME, &hostname)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
    oph_server_conf_unload(&conf_db);
    return -1;
  }

  if(oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_PORT, &port)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
    oph_server_conf_unload(&conf_db);
    return -1;
  }

  if(oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_MPL, &max_length)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
    oph_server_conf_unload(&conf_db);
    return -1;
  }
  
  max_packet_length = strtoll(max_length, NULL, 10);

	if(oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_TTL, &ttl)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
    oph_server_conf_unload(&conf_db);
    return -1;
  }
  
  client_ttl = strtol(ttl, NULL, 10);

	if(oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_OMP_THREADS, &omp)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
    oph_server_conf_unload(&conf_db);
    return -1;
  }
  
  omp_threads = strtol(omp, NULL, 10);

	if(oph_load_plugins (&plugin_table, &oph_function_table)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to load plugin table\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to load plugin table\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
    oph_server_conf_unload(&conf_db);
		return -1;
	}

  //Setup MetaDB
  if(oph_metadb_load_schema (&db_table, 0)){
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to load MetaDB\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Unable to load MetaDB\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
    oph_metadb_unload_schema (db_table);
    oph_server_conf_unload(&conf_db);
    return -1;
  }


  //Startup TCP/IP listening
	if(oph_net_listen(hostname, port, &addrlen, &listenfd) != 0)
  {
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while listening TCP socket\n");
    logging(LOG_ERROR,__FILE__,__LINE__,"Error while listening TCP socket\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
    oph_metadb_unload_schema (db_table);
    oph_server_conf_unload(&conf_db);
    return -1;
  }

  cliaddr = (struct sockaddr	*) malloc(addrlen);
  if(cliaddr == NULL)
  {
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to allocate buffer for client address\n");
    logging(LOG_ERROR,__FILE__,__LINE__,"Unable to allocate buffer for client address\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
    oph_metadb_unload_schema (db_table);
    oph_server_conf_unload(&conf_db);
    return -1;
  }

  //Signal(SIGPIPE, SIG_IGN);
	//Signal(SIGINT, sig_int);

	clilen = addrlen;

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Waiting for a request...\n");
	logging(LOG_DEBUG,__FILE__,__LINE__,"Waiting for a request...\n");
  if(oph_net_accept(listenfd, cliaddr, &clilen, &tmpconnfd) != 0){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error on connection\n");
		logging(LOG_ERROR,__FILE__,__LINE__,"Error on connection\n");
    free(cliaddr);
    oph_metadb_unload_schema (db_table);
    oph_server_conf_unload(&conf_db);
    oph_unload_plugins(&plugin_table, &oph_function_table);
    return -1;
  }

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection established on socket %d\n",tmpconnfd);
	logging(LOG_DEBUG,__FILE__,__LINE__,"Connection established on socket %d\n",tmpconnfd);

  connfd = (int *)malloc(sizeof(int));
  *connfd = tmpconnfd;

  run_server_thread((void *) connfd);

  //Cleanup procedures
  free(cliaddr);
  oph_metadb_unload_schema (db_table);
  oph_server_conf_unload(&conf_db);
  oph_unload_plugins(&plugin_table, &oph_function_table);

  return 0;
}

void *run_server_thread(void *arg)
{
	void oph_io_server_thread(int);

	oph_io_server_thread(*((int *)arg));

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Closing the connection...\n");
	logging(LOG_DEBUG,__FILE__,__LINE__,"Closing the connection...\n");

	if(close(*((int *)arg)) == -1) pmesg(LOG_WARNING,__FILE__,__LINE__,"Error while closing connection!\n");

  free((int *)arg);

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection closed\n");
	logging(LOG_DEBUG,__FILE__,__LINE__,"Connection closed\n");
	return(NULL);
}

void sig_int(int signo)
{
	UNUSED(signo)
}

