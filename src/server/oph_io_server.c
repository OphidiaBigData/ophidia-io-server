/*
    Ophidia IO Server
    Copyright (C) 2014-2021 CMCC Foundation

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

#include "oph_io_server_thread.h"

#include <signal.h>
#include <unistd.h>
#include <malloc.h>
#include "debug.h"

#include "hashtbl.h"

#include "oph_server_confs.h"
#include "oph_metadb_interface.h"
#include "oph_network.h"
#include "oph_query_expression_evaluator.h"
#include "oph_query_plugin_loader.h"

#include "oph_license.h"

#ifdef OPH_IO_SERVER_ESDM
#include <esdm.h>
#endif

//TODO put globals into global struct 
//Global server variables (read-only)
unsigned long long max_packet_length = 0;
unsigned short omp_threads = 0;
unsigned short client_ttl = 0;
unsigned short disable_mem_check = 0;
unsigned long long memory_buffer = 0;
unsigned short cache_line_size = 0;
unsigned long long cache_size = 0;

pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t libtool_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t nc_lock = PTHREAD_MUTEX_INITIALIZER;

oph_metadb_db_row *db_table = NULL;
HASHTBL *plugin_table = NULL;
oph_query_expr_symtable *oph_function_table = NULL;

//Global only in this files (for garbage collection purpose)
struct sockaddr *cliaddr;
HASHTBL *conf_db = NULL;
char *oph_server_conf_file = OPH_SERVER_CONF_FILE_PATH;

int main(int argc, char *argv[])
{
#ifdef DEBUG
	int msglevel = LOG_DEBUG_T;
#else
	int msglevel = LOG_INFO_T;
#endif

	int listenfd, tmpconnfd, *connfd = NULL;
	void release(int);
	void *server_child(void *);
	pthread_t tid;
	socklen_t clilen, addrlen;
	set_debug_level(msglevel);

	int ch;
	unsigned short int instance = 0;

	static char *USAGE = "\nUSAGE:\noph_io_server [-i <instance_number>]\n";

	fprintf(stdout, "%s", OPH_VERSION);
	fprintf(stdout, OPH_DISCLAIMER, "oph_io_server", "oph_io_server");

	while ((ch = getopt(argc, argv, "c:dhi:xz")) != -1) {
		switch (ch) {
			case 'c':
				oph_server_conf_file = optarg;
				break;
			case 'h':
				fprintf(stdout, "%s", USAGE);
				return 0;
			case 'i':
				instance = (unsigned short int) strtol(optarg, NULL, 10);
				break;
			case 'd':
				disable_mem_check = 1;
				break;
			case 'x':
				fprintf(stdout, "%s", OPH_WARRANTY);
				return 0;
			case 'z':
				fprintf(stdout, "%s", OPH_CONDITIONS);
				return 0;
			default:
				fprintf(stdout, "%s", USAGE);
				return 0;
		}
	}


	if (disable_mem_check == 1) {
		pmesg(LOG_INFO, __FILE__, __LINE__, "Disable Memory check\n");
	}
	if (instance == 0) {
		pmesg(LOG_INFO, __FILE__, __LINE__, "Using default (first) instance in configuration file\n");
	}
	//mallopt(M_TRIM_THRESHOLD, 1024);
	//mallopt(M_TOP_PAD, 0);
	//mallopt(M_MMAP_THRESHOLD, 1);

	//Setup section: 1 - load conf files; 2 - load metaDB

	//Load params from conf files
	if (oph_server_conf_load(instance, &conf_db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading configuration file\n");
		//logging(LOG_ERROR,__FILE__,__LINE__,"Error while loading configuration file\n");
		return -1;
	}

	char *hostname = 0;
	char *port = 0;
	char *max_length = 0;
	char *ttl = 0;
	char *dir = 0;
	char *omp = 0;
	char *mem_buf = 0;
	char *cache_line = 0;
	char *cache = 0;
	char *working_dir = 0;

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_DIR, &dir)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get server dir param\n");
		//logging(LOG_ERROR,__FILE__,__LINE__,"Unable to get hostname param\n");
		//oph_server_conf_unload(&conf_db);
		//return -1;
		dir = OPH_IO_SERVER_PREFIX;
	}
	//Setup debug and MetaDB directories
	set_log_prefix(dir);
	oph_metadb_set_data_prefix(dir);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_HOSTNAME, &hostname)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_PORT, &port)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_MPL, &max_length)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	max_packet_length = strtoll(max_length, NULL, 10);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_TTL, &ttl)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get hostname param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	client_ttl = strtol(ttl, NULL, 10);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_OMP_THREADS, &omp)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of OpenMP threads param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get number of OpenMP threads param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	omp_threads = strtol(omp, NULL, 10);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_MEMORY_BUFFER, &mem_buf)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get memory buffer param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get memory buffer param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	memory_buffer = strtol(mem_buf, NULL, 10);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_CACHE_SIZE, &cache)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get cache size buffer param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get cache size buffer param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	cache_size = strtoll(cache, NULL, 10);

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_CACHE_LINE_SIZE, &cache_line)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get cache line size buffer param\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get cache line size buffer param\n");
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	cache_line_size = strtol(cache_line, NULL, 10);

	if (!oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_WORKING_DIR, &working_dir) && working_dir) {
		if (chdir(working_dir)) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to set working directory '%s'\n", working_dir);
			logging(LOG_WARNING, __FILE__, __LINE__, "Unable to set working directory '%s'\n", working_dir);
		}
	}

	if (oph_load_plugins(&plugin_table, &oph_function_table)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load plugin table\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to load plugin table\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}
	//Setup MetaDB
	if (oph_metadb_load_schema(&db_table, 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to load MetaDB\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to load MetaDB\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}
	//Startup TCP/IP listening
	if (oph_net_listen(hostname, port, &addrlen, &listenfd) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while listening TCP socket\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error while listening TCP socket\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	cliaddr = (struct sockaddr *) malloc(addrlen);
	if (cliaddr == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for client address\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for client address\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}
	//Signal(SIGPIPE, SIG_IGN);
	oph_net_signal(SIGINT, release);
	oph_net_signal(SIGABRT, release);
	oph_net_signal(SIGQUIT, release);

#ifdef OPH_IO_SERVER_ESDM
	if (esdm_init()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "ESDM cannot be initialized\n");
		oph_unload_plugins(&plugin_table, &oph_function_table);
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}
#endif

	//Startup client connections
	for (;;) {
		clilen = addrlen;


		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Waiting for a request...\n");
		logging(LOG_DEBUG, __FILE__, __LINE__, "Waiting for a request...\n");

		if (oph_net_accept(listenfd, cliaddr, &clilen, &tmpconnfd) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error on connection\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error on connection\n");
			continue;
		}

		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Connection established on socket %d\n", tmpconnfd);
		logging(LOG_DEBUG, __FILE__, __LINE__, "Connection established on socket %d\n", tmpconnfd);

		//TODO Manage multiple connections with poll

		connfd = (int *) malloc(sizeof(int));
		*connfd = tmpconnfd;
		if (pthread_create(&tid, NULL, &server_child, (void *) connfd) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error creating thread\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error creating thread\n");
			continue;
		}
		connfd = NULL;

	}

	//Cleanup procedures
	free(cliaddr);
	oph_metadb_unload_schema(db_table);
	oph_server_conf_unload(&conf_db);
	oph_unload_plugins(&plugin_table, &oph_function_table);

	return 0;
}

void *server_child(void *arg)
{
	void oph_io_server_thread(int, pthread_t);

	if (pthread_detach(pthread_self()) != 0)
		return (NULL);

	pthread_t tid = pthread_self();

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "LAUNCHING THREAD...\n");
	oph_io_server_thread(*((int *) arg), tid);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Closing the connection...\n");
	logging(LOG_DEBUG, __FILE__, __LINE__, "Closing the connection...\n");

	if (close(*((int *) arg)) == -1)
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Error while closing connection!\n");

	free((int *) arg);

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Connection closed\n");
	logging(LOG_DEBUG, __FILE__, __LINE__, "Connection closed\n");

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "CLOSING THREAD...\n");
	return (NULL);
}

//Garbage collecition function
void release(int signo)
{
	//Cleanup procedures
	logging(LOG_DEBUG, __FILE__, __LINE__, "Catched signal %d\n", signo);
	free(cliaddr);
	oph_metadb_unload_schema(db_table);
	oph_unload_plugins(&plugin_table, &oph_function_table);
	oph_server_conf_unload(&conf_db);

#ifdef OPH_IO_SERVER_ESDM
	esdm_finalize();
#endif

	exit(0);
}
