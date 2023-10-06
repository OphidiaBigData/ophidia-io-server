/*
    Ophidia IO Server
    Copyright (C) 2014-2023 CMCC Foundation

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

#include "oph_metadb_interface.h"

#include "debug.h"
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include "oph_server_confs.h"

#ifdef OPH_IO_PMEM
#include <memkind.h>
struct memkind *pmem_kind = 0;
#endif

//Global mutex variable 
pthread_mutex_t metadb_mutex = PTHREAD_MUTEX_INITIALIZER;
char *oph_server_conf_file = OPH_SERVER_CONF_FILE_PATH;
unsigned short disable_mem_check = 0;

void *test_metadb(void *arg);

int main(int argc, char *argv[])
{
	oph_metadb_db_row *db_table = NULL;

	set_debug_level(LOG_DEBUG);
	set_log_prefix(OPH_IO_SERVER_PREFIX);

	int ch;
	unsigned short int instance = 0;
	unsigned short int help = 0;

	while ((ch = getopt(argc, argv, "c:hi:")) != -1) {
		switch (ch) {
			case 'c':
				oph_server_conf_file = optarg;
				break;
			case 'h':
				help = 1;
				break;
			case 'i':
				instance = (unsigned short int) strtol(optarg, NULL, 10);
				break;
		}
	}

	if (help) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Execute MetaDB Reader: oph_metadb_client -i <instance_number>\n");
		exit(0);
	}
	if (instance == 0) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Using default (first) instance in configuration file\n");
	}

	HASHTBL *conf_db = NULL;

	//Load params from conf files
	if (oph_server_conf_load(instance, &conf_db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading configuration file\n");
		//logging(LOG_ERROR,__FILE__,__LINE__,"Error while loading configuration file\n");
		return -1;
	}

	char *dir = 0;

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_DIR, &dir)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get server dir param\n");
		dir = OPH_IO_SERVER_PREFIX;
	}
	oph_metadb_set_data_prefix(dir);

	if (oph_metadb_load_schema(&db_table, 0)) {
		printf("Unable to load MetaDB\n");
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}

	int i = 0;
	int thread_number = 10;

	pthread_t tid[thread_number];
	int n = 0;

	for (i = 0; i < thread_number; i++) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Creating new thread\n");
		if ((n = pthread_create(&tid[i], NULL, &test_metadb, (void *) db_table)) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error creating thread\n");
		}
	}

	for (i = 0; i < thread_number; i++) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Joining thread\n");
		if ((n = pthread_join(tid[i], NULL)) != 0) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Error joining thread\n");
		}
	}

	oph_server_conf_unload(&conf_db);
	oph_metadb_unload_schema(db_table);

	return 0;

}

void *test_metadb(void *arg)
{
	int tid = pthread_self();

	//Pthread_detach(tid);

	oph_metadb_db_row *db_table = (oph_metadb_db_row *) arg;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Executing thread %d\n", tid);

	//Create DB record
	oph_metadb_db_row *test_row = NULL;
	oph_iostore_resource_id db_id;
	db_id.id_length = strlen("trial");
	db_id.id = (void *) strndup("trial", strlen("trial"));
	if (oph_metadb_setup_db_struct("trial", "Memory", 0, &db_id, 0, &test_row)) {
		printf("Unable to create DB record\n");
		return (NULL);
	}
	//Add db record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}
	if (oph_metadb_add_db(&db_table, test_row)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to add data to MetaDB\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}

	oph_metadb_db_row *test_row1 = NULL;
	//Find DB record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}
	if (oph_metadb_find_db(db_table, test_row->db_name, test_row->device, &test_row1) || test_row1 == NULL) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to add data to MetaDB\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		return (NULL);
	}
	//Create frag record
	oph_metadb_frag_row *test_frag_row = NULL;
	oph_iostore_resource_id frag_id;
	frag_id.id_length = strlen("trial1");
	frag_id.id = (void *) strndup("trial1", strlen("trial1"));
	if (oph_metadb_setup_frag_struct("Fragment", "Memory", 0, &db_id, &frag_id, 100, &test_frag_row)) {
		printf("Unable to create Frag record\n");
		return (NULL);
	}
	//Add frag record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (oph_metadb_add_frag(test_row1, test_frag_row)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to add data to MetaDB\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	//Update db record
	test_row->frag_number++;
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (oph_metadb_update_db(db_table, test_row)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to add data to MetaDB\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_db_struct(test_row);
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}

	oph_metadb_cleanup_db_struct(test_row);
	test_row = NULL;

	//Display DB record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	printf("%s %llu %llu\n", test_row1->db_name, test_row1->file_offset, test_row1->frag_number);
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	//Update Frag record
	test_frag_row->frag_size = 555;
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (oph_metadb_update_frag(test_row1, test_frag_row)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to add data to MetaDB\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}

	oph_metadb_frag_row *test_frag_row1 = NULL;
	//Find frag record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (oph_metadb_find_frag(test_row1, test_frag_row->frag_name, &test_frag_row1) || !test_frag_row1) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to find frag in MetaDB\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		oph_metadb_cleanup_frag_struct(test_frag_row);
		return (NULL);
	}

	oph_metadb_cleanup_frag_struct(test_frag_row);
	test_frag_row = NULL;

	//Display frag record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		return (NULL);
	}
	printf("%s %llu %llu\n", test_frag_row1->frag_name, test_frag_row1->file_offset, test_frag_row1->frag_size);
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		return (NULL);
	}
	//Remove frag record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		return (NULL);
	}
	if (oph_metadb_remove_frag(test_row1, test_frag_row1->frag_name, NULL)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to remove data from MetaDB\n");
		return (NULL);
	}
	if (pthread_mutex_unlock(&metadb_mutex) != 0) {
		printf("Unable to unlock mutex\n");
		return (NULL);
	}
	//Remove DB record
	if (pthread_mutex_lock(&metadb_mutex) != 0) {
		printf("Unable to lock mutex\n");
		return (NULL);
	}
	if (oph_metadb_remove_db(&db_table, test_row1->db_name, test_row1->device)) {
		pthread_mutex_unlock(&metadb_mutex);
		printf("Unable to remove data from MetaDB\n");
		return (NULL);
	}
	pthread_mutex_unlock(&metadb_mutex);

	return (NULL);
}
