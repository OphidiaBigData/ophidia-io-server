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

#include "oph_server_confs.h"

#include "oph_license.h"

#ifdef OPH_IO_PMEM
#include <memkind.h>
struct memkind *pmem_kind = 0;
#endif

unsigned short disable_mem_check = 0;
char *oph_server_conf_file = OPH_SERVER_CONF_FILE_PATH;

int main(int argc, char *argv[])
{
	oph_metadb_db_row *db_table = NULL;

	set_debug_level(LOG_INFO);

	int ch;
	unsigned short int instance = 0;

	static char *USAGE = "\nUSAGE:\noph_metadb_reader [-i <instance_number>]\n";

	fprintf(stdout, OPH_VERSION2, "MetaDB read client");
	fprintf(stdout, OPH_DISCLAIMER, "oph_metadb_reader", "oph_metadb_reader");

	while ((ch = getopt(argc, argv, "c:hi:xz")) != -1) {
		switch (ch) {
			case 'c':
				oph_server_conf_file = optarg;
				break;
			case 'i':
				instance = (unsigned short int) strtol(optarg, NULL, 10);
				break;
			case 'h':
				fprintf(stdout, "%s", USAGE);
				return 0;
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

	if (instance == 0) {
		pmesg(LOG_INFO, __FILE__, __LINE__, "Using default (first) instance in configuration file\n");
	}

	HASHTBL *conf_db = NULL;

	//Load params from conf files
	if (oph_server_conf_load(instance, &conf_db)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading configuration file\n");
		return -1;
	}

	char *dir = 0;

	if (oph_server_conf_get_param(conf_db, OPH_SERVER_CONF_DIR, &dir)) {
		pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to get server dir param\n");
		dir = OPH_IO_SERVER_PREFIX;
	}
	//Setup log and MetaDB directories
	set_log_prefix(dir);
	oph_metadb_set_data_prefix(dir);


	if (oph_metadb_load_schema(&db_table, 0)) {
		printf("Unable to load MetaDB\n");
		oph_metadb_unload_schema(db_table);
		oph_server_conf_unload(&conf_db);
		return -1;
	}
	//Print info about all DB and all Fragments

	oph_metadb_db_row *test_row = NULL;
	oph_metadb_frag_row *test_frag_row = NULL;

	test_row = db_table;
	char tmp[1024];
	int i;

	if (test_row != NULL) {
		printf("+============+================================+======================+============+======================+==========================================+\n");
		printf("| %-10s | %-30s | %-20s | %-10s | %-20s | %-40s |\n", "TYPE", "NAME", "DEVICE", "PERSISTENT", "FILE OFFSET", "EXTRA INFO");
		printf("+============+================================+======================+============+======================+==========================================+\n");
		while (test_row) {
			//Display DB record
			snprintf(tmp, 1024, "%s: %llu", "Frag num", test_row->frag_number);
			printf("| %-10s | %-30s | %-20s | %-10s | %-20llu | %-40s |\n", "DB", test_row->db_name, test_row->device, (test_row->is_persistent ? "YES" : "NO"), test_row->file_offset,
			       tmp);
			//Display all fragments into DB
			if (test_row->table != NULL) {
				for (i = 0; i < test_row->table->size; i++) {
					test_frag_row = test_row->table->rows[i];
					while (test_frag_row) {
						snprintf(tmp, 1024, "%s: %llu B", "Frag size", test_frag_row->frag_size);
						printf("| %-10s | %-30s | %-20s | %-10s | %-20llu | %-40s |\n", "->FRAG", test_frag_row->frag_name, test_frag_row->device,
						       (test_row->is_persistent ? "YES" : "NO"), test_frag_row->file_offset, tmp);
						test_frag_row = (oph_metadb_frag_row *) test_frag_row->next_frag;
					}
				}
				test_frag_row = NULL;
			}
			test_row = (oph_metadb_db_row *) test_row->next_db;
			if (test_row)
				printf("+------------+--------------------------------+----------------------+------------+----------------------+------------------------------------------+\n");

		}
		printf("+============+================================+======================+============+======================+==========================================+\n");
	} else {
		printf("+======================+\n");
		printf("| %-20s |\n", "EMPTY SERVER");
		printf("+======================+\n");
	}
	oph_metadb_unload_schema(db_table);
	oph_server_conf_unload(&conf_db);

	return 0;
}
