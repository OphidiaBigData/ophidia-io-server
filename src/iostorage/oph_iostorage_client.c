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

#include "oph_iostorage_interface.h"

#include "debug.h"
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

pthread_mutex_t libtool_lock = PTHREAD_MUTEX_INITIALIZER;
unsigned short disable_mem_check = 0;

int main()
{

	set_debug_level(LOG_DEBUG);
	set_log_prefix(OPH_IO_SERVER_PREFIX);

	//TEST I/O storage interface
	char test_device[] = "memory";
	oph_iostore_handler *dev_handle = NULL;
	if (oph_iostore_setup(test_device, &dev_handle) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup device library\n");
		return 0;
	}

	oph_iostore_db_record_set db_record;
	db_record.db_name = (char *) strndup("test DB", strlen("test DB"));
	oph_iostore_resource_id *res_id = NULL;
	if (oph_iostore_put_db(dev_handle, &db_record, &res_id) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to put new DB into device\n");
		free(db_record.db_name);
		oph_iostore_cleanup(dev_handle);
		return 0;
	}
	free(db_record.db_name);

	oph_iostore_db_record_set *db_record1 = NULL;
	if (oph_iostore_get_db(dev_handle, res_id, &db_record1) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get a DB from device\n");
		oph_iostore_cleanup(dev_handle);
		free(res_id->id);
		free(res_id);
		return 0;
	}

	printf("Retrieved DB is %s\n", db_record1->db_name);
	free(db_record1->db_name);
	free(db_record1);

	if (oph_iostore_delete_db(dev_handle, res_id) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete DB from device\n");
		oph_iostore_cleanup(dev_handle);
		free(res_id->id);
		free(res_id);
		return 0;
	}

	free(res_id->id);
	free(res_id);

	res_id = NULL;
	oph_iostore_frag_record_set *frag_record = NULL;
	if (oph_iostore_create_sample_frag(10, 10, &frag_record)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create frag record\n");
		oph_iostore_cleanup(dev_handle);
		return 0;
	}
	frag_record->frag_name = strdup("Test Frag");

	if (oph_iostore_put_frag(dev_handle, frag_record, &res_id) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to put new frag into device\n");
		oph_iostore_destroy_frag_recordset(&frag_record);
		oph_iostore_cleanup(dev_handle);
		return 0;
	}

	oph_iostore_frag_record_set *frag_record1 = NULL;
	if (oph_iostore_get_frag(dev_handle, res_id, &frag_record1) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get a frag from device\n");
		oph_iostore_cleanup(dev_handle);
		free(res_id->id);
		free(res_id);
		return 0;
	}

	printf("Retrieved Frag has %s field\n", frag_record1->field_name[0]);

	if (oph_iostore_delete_frag(dev_handle, res_id) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to delete frag from device\n");
		oph_iostore_cleanup(dev_handle);
		free(res_id->id);
		free(res_id);
		return 0;
	}

	free(res_id->id);
	free(res_id);
	oph_iostore_cleanup(dev_handle);

	return 0;
}
