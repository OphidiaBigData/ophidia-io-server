/*
    Ophidia IO Server
    Copyright (C) 2014-2022 CMCC Foundation

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

#define _GNU_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <debug.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>

#include "oph_server_utility.h"
#include "oph_io_server_query_manager.h"
#include <netcdf.h>

int main(int argc, char *argv[])
{
#ifdef DEBUG
	int msglevel = LOG_DEBUG_T;
#else
	int msglevel = LOG_INFO_T;
#endif

	set_debug_level(msglevel);

	if (argc != 6) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Get input values
	int shm_id = strtol(argv[1], NULL, 10);
	char *src_path = argv[2];
	char *measure_name = argv[3];

	//Check number of values in start and count
	char *c1 = argv[4], *c2 = argv[5];
	int n1 = 0, n2 = 0;
	do {
		if (*c1 == ';')
			n1++;
	}
	while (*c1++);
	do {
		if (*c2 == ';')
			n2++;
	}
	while (*c2++);

	if (n1 != n2) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Convert strings back to arrays of size_t
	size_t *start = (size_t *) malloc(n1 * sizeof(size_t));
	size_t *count = (size_t *) malloc(n2 * sizeof(size_t));
	char *start_ptr = argv[4], *count_ptr = argv[5];
	int i = 0;
	for (i = 0; i < n1; i++) {
		start[i] = (size_t) strtol(start_ptr, &start_ptr, 10);
		count[i] = (size_t) strtol(count_ptr, &count_ptr, 10);
		*start_ptr++;
		*count_ptr++;
	}

	//Fill binary cache
	int res = -1, retval = -1, ncfile_id = 0, varid = 0;
	char *buffer = NULL;

	//Attach shared memory segment to process
	if ((buffer = shmat(shm_id, NULL, 0)) == (char *) -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to attach shared memory segment\n");
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if ((retval = nc_open(src_path, NC_NOWRITE, &ncfile_id))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(retval));
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if ((retval = nc_inq_varid(ncfile_id, measure_name, &varid))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		nc_close(ncfile_id);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Get information from id
	nc_type vartype;
	if ((retval = nc_inq_vartype(ncfile_id, varid, &vartype))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		nc_close(ncfile_id);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	switch (vartype) {
		case NC_BYTE:
		case NC_CHAR:
			res = nc_get_vara_uchar(ncfile_id, varid, start, count, (unsigned char *) (buffer));
			break;
		case NC_SHORT:
			res = nc_get_vara_short(ncfile_id, varid, start, count, (short *) (buffer));
			break;
		case NC_INT:
			res = nc_get_vara_int(ncfile_id, varid, start, count, (int *) (buffer));
			break;
		case NC_INT64:
			res = nc_get_vara_longlong(ncfile_id, varid, start, count, (long long *) (buffer));
			break;
		case NC_FLOAT:
			res = nc_get_vara_float(ncfile_id, varid, start, count, (float *) (buffer));
			break;
		case NC_DOUBLE:
			res = nc_get_vara_double(ncfile_id, varid, start, count, (double *) (buffer));
			break;
		default:
			res = nc_get_vara_double(ncfile_id, varid, start, count, (double *) (buffer));
	}
	nc_close(ncfile_id);

	//Detach shared memory segment
	shmdt(buffer);

	if (res != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		return OPH_IO_SERVER_MEMORY_ERROR;
	} else {
		pmesg(LOG_INFO, __FILE__, __LINE__, "Data correctly loaded in shared memory\n");
		return OPH_IO_SERVER_SUCCESS;
	}
}
