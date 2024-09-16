/*
    Ophidia IO Server
    Copyright (C) 2014-2024 CMCC Foundation

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

#define INT_LEN 13
#define MSG_LEN 4096

int _oph_ioserver_nc_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, size_t **id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (size_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_ioserver_nc_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else {
		*(id[i]) = (size_t) (residual + 1);
	}
	return 0;
}

int oph_ioserver_nc_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, size_t **id)
{
	if (n > 0) {
		int i;
		unsigned long total = 1;
		for (i = 0; i < n; ++i)
			total *= sizemax[i];
		_oph_ioserver_nc_get_dimension_id(ID - 1, total, sizemax, id, 0, n);
	}
	return 0;
}

int main(int argc, char *argv[])
{
#ifdef DEBUG
	int msglevel = LOG_DEBUG_T;
#else
	int msglevel = LOG_INFO_T;
#endif

	set_debug_level(msglevel);

	if (argc > 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Get input values
	char msg[MSG_LEN] = { '\0' };
	if (read(STDIN_FILENO, msg, MSG_LEN) == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get message from master process: %s\n", strerror(errno));
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arguments %s\n", msg);

	//Convert string to input values
	int i = 0;
	char *ptr = NULL;
	if ((ptr = strchr(msg, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	int shm_id = strtol(msg, NULL, 10);
	char *src_path = ptr + 1;
	if ((ptr = strchr(ptr + 1, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	char *measure_name = ptr + 1;
	if ((ptr = strchr(ptr + 1, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	//Check number of values in start and count
	char *start_msg = ptr + 1;
	if ((ptr = strchr(ptr + 1, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	char *count_msg = ptr + 1;
	if ((ptr = strchr(ptr + 1, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	int offset = strtol(ptr + 1, NULL, 10);
	if ((ptr = strchr(ptr + 1, '|')) == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	*ptr = 0;
	unsigned long long tuples = strtoll(ptr + 1, NULL, 10);
	unsigned long long idDim = 0;
	char *sizemax_msg = NULL;
	int nexp = 1;
	char *dims_index_msg = NULL;
	char *dims_start_msg = NULL;
	if (tuples > 1) {
		if ((ptr = strchr(ptr + 1, '|')) == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		*ptr = 0;
		idDim = strtoll(ptr + 1, NULL, 10);
		if ((ptr = strchr(ptr + 1, '|')) == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		*ptr = 0;
		nexp = strtol(ptr + 1, NULL, 10);
		if ((ptr = strchr(ptr + 1, '|')) == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		*ptr = 0;
		sizemax_msg = ptr + 1;
		if ((ptr = strchr(ptr + 1, '|')) == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		*ptr = 0;
		dims_index_msg = ptr + 1;
		if ((ptr = strchr(ptr + 1, '|')) == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to parse message from master process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		*ptr = 0;
		dims_start_msg = ptr + 1;
	}

	char *c1 = start_msg;
	char *c2 = count_msg;
	int j, n1 = 0, n2 = 0;
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
	char *start_ptr = start_msg, *count_ptr = count_msg;
	for (i = 0; i < n1; i++) {
		start[i] = (size_t) strtol(start_ptr, &start_ptr, 10);
		count[i] = (size_t) strtol(count_ptr, &count_ptr, 10);
		*start_ptr++;
		*count_ptr++;
	}

	unsigned int *sizemax = NULL;
	int *dims_index = NULL;
	int *dims_start = NULL;
	if (tuples > 1) {
		char *c3 = sizemax_msg;
		int n3 = 0;
		do {
			if (*c3 == ';')
				n3++;
		}
		while (*c3++);
		if (nexp != n3) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		c3 = dims_index_msg;
		n3 = 0;
		do {
			if (*c3 == ';')
				n3++;
		}
		while (*c3++);
		if (n1 != n3) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		c3 = dims_start_msg;
		n3 = 0;
		do {
			if (*c3 == ';')
				n3++;
		}
		while (*c3++);
		if (n1 != n3) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Arguments are not correct\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		sizemax = (unsigned int *) malloc(nexp * sizeof(unsigned int));
		dims_index = (int *) malloc(n1 * sizeof(int));
		dims_start = (int *) malloc(n1 * sizeof(int));
		char *sizemax_ptr = sizemax_msg;
		char *dims_index_ptr = dims_index_msg;
		char *dims_start_ptr = dims_start_msg;
		for (i = 0; i < nexp; i++) {
			sizemax[i] = (unsigned int) strtol(sizemax_ptr, &sizemax_ptr, 10);
			*sizemax_ptr++;
		}
		for (i = 0; i < n1; i++) {
			dims_index[i] = (int) strtol(dims_index_ptr, &dims_index_ptr, 10);
			dims_start[i] = (int) strtol(dims_start_ptr, &dims_start_ptr, 10);
			*dims_index_ptr++;
			*dims_start_ptr++;
		}
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

	unsigned int num_elems = 1;
	for (i = 0; i < n2; ++i)
		num_elems *= count[i];

	size_t *start_pointer[nexp];
	if (tuples > 1) {
		for (j = 0; j < nexp; j++) {
			for (i = 0; i < n1; i++) {
				if ( /*dims_type[i] && */ dims_index[i] == j) {	// Check on type is useless due to the specific setting of dims_index
					start_pointer[j] = &(start[i]);
					break;
				}
			}
		}
	}

	unsigned long long ii;
	for (ii = 0; ii < tuples; idDim++) {

		if (tuples > 1) {
			oph_ioserver_nc_compute_dimension_id(idDim, sizemax, nexp, start_pointer);
			for (i = 0; i < nexp; i++) {
				*(start_pointer[i]) -= 1;
				for (j = 0; j < n1; j++) {
					if (start_pointer[i] == &(start[j])) {
						*(start_pointer[i]) += dims_start[j];
						/* TODO
						   // Correction due to multiple files
						   if (j == dim_unlim)
						   *(start_pointer[i]) -= offset;
						 */
					}
				}
			}
		}

		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = nc_get_vara_uchar(ncfile_id, varid, start, count, (unsigned char *) buffer + offset);
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncfile_id, varid, start, count, (short *) buffer + offset);
				break;
			case NC_INT:
				res = nc_get_vara_int(ncfile_id, varid, start, count, (int *) buffer + offset);
				break;
			case NC_INT64:
				res = nc_get_vara_longlong(ncfile_id, varid, start, count, (long long *) buffer + offset);
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncfile_id, varid, start, count, (float *) buffer + offset);
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncfile_id, varid, start, count, (double *) buffer + offset);
				break;
			default:
				res = nc_get_vara_double(ncfile_id, varid, start, count, (double *) buffer + offset);
		}

		ii++;
		if (ii < tuples)
			offset += num_elems;
	}

	nc_close(ncfile_id);

	//Detach shared memory segment
	shmdt(buffer);

	free(start);
	free(count);
	if (tuples > 1) {
		free(sizemax);
		free(dims_start);
	}

	if (res != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		return OPH_IO_SERVER_MEMORY_ERROR;
	} else {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Data correctly loaded in shared memory\n");
		return OPH_IO_SERVER_SUCCESS;
	}
}
