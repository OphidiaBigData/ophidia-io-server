/*
    Ophidia IO Server
    Copyright (C) 2014-2018 CMCC Foundation

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

#ifdef OPH_IO_SERVER_NETCDF
#define _GNU_SOURCE

#include "oph_io_server_query_manager.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <debug.h>
#include <errno.h>
#include <pthread.h>

#include "oph_server_utility.h"
#include "oph_query_engine_language.h"

#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_functions.h"
#include "oph_query_plugin_loader.h"

#include <math.h>
#include "oph-lib-binary-io.h"

#define DIM_VALUE "?1"
#define UNCOMPRESSED_VALUE "?2"
#define COMPRESSED_VALUE "oph_compress('','',?2)"

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern pthread_rwlock_t rwlock;
extern pthread_mutex_t nc_lock;
extern HASHTBL *plugin_table;
extern unsigned long long memory_buffer;

#ifdef DEBUG
#include "taketime.h"
static int timeval_add(res, x, y)
struct timeval *res, *x, *y;
{
	res->tv_sec = x->tv_sec + y->tv_sec;
	res->tv_usec = x->tv_usec + y->tv_usec;
	while (res->tv_usec > MILLION) {
		res->tv_usec -= MILLION;
		res->tv_sec++;
	}
	return 0;
}
#endif


int _oph_ioserver_nc_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, size_t ** id, int i, int n)
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

int oph_ioserver_nc_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, size_t ** id)
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

int oph_ioserver_nc_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var)
{
	short int i = 0;
	long long addr = 0;
	long long index = 0;
	long long total_iter = 1;

	for (i = 0; i < tot_dim_number; i++) {
		if (i != 0)
			counters[i] = 0;
		total_iter *= (limits[i] - counters[i]);
	}

	for (index = 0; index < total_iter; index++) {
		addr = 0;
		for (i = 0; i < tot_dim_number; i++) {
			addr += counters[i] * products[i];
		}
		memcpy(binary_insert + index * sizeof_var, binary_cache + addr * sizeof_var, sizeof_var);

		//Increase counters starting from most rapidly varying dimension
		for (i = tot_dim_number - 1; i >= 0; i--) {
			counters[i]++;
			if (counters[i] < limits[i]) {
				break;
			} else {
				counters[i] = 0;
			}
		}
	}

	return 0;
}

int _oph_ioserver_nc_read(char *src_path, char *measure_name, long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int dim_num, short int *dims_type,
			  short int *dims_index, int *dims_start, int *dims_end, oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size)
{
	if (!src_path || !measure_name || !tuplexfrag_number || !frag_key_start || !dim_num || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}

	if (strstr(src_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		return OPH_IO_SERVER_PARSE_ERROR;
	}
	if (!strstr(src_path, "http://") && !strstr(src_path, "https://")) {
		char *pointer = src_path;
		while (pointer && (*pointer == ' '))
			pointer++;
		if (pointer) {
			if (*pointer != '/') {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Path to NetCDF file should be absolute\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Path to NetCDF file should be absolute\n");
				return OPH_IO_SERVER_PARSE_ERROR;
			}
		}
	}
	//Check the order and field list values
	int measure_pos = -1, id_dim_pos = -1;
	int i = 0;
	for (i = 0; i < binary_frag->field_num; i++) {
		if (binary_frag->field_type[i] == OPH_IOSTORE_STRING_TYPE) {
			measure_pos = i;
		} else if (binary_frag->field_type[i] == OPH_IOSTORE_LONG_TYPE) {
			id_dim_pos = i;
		}
	}
	if (measure_pos == id_dim_pos || measure_pos == -1 || id_dim_pos == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while matching fields to fragment\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error while matching fields to fragment\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Open netcdf file
	int ncid = 0;
	int retval, j = 0;

	if (pthread_mutex_lock(&nc_lock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if ((retval = nc_open(src_path, NC_NOWRITE, &ncid))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(retval));
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if (pthread_mutex_unlock(&nc_lock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Extract measured variable information
	int varid = 0;
	if ((retval = nc_inq_varid(ncid, measure_name, &varid))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Get information from id
	nc_type vartype;
	if ((retval = nc_inq_vartype(ncid, varid, &vartype))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Check ndims value
	int ndims;
	if ((retval = nc_inq_varndims(ncid, varid, &ndims))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if (ndims != dim_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Compute array_length from implicit dims
	int array_length = 1;
	short int nimp = 0, nexp = 0;
	for (i = 0; i < ndims; i++) {
		if (!dims_type[i]) {
			array_length *= dims_end[i] - dims_start[i] + 1;
			nimp++;
		} else {
			nexp++;
		}
	}

	long long sizeof_var = 0;
	switch (vartype) {
		case NC_BYTE:
		case NC_CHAR:
			sizeof_var = (array_length) * sizeof(char);
			break;
		case NC_SHORT:
			sizeof_var = (array_length) * sizeof(short);
			break;
		case NC_INT:
			sizeof_var = (array_length) * sizeof(int);
			break;
		case NC_INT64:
			sizeof_var = (array_length) * sizeof(long long);
			break;
		case NC_FLOAT:
			sizeof_var = (array_length) * sizeof(float);
			break;
		case NC_DOUBLE:
			sizeof_var = (array_length) * sizeof(double);
			break;
		default:
			sizeof_var = (array_length) * sizeof(double);
	}

	//TODO - Check that memory for the two arrays is actually available
	//Flag set to 1 if whole fragment fits in memory
	unsigned long long memory_size = memory_buffer * (unsigned long long) 1048576;
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size / 2 ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Flag set to 1 if dimension are in the order specified in the file
	short int dimension_ordered = 1;
	for (i = 0; i < ndims; i++) {
		if (dims_index[i] != i) {
			dimension_ordered = 0;
			break;
		}
	}

	//If flag is set fragment reordering is required
	short int transpose = 1;
	if (dimension_ordered) {
		transpose = 0;
	}
	//Find most external dimension with size bigger than 1
	int most_extern_id = 0;
	for (i = 0; i < nexp; i++) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			if ((dims_end[j] - dims_start[j]) > 0) {
				most_extern_id = i;
				break;
			}
		}
	}

	//Check if only most external dimension (bigger than 1) is splitted
	long long curr_rows = 1;
	long long relative_rows = 0;
	short int whole_explicit = 1;
	for (i = ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			relative_rows = (int) (tuplexfrag_number / curr_rows);
			curr_rows *= (dims_end[j] - dims_start[j] + 1);
			if (relative_rows < (dims_end[j] - dims_start[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}

	//If external explicit is not integer
	if ((tuplexfrag_number % curr_rows) != 0)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented\n");
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Create binary array
	char *binary_cache = 0;
	int res;

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if (transpose) {
		//Create a binary array to store the whole fragment
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
				break;
			default:
				res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(binary_cache);
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}


	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	switch (vartype) {
		case NC_BYTE:
		case NC_CHAR:
			res = oph_iob_bin_array_create_b(&binary_insert, array_length * tuplexfrag_number);
			break;
		case NC_SHORT:
			res = oph_iob_bin_array_create_s(&binary_insert, array_length * tuplexfrag_number);
			break;
		case NC_INT:
			res = oph_iob_bin_array_create_i(&binary_insert, array_length * tuplexfrag_number);
			break;
		case NC_INT64:
			res = oph_iob_bin_array_create_l(&binary_insert, array_length * tuplexfrag_number);
			break;
		case NC_FLOAT:
			res = oph_iob_bin_array_create_f(&binary_insert, array_length * tuplexfrag_number);
			break;
		case NC_DOUBLE:
			res = oph_iob_bin_array_create_d(&binary_insert, array_length * tuplexfrag_number);
			break;
		default:
			res = oph_iob_bin_array_create_d(&binary_insert, array_length * tuplexfrag_number);
	}
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long *idDim = (unsigned long long *) calloc(tuplexfrag_number, sizeof(unsigned long long));
	if (!(idDim)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	for (i = 0; i < tuplexfrag_number; i++) {
		idDim[i] = frag_key_start + i;
	}


	//start and count array must be sorted based on the actual order of dimensions in the nc file
	//sizemax must be sorted based on the actual oph_level value
	unsigned int *sizemax = (unsigned int *) malloc(nexp * sizeof(unsigned int));
	size_t *start = (size_t *) malloc(ndims * sizeof(size_t));
	size_t *count = (size_t *) malloc(ndims * sizeof(size_t));
	//Sort start in base of oph_level of explicit dimension
	size_t **start_pointer = (size_t **) malloc(nexp * sizeof(size_t *));

	//idDim controls the start array for the fragment
	short int flag = 0;
	for (j = 0; j < nexp; j++) {
		flag = 0;
		//Find dimension with index = i
		for (i = 0; i < ndims; i++) {
			if (dims_type[i] && dims_index[i] == j) {
				//Modified to allow subsetting
				sizemax[j] = dims_end[i] - dims_start[i] + 1;
				start_pointer[j] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			if (binary_cache)
				free(binary_cache);
			free(binary_insert);
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(idDim);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	oph_ioserver_nc_compute_dimension_id(idDim[0], sizemax, nexp, start_pointer);

	for (i = 0; i < nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (j = 0; j < ndims; j++) {
			if (start_pointer[i] == &(start[j])) {
				*(start_pointer[i]) += dims_start[j];
			}
		}
	}

	relative_rows = 0;
	curr_rows = 1;
	for (i = ndims - 1; i >= 0; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//Explicit
		if (dims_type[j]) {
			if (dims_index[j] != most_extern_id)
				count[j] = dims_end[j] - dims_start[j] + 1;
			else {
				count[j] = (int) (tuplexfrag_number / curr_rows);
			}
			curr_rows *= count[j];
		} else {
			//Implicit
			//Modified to allow subsetting
			count[j] = dims_end[j] - dims_start[j] + 1;
			start[j] = dims_start[j];
		}
	}

	//Check
	unsigned long long total = 1;
	for (i = 0; i < ndims; i++)
		total *= count[i];

	if (total != array_length * tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
#ifdef DEBUG
	struct timeval start_read_time, end_read_time, total_read_time;
	struct timeval start_transpose_time, end_transpose_time, intermediate_transpose_time, total_transpose_time;
	total_transpose_time.tv_usec = 0;
	total_transpose_time.tv_sec = 0;

	gettimeofday(&start_read_time, NULL);
#endif

	//Fill binary cache
	res = -1;
	if (transpose) {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = nc_get_vara_uchar(ncid, varid, start, count, (unsigned char *) (binary_cache));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (binary_cache));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (binary_cache));
				break;
			case NC_INT64:
				res = nc_get_vara_longlong(ncid, varid, start, count, (long long *) (binary_cache));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (binary_cache));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (binary_cache));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (binary_cache));
		}
	} else {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = nc_get_vara_uchar(ncid, varid, start, count, (unsigned char *) (binary_insert));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (binary_insert));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (binary_insert));
				break;
			case NC_INT64:
				res = nc_get_vara_longlong(ncid, varid, start, count, (long long *) (binary_insert));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (binary_insert));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (binary_insert));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (binary_insert));
		}
	}

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (res != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	pthread_mutex_lock(&nc_lock);
	nc_close(ncid);
	pthread_mutex_unlock(&nc_lock);

	free(start);
	free(start_pointer);
	free(sizemax);

	if (transpose) {
		//Prepare structures for buffer insert update
		size_t sizeof_type = (int) sizeof_var / array_length;

		unsigned int *counters = (unsigned int *) malloc(ndims * sizeof(unsigned int));
		unsigned int *products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *limits = (unsigned int *) malloc(ndims * sizeof(unsigned));
		int *file_indexes = (int *) malloc(ndims * sizeof(int));

		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			products[dims_index[i]] = 1;
			limits[dims_index[i]] = count[i];
			file_indexes[dims_index[i]] = k++;
		}

		//Compute products
		for (k = 0; k < ndims; k++) {
			//Last dimension in file has product 1
			for (i = (file_indexes[k] + 1); i < ndims; i++) {
				flag = 0;
				//For each index following multiply
				for (j = 0; j < ndims; j++) {
					if (file_indexes[j] == i) {
						products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					if (binary_cache)
						free(binary_cache);
					free(binary_insert);
					free(idDim);
					free(count);
					free(counters);
					free(file_indexes);
					free(products);
					free(limits);
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			}
		}
		free(file_indexes);

#ifdef DEBUG
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_ioserver_nc_cache_to_buffer(ndims, counters, limits, products, binary_cache, binary_insert, sizeof_type);
#ifdef DEBUG
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
		printf("Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif
		free(counters);
		free(products);
		free(limits);
	}

	free(count);
	if (binary_cache)
		free(binary_cache);

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(idDim);
		free(binary_insert);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	char **value_list = (char **) calloc(arg_count, sizeof(char *));
	if (!(value_list)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		for (i = 0; i < arg_count; i++)
			if (args[i])
				free(args[i]);
		free(args);
		free(idDim);
		free(binary_insert);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	value_list[id_dim_pos] = DIM_VALUE;
	if (compressed_flag == 1) {
		value_list[measure_pos] = COMPRESSED_VALUE;
	} else {
		value_list[measure_pos] = UNCOMPRESSED_VALUE;
	}

	for (i = 0; i < arg_count; i++) {
		args[i] = (oph_query_arg *) calloc(1, sizeof(oph_query_arg));
		if (!args[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(idDim);
			free(binary_insert);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	args[id_dim_pos]->arg_length = sizeof(unsigned long long);
	args[id_dim_pos]->arg_type = OPH_QUERY_TYPE_LONG;
	args[id_dim_pos]->arg_is_null = 0;
	args[measure_pos]->arg_length = sizeof_var;
	args[measure_pos]->arg_type = OPH_QUERY_TYPE_BLOB;
	args[measure_pos]->arg_is_null = 0;

	unsigned long long row_size = 0;
	oph_iostore_frag_record *new_record = NULL;
	unsigned long long cumulative_size = 0;

	for (i = 0; i < tuplexfrag_number; i++) {

		args[id_dim_pos]->arg = (unsigned long long *) (&(idDim[i]));
		args[measure_pos]->arg = (char *) (binary_insert + i * sizeof_var);

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(idDim);
			free(binary_insert);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Add record to partial record set
		binary_frag->record_set[i] = new_record;
		//Update current record size
		cumulative_size += row_size;

		new_record = NULL;
		row_size = 0;
	}

	for (i = 0; i < arg_count; i++)
		if (args[i])
			free(args[i]);
	free(args);
	free(value_list);
	free(idDim);
	free(binary_insert);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

#endif
