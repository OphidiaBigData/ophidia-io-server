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
extern unsigned short cache_line_size;
extern unsigned long long cache_size;

#define MB_SIZE 1048576

//#define OPH_IO_SERVER_NETCDF_BLOCK

#ifdef OPH_IO_SERVER_NETCDF

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

int _oph_ioserver_nc_cache_to_buffer3(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *blocks, unsigned int *src_products, char *src_binary,
				      unsigned int *dst_products, char *dst_binary, size_t sizeof_var)
{
	//At lease 1 dim shuld be defined

	short int i = 0, j = 0;
	long long k = 0, index = 0;
	long long dst_addr = 0, src_addr = 0;
	long long total_iter = 1;

	unsigned int tmp_start[tot_dim_number];
	unsigned int tmp_end[tot_dim_number];

	for (i = 0; i < tot_dim_number; i++) {
		if (i != 0)
			counters[i] = 0;
		if (i != tot_dim_number - 1)
			total_iter *= (limits[i] - counters[i]);
		tmp_start[i] = 0;
		tmp_end[i] = blocks[i];

		//Extend products to consider size of var
		src_products[i] *= sizeof_var;
		dst_products[i] *= sizeof_var;
	}

	long long src_jump = src_products[tot_dim_number - 1];
	long long dst_jump = dst_products[tot_dim_number - 1];
	long long inner_blocks = 0;
	long long inner_counter = 0;

	total_iter *= ceil((double) (limits[tot_dim_number - 1] - counters[tot_dim_number - 1]) / blocks[tot_dim_number - 1]);

	for (index = 0; index < total_iter; index++) {
		dst_addr = src_addr = 0;

		//External dimensions
		for (i = 0; i < tot_dim_number - 1; i++) {
			src_addr += (long long) counters[i] * src_products[i];
			dst_addr += (long long) counters[i] * dst_products[i];
		}

		//Internal dimension
		inner_counter = tmp_start[tot_dim_number - 1];
		inner_blocks = tmp_end[tot_dim_number - 1];
		for (k = inner_counter; k < inner_blocks; k++) {
			memcpy(dst_binary + (dst_addr + k * dst_jump), src_binary + (src_addr + k * src_jump), sizeof_var);
		}

		//Increase block counters starting from most internal dimension (excluding the most internal)
		for (i = tot_dim_number - 2; i >= 0; i--) {
			counters[i]++;
			if (counters[i] < tmp_end[i]) {
				break;
			} else {
				counters[i] = tmp_start[i];

				if (i == 0) {
					for (j = tot_dim_number - 1; j >= 0; j--) {
						tmp_start[j] += blocks[j];
						tmp_end[j] += blocks[j];

						if (tmp_start[j] < limits[j]) {
							counters[j] = tmp_start[j];
							if (tmp_end[j] > limits[j])
								tmp_end[j] = limits[j];
							break;
						} else {
							counters[j] = tmp_start[j] = 0;
							tmp_end[j] = blocks[j];
						}
					}
				}
			}
		}
	}

	return 0;
}


int _oph_ioserver_nc_cache_to_buffer2(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *blocks, unsigned int *src_products, char *src_binary,
				      unsigned int *dst_products, char *dst_binary, size_t sizeof_var)
{
	short int i = 0, j = 0;
	long long dst_addr = 0, src_addr = 0;
	long long index = 0;
	long long total_iter = 1;

	unsigned int tmp_start[tot_dim_number];
	unsigned int tmp_end[tot_dim_number];

	for (i = 0; i < tot_dim_number; i++) {
		if (i != 0) {
			counters[i] = 0;
		}
		total_iter *= (limits[i] - counters[i]);
		tmp_start[i] = 0;
		tmp_end[i] = blocks[i];
		src_products[i] *= sizeof_var;
		dst_products[i] *= sizeof_var;
	}

	for (index = 0; index < total_iter; index++) {
		dst_addr = src_addr = 0;

		for (i = 0; i < tot_dim_number; i++) {
			src_addr += (long long) counters[i] * src_products[i];
			dst_addr += (long long) counters[i] * dst_products[i];
		}
		memcpy(dst_binary + dst_addr, src_binary + src_addr, sizeof_var);

		//Increase block counters starting from most rapidly varying dimension
		for (i = tot_dim_number - 1; i >= 0; i--) {
			counters[i]++;
			if (counters[i] < tmp_end[i]) {
				break;
			} else {
				counters[i] = tmp_start[i];

				if (i == 0) {
					for (j = tot_dim_number - 1; j >= 0; j--) {
						tmp_start[j] += blocks[j];
						tmp_end[j] += blocks[j];

						if (tmp_start[j] < limits[j]) {
							counters[j] = tmp_start[j];
							if (tmp_end[j] > limits[j])
								tmp_end[j] = limits[j];
							break;
						} else {
							counters[j] = tmp_start[j] = 0;
							tmp_end[j] = blocks[j];
						}
					}
				}
			}
		}
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
		products[i] *= sizeof_var;
	}

	for (index = 0; index < total_iter; index++) {
		addr = 0;
		for (i = 0; i < tot_dim_number; i++) {
			addr += (long long) counters[i] * products[i];
		}
		memcpy(binary_insert + index * sizeof_var, binary_cache + addr, sizeof_var);

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

int _oph_ioserver_nc_read_v2(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ncid, int ndims, int nimp, int nexp, short int *dims_type,
			     short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, nc_type vartype, int varid, int id_dim_pos, int measure_pos,
			     unsigned long long array_length, unsigned long long _array_length, int internal_size, char **binary_cache, char **binary_insert, char is_last, char dimension_ordered)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ncid || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !_tuplexfrag_number || !binary_frag
	    || !frag_size || !sizeof_var || !varid || !array_length || !_array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
#ifdef DEBUG
	pmesg(LOG_INFO, __FILE__, __LINE__, "Using IMPORT algorithm v2.0\n");
#endif

	int i = 0, j = 0;

	//TODO - Check that memory for the two arrays is actually available
	//Flag set to 1 if whole fragment fits in memory
	unsigned long long memory_size = memory_buffer * (unsigned long long) MB_SIZE;
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size / 2 ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
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
	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create binary array
	int res = 0;
	if (!*binary_cache && transpose) {
		//Create a binary array to store the whole fragment
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_cache, array_length * tuplexfrag_number);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_cache, array_length * tuplexfrag_number);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(*binary_cache);
			*binary_cache = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		if (memory_check()) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}
	//Create array for rows to be insert
	if (!*binary_insert) {
		res = 0;
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_insert, array_length * tuplexfrag_number);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_insert, array_length * tuplexfrag_number);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	unsigned long long idDim = frag_key_start;

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
				if (i != dim_unlim)
					sizemax[j] = dims_end[i] - dims_start[i] + 1;
				else
					sizemax[j] = dim_unlim_size;
				start_pointer[j] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	oph_ioserver_nc_compute_dimension_id(idDim, sizemax, nexp, start_pointer);

	for (i = 0; i < nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (j = 0; j < ndims; j++) {
			if (start_pointer[i] == &(start[j])) {
				*(start_pointer[i]) += dims_start[j];
				// Correction due to multiple files
				if (j == dim_unlim)
					*(start_pointer[i]) -= offset;
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
				count[j] = (int) (_tuplexfrag_number / curr_rows);
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

	if (total != _array_length * _tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
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

	if (dims_type[dim_unlim] && (offset * internal_size > tuplexfrag_number)) {

		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported configuration\n");
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Fill binary cache
	res = -1;
	if (transpose) {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res =
				    nc_get_vara_uchar(ncid, varid, start, count,
						      (unsigned char *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT64:
				res =
				    nc_get_vara_longlong(ncid, varid, start, count, (long long *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
		}
	} else {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res =
				    nc_get_vara_uchar(ncid, varid, start, count,
						      (unsigned char *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT64:
				res =
				    nc_get_vara_longlong(ncid, varid, start, count,
							 (long long *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
		}
	}

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (res != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
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

	if (!is_last) {
		free(count);
		return OPH_IO_SERVER_SUCCESS;
	}

	if (transpose) {
		//Prepare structures for buffer insert update
		size_t sizeof_type = (int) sizeof_var / array_length;

		unsigned int *dst_products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *counters = (unsigned int *) malloc(ndims * sizeof(unsigned int));
		unsigned int *src_products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *limits = (unsigned int *) malloc(ndims * sizeof(unsigned));

		int *file_indexes = (int *) malloc(ndims * sizeof(int));

		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			src_products[dims_index[i]] = 1;
			dst_products[dims_index[i]] = 1;
			limits[dims_index[i]] = i == dim_unlim ? dim_unlim_size : count[i];
			file_indexes[dims_index[i]] = k++;
		}

		//TODO Better block size management - Activate this only in case internal dimension is out of order and the largest
		//Make sure most internal dimension (for write-oriented array) is at least multiple of cache line
		unsigned int *blocks = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int line_size = floor(cache_line_size / sizeof_type);
		unsigned long long max_blocks = floor((cache_size / 2) / (sizeof_type));
		unsigned int block_size = floor(pow(max_blocks, (float) 1 / ndims));
		block_size = (block_size <= line_size ? block_size : block_size - block_size % line_size);
		for (i = ndims - 1; i >= 0; i--) {
			blocks[i] = (limits[i] < block_size ? limits[i] : block_size);
		}

		//Compute products
		for (k = 0; k < ndims; k++) {
			//Compute products for new buffer
			for (j = ndims - 1; j > k; j--) {
				dst_products[k] *= limits[j];
			}

			//Last dimension in file has product 1
			for (i = (file_indexes[k] + 1); i < ndims; i++) {
				flag = 0;
				//For each index following multiply
				for (j = 0; j < ndims; j++) {
					if (file_indexes[j] == i) {
						src_products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					if (*binary_cache) {
						free(*binary_cache);
						*binary_cache = NULL;
					}
					free(*binary_insert);
					*binary_insert = NULL;
					free(count);
					free(file_indexes);
					free(counters);
					free(src_products);
					free(limits);
					free(blocks);
					free(dst_products);
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			}
		}
		free(file_indexes);

#ifdef DEBUG
		gettimeofday(&start_transpose_time, NULL);
#endif
		_oph_ioserver_nc_cache_to_buffer3(ndims, counters, limits, blocks, src_products, *binary_cache, dst_products, *binary_insert, sizeof_type);
#ifdef DEBUG
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
		pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif
		free(counters);
		free(src_products);
		free(limits);
		free(blocks);
		free(dst_products);
	}

	free(count);

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(*binary_insert);
		*binary_insert = NULL;
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
		free(*binary_insert);
		*binary_insert = NULL;
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
			free(*binary_insert);
			*binary_insert = NULL;
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	args[id_dim_pos]->arg_length = sizeof(unsigned long long);
	args[id_dim_pos]->arg_type = OPH_QUERY_TYPE_LONG;
	args[id_dim_pos]->arg_is_null = 0;
	args[id_dim_pos]->arg = (unsigned long long *) (&idDim);
	args[measure_pos]->arg_length = sizeof_var;
	args[measure_pos]->arg_type = OPH_QUERY_TYPE_BLOB;
	args[measure_pos]->arg_is_null = 0;

	unsigned long long row_size = 0;
	oph_iostore_frag_record *new_record = NULL;
	unsigned long long cumulative_size = 0;

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++, idDim++) {

		args[measure_pos]->arg = *binary_insert + ii * sizeof_var;

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(*binary_insert);
			*binary_insert = NULL;
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Add record to partial record set
		binary_frag->record_set[ii] = new_record;
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

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read_v1(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ncid, int ndims, int nimp, int nexp, short int *dims_type,
			     short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, nc_type vartype, int varid, int id_dim_pos, int measure_pos,
			     unsigned long long array_length, unsigned long long _array_length, int internal_size, char **binary_cache, char **binary_insert, char is_last, char dimension_ordered)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ncid || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !_tuplexfrag_number || !binary_frag
	    || !frag_size || !sizeof_var || !varid || !array_length || !_array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
#ifdef DEBUG
	pmesg(LOG_INFO, __FILE__, __LINE__, "Using IMPORT algorithm v1.0\n");
#endif

	int i = 0, j = 0;

	//TODO - Check that memory for the two arrays is actually available
	//Flag set to 1 if whole fragment fits in memory
	unsigned long long memory_size = memory_buffer * (unsigned long long) MB_SIZE;
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size / 2 ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
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
	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create binary array
	int res = 0;
	if (!*binary_cache && transpose) {
		//Create a binary array to store the whole fragment
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_cache, array_length * tuplexfrag_number);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_cache, array_length * tuplexfrag_number);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_cache, array_length * tuplexfrag_number);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(*binary_cache);
			*binary_cache = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		if (memory_check()) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}
	//Create array for rows to be insert
	if (!*binary_insert) {
		res = 0;
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_insert, array_length * tuplexfrag_number);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_insert, array_length * tuplexfrag_number);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_insert, array_length * tuplexfrag_number);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	unsigned long long idDim = frag_key_start;

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
				if (i != dim_unlim)
					sizemax[j] = dims_end[i] - dims_start[i] + 1;
				else
					sizemax[j] = dim_unlim_size;
				start_pointer[j] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	oph_ioserver_nc_compute_dimension_id(idDim, sizemax, nexp, start_pointer);

	for (i = 0; i < nexp; i++) {
		*(start_pointer[i]) -= 1;
		for (j = 0; j < ndims; j++) {
			if (start_pointer[i] == &(start[j])) {
				*(start_pointer[i]) += dims_start[j];
				// Correction due to multiple files
				if (j == dim_unlim)
					*(start_pointer[i]) -= offset;
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
				count[j] = (int) (_tuplexfrag_number / curr_rows);
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

	if (total != _array_length * _tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
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

	if (dims_type[dim_unlim] && (offset * internal_size > tuplexfrag_number)) {

		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unsupported configuration\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unsupported configuration\n");
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Fill binary cache
	res = -1;
	if (transpose) {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res =
				    nc_get_vara_uchar(ncid, varid, start, count,
						      (unsigned char *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT64:
				res =
				    nc_get_vara_longlong(ncid, varid, start, count, (long long *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
		}
	} else {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res =
				    nc_get_vara_uchar(ncid, varid, start, count,
						      (unsigned char *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_SHORT:
				res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT:
				res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_INT64:
				res =
				    nc_get_vara_longlong(ncid, varid, start, count,
							 (long long *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_FLOAT:
				res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			case NC_DOUBLE:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
				break;
			default:
				res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert) + offset * internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number));
		}
	}

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (res != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
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

	if (dims_type[dim_unlim] && !is_last) {
		free(count);
		return OPH_IO_SERVER_SUCCESS;
	}

	if (transpose) {
		//Prepare structures for buffer insert update
		size_t sizeof_type = (int) sizeof_var / array_length;

		unsigned int *counters = (unsigned int *) malloc(ndims * sizeof(unsigned int));
		unsigned int *src_products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *limits = (unsigned int *) malloc(ndims * sizeof(unsigned));

		int *file_indexes = (int *) malloc(ndims * sizeof(int));

		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			src_products[dims_index[i]] = 1;
			limits[dims_index[i]] = i == dim_unlim ? dim_unlim_size : count[i];
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
						src_products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					if (*binary_cache) {
						free(*binary_cache);
						*binary_cache = NULL;
					}
					free(*binary_insert);
					*binary_insert = NULL;
					free(count);
					free(file_indexes);
					free(counters);
					free(src_products);
					free(limits);
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			}
		}
		free(file_indexes);

#ifdef DEBUG
		gettimeofday(&start_transpose_time, NULL);
#endif
		oph_ioserver_nc_cache_to_buffer(ndims, counters, limits, src_products, *binary_cache, *binary_insert, sizeof_type);
#ifdef DEBUG
		gettimeofday(&end_transpose_time, NULL);
		timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
		timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
		pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif
		free(counters);
		free(src_products);
		free(limits);
	}

	free(count);

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(*binary_insert);
		*binary_insert = NULL;
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
		free(*binary_insert);
		*binary_insert = NULL;
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
			free(*binary_insert);
			*binary_insert = NULL;
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	args[id_dim_pos]->arg_length = sizeof(unsigned long long);
	args[id_dim_pos]->arg_type = OPH_QUERY_TYPE_LONG;
	args[id_dim_pos]->arg_is_null = 0;
	args[id_dim_pos]->arg = (unsigned long long *) (&idDim);
	args[measure_pos]->arg_length = sizeof_var;
	args[measure_pos]->arg_type = OPH_QUERY_TYPE_BLOB;
	args[measure_pos]->arg_is_null = 0;

	unsigned long long row_size = 0;
	oph_iostore_frag_record *new_record = NULL;
	unsigned long long cumulative_size = 0;

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++, idDim++) {

		args[measure_pos]->arg = *binary_insert + ii * sizeof_var;

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(*binary_insert);
			*binary_insert = NULL;
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Add record to partial record set
		binary_frag->record_set[ii] = new_record;
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

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

// This version is not optimized in case the unlimited dimension is implicit!!!!! Use another version instead
int _oph_ioserver_nc_read_v0(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ncid, int ndims, int nimp, int nexp, short int *dims_type,
			     short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, nc_type vartype, int varid, int id_dim_pos, int measure_pos,
			     unsigned long long array_length, unsigned long long _array_length, int internal_size, char **binary_cache, char **binary_insert, char is_last)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ncid || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !_tuplexfrag_number || !binary_frag
	    || !frag_size || !sizeof_var || !varid || !array_length || !_array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
#ifdef DEBUG
	pmesg(LOG_INFO, __FILE__, __LINE__, "Using IMPORT algorithm v0.0\n");
#endif

	int i = 0, j = 0;

	//Flag set to 1 if implicit dimension are in the order specified in the file
	char dimension_ordered = 1;
	unsigned int curr_lev = 0;
	for (i = 0; i < ndims; i++) {
		if (!dims_type[i]) {
			if ((dims_index[i] - nexp) < curr_lev) {
				dimension_ordered = 0;
				break;
			}
			curr_lev = (dims_index[i] - nexp);
		}
	}


	//If flag is set fragment reordering is required
	char transpose = 1;
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
	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create binary array
	int res;
	if (!*binary_cache && transpose) {
		//Create a binary array to store the data from file
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_cache, array_length);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_cache, array_length);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_cache, array_length);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_cache, array_length);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_cache, array_length);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_cache, array_length);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_cache, array_length);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(*binary_cache);
			*binary_cache = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	if (!*binary_insert) {
		res = 0;
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
				res = oph_iob_bin_array_create_b(binary_insert, array_length);
				break;
			case NC_SHORT:
				res = oph_iob_bin_array_create_s(binary_insert, array_length);
				break;
			case NC_INT:
				res = oph_iob_bin_array_create_i(binary_insert, array_length);
				break;
			case NC_INT64:
				res = oph_iob_bin_array_create_l(binary_insert, array_length);
				break;
			case NC_FLOAT:
				res = oph_iob_bin_array_create_f(binary_insert, array_length);
				break;
			case NC_DOUBLE:
				res = oph_iob_bin_array_create_d(binary_insert, array_length);
				break;
			default:
				res = oph_iob_bin_array_create_d(binary_insert, array_length);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	unsigned long long idDim = frag_key_start;

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
				if (i != dim_unlim)
					sizemax[j] = dims_end[i] - dims_start[i] + 1;
				else
					sizemax[j] = dim_unlim_size;
				start_pointer[j] = &(start[i]);
				flag = 1;
				break;
			}
		}
		if (!flag) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Invalid explicit dimensions in task string \n");
			if (*binary_cache) {
				free(*binary_cache);
				*binary_cache = NULL;
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
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
			count[j] = 1;
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

	if (total != _array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", _array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		if (*binary_cache) {
			free(*binary_cache);
			*binary_cache = NULL;
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Prepare structures for buffer insert update
	size_t sizeof_type = (int) sizeof_var / array_length;

	unsigned int *counters = NULL;
	unsigned int *src_products = NULL;
	unsigned int *limits = NULL;

	if (transpose) {

		counters = (unsigned int *) malloc(nimp * sizeof(unsigned int));
		src_products = (unsigned int *) malloc(nimp * sizeof(unsigned));
		limits = (unsigned int *) malloc(nimp * sizeof(unsigned));

		int *file_indexes = (int *) malloc(nimp * sizeof(int));
		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			//Implicit
			if (!dims_type[i]) {
				counters[dims_index[i] - nexp] = 0;
				src_products[dims_index[i] - nexp] = 1;
				limits[dims_index[i] - nexp] = count[i];
				file_indexes[dims_index[i] - nexp] = k++;
			}
		}

		//Compute products
		for (k = 0; k < nimp; k++) {

			//Last dimension in file has product 1
			for (i = (file_indexes[k] + 1); i < nimp; i++) {
				flag = 0;
				//For each index following multiply
				for (j = 0; j < nimp; j++) {
					if (file_indexes[j] == i) {
						src_products[k] *= limits[j];
						flag = 1;
						break;
					}
				}
				if (!flag) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Invalid dimensions in task string \n");
					if (*binary_cache) {
						free(*binary_cache);
						*binary_cache = NULL;
					}
					free(*binary_insert);
					*binary_insert = NULL;
					pthread_mutex_lock(&nc_lock);
					nc_close(ncid);
					pthread_mutex_unlock(&nc_lock);
					free(start);
					free(count);
					free(start_pointer);
					free(sizemax);
					free(file_indexes);
					free(counters);
					free(src_products);
					free(limits);
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			}
		}
		free(file_indexes);
	}

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (transpose) {
			free(*binary_cache);
			*binary_cache = NULL;
			free(counters);
			free(src_products);
			free(limits);
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
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
		if (transpose) {
			free(*binary_cache);
			*binary_cache = NULL;
			free(counters);
			free(src_products);
			free(limits);
		}
		free(*binary_insert);
		*binary_insert = NULL;
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
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
			if (transpose) {
				free(*binary_cache);
				*binary_cache = NULL;
				free(counters);
				free(src_products);
				free(limits);
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	args[id_dim_pos]->arg_length = sizeof(unsigned long long);
	args[id_dim_pos]->arg_type = OPH_QUERY_TYPE_LONG;
	args[id_dim_pos]->arg_is_null = 0;
	args[id_dim_pos]->arg = (unsigned long long *) (&idDim);
	args[measure_pos]->arg_length = sizeof_var;
	args[measure_pos]->arg_type = OPH_QUERY_TYPE_BLOB;
	args[measure_pos]->arg_is_null = 0;
	args[measure_pos]->arg = *binary_insert;

	unsigned long long row_size = 0;
	oph_iostore_frag_record *new_record = NULL;
	unsigned long long cumulative_size = 0;

#ifdef DEBUG
	struct timeval start_read_time, end_read_time, intermediate_read_time, total_read_time;
	struct timeval start_transpose_time, end_transpose_time, intermediate_transpose_time, total_transpose_time;
	total_transpose_time.tv_usec = 0;
	total_transpose_time.tv_sec = 0;

	gettimeofday(&start_read_time, NULL);
#endif

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++, idDim++) {

		oph_ioserver_nc_compute_dimension_id(idDim, sizemax, nexp, start_pointer);

		for (i = 0; i < nexp; i++) {
			*(start_pointer[i]) -= 1;
			for (j = 0; j < ndims; j++) {
				if (start_pointer[i] == &(start[j])) {
					*(start_pointer[i]) += dims_start[j];
					// Correction due to multiple files
					if (j == dim_unlim)
						*(start_pointer[i]) -= offset;
				}
			}
		}

#ifdef DEBUG
		//gettimeofday(&start_read_time, NULL);
#endif
		//Fill binary cache
		res = -1;
		if (transpose) {
			switch (vartype) {
				case NC_BYTE:
				case NC_CHAR:
					res = nc_get_vara_uchar(ncid, varid, start, count, (unsigned char *) (*binary_cache));
					break;
				case NC_SHORT:
					res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_cache));
					break;
				case NC_INT:
					res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_cache));
					break;
				case NC_INT64:
					res = nc_get_vara_longlong(ncid, varid, start, count, (long long *) (*binary_cache));
					break;
				case NC_FLOAT:
					res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_cache));
					break;
				case NC_DOUBLE:
					res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache));
					break;
				default:
					res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_cache));
			}
		} else {
			switch (vartype) {
				case NC_BYTE:
				case NC_CHAR:
					res = nc_get_vara_uchar(ncid, varid, start, count, (unsigned char *) (*binary_insert));
					break;
				case NC_SHORT:
					res = nc_get_vara_short(ncid, varid, start, count, (short *) (*binary_insert));
					break;
				case NC_INT:
					res = nc_get_vara_int(ncid, varid, start, count, (int *) (*binary_insert));
					break;
				case NC_INT64:
					res = nc_get_vara_longlong(ncid, varid, start, count, (long long *) (*binary_insert));
					break;
				case NC_FLOAT:
					res = nc_get_vara_float(ncid, varid, start, count, (float *) (*binary_insert));
					break;
				case NC_DOUBLE:
					res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert));
					break;
				default:
					res = nc_get_vara_double(ncid, varid, start, count, (double *) (*binary_insert));
			}
		}
		if (res != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(*binary_cache);
				*binary_cache = NULL;
				free(counters);
				free(src_products);
				free(limits);
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
#ifdef DEBUG
		//gettimeofday(&end_read_time, NULL);
		//timeval_subtract(&intermediate_read_time, &end_transpose_time, &start_transpose_time);
		//timeval_add(&total_read_time, &total_read_time, &intermediate_read_time);
#endif

		if (transpose) {
#ifdef DEBUG
			//gettimeofday(&start_transpose_time, NULL);
#endif
			oph_ioserver_nc_cache_to_buffer(nimp, counters, limits, src_products, *binary_cache, *binary_insert, sizeof_type);
#ifdef DEBUG
			//gettimeofday(&end_transpose_time, NULL);
			//timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
			//timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif
		}

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(*binary_cache);
				*binary_cache = NULL;
				free(counters);
				free(src_products);
				free(limits);
			}
			free(*binary_insert);
			*binary_insert = NULL;
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Add record to partial record set
		binary_frag->record_set[ii] = new_record;
		//Update current record size
		cumulative_size += row_size;

		new_record = NULL;
		row_size = 0;
	}

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	//timeval_add(&total_read_time, &total_read_time, &intermediate_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
	if (transpose)
		printf("Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif

	pthread_mutex_lock(&nc_lock);
	nc_close(ncid);
	pthread_mutex_unlock(&nc_lock);

	free(count);
	free(start);
	free(start_pointer);
	free(sizemax);

	if (transpose) {
		free(counters);
		free(src_products);
		free(limits);
	}

	for (i = 0; i < arg_count; i++)
		if (args[i])
			free(args[i]);
	free(args);
	free(value_list);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read(char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int dim_num, short int *dims_type,
			  short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size)
{
	if (!src_path || !measure_name || !tuplexfrag_number || !frag_key_start || !dim_num || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Common part of code
	if (strstr(src_path, "..")) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "The use of '..' is forbidden\n");
		return OPH_IO_SERVER_PARSE_ERROR;
	}
	if (*src_path == OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Wrong use of the separator\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Wrong use of the separator\n");
		return OPH_IO_SERVER_PARSE_ERROR;
	}
	// Parse for multiple files
	int k = 1, src_paths_num = *src_path ? 1 : 0, return_value = OPH_IO_SERVER_SUCCESS, offset = 0;	// Used to understand the real index of unlimited dimension
	int _dims_start[dim_num], _dims_end[dim_num];
	int dim_unlim_size = dims_end[dim_unlim] - dims_start[dim_unlim] + 1;
	size_t lenp = 0;

	char *pch = src_path, *save_pointer = NULL;
	while (pch && *pch) {
		if (*pch == OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR)
			src_paths_num++;
		pch++;
	}

	unsigned long long _tuplexfrag_number;
	long long _frag_key_start, _f1, _f2;
	char *binary_cache = NULL, *binary_insert = NULL;

	char src_paths[1 + strlen(src_path)];
	strcpy(src_paths, src_path);
	src_path = NULL;
	while ((pch = strtok_r(src_path ? NULL : src_paths, OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR2, &save_pointer))) {

		src_path = pch;

		memcpy(_dims_start, dims_start, dim_num * sizeof(int));
		memcpy(_dims_end, dims_end, dim_num * sizeof(int));

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
			pthread_mutex_unlock(&nc_lock);
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

		_tuplexfrag_number = tuplexfrag_number;
		_frag_key_start = frag_key_start;

		int internal_size = 1;
		if (src_paths_num > 1) {
			int dim_id[ndims];
			if ((retval = nc_inq_vardimid(ncid, varid, dim_id))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension ids\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension ids\n");
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			if ((retval = nc_inq_dimlen(ncid, dim_id[dim_unlim], &lenp))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension real size\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension real size\n");
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			// Reduce subset to current file range
			if (_dims_start[dim_unlim] < offset)
				_dims_start[dim_unlim] = offset;
			if (_dims_end[dim_unlim] >= lenp + offset)
				_dims_end[dim_unlim] = lenp + offset - 1;
			short int dims_value[ndims], first = 1;
			for (i = 0; i < ndims; ++i)
				dims_value[dims_index[i]] = i;
			for (i = dim_unlim + 1; i < ndims; ++i)
				if (dims_type[i] == dims_type[dim_unlim]) {
					if (first && dims_type[i])	// Don't consider the most external if explicit
						first = 0;
					else
						internal_size *= dims_end[i] - dims_start[i] + 1;
				}
			if (dims_type[dim_unlim]) {
				int internal_size2 = 1;
				for (i = dims_index[dim_unlim] + 1; i < ndims; ++i)
					if (dims_type[dims_value[i]])
						internal_size2 *= dims_end[dims_value[i]] - dims_start[dims_value[i]] + 1;
				_f1 = (_frag_key_start - 1) / internal_size2 % dim_unlim_size;
				_f2 = _f1 + _tuplexfrag_number / internal_size2;	// It is ok
				if ((_dims_start[dim_unlim] >= _f2) || (_dims_end[dim_unlim] < _f1))
					_tuplexfrag_number = 0;
				while (_tuplexfrag_number && (_dims_end[dim_unlim] < _f2 - 1)) {	// It is ok
					_tuplexfrag_number--;
					_f2 = _f1 + _tuplexfrag_number / internal_size2;
				}
				while (_tuplexfrag_number && (_dims_start[dim_unlim] > _f1)) {
					_frag_key_start++;
					_tuplexfrag_number--;
					_f1 = (_frag_key_start - 1) / internal_size2 % dim_unlim_size;
				}
				if (!_tuplexfrag_number) {
					pthread_mutex_lock(&nc_lock);
					nc_close(ncid);
					pthread_mutex_unlock(&nc_lock);
					offset += lenp;
					continue;
				}
			}
			// Rescaling
			_dims_start[dim_unlim] -= offset;
			_dims_end[dim_unlim] -= offset;
		}
		//Compute array_length from implicit dims
		unsigned long long array_length = 1, _array_length;
		short int nimp = 0, nexp = 0;
		for (i = 0; i < ndims; i++) {
			if (!dims_type[i]) {
				array_length *= dims_end[i] - dims_start[i] + 1;
				nimp++;
			} else {
				nexp++;
			}
		}
		if (dims_type[dim_unlim])
			_array_length = array_length;
		else
			_array_length = array_length * (_dims_end[dim_unlim] - _dims_start[dim_unlim] + 1) / dim_unlim_size;

		unsigned long long sizeof_var = 0;
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

		//Flag set to 1 if dimension are in the order specified in the file
		char dimension_ordered = 1;
		for (i = 0; i < ndims; i++) {
			if (dims_index[i] != i) {
				dimension_ordered = 0;
				break;
			}
		}

		if (dimension_ordered)
			return_value =
			    _oph_ioserver_nc_read_v0(measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ncid, ndims, nimp, nexp, dims_type, dims_index, _dims_start, _dims_end,
						     dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, vartype, varid, id_dim_pos, measure_pos, array_length,
						     _array_length, internal_size, &binary_cache, &binary_insert, k == src_paths_num);
		else
#ifdef OPH_IO_SERVER_NETCDF_BLOCK
			return_value =
			    _oph_ioserver_nc_read_v1(measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ncid, ndims, nimp, nexp, dims_type, dims_index, _dims_start, _dims_end,
						     dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, vartype, varid, id_dim_pos, measure_pos, array_length,
						     _array_length, internal_size, &binary_cache, &binary_insert, k == src_paths_num, dimension_ordered);
#else
			return_value =
			    _oph_ioserver_nc_read_v2(measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ncid, ndims, nimp, nexp, dims_type, dims_index, _dims_start, _dims_end,
						     dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, vartype, varid, id_dim_pos, measure_pos, array_length,
						     _array_length, internal_size, &binary_cache, &binary_insert, k == src_paths_num, dimension_ordered);
#endif

		if (return_value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading the file %s\n", src_path);
			logging(LOG_ERROR, __FILE__, __LINE__, "Error while loading the file %s\n", src_path);
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			break;
		}
		// Update offset for the next loop
		offset += lenp;
		k++;
	}

	if (binary_cache)
		free(binary_cache);
	if (binary_insert)
		free(binary_insert);

	return return_value;
}
#endif

int _oph_ioserver_rand_data(long long tuplexfrag_number, long long frag_key_start, char compressed_flag, long long array_length, char *measure_type, char *algorithm,
			    oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size)
{
	if (!tuplexfrag_number || !frag_key_start || !array_length || !measure_type || !algorithm || !binary_frag || !frag_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
	//Check the order and field list values
	int measure_pos = -1, id_dim_pos = -1;
	int i;
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

	char type_flag = oph_util_get_measure_type(measure_type);
	if (!type_flag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_INVALID_QUERY_VALUE, "measure type", measure_type);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_INVALID_QUERY_VALUE, "measure type", measure_type);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	char rand_alg = 0;
	if (strcmp(algorithm, OPH_QUERY_ENGINE_LANG_VAL_RAND_ALGO_TEMP) == 0) {
		rand_alg = 1;
	} else if (strcmp(algorithm, OPH_QUERY_ENGINE_LANG_VAL_RAND_ALGO_DEFAULT) == 0) {
		rand_alg = 0;
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_INVALID_QUERY_VALUE, "algorithm type", algorithm);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_INVALID_QUERY_VALUE, "algorithm type", algorithm);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	unsigned long long sizeof_var = 0;

	if (type_flag == OPH_MEASURE_BYTE_FLAG)
		sizeof_var = array_length * sizeof(char);
	else if (type_flag == OPH_MEASURE_SHORT_FLAG)
		sizeof_var = array_length * sizeof(short);
	else if (type_flag == OPH_MEASURE_INT_FLAG)
		sizeof_var = array_length * sizeof(int);
	else if (type_flag == OPH_MEASURE_LONG_FLAG)
		sizeof_var = array_length * sizeof(long long);
	else if (type_flag == OPH_MEASURE_FLOAT_FLAG)
		sizeof_var = array_length * sizeof(float);
	else if (type_flag == OPH_MEASURE_DOUBLE_FLAG)
		sizeof_var = array_length * sizeof(double);
	else if (type_flag == OPH_MEASURE_BIT_FLAG) {
		sizeof_var = array_length * sizeof(char) / 8;
		if (array_length % 8)
			sizeof_var++;
		array_length = sizeof_var;	// a bit array correspond to a char array with 1/8 elements
	}
	//TODO - Check that memory for the array is actually available
	//Flag set to 1 if whole fragment fits in memory
	unsigned long long memory_size = memory_buffer * (unsigned long long) MB_SIZE;
	short int whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_NOT_AVAIL_ERROR, tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_NOT_AVAIL_ERROR, tuplexfrag_number * sizeof_var);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	//Create binary array
	char *binary = 0;
	int res = 0;

	if (type_flag == OPH_MEASURE_BYTE_FLAG)
		res = oph_iob_bin_array_create_b(&binary, array_length);
	else if (type_flag == OPH_MEASURE_SHORT_FLAG)
		res = oph_iob_bin_array_create_s(&binary, array_length);
	else if (type_flag == OPH_MEASURE_INT_FLAG)
		res = oph_iob_bin_array_create_i(&binary, array_length);
	else if (type_flag == OPH_MEASURE_LONG_FLAG)
		res = oph_iob_bin_array_create_l(&binary, array_length);
	else if (type_flag == OPH_MEASURE_FLOAT_FLAG)
		res = oph_iob_bin_array_create_f(&binary, array_length);
	else if (type_flag == OPH_MEASURE_DOUBLE_FLAG)
		res = oph_iob_bin_array_create_d(&binary, array_length);
	else if (type_flag == OPH_MEASURE_BIT_FLAG)
		res = oph_iob_bin_array_create_c(&binary, array_length);
	else
		res = oph_iob_bin_array_create_d(&binary, array_length);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(binary);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long idDim = frag_key_start;

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		free(binary);
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
		free(binary);
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
			free(binary);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	args[id_dim_pos]->arg_length = sizeof(unsigned long long);
	args[id_dim_pos]->arg_type = OPH_QUERY_TYPE_LONG;
	args[id_dim_pos]->arg_is_null = 0;
	args[id_dim_pos]->arg = (unsigned long long *) (&idDim);
	args[measure_pos]->arg_length = sizeof_var;
	args[measure_pos]->arg_type = OPH_QUERY_TYPE_BLOB;
	args[measure_pos]->arg_is_null = 0;
	args[measure_pos]->arg = (char *) (binary);

	unsigned long long row_size = 0;
	oph_iostore_frag_record *new_record = NULL;
	unsigned long long cumulative_size = 0;

	for (i = 0; i < tuplexfrag_number; i++, idDim++) {

		if (oph_util_build_rand_row(binary, array_length, type_flag, rand_alg)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_BINARY_ARRAY_LOAD);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_BINARY_ARRAY_LOAD);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(binary);
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			free(binary);
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
	free(binary);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}
