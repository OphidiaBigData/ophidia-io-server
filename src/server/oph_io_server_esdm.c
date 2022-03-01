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
#define OPH_ODB_DIM_DIMENSION_TYPE_SIZE 64

extern int msglevel;
//extern pthread_mutex_t metadb_mutex;
extern pthread_rwlock_t rwlock;
extern pthread_mutex_t nc_lock;
extern HASHTBL *plugin_table;
extern unsigned long long memory_buffer;
extern unsigned short cache_line_size;
extern unsigned long long cache_size;

#define MB_SIZE 1048576

//#define OPH_IO_SERVER_ESDM_BLOCK

#ifdef OPH_IO_SERVER_ESDM

#define OPH_ESDM_PREFIX "esdm://"

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




// ESDM kernels *********************************************
// This part needs to be linked from an external package ****

#define OPH_ESDM_SEPARATOR ","

#define OPH_ESDM_FUNCTION_NOP "nop"
#define OPH_ESDM_FUNCTION_STREAM "stream"

#define OPH_ESDM_FUNCTION_MAX "max"
#define OPH_ESDM_FUNCTION_MIN "min"
#define OPH_ESDM_FUNCTION_AVG "avg"
#define OPH_ESDM_FUNCTION_SUM "sum"
#define OPH_ESDM_FUNCTION_STD "std"
#define OPH_ESDM_FUNCTION_VAR "var"

#define OPH_ESDM_FUNCTION_SUM_SCALAR "sum_scalar"
#define OPH_ESDM_FUNCTION_MUL_SCALAR "mul_scalar"

#define OPH_ESDM_FUNCTION_ABS "abs"
#define OPH_ESDM_FUNCTION_SQR "sqr"
#define OPH_ESDM_FUNCTION_SQRT "sqrt"
#define OPH_ESDM_FUNCTION_CEIL "ceil"
#define OPH_ESDM_FUNCTION_FLOOR "floor"
#define OPH_ESDM_FUNCTION_ROUND "round"
#define OPH_ESDM_FUNCTION_INT "int"
#define OPH_ESDM_FUNCTION_NINT "nint"

#define OPH_ESDM_FUNCTION_POW "pow"
#define OPH_ESDM_FUNCTION_EXP "exp"
#define OPH_ESDM_FUNCTION_LOG "log"
#define OPH_ESDM_FUNCTION_LOG10 "log10"

#define OPH_ESDM_FUNCTION_SIN "sin"
#define OPH_ESDM_FUNCTION_COS "cos"
#define OPH_ESDM_FUNCTION_TAN "tan"
#define OPH_ESDM_FUNCTION_ASIN "asin"
#define OPH_ESDM_FUNCTION_ACOS "acos"
#define OPH_ESDM_FUNCTION_ATAN "atan"
#define OPH_ESDM_FUNCTION_SINH "sinh"
#define OPH_ESDM_FUNCTION_COSH "cosh"
#define OPH_ESDM_FUNCTION_TANH "tanh"

#define OPH_ESDM_FUNCTION_RECI "reci"
#define OPH_ESDM_FUNCTION_NOT "not"

typedef struct _oph_esdm_stream_data_t {
	char *operation;
	char *args;
	void *buff;
	char valid;
	double value;
	double value2;
	uint64_t number;
	void *fill_value;
} oph_esdm_stream_data_t;

typedef struct _oph_esdm_stream_data_out_t {
	double value;
	double value2;
	uint64_t number;
} oph_esdm_stream_data_out_t;

void *oph_esdm_stream_func(esdm_dataspace_t * space, void *buff, void *user_ptr, void *esdm_fill_value)
{
	UNUSED(esdm_fill_value);

	if (!space || !buff || !user_ptr)
		return NULL;

	oph_esdm_stream_data_t *stream_data = (oph_esdm_stream_data_t *) user_ptr;
	if (!stream_data->operation)
		return NULL;

	char *args = stream_data->args ? strdup(stream_data->args) : NULL;	// Copy for strtok

	int64_t i, idx, ndims = esdm_dataspace_get_dims(space);
	int64_t const *s = esdm_dataspace_get_size(space);
	//int64_t const *si = esdm_dataspace_get_offset(space);
	int64_t ci[ndims], ei[ndims];
	for (i = 0; i < ndims; ++i) {
		ci[i] = 0;	// + si[i]
		ei[i] = s[i];	// + si[i]
	}

	uint64_t k = 1, n = esdm_dataspace_element_count(space);
	esdm_type_t type = esdm_dataspace_get_type(space);
	void *fill_value = stream_data->fill_value;
	oph_esdm_stream_data_out_t *tmp = NULL;

	if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NOP) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STREAM)) {

		// TODO: copy only the data related to the dataspace
		memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v < a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if ((!fill_value || (a[idx] != fv)) && (!tmp->number || (v > a[idx]))) {
					v = a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->value = 0;
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_VAR)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->value = 0;
		tmp->value2 = 0;
		tmp->number = 0;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				if (!fill_value || (a[idx] != fv)) {
					tmp->value += a[idx];
					tmp->value2 += a[idx] * a[idx];
					tmp->number++;
				}
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM_SCALAR)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] + scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MUL_SCALAR)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * scalar : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ABS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? abs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? fabs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? fabs(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SQRT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sqrt(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SQR)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? a[idx] * a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_CEIL)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? ceil(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_FLOOR) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_INT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ROUND) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NINT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? floor(a[idx] + 0.5) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_POW)) {

		if (!args) {
			// TODO: copy only the data related to the dataspace
			memcpy(stream_data->buff, buff, esdm_dataspace_total_bytes(space));
			return NULL;
		}

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		char *save_pointer = NULL, *arg = strtok_r(args, OPH_ESDM_SEPARATOR, &save_pointer);

		if (type == SMD_DTYPE_INT8) {

			char scalar = strtol(arg, NULL, 10);
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short scalar = strtol(arg, NULL, 10);
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int scalar = strtol(arg, NULL, 10);
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long scalar = strtoll(arg, NULL, 10);
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float scalar = strtof(arg, NULL);
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double scalar = strtod(arg, NULL);
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? pow(a[idx], scalar) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_EXP)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? exp(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_LOG)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_LOG10)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? log10(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_COS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_TAN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ASIN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? asin(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ACOS)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? acos(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_ATAN)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? atan(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SINH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? sinh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_COSH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? cosh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_TANH)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? tanh(a[idx]) : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_RECI)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? 1.0 / a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_NOT)) {

		tmp = (oph_esdm_stream_data_out_t *) malloc(sizeof(oph_esdm_stream_data_out_t));
		tmp->number = 1;

		if (type == SMD_DTYPE_INT8) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(char);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT16) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(short);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT32) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(int);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(long long);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(float);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else if (type == SMD_DTYPE_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(double);
			for (k = 0; k < n; k++) {
				idx = 0;
				for (i = 0; i < ndims; i++)
					idx = idx * s[i] + ci[i];
				v = !fill_value || (a[idx] != fv) ? !a[idx] : fv;
				memcpy(stream_data->buff + idx * step, &v, step);
				for (i = ndims - 1; i >= 0; i--) {
					ci[i]++;
					if (ci[i] < ei[i])
						break;
					ci[i] = 0;	// si[i];
				}
			}
			tmp->value = v;

		} else {
			free(tmp);
			if (args)
				free(args);
			return NULL;
		}

	}

	if (args)
		free(args);

	return tmp;
}

void oph_esdm_reduce_func(esdm_dataspace_t * space, void *user_ptr, void *stream_func_out)
{
	oph_esdm_stream_data_out_t *tmp = (oph_esdm_stream_data_out_t *) stream_func_out;

	do {

		if (!space || !user_ptr || (tmp && !tmp->number))
			break;

		esdm_type_t type = esdm_dataspace_get_type(space);
		oph_esdm_stream_data_t *stream_data = (oph_esdm_stream_data_t *) user_ptr;
		if (!stream_data->operation)
			break;

		if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MAX)) {

			if (!tmp)
				break;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) tmp->value;
				if (stream_data->valid) {
					char pre = *(char *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(char));
				} else {
					memcpy(stream_data->buff, &v, sizeof(char));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) tmp->value;
				if (stream_data->valid) {
					short pre = *(short *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(short));
				} else {
					memcpy(stream_data->buff, &v, sizeof(short));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) tmp->value;
				if (stream_data->valid) {
					int pre = *(int *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(int));
				} else {
					memcpy(stream_data->buff, &v, sizeof(int));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) tmp->value;
				if (stream_data->valid) {
					long long pre = *(long long *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(long long));
				} else {
					memcpy(stream_data->buff, &v, sizeof(long long));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) tmp->value;
				if (stream_data->valid) {
					float pre = *(float *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(float));
				} else {
					memcpy(stream_data->buff, &v, sizeof(float));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) tmp->value;
				if (stream_data->valid) {
					double pre = *(double *) stream_data->buff;
					if (pre < v)
						memcpy(stream_data->buff, &v, sizeof(double));
				} else {
					memcpy(stream_data->buff, &v, sizeof(double));
					stream_data->valid = 1;
				}

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_MIN)) {

			if (!tmp)
				break;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) tmp->value;
				if (stream_data->valid) {
					char pre = *(char *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(char));
				} else {
					memcpy(stream_data->buff, &v, sizeof(char));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) tmp->value;
				if (stream_data->valid) {
					short pre = *(short *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(short));
				} else {
					memcpy(stream_data->buff, &v, sizeof(short));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) tmp->value;
				if (stream_data->valid) {
					int pre = *(int *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(int));
				} else {
					memcpy(stream_data->buff, &v, sizeof(int));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) tmp->value;
				if (stream_data->valid) {
					long long pre = *(long long *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(long long));
				} else {
					memcpy(stream_data->buff, &v, sizeof(long long));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) tmp->value;
				if (stream_data->valid) {
					float pre = *(float *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(float));
				} else {
					memcpy(stream_data->buff, &v, sizeof(float));
					stream_data->valid = 1;
				}

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) tmp->value;
				if (stream_data->valid) {
					double pre = *(double *) stream_data->buff;
					if (pre > v)
						memcpy(stream_data->buff, &v, sizeof(double));
				} else {
					memcpy(stream_data->buff, &v, sizeof(double));
					stream_data->valid = 1;
				}

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_SUM)) {

			if (!tmp)
				break;

			if (!stream_data->valid) {
				stream_data->valid = 1;
				stream_data->value = 0;
				stream_data->number = 0;
			}
			stream_data->value += tmp->value;
			if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_AVG))
				stream_data->number += tmp->number;
			else
				stream_data->number = 1;

			if (type == SMD_DTYPE_INT8) {

				char v = (char) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(char));

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(short));

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(int));

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(long long));

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(float));

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) (stream_data->value / stream_data->number);
				memcpy(stream_data->buff, &v, sizeof(double));

			}

		} else if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD) || !strcmp(stream_data->operation, OPH_ESDM_FUNCTION_VAR)) {

			if (!tmp)
				break;

			if (!stream_data->valid) {
				stream_data->valid = 1;
				stream_data->value = 0;
				stream_data->value2 = 0;
				stream_data->number = 0;
			}
			stream_data->value += tmp->value;
			stream_data->value2 += tmp->value2;
			stream_data->number += tmp->number;

			double result = (stream_data->value2 - stream_data->value * stream_data->value / stream_data->number) / stream_data->number;
			if (stream_data->number > 1)
				result *= stream_data->number / (stream_data->number - 1.0);
			if (!strcmp(stream_data->operation, OPH_ESDM_FUNCTION_STD))
				result = sqrt(result);

			if (type == SMD_DTYPE_INT8) {

				char v = (char) result;
				memcpy(stream_data->buff, &v, sizeof(char));

			} else if (type == SMD_DTYPE_INT16) {

				short v = (short) result;
				memcpy(stream_data->buff, &v, sizeof(short));

			} else if (type == SMD_DTYPE_INT32) {

				int v = (int) result;
				memcpy(stream_data->buff, &v, sizeof(int));

			} else if (type == SMD_DTYPE_INT64) {

				long long v = (long long) result;
				memcpy(stream_data->buff, &v, sizeof(long long));

			} else if (type == SMD_DTYPE_FLOAT) {

				float v = (float) result;
				memcpy(stream_data->buff, &v, sizeof(float));

			} else if (type == SMD_DTYPE_DOUBLE) {

				double v = (double) result;
				memcpy(stream_data->buff, &v, sizeof(double));

			}
		}

	} while (0);

	if (stream_func_out)
		free(stream_func_out);
}

// ESDM kernels *********************************************









int _oph_ioserver_esdm_get_dimension_id(unsigned long residual, unsigned long total, unsigned int *sizemax, size_t ** id, int i, int n)
{
	if (i < n - 1) {
		unsigned long tmp;
		tmp = total / sizemax[i];
		*(id[i]) = (size_t) (residual / tmp + 1);
		residual %= tmp;
		_oph_ioserver_esdm_get_dimension_id(residual, tmp, sizemax, id, i + 1, n);
	} else {
		*(id[i]) = (size_t) (residual + 1);
	}
	return 0;
}

int oph_ioserver_esdm_compute_dimension_id(unsigned long ID, unsigned int *sizemax, int n, size_t ** id)
{
	if (n > 0) {
		int i;
		unsigned long total = 1;
		for (i = 0; i < n; ++i)
			total *= sizemax[i];
		_oph_ioserver_esdm_get_dimension_id(ID - 1, total, sizemax, id, 0, n);
	}
	return 0;
}

int _oph_ioserver_esdm_cache_to_buffer3(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *blocks, unsigned int *src_products, char *src_binary,
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


int _oph_ioserver_esdm_cache_to_buffer2(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *blocks, unsigned int *src_products, char *src_binary,
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

int oph_ioserver_esdm_cache_to_buffer(short int tot_dim_number, unsigned int *counters, unsigned int *limits, unsigned int *products, char *binary_cache, char *binary_insert, size_t sizeof_var)
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

int _oph_ioserver_esdm_read_v2(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, esdm_container_t * container, esdm_dataset_t * dataset,
			       int ndims, int nimp, int nexp, short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, oph_iostore_frag_record_set * binary_frag,
			       unsigned long long *frag_size, unsigned long long sizeof_var, esdm_type_t vartype, int id_dim_pos, int measure_pos, unsigned long long array_length, char *sub_operation,
			       char *sub_args, short int dimension_ordered)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !container || !dataset || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag
	    || !frag_size || !sizeof_var || !array_length) {
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if (transpose) {
		//Create a binary array to store the whole fragment
		if (vartype == SMD_DTYPE_INT8)
			res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT16)
			res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT32)
			res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT64)
			res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_FLOAT)
			res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
		else
			res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(binary_cache);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}


	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (vartype == SMD_DTYPE_INT8)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT16)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT32)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT64)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_FLOAT)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++) {
		idDim[ii] = frag_key_start + ii;
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
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(idDim);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	oph_ioserver_esdm_compute_dimension_id(idDim[0], sizemax, nexp, start_pointer);

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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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

	esdm_dataspace_t *subspace = NULL;
	if ((esdm_dataspace_create_full(ndims, count, start, vartype, &subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	size_t sizeof_type = (int) sizeof_var / array_length;

	//Fill binary cache
	esdm_status retval;
	if (sub_operation) {
		char fill_value[sizeof_type], *pointer = NULL;
		retval = esdm_dataset_get_fill_value(dataset, fill_value);
		if (!retval) {
			oph_esdm_stream_data_t stream_data;
			stream_data.operation = sub_operation;
			stream_data.args = sub_args;
			stream_data.buff = pointer = transpose ? binary_cache : binary_insert;
			stream_data.valid = 0;
			stream_data.fill_value = fill_value;
			for (j = 0; j < array_length; j++, pointer += sizeof_type)
				memcpy(pointer, fill_value, sizeof_type);
			retval = esdm_read_stream(dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func);
		}
	} else
		retval = esdm_read(dataset, transpose ? binary_cache : binary_insert, subspace);

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (retval) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	pthread_mutex_lock(&nc_lock);
	esdm_dataset_close(dataset);
	esdm_container_close(container);
	pthread_mutex_unlock(&nc_lock);

	free(start);
	free(start_pointer);
	free(sizemax);

	if (transpose) {

		//Prepare structures for buffer insert update
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
			limits[dims_index[i]] = count[i];
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
					if (binary_cache)
						free(binary_cache);
					free(binary_insert);
					free(idDim);
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
		_oph_ioserver_esdm_cache_to_buffer3(ndims, counters, limits, blocks, src_products, binary_cache, dst_products, binary_insert, sizeof_type);
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

	for (ii = 0; ii < tuplexfrag_number; ii++) {

		args[id_dim_pos]->arg = (unsigned long long *) (&(idDim[ii]));
		args[measure_pos]->arg = (char *) (binary_insert + ii * sizeof_var);

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
	free(idDim);
	free(binary_insert);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_esdm_read_v1(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, esdm_container_t * container, esdm_dataset_t * dataset,
			       int ndims, int nimp, int nexp, short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, oph_iostore_frag_record_set * binary_frag,
			       unsigned long long *frag_size, unsigned long long sizeof_var, esdm_type_t vartype, int id_dim_pos, int measure_pos, unsigned long long array_length, char *sub_operation,
			       char *sub_args, short int dimension_ordered)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !container || !dataset || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag
	    || !frag_size || !sizeof_var || !array_length) {
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if (transpose) {
		//Create a binary array to store the whole fragment
		if (vartype == SMD_DTYPE_INT8)
			res = oph_iob_bin_array_create_b(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT16)
			res = oph_iob_bin_array_create_s(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT32)
			res = oph_iob_bin_array_create_i(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_INT64)
			res = oph_iob_bin_array_create_l(&binary_cache, array_length * tuplexfrag_number);
		else if (vartype == SMD_DTYPE_FLOAT)
			res = oph_iob_bin_array_create_f(&binary_cache, array_length * tuplexfrag_number);
		else
			res = oph_iob_bin_array_create_d(&binary_cache, array_length * tuplexfrag_number);
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(binary_cache);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}


	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (vartype == SMD_DTYPE_INT8)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT16)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT32)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_INT64)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length * tuplexfrag_number);
	else if (vartype == SMD_DTYPE_FLOAT)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length * tuplexfrag_number);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length * tuplexfrag_number);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++) {
		idDim[ii] = frag_key_start + ii;
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
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(idDim);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	oph_ioserver_esdm_compute_dimension_id(idDim[0], sizemax, nexp, start_pointer);

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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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

	esdm_dataspace_t *subspace = NULL;
	if ((esdm_dataspace_create_full(ndims, count, start, vartype, &subspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	size_t sizeof_type = (int) sizeof_var / array_length;

	//Fill binary cache
	esdm_status retval;
	if (sub_operation) {
		char fill_value[sizeof_type], *pointer = NULL;
		retval = esdm_dataset_get_fill_value(dataset, fill_value);
		if (!retval) {
			oph_esdm_stream_data_t stream_data;
			stream_data.operation = sub_operation;
			stream_data.args = sub_args;
			stream_data.buff = pointer = transpose ? binary_cache : binary_insert;
			stream_data.valid = 0;
			stream_data.fill_value = fill_value;
			for (j = 0; j < array_length; j++, pointer += sizeof_type)
				memcpy(pointer, fill_value, sizeof_type);
			retval = esdm_read_stream(dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func);
		}
	} else
		retval = esdm_read(dataset, transpose ? binary_cache : binary_insert, subspace);

#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

	if (retval) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		free(idDim);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	pthread_mutex_lock(&nc_lock);
	esdm_dataset_close(dataset);
	esdm_container_close(container);
	pthread_mutex_unlock(&nc_lock);

	free(start);
	free(start_pointer);
	free(sizemax);

	if (transpose) {

		//Prepare structures for buffer insert update
		unsigned int *counters = (unsigned int *) malloc(ndims * sizeof(unsigned int));
		unsigned int *src_products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *limits = (unsigned int *) malloc(ndims * sizeof(unsigned));

		int *file_indexes = (int *) malloc(ndims * sizeof(int));

		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			src_products[dims_index[i]] = 1;
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
						src_products[k] *= limits[j];
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
		oph_ioserver_esdm_cache_to_buffer(ndims, counters, limits, src_products, binary_cache, binary_insert, sizeof_type);
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

	for (ii = 0; ii < tuplexfrag_number; ii++) {

		args[id_dim_pos]->arg = (unsigned long long *) (&(idDim[ii]));
		args[measure_pos]->arg = (char *) (binary_insert + ii * sizeof_var);

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
	free(idDim);
	free(binary_insert);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_esdm_read_v0(char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, esdm_container_t * container, esdm_dataset_t * dataset,
			       int ndims, int nimp, int nexp, short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, oph_iostore_frag_record_set * binary_frag,
			       unsigned long long *frag_size, unsigned long long sizeof_var, esdm_type_t vartype, int id_dim_pos, int measure_pos, unsigned long long array_length, char *sub_operation,
			       char *sub_args)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !container || !dataset || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag
	    || !frag_size || !sizeof_var || !array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
#ifdef DEBUG
	pmesg(LOG_INFO, __FILE__, __LINE__, "Using IMPORT algorithm v0.0\n");
#endif

	int i = 0, j = 0;

	//Flag set to 1 if implicit dimension are in the order specified in the file
	short int dimension_ordered = 1;
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	if (transpose) {
		//Create a binary array to store the data from file

		if (vartype == SMD_DTYPE_INT8)
			res = oph_iob_bin_array_create_b(&binary_cache, array_length);
		else if (vartype == SMD_DTYPE_INT16)
			res = oph_iob_bin_array_create_s(&binary_cache, array_length);
		else if (vartype == SMD_DTYPE_INT32)
			res = oph_iob_bin_array_create_i(&binary_cache, array_length);
		else if (vartype == SMD_DTYPE_INT64)
			res = oph_iob_bin_array_create_l(&binary_cache, array_length);
		else if (vartype == SMD_DTYPE_FLOAT)
			res = oph_iob_bin_array_create_f(&binary_cache, array_length);
		else
			res = oph_iob_bin_array_create_d(&binary_cache, array_length);

		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			free(binary_cache);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	char *binary_insert = 0;
	res = 0;
	if (vartype == SMD_DTYPE_INT8)
		res = oph_iob_bin_array_create_b(&binary_insert, array_length);
	else if (vartype == SMD_DTYPE_INT16)
		res = oph_iob_bin_array_create_s(&binary_insert, array_length);
	else if (vartype == SMD_DTYPE_INT32)
		res = oph_iob_bin_array_create_i(&binary_insert, array_length);
	else if (vartype == SMD_DTYPE_INT64)
		res = oph_iob_bin_array_create_l(&binary_insert, array_length);
	else if (vartype == SMD_DTYPE_FLOAT)
		res = oph_iob_bin_array_create_f(&binary_insert, array_length);
	else
		res = oph_iob_bin_array_create_d(&binary_insert, array_length);
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_MEMORY_ERROR;
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
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

	/*oph_ioserver_esdm_compute_dimension_id(idDim, sizemax, nexp, start_pointer);

	   for (i = 0; i < nexp; i++) {
	   *(start_pointer[i]) -= 1;
	   for (j = 0; j < ndims; j++) {
	   if (start_pointer[i] == &(start[j])) {
	   *(start_pointer[i]) += dims_start[j];
	   }
	   }
	   } */

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

	if (total != array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TOTAL = %d\n", array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array creation: %d\n", res);
		if (binary_cache)
			free(binary_cache);
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
					if (binary_cache)
						free(binary_cache);
					free(binary_insert);
					pthread_mutex_lock(&nc_lock);
					esdm_dataset_close(dataset);
					esdm_container_close(container);
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
			free(binary_cache);
			free(counters);
			free(src_products);
			free(limits);
		}
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
			free(binary_cache);
			free(counters);
			free(src_products);
			free(limits);
		}
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
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
				free(binary_cache);
				free(counters);
				free(src_products);
				free(limits);
			}
			free(binary_insert);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
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
	args[measure_pos]->arg = (char *) binary_insert;

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

	esdm_dataspace_t *subspace = NULL;
	oph_esdm_stream_data_t stream_data;
	char fill_value[sizeof_type], *pointer = NULL;

	if (esdm_dataset_get_fill_value(dataset, fill_value)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to get the fill value\n");
		for (i = 0; i < arg_count; i++)
			if (args[i])
				free(args[i]);
		free(args);
		free(value_list);
		if (transpose) {
			free(binary_cache);
			free(counters);
			free(src_products);
			free(limits);
		}
		free(binary_insert);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		free(start);
		free(count);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++) {

		oph_ioserver_esdm_compute_dimension_id(idDim, sizemax, nexp, start_pointer);

		for (i = 0; i < nexp; i++) {
			*(start_pointer[i]) -= 1;
			for (j = 0; j < ndims; j++) {
				if (start_pointer[i] == &(start[j])) {
					*(start_pointer[i]) += dims_start[j];
				}
			}
		}

		if ((esdm_dataspace_create_full(ndims, count, start, vartype, &subspace))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in subspace creation\n");
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(binary_cache);
				free(counters);
				free(src_products);
				free(limits);
			}
			free(binary_insert);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//Fill binary cache
		if (sub_operation) {

			stream_data.operation = sub_operation;
			stream_data.args = sub_args;
			stream_data.buff = pointer = transpose ? binary_cache : binary_insert;
			stream_data.valid = 0;
			stream_data.fill_value = fill_value;
			for (j = 0; j < array_length; j++, pointer += sizeof_type)
				memcpy(pointer, fill_value, sizeof_type);
			if (esdm_read_stream(dataset, subspace, &stream_data, oph_esdm_stream_func, oph_esdm_reduce_func)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
				for (i = 0; i < arg_count; i++)
					if (args[i])
						free(args[i]);
				free(args);
				free(value_list);
				if (transpose) {
					free(binary_cache);
					free(counters);
					free(src_products);
					free(limits);
				}
				free(binary_insert);
				pthread_mutex_lock(&nc_lock);
				esdm_dataset_close(dataset);
				esdm_container_close(container);
				pthread_mutex_unlock(&nc_lock);
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}

		} else if (esdm_read(dataset, transpose ? binary_cache : binary_insert, subspace)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(binary_cache);
				free(counters);
				free(src_products);
				free(limits);
			}
			free(binary_insert);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		if (transpose)
			oph_ioserver_esdm_cache_to_buffer(nimp, counters, limits, src_products, binary_cache, binary_insert, sizeof_type);

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(binary_cache);
				free(counters);
				free(src_products);
				free(limits);
			}
			free(binary_insert);
			pthread_mutex_lock(&nc_lock);
			esdm_dataset_close(dataset);
			esdm_container_close(container);
			pthread_mutex_unlock(&nc_lock);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		idDim++;

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
	esdm_dataset_close(dataset);
	esdm_container_close(container);
	pthread_mutex_unlock(&nc_lock);

	free(count);
	free(start);
	free(start_pointer);
	free(sizemax);

	if (transpose) {
		free(binary_cache);
		free(counters);
		free(src_products);
		free(limits);
	}

	for (i = 0; i < arg_count; i++)
		if (args[i])
			free(args[i]);
	free(args);
	free(value_list);
	free(binary_insert);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_esdm_read(char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int dim_num, short int *dims_type,
			    short int *dims_index, int *dims_start, int *dims_end, char *sub_operation, char *sub_args, oph_iostore_frag_record_set * binary_frag, unsigned long long *frag_size)
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
	if (!strstr(src_path, OPH_ESDM_PREFIX)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "URL of an ESDM container should start with esdm://containername\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "URL of an ESDM container should start with esdm://containername\n");
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	char *container_name = src_path + strlen(OPH_ESDM_PREFIX);

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
	//Open ESDM container

	esdm_status ret;
	esdm_container_t *container = NULL;
	esdm_dataset_t *dataset = NULL;
	esdm_dataspace_t *dspace = NULL;

	if (pthread_mutex_lock(&nc_lock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	if ((ret = esdm_container_open(container_name, ESDM_MODE_FLAG_READ, &container))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM container '%s': %s\n", src_path, container_name);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM container '%s': %s\n", src_path, container_name);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Extract measured variable information
	if ((ret = esdm_dataset_open(container, measure_name, ESDM_MODE_FLAG_READ, &dataset))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM variable '%s': %s\n", src_path, measure_name);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to open ESDM variable '%s': %s\n", src_path, measure_name);
		esdm_container_close(container);
		return OPH_IO_SERVER_EXEC_ERROR;
	}

	if (pthread_mutex_unlock(&nc_lock) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Extract measured variable information
	if ((ret = esdm_dataset_get_dataspace(dataset, &dspace))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information from %s\n", src_path);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information from %s\n", src_path);
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	int ndims = dspace->dims;
	if (ndims != dim_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
		pthread_mutex_lock(&nc_lock);
		esdm_dataset_close(dataset);
		esdm_container_close(container);
		pthread_mutex_unlock(&nc_lock);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Compute array_length from implicit dims
	unsigned long long array_length = 1;
	short int nimp = 0, nexp = 0;
	for (i = 0; i < ndims; i++) {
		if (!dims_type[i]) {
			array_length *= dims_end[i] - dims_start[i] + 1;
			nimp++;
		} else {
			nexp++;
		}
	}

	unsigned long long sizeof_var = 0;
	esdm_type_t vartype = dspace->type;
	if (vartype == SMD_DTYPE_INT8) {
		sizeof_var = (array_length) * sizeof(char);
	} else if (vartype == SMD_DTYPE_INT16) {
		sizeof_var = (array_length) * sizeof(short);
	} else if (vartype == SMD_DTYPE_INT32) {
		sizeof_var = (array_length) * sizeof(int);
	} else if (vartype == SMD_DTYPE_INT64) {
		sizeof_var = (array_length) * sizeof(long long);
	} else if (vartype == SMD_DTYPE_FLOAT) {
		sizeof_var = (array_length) * sizeof(float);
	} else {
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
		return _oph_ioserver_esdm_read_v0(measure_name, tuplexfrag_number, frag_key_start, compressed_flag, container, dataset, ndims, nimp, nexp, dims_type, dims_index, dims_start, dims_end,
						  binary_frag, frag_size, sizeof_var, dspace->type, id_dim_pos, measure_pos, array_length, sub_operation, sub_args);
	else
#ifdef OPH_IO_SERVER_ESDM_BLOCK
		return _oph_ioserver_esdm_read_v1(measure_name, tuplexfrag_number, frag_key_start, compressed_flag, container, dataset, ndims, nimp, nexp, dims_type, dims_index, dims_start, dims_end,
						  binary_frag, frag_size, sizeof_var, dspace->type, id_dim_pos, measure_pos, array_length, sub_operation, sub_args, dimension_ordered);
#else
		return _oph_ioserver_esdm_read_v2(measure_name, tuplexfrag_number, frag_key_start, compressed_flag, container, dataset, ndims, nimp, nexp, dims_type, dims_index, dims_start, dims_end,
						  binary_frag, frag_size, sizeof_var, dspace->type, id_dim_pos, measure_pos, array_length, sub_operation, sub_args, dimension_ordered);
#endif

}
#endif
