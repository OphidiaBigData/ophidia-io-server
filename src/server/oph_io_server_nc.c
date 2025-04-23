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

#include "oph_io_server_query_manager.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <debug.h>
#include <errno.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <unistd.h>

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

#define OPH_NC_LOAD_EXEC OPH_IO_SERVER_PREFIX "/bin/oph_io_server_nc_load"
#define INT_LEN 16
#define LONG_LEN 24
#define MSG_LEN 4096

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

typedef struct Buffer {
	char *cache;
	char *insert;
	char *support;		// Used for onload operations
	int cache_sh;
	int insert_sh;
	int support_sh;		// TODO: to be used for onload operations
} Buffer;

#define _oph_ioserver_nc_clear_buffer_cache(buff) _oph_ioserver_nc_clear_buffer_(buff, 1, 0)
#define _oph_ioserver_nc_clear_buffer_insert(buff) _oph_ioserver_nc_clear_buffer_(buff, 0, 0)
#define _oph_ioserver_nc_clear_buffer(buff) _oph_ioserver_nc_clear_buffer_(buff, 0, 0)
int _oph_ioserver_nc_clear_buffer_(Buffer *buff, char is_cache, char is_all)
{
	if (is_cache || is_all) {
#ifdef OPH_PAR_NC4
		if (buff->cache_sh) {
			shmctl(buff->cache_sh, IPC_RMID, NULL);
			buff->cache_sh = 0;
		}
#endif
		if (buff->cache) {
			free(buff->cache);
			buff->cache = NULL;
		}
	}
	if (!is_cache || is_all) {
#ifdef OPH_PAR_NC4
		if (buff->insert_sh) {
			shmctl(buff->insert_sh, IPC_RMID, NULL);
			buff->insert_sh = 0;
		}
#endif
		if (buff->insert) {
			free(buff->insert);
			buff->insert = NULL;
		}
		if (buff->support) {
			free(buff->support);
			buff->support = NULL;
		}
	}
}

int _oph_ioserver_nc_init_buffer(Buffer *buff)
{
	*buff = (Buffer) {
	NULL, NULL, NULL, 0, 0, 0};
	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_create_buffer(Buffer *buff, char transpose, char shared, nc_type vartype, long long elems)
{
	if (transpose) {
#ifdef OPH_PAR_NC4
		if (shared) {
			if (buff->cache_sh)
				return OPH_IO_SERVER_SUCCESS;
		} else
#endif
		{
			if (buff->cache)
				return OPH_IO_SERVER_SUCCESS;
		}
	} else {
#ifdef OPH_PAR_NC4
		if (shared) {
			if (buff->insert_sh)
				return OPH_IO_SERVER_SUCCESS;
		} else
#endif
		{
			if (buff->insert)
				return OPH_IO_SERVER_SUCCESS;
		}
	}

	int res = 0;

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create a binary array cache to store the whole fragment
	if (transpose) {
		switch (vartype) {
			case NC_BYTE:
			case NC_CHAR:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_b(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_b(&(buff->cache), elems);
				break;
			case NC_SHORT:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_s(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_s(&(buff->cache), elems);
				break;
			case NC_INT:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_i(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_i(&(buff->cache), elems);
				break;
			case NC_INT64:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_l(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_l(&(buff->cache), elems);
				break;
			case NC_FLOAT:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_f(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_f(&(buff->cache), elems);
				break;
			case NC_DOUBLE:
			default:
#ifdef OPH_PAR_NC4
				if (shared)
					res = oph_iob_bin_array_shared_create_d(&(buff->cache_sh), elems);
				else
#endif
					res = oph_iob_bin_array_create_d(&(buff->cache), elems);
		}
		if (res) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_nc_clear_buffer(buff);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	}

	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_nc_clear_buffer(buff);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//Create array for rows to be insert
	res = 0;
	switch (vartype) {
		case NC_BYTE:
		case NC_CHAR:
#ifdef OPH_PAR_NC4
			//Memory is shared only if shared flag is enabled and no transpose is requried
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_b(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_b(&(buff->insert), elems);
			break;
		case NC_SHORT:
#ifdef OPH_PAR_NC4
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_s(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_s(&(buff->insert), elems);
			break;
		case NC_INT:
#ifdef OPH_PAR_NC4
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_i(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_i(&(buff->insert), elems);
			break;
		case NC_INT64:
#ifdef OPH_PAR_NC4
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_l(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_l(&(buff->insert), elems);
			break;
		case NC_FLOAT:
#ifdef OPH_PAR_NC4
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_f(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_f(&(buff->insert), elems);
			break;
		case NC_DOUBLE:
		default:
#ifdef OPH_PAR_NC4
			if (!transpose && shared)
				res = oph_iob_bin_array_shared_create_d(&(buff->insert_sh), elems);
			else
#endif
				res = oph_iob_bin_array_create_d(&(buff->insert), elems);
	}
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_nc_clear_buffer(buff);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_create_support(Buffer *buff, char shared, unsigned long long raw_size)
{
	if (memory_check()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	int res = 0;
	//Create array for support
#ifdef OPH_PAR_NC4
	if (shared) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "This feature is not supported yet");
		logging(LOG_ERROR, __FILE__, __LINE__, "This feature is not supported yet");
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
#endif
	{
		if (buff->support)
			return OPH_IO_SERVER_SUCCESS;
		buff->support = (char *) malloc(raw_size);
		res = !buff->support;
	}
	if (res) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	return OPH_IO_SERVER_SUCCESS;
}

#include <math.h>
#include <ctype.h>

#define NC_SEPARATOR ","

#define NC_FUNCTION_NOP "nop"
#define NC_FUNCTION_STREAM "stream"

#define NC_FUNCTION_MAX "max"
#define NC_FUNCTION_MIN "min"
#define NC_FUNCTION_AVG "avg"
#define NC_FUNCTION_SUM "sum"
#define NC_FUNCTION_STD "std"
#define NC_FUNCTION_VAR "var"
#define NC_FUNCTION_STAT "stat"

#define NC_FUNCTION_OUTLIER "outlier"

#define NC_FUNCTION_SUM_SCALAR "sum_scalar"
#define NC_FUNCTION_MUL_SCALAR "mul_scalar"

#define NC_FUNCTION_ABS "abs"
#define NC_FUNCTION_SQR "sqr"
#define NC_FUNCTION_SQRT "sqrt"
#define NC_FUNCTION_CEIL "ceil"
#define NC_FUNCTION_FLOOR "floor"
#define NC_FUNCTION_ROUND "round"
#define NC_FUNCTION_INT "int"
#define NC_FUNCTION_NINT "nint"

#define NC_FUNCTION_POW "pow"
#define NC_FUNCTION_EXP "exp"
#define NC_FUNCTION_LOG "log"
#define NC_FUNCTION_LOG10 "log10"

#define NC_FUNCTION_SIN "sin"
#define NC_FUNCTION_COS "cos"
#define NC_FUNCTION_TAN "tan"
#define NC_FUNCTION_ASIN "asin"
#define NC_FUNCTION_ACOS "acos"
#define NC_FUNCTION_ATAN "atan"
#define NC_FUNCTION_SINH "sinh"
#define NC_FUNCTION_COSH "cosh"
#define NC_FUNCTION_TANH "tanh"

#define NC_FUNCTION_RECI "reci"
#define NC_FUNCTION_NOT "not"

#define NC_FUNCTION_OP_N 3
#define NC_FUNCTION_OP_SET '1'
#define NC_FUNCTION_OP_LESS_THAN '<'
#define NC_FUNCTION_OP_MORE_THAN '>'

#define NC_FILL_VALUE "_FillValue"
#define NC_FILL_VALUE_EPS(x) (0.001 * x)

size_t _oph_nc_sizeof(nc_type vartype)
{
	switch (vartype) {
		case NC_BYTE:
		case NC_CHAR:
			return sizeof(unsigned char);
		case NC_SHORT:
			return sizeof(short);
		case NC_INT:
			return sizeof(int);
		case NC_INT64:
			return sizeof(long long);
		case NC_FLOAT:
			return sizeof(float);
		case NC_DOUBLE:
		default:
			return sizeof(double);
	}
	return 0;
}

int _oph_nc_is_a_reduce_func(const char *operation, const char *args)
{
	if (!operation)
		return 0;

	if (!strcmp(operation, NC_FUNCTION_MAX))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_MIN))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_AVG))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_SUM))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_STD))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_VAR))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_OUTLIER))
		return 1;
	if (!strcmp(operation, NC_FUNCTION_STAT)) {
		int i, option = 0;
		for (i = 0; i < NC_FUNCTION_OP_N; ++i)
			if (args && args[0]) {
				if (args[0] == NC_FUNCTION_OP_SET)
					option++;
				args++;
			} else
				break;
		return option;
	}

	return 0;
}

int _oph_nc_reduce_func(void *buff, void *result, nc_type type, unsigned long long n, char *sub_operation, char *sub_args, void *fill_value)
{
	if (!buff || !sub_operation)
		return OPH_IO_SERVER_NULL_PARAM;

	char *args = sub_args ? strdup(sub_args) : NULL;	// Copy for strtok
	unsigned long long k, number = 0;
	double value1 = 0.0, value2 = 0.0, value3 = 0.0;

	if (!strcmp(sub_operation, NC_FUNCTION_NOP) || !strcmp(sub_operation, NC_FUNCTION_STREAM)) {

		if (result)
			memcpy(result, buff, n * _oph_nc_sizeof(type));

	} else if (!strcmp(sub_operation, NC_FUNCTION_MAX)) {

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) && (!number || (v < a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_MIN)) {

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (a[k] != fv)) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if ((!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) && (!number || (v > a[k]))) {
					v = a[k];
				}
			}
			memcpy(result ? result : buff, &v, sizeof(v));

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_AVG) || !strcmp(sub_operation, NC_FUNCTION_SUM)) {

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					number++;
				}
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					number++;
				}
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					number++;
				}
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					number++;
				}
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					value1 += a[k];
					number++;
				}
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					value1 += a[k];
					number++;
				}
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_STD) || !strcmp(sub_operation, NC_FUNCTION_VAR)) {

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					value1 += a[k];
					value2 += a[k] * a[k];
					number++;
				}
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_STAT)) {

		char *save_pointer = NULL, *arg = args ? strtok_r(args, NC_SEPARATOR, &save_pointer) : NULL;
		char i, option = 0, tbyte = 1;
		for (i = 0; i < NC_FUNCTION_OP_N; ++i)
			if (arg && arg[0]) {
				if (arg[0] == NC_FUNCTION_OP_SET)
					option |= tbyte;
				arg++;
				tbyte <<= 1;
			} else
				break;
		if (!option) {

			// No operation is executed in this case

		} else if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(char *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(short *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else if (type == NC_INT) {

			int *a = (int *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(int *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(long long *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv)) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(float *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v1 = 0, v2 = 0, fv = fill_value ? *(double *) fill_value : 0;
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv))) {
					if ((option & 1) && (!number || (v1 > a[k])))	// Min
						v1 = a[k];
					if ((option & 2) && (!number || (v2 < a[k])))	// Max
						v2 = a[k];
					if (option & 4)	// Avg
						value3 += a[k];
					number++;
				}
			}
			value1 = v1;
			value2 = v2;

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_OUTLIER)) {

		number = 1;	// Use only to avoid errors during the reduction phase

		char *save_pointer = NULL, *arg = args ? strtok_r(args, NC_SEPARATOR, &save_pointer) : NULL;
		char thresh_type = NC_FUNCTION_OP_MORE_THAN;
		if (arg) {
			if (!isdigit(arg[0])) {
				if (arg[0] == NC_FUNCTION_OP_LESS_THAN)
					thresh_type = NC_FUNCTION_OP_LESS_THAN;
				arg++;
			}
		}
		if (!arg || !arg[0]) {

			// No element is considered outlier in case the threshold is not given

		} else if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, fv = fill_value ? *(char *) fill_value : 0, v = strtol(arg, NULL, 10);
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, fv = fill_value ? *(short *) fill_value : 0, v = strtol(arg, NULL, 10);
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, fv = fill_value ? *(int *) fill_value : 0, v = strtol(arg, NULL, 10);
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, fv = fill_value ? *(long long *) fill_value : 0, v = strtoll(arg, NULL, 10);
			for (k = 0; k < n; k++) {
				if (!fill_value || (a[k] != fv))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, fv = fill_value ? *(float *) fill_value : 0, v = strtof(arg, NULL);
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, fv = fill_value ? *(double *) fill_value : 0, v = strtod(arg, NULL);
			for (k = 0; k < n; k++) {
				if (!fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)))
					switch (thresh_type) {
						case NC_FUNCTION_OP_LESS_THAN:
							if (v > a[k])
								value1++;
							break;
						case NC_FUNCTION_OP_MORE_THAN:
						default:
							if (v < a[k])
								value1++;
					}
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_SUM_SCALAR)) {

		if (!args) {
			if (result)
				memcpy(result, buff, n * _oph_nc_sizeof(type));
			return OPH_IO_SERVER_SUCCESS;
		}
		if (!result)
			result = buff;

		char *save_pointer = NULL, *arg = args ? strtok_r(args, NC_SEPARATOR, &save_pointer) : NULL;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char scalar = arg ? strtol(arg, NULL, 10) : 0;
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short scalar = arg ? strtol(arg, NULL, 10) : 0;
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int scalar = arg ? strtol(arg, NULL, 10) : 0;
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long scalar = arg ? strtoll(arg, NULL, 10) : 0;
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float scalar = arg ? strtof(arg, NULL) : 0;
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double scalar = arg ? strtod(arg, NULL) : 0;
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] + scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_MUL_SCALAR)) {

		if (!args) {
			if (result)
				memcpy(result, buff, n * _oph_nc_sizeof(type));
			return OPH_IO_SERVER_SUCCESS;
		}
		if (!result)
			result = buff;

		number = 1;

		char *save_pointer = NULL, *arg = args ? strtok_r(args, NC_SEPARATOR, &save_pointer) : NULL;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char scalar = arg ? strtol(arg, NULL, 10) : 1;
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short scalar = arg ? strtol(arg, NULL, 10) : 1;
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int scalar = arg ? strtol(arg, NULL, 10) : 1;
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long scalar = arg ? strtoll(arg, NULL, 10) : 1;
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float scalar = arg ? strtof(arg, NULL) : 1;
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double scalar = arg ? strtod(arg, NULL) : 1;
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] * scalar : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_ABS)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? abs(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_SQRT)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= 0)) ? sqrt(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_SQR)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? a[k] * a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(sub_operation, NC_FUNCTION_CEIL)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? ceil(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(sub_operation, NC_FUNCTION_FLOOR) || !strcmp(sub_operation, NC_FUNCTION_INT)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? floor(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(sub_operation, NC_FUNCTION_ROUND) || !strcmp(sub_operation, NC_FUNCTION_NINT)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? floor(a[k] + 0.5) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_POW)) {

		if (!result)
			result = buff;

		if (!args) {
			if (result)
				memcpy(result, buff, n * _oph_nc_sizeof(type));
			return OPH_IO_SERVER_SUCCESS;
		}

		number = 1;

		char *save_pointer = NULL, *arg = args ? strtok_r(args, NC_SEPARATOR, &save_pointer) : NULL;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char scalar = arg ? strtol(arg, NULL, 10) : 1;
			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short scalar = arg ? strtol(arg, NULL, 10) : 1;
			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int scalar = arg ? strtol(arg, NULL, 10) : 1;
			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long scalar = arg ? strtoll(arg, NULL, 10) : 1;
			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float scalar = arg ? strtof(arg, NULL) : 1;
			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double scalar = arg ? strtod(arg, NULL) : 1;
			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? pow(a[k], scalar) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_EXP)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? exp(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_LOG)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] > 0)) ? log(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_LOG10)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] > 0)) ? log10(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_SIN)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}


		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? sin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_COS)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? cos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_TAN)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? tan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_ASIN)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= -1) && (a[k] <= 1)) ? asin(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_ACOS)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && (a[k] >= -1) && (a[k] <= 1)) ? acos(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_ATAN)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? atan(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_SINH)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? sinh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_COSH)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? cosh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_TANH)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? tanh(a[k]) : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		// TODO: to be optimized for integer values
	} else if (!strcmp(sub_operation, NC_FUNCTION_RECI)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((a[k] != fv) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || ((abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) && a[k]) ? 1.0 / a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_NOT)) {

		if (!result)
			result = buff;

		number = 1;

		if ((type == NC_BYTE) || (type == NC_CHAR)) {

			char *a = (char *) buff, v = 0, fv = fill_value ? *(char *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_SHORT) {

			short *a = (short *) buff, v = 0, fv = fill_value ? *(short *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT) {

			int *a = (int *) buff, v = 0, fv = fill_value ? *(int *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_INT64) {

			long long *a = (long long *) buff, v = 0, fv = fill_value ? *(long long *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (a[k] != fv) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_FLOAT) {

			float *a = (float *) buff, v = 0, fv = fill_value ? *(float *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else if (type == NC_DOUBLE) {

			double *a = (double *) buff, v = 0, fv = fill_value ? *(double *) fill_value : 0;
			size_t step = sizeof(v);
			for (k = 0; k < n; k++) {
				v = !fill_value || (abs(a[k] - fv) < NC_FILL_VALUE_EPS(fv)) ? !a[k] : fv;
				memcpy(result + k * step, &v, step);
			}

		} else {
			if (args)
				free(args);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

	}

	if (args)
		free(args);

	if (!number)
		return OPH_IO_SERVER_SUCCESS;

	// Final reduction
	if (!strcmp(sub_operation, NC_FUNCTION_AVG) || !strcmp(sub_operation, NC_FUNCTION_SUM) || !strcmp(sub_operation, NC_FUNCTION_OUTLIER)) {

		if (strcmp(sub_operation, NC_FUNCTION_AVG))
			number = 1;

		if (type == NC_CHAR) {

			char v = (char) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_SHORT) {

			short v = (short) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT) {

			int v = (int) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT64) {

			long long v = (long long) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_FLOAT) {

			float v = (float) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_DOUBLE) {

			double v = (double) (value1 / number);
			memcpy(result ? result : buff, &v, sizeof(v));

		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_STD) || !strcmp(sub_operation, NC_FUNCTION_VAR)) {

		value3 = (value2 - value1 * value1 / number) / number;
		if (number > 1)
			value3 *= number / (number - 1.0);
		if (!strcmp(sub_operation, NC_FUNCTION_STD))
			value3 = sqrt(value3);

		if (type == NC_CHAR) {

			char v = (char) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_SHORT) {

			short v = (short) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT) {

			int v = (int) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_INT64) {

			long long v = (long long) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_FLOAT) {

			float v = (float) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		} else if (type == NC_DOUBLE) {

			double v = (double) value3;
			memcpy(result ? result : buff, &v, sizeof(v));

		}

	} else if (!strcmp(sub_operation, NC_FUNCTION_STAT)) {

		char i, option = 0, tbyte = 1, *arg = sub_args;
		size_t offset = 0;
		for (i = 0; i < NC_FUNCTION_OP_N; ++i)
			if (arg && arg[0]) {
				if (arg[0] == NC_FUNCTION_OP_SET)
					option |= tbyte;
				arg++;
				tbyte <<= 1;
			} else
				break;

		if (type == NC_CHAR) {

			char v;
			if (option & 1) {
				v = (char) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (char) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (char) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}

		} else if (type == NC_SHORT) {

			short v;
			if (option & 1) {
				v = (short) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (short) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (short) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}

		} else if (type == NC_INT) {

			int v;
			if (option & 1) {
				v = (int) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (int) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (int) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}

		} else if (type == NC_INT64) {

			long long v;
			if (option & 1) {
				v = (long long) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (long long) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (long long) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}

		} else if (type == NC_FLOAT) {

			float v;
			if (option & 1) {
				v = (float) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (float) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (float) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}

		} else if (type == NC_DOUBLE) {

			double v;
			if (option & 1) {
				v = (double) value1;
				memcpy(result ? result : buff, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 2) {
				v = (double) value2;
				memcpy(result + offset, &v, sizeof(v));
				offset += sizeof(v);
			}
			if (option & 4) {
				v = (double) (value3 / number);
				memcpy(result + offset, &v, sizeof(v));
				//offset += sizeof(v);  // Useless
			}
		}
	}

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read_data_v0(Buffer *buff, int offset, char transpose, char shared, nc_type vartype, int ndims, char *src_path, char *measure_name, size_t *start, size_t *count, int ncid,
				  int varid, unsigned long long tuples, unsigned long long idDim, int nexp, unsigned int *sizemax, short int *dims_type, short int *dims_index, int *dims_start,
				  char *sub_operation, char *sub_args, void *fill_value, char check_for_reduce_func)
{
#ifdef OPH_PAR_NC4
	if (shared) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Loading data from binary\n");
		logging(LOG_DEBUG, __FILE__, __LINE__, "Loading data from binary\n");

		int shm_id = transpose ? buff->cache_sh : buff->insert_sh;

		//Setup message for child process
		char msg[MSG_LEN] = { '\0' };
		int msg_len = strlen(src_path) + strlen(measure_name) + 2 * (ndims + 1) * (INT_LEN + 1) + (LONG_LEN + 1) + (tuples > 1 ? (LONG_LEN + 1) + (2 * ndims + 1) * (INT_LEN + 1) : 0);
		if (msg_len > MSG_LEN) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup message for child process\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to setup message for child process\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		int c = snprintf(msg, MSG_LEN, "%d|%s|%s|", shm_id, src_path, measure_name);
		int i = 0, index = 0;
		for (i = 0; i < ndims; i++)
			index += snprintf(&msg[c + index], INT_LEN + 1, "%d;", (size_t *) start[i]);
		msg[c + index++] = '|';
		for (i = 0; i < ndims; i++)
			index += snprintf(&msg[c + index], INT_LEN + 1, "%d;", (size_t *) count[i]);
		msg[c + index++] = '|';
		index += snprintf(&msg[c + index], INT_LEN + 1, "%d", offset);
		msg[c + index++] = '|';
		index += snprintf(&msg[c + index], LONG_LEN + 1, "%lld", tuples);
		if (tuples > 1) {
			msg[c + index++] = '|';
			index += snprintf(&msg[c + index], LONG_LEN + 1, "%lld", idDim);
			msg[c + index++] = '|';
			index += snprintf(&msg[c + index], INT_LEN + 1, "%d", nexp);
			msg[c + index++] = '|';
			for (i = 0; i < nexp; i++)
				index += snprintf(&msg[c + index], INT_LEN + 1, "%d;", sizemax[i]);
			msg[c + index++] = '|';
			for (i = 0; i < ndims; i++)
				index += snprintf(&msg[c + index], INT_LEN + 1, "%d;", dims_type[i] ? dims_index[i] : -1);
			msg[c + index++] = '|';
			for (i = 0; i < ndims; i++)
				index += snprintf(&msg[c + index], INT_LEN + 1, "%d;", dims_start[i]);
		}

		pmesg(LOG_DEBUG, __FILE__, __LINE__, "MESSAGE IS %s\n", msg);

		//Open pipe
		int fd[2];
		if (pipe(fd) == -1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in opening pipe: %s\n", strerror(errno));
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in opening pipe: %s\n", strerror(errno));
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		pid_t child_pid;
		//Fork process to fill shared memory
		if ((child_pid = fork()) == 0) {
			//Replace SDTIN with pipe
			close(STDIN_FILENO);
			dup(fd[0]);
			close(fd[0]);
			close(fd[1]);

			char *exec_path = OPH_NC_LOAD_EXEC;
			if (execvp(exec_path, (char *[]) {
				   exec_path, NULL
				   }) == -1)
				exit(errno);
			else
				exit(0);
		}
		//Close read end of pipe
		close(fd[0]);
		//Send message to child process
		if (write(fd[1], msg, msg_len) == -1) {
			close(fd[1]);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to send shared memory id to child\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to send shared memory id to child\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		close(fd[1]);

		//Wait for child process to end
		int status;
		waitpid(child_pid, &status, 0);
		if ((WIFEXITED(status)) == 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Loading process status is: %d\n", status);
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Shared cache ID is: %d\n", shm_id);

		if ((status = WEXITSTATUS(status)) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", strerror(status));
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", strerror(status));
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
	} else {
#endif
		pmesg(LOG_DEBUG, __FILE__, __LINE__, "Loading data directly\n");
		logging(LOG_DEBUG, __FILE__, __LINE__, "Loading data directly\n");

		//Fill binary cache
		int res = -1;
		char *buffer = transpose ? buff->cache : buff->insert;
		int ncid_int = 0, varid_int = 0;
		if (ncid == 0 || varid == 0) {
			if (pthread_mutex_lock(&nc_lock) != 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			if ((res = nc_open(src_path, NC_NOWRITE, &ncid_int))) {
				pthread_mutex_unlock(&nc_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(res));
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(res));
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			if (pthread_mutex_unlock(&nc_lock) != 0) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			//Extract measured variable information
			int varid = 0;
			if ((res = nc_inq_varid(ncid_int, measure_name, &varid_int))) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(res));
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(res));
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid_int);
				pthread_mutex_unlock(&nc_lock);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		} else {
			ncid_int = ncid;
			varid_int = varid;
		}

		if (buff->support) {

			// Load data in the support buffer
			switch (vartype) {
				case NC_BYTE:
				case NC_CHAR:
					res = nc_get_vara_uchar(ncid_int, varid_int, start, count, (unsigned char *) buff->support);
					break;
				case NC_SHORT:
					res = nc_get_vara_short(ncid_int, varid_int, start, count, (short *) buff->support);
					break;
				case NC_INT:
					res = nc_get_vara_int(ncid_int, varid_int, start, count, (int *) buff->support);
					break;
				case NC_INT64:
					res = nc_get_vara_longlong(ncid_int, varid_int, start, count, (long long *) buff->support);
					break;
				case NC_FLOAT:
					res = nc_get_vara_float(ncid_int, varid_int, start, count, (float *) buff->support);
					break;
				case NC_DOUBLE:
				default:
					res = nc_get_vara_double(ncid_int, varid_int, start, count, (double *) buff->support);
			}

			// Evaluate the operation
			size_t value_size = _oph_nc_sizeof(vartype);
			void *result = malloc(value_size * check_for_reduce_func);
			int i;
			unsigned long long n = 1;
			for (i = 0; i < ndims; i++)
				n *= count[i];
			if (_oph_nc_reduce_func(buff->support, result, vartype, n, sub_operation, sub_args, fill_value)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to evaluate onload operation\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to evaluate onload operation\n");
				free(result);
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			// Store the result
			switch (vartype) {
				case NC_BYTE:
				case NC_CHAR:
					memcpy((unsigned char *) buffer + offset, result, value_size);
					break;
				case NC_SHORT:
					memcpy((short *) buffer + offset, result, value_size);
					break;
				case NC_INT:
					memcpy((int *) buffer + offset, result, value_size);
					break;
				case NC_INT64:
					memcpy((long long *) buffer + offset, result, value_size);
					break;
				case NC_FLOAT:
					memcpy((float *) buffer + offset, result, value_size);
					break;
				case NC_DOUBLE:
				default:
					memcpy((double *) buffer + offset, result, value_size);
			}

			free(result);

		} else {

			void *target = NULL;
			switch (vartype) {
				case NC_BYTE:
				case NC_CHAR:
					res = nc_get_vara_uchar(ncid_int, varid_int, start, count, target = (unsigned char *) buffer + offset);
					break;
				case NC_SHORT:
					res = nc_get_vara_short(ncid_int, varid_int, start, count, target = (short *) buffer + offset);
					break;
				case NC_INT:
					res = nc_get_vara_int(ncid_int, varid_int, start, count, target = (int *) buffer + offset);
					break;
				case NC_INT64:
					res = nc_get_vara_longlong(ncid_int, varid_int, start, count, target = (long long *) buffer + offset);
					break;
				case NC_FLOAT:
					res = nc_get_vara_float(ncid_int, varid_int, start, count, target = (float *) buffer + offset);
					break;
				case NC_DOUBLE:
				default:
					res = nc_get_vara_double(ncid_int, varid_int, start, count, target = (double *) buffer + offset);
			}

			if (sub_operation) {
				// Evaluate the operation
				int i;
				unsigned long long n = 1;
				for (i = 0; i < ndims; i++)
					n *= count[i];
				if (_oph_nc_reduce_func(target, NULL, vartype, n, sub_operation, sub_args, fill_value)) {	// In place
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to evaluate onload operation\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Unable to evaluate onload operation\n");
					return OPH_IO_SERVER_EXEC_ERROR;
				}
			}
		}

		if (ncid == 0 || varid == 0) {
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid_int);
			pthread_mutex_unlock(&nc_lock);
		}
		if (res != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %s\n", nc_strerror(res));
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
#ifdef OPH_PAR_NC4
	}
#endif
	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read_data(Buffer *buff, int offset, char transpose, char shared, nc_type vartype, int ndims, char *src_path, char *measure_name, size_t *start, size_t *count, char *sub_operation,
			       char *sub_args, void *fill_value, char check_for_reduce_func)
{
	return _oph_ioserver_nc_read_data_v0(buff, offset, transpose, shared, vartype, ndims, src_path, measure_name, start, count, 0, 0, 1, 0, 0, NULL, NULL, NULL, NULL, sub_operation, sub_args,
					     fill_value, check_for_reduce_func);
}

#define _oph_ioserver_nc_release_buffer_cache(buff, buffer) _oph_ioserver_nc_release_buffer(buff, buffer, 1)
#define _oph_ioserver_nc_release_buffer_insert(buff, buffer) _oph_ioserver_nc_release_buffer(buff, buffer, 0)
int _oph_ioserver_nc_release_buffer(Buffer *buff, char *buffer, char is_cache)
{
#ifdef OPH_PAR_NC4
	if (is_cache) {
		if (buff->cache_sh)
			shmdt(buffer);
	} else {
		if (buff->insert_sh)
			shmdt(buffer);
	}
#endif
	return OPH_IO_SERVER_SUCCESS;
}

#define _oph_ioserver_nc_get_buffer_cache(buff, buffer) _oph_ioserver_nc_get_buffer(buff, buffer, 1)
#define _oph_ioserver_nc_get_buffer_insert(buff, buffer) _oph_ioserver_nc_get_buffer(buff, buffer, 0)
int _oph_ioserver_nc_get_buffer(Buffer *buff, char **buffer, char is_cache)
{
	if (is_cache) {
#ifdef OPH_PAR_NC4
		if (buff->cache_sh) {
			//Attach shared memory segment to process
			if ((*buffer = shmat(buff->cache_sh, NULL, 0)) == (char *) -1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
		} else
#endif
			*buffer = buff->cache;
	} else {
#ifdef OPH_PAR_NC4
		if (buff->insert_sh) {
			//Attach shared memory segment to process
			if ((*buffer = shmat(buff->insert_sh, NULL, 0)) == (char *) -1) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}
		} else
#endif
			*buffer = buff->insert;
	}
	return OPH_IO_SERVER_SUCCESS;
}

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

int _oph_ioserver_nc_read_v2(char is_netcdf4, char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ndims, int nimp, int nexp,
			     short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set *binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, unsigned long long raw_sizeof_var, nc_type vartype, int id_dim_pos,
			     int measure_pos, unsigned long long array_length, unsigned long long _array_length, int internal_size, Buffer *buff, char is_last, char *sub_operation, char *sub_args,
			     void *fill_value, char check_for_reduce_func, char dimension_ordered)
{
	if (!src_path || !measure_name || !tuplexfrag_number || !frag_key_start || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size
	    || !sizeof_var || !array_length || !_tuplexfrag_number || !_array_length || !buff) {
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
	char whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size / 2 ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//If flag is set fragment reordering is required
	char transpose = !dimension_ordered;

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
	char whole_explicit = 1;
	for (i = ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			relative_rows = (int) (_tuplexfrag_number / curr_rows);
			curr_rows *= (dims_end[j] - dims_start[j] + 1);
			if (relative_rows < (dims_end[j] - dims_start[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}

	//If external explicit is not integer
	if (_tuplexfrag_number % curr_rows)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuples divided by %d\n", _tuplexfrag_number, curr_rows);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuple divided by %d\n", _tuplexfrag_number, curr_rows);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Create binary array
	long long elems = array_length * tuplexfrag_number;
	if (_oph_ioserver_nc_create_buffer(buff, transpose, is_netcdf4, vartype, elems)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	if (check_for_reduce_func && _oph_ioserver_nc_create_support(buff, is_netcdf4, raw_sizeof_var)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
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
	char flag = 0;
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
			_oph_ioserver_nc_clear_buffer(buff);
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
		if (dims_type[i] || !check_for_reduce_func)
			total *= count[i];
		else if (check_for_reduce_func > 1)
			total *= count[i];

	if (total != _array_length * _tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = %d, TOTAL = %d\n", _array_length, _tuplexfrag_number, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = %d, TOTAL = %d\n", _array_length, _tuplexfrag_number, total);
		_oph_ioserver_nc_clear_buffer(buff);
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

	char dim_unlim_whole = 1;
	offset *= internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number);
	if (!dims_index[dim_unlim] && dims_type[dim_unlim]) {
		offset = 0;
		is_last = 1;
		dim_unlim_whole = 0;
	}
	//Fill binary cache
	if (_oph_ioserver_nc_read_data(buff, offset, transpose, is_netcdf4, vartype, ndims, src_path, measure_name, start, count, sub_operation, sub_args, fill_value, check_for_reduce_func)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		_oph_ioserver_nc_clear_buffer(buff);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

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

		int *file_indexes = (int *) malloc(ndims * sizeof(int)), k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			src_products[dims_index[i]] = 1;
			dst_products[dims_index[i]] = 1;
			limits[dims_index[i]] = dim_unlim_whole && (i == dim_unlim) ? dim_unlim_size : count[i];
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
					_oph_ioserver_nc_clear_buffer(buff);
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
		//Attach shared memory segment to process
		char *buffer_in = NULL, *buffer_out = NULL;
		if (_oph_ioserver_nc_get_buffer_cache(buff, &buffer_in) || _oph_ioserver_nc_get_buffer_insert(buff, &buffer_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_nc_clear_buffer(buff);
			free(count);
			free(file_indexes);
			free(counters);
			free(src_products);
			free(limits);
			free(blocks);
			free(dst_products);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		_oph_ioserver_nc_cache_to_buffer3(ndims, counters, limits, blocks, src_products, buffer_in, dst_products, buffer_out, sizeof_type);

		//Detach shared memory segment
		_oph_ioserver_nc_release_buffer_cache(buff, buffer_in);
		_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
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
	//Free only cache
	_oph_ioserver_nc_clear_buffer_cache(buff);

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_nc_clear_buffer_insert(buff);
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
		_oph_ioserver_nc_clear_buffer_insert(buff);
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
			_oph_ioserver_nc_clear_buffer_insert(buff);
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

	char *buffer = NULL;
	if (_oph_ioserver_nc_get_buffer_insert(buff, &buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		for (i = 0; i < arg_count; i++)
			if (args[i])
				free(args[i]);
		free(args);
		free(value_list);
		_oph_ioserver_nc_clear_buffer_insert(buff);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++, idDim++) {

		args[measure_pos]->arg = (char *) (buffer + ii * sizeof_var);

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			_oph_ioserver_nc_release_buffer_insert(buff, buffer);
			_oph_ioserver_nc_clear_buffer_insert(buff);
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
	_oph_ioserver_nc_release_buffer_insert(buff, buffer);
	_oph_ioserver_nc_clear_buffer_insert(buff);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read_v1(char is_netcdf4, char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ndims, int nimp, int nexp,
			     short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set *binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, unsigned long long raw_sizeof_var, nc_type vartype, int id_dim_pos,
			     int measure_pos, unsigned long long array_length, unsigned long long _array_length, int internal_size, Buffer *buff, char is_last, char *sub_operation, char *sub_args,
			     void *fill_value, char check_for_reduce_func, char dimension_ordered)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size
	    || !sizeof_var || !array_length || !_tuplexfrag_number || !_array_length || !buff) {
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
	char whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size / 2 ? 0 : 1);

	if (!whole_fragment) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read fragment in memory. Memory required is: %lld\n", tuplexfrag_number * sizeof_var);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	//If flag is set fragment reordering is required
	char transpose = !dimension_ordered;

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
	char whole_explicit = 1;
	for (i = ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			relative_rows = (int) (_tuplexfrag_number / curr_rows);
			curr_rows *= (dims_end[j] - dims_start[j] + 1);
			if (relative_rows < (dims_end[j] - dims_start[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}

	//If external explicit is not integer
	if (_tuplexfrag_number % curr_rows)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuples divided by %d\n", _tuplexfrag_number, curr_rows);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuple divided by %d\n", _tuplexfrag_number, curr_rows);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Create binary array
	long long elems = array_length * tuplexfrag_number;
	if (_oph_ioserver_nc_create_buffer(buff, transpose, is_netcdf4, vartype, elems)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	if (check_for_reduce_func && _oph_ioserver_nc_create_support(buff, is_netcdf4, raw_sizeof_var)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
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
	char flag = 0;
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
			_oph_ioserver_nc_clear_buffer(buff);
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
		if (dims_type[i] || !check_for_reduce_func)
			total *= count[i];
		else if (check_for_reduce_func > 1)
			total *= count[i];

	if (total != _array_length * _tuplexfrag_number) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = %d, TOTAL = %d\n", _array_length, _tuplexfrag_number, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = %d, TOTAL = %d\n", _array_length, _tuplexfrag_number, total);
		_oph_ioserver_nc_clear_buffer(buff);
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

	char dim_unlim_whole = 1;
	offset *= internal_size * (dims_type[dim_unlim] ? array_length : tuplexfrag_number);
	if (!dims_index[dim_unlim] && dims_type[dim_unlim]) {
		offset = 0;
		is_last = 1;
		dim_unlim_whole = 0;
	}
	//Fill binary cache
	if (_oph_ioserver_nc_read_data(buff, offset, transpose, is_netcdf4, vartype, ndims, src_path, measure_name, start, count, sub_operation, sub_args, fill_value, check_for_reduce_func)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
		_oph_ioserver_nc_clear_buffer(buff);
		free(count);
		free(start);
		free(start_pointer);
		free(sizemax);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	pmesg(LOG_INFO, __FILE__, __LINE__, "Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
#endif

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

		unsigned int *counters = (unsigned int *) malloc(ndims * sizeof(unsigned int));
		unsigned int *src_products = (unsigned int *) malloc(ndims * sizeof(unsigned));
		unsigned int *limits = (unsigned int *) malloc(ndims * sizeof(unsigned));

		int *file_indexes = (int *) malloc(ndims * sizeof(int));

		int k = 0;

		//Setup arrays for recursive selection
		for (i = 0; i < ndims; i++) {
			counters[dims_index[i]] = 0;
			src_products[dims_index[i]] = 1;
			limits[dims_index[i]] = dim_unlim_whole && (i == dim_unlim) ? dim_unlim_size : count[i];
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
					_oph_ioserver_nc_clear_buffer(buff);
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
		//Attach shared memory segment to process
		char *buffer_in = NULL, *buffer_out = NULL;
		if (_oph_ioserver_nc_get_buffer_cache(buff, &buffer_in) || _oph_ioserver_nc_get_buffer_insert(buff, &buffer_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_nc_clear_buffer(buff);
			free(count);
			free(file_indexes);
			free(counters);
			free(src_products);
			free(limits);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		oph_ioserver_nc_cache_to_buffer(ndims, counters, limits, src_products, buffer_in, buffer_out, sizeof_type);

		//Detach shared memory segment
		_oph_ioserver_nc_release_buffer_cache(buff, buffer_in);
		_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
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
	//Free only cache
	_oph_ioserver_nc_clear_buffer_cache(buff);

	int arg_count = binary_frag->field_num;
	oph_query_arg **args = (oph_query_arg **) calloc(arg_count, sizeof(oph_query_arg *));
	if (!(args)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		_oph_ioserver_nc_clear_buffer_insert(buff);
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
		_oph_ioserver_nc_clear_buffer_insert(buff);
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
			_oph_ioserver_nc_clear_buffer_insert(buff);
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

	char *buffer = NULL;
	if (_oph_ioserver_nc_get_buffer_insert(buff, &buffer)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		for (i = 0; i < arg_count; i++)
			if (args[i])
				free(args[i]);
		free(args);
		free(value_list);
		_oph_ioserver_nc_clear_buffer_insert(buff);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}

	unsigned long long ii;
	for (ii = 0; ii < tuplexfrag_number; ii++, idDim++) {

		args[measure_pos]->arg = (char *) (buffer + ii * sizeof_var);

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			_oph_ioserver_nc_release_buffer_insert(buff, buffer);
			_oph_ioserver_nc_clear_buffer_insert(buff);
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
	_oph_ioserver_nc_release_buffer_insert(buff, buffer);
	_oph_ioserver_nc_clear_buffer_insert(buff);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read_v0_n4(char is_netcdf4, char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ndims, int nimp,
				int nexp, short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number,
				int offset, oph_iostore_frag_record_set *binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, unsigned long long raw_sizeof_var, nc_type vartype,
				int id_dim_pos, int measure_pos, unsigned long long array_length, unsigned long long _array_length, int internal_size, Buffer *buff, char is_last, char *sub_operation,
				char *sub_args, void *fill_value, char check_for_reduce_func)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size
	    || !sizeof_var || !array_length || !_tuplexfrag_number || !_array_length || !buff) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_NULL_INPUT_PARAM);
		return OPH_IO_SERVER_NULL_PARAM;
	}
#ifdef DEBUG
	pmesg(LOG_INFO, __FILE__, __LINE__, "Using IMPORT algorithm v0.1\n");
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
	char transpose = !dimension_ordered;

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
	char whole_explicit = 1;
	for (i = ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			relative_rows = (int) (_tuplexfrag_number / curr_rows);
			curr_rows *= (dims_end[j] - dims_start[j] + 1);
			if (relative_rows < (dims_end[j] - dims_start[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}

	//If external explicit is not integer
	if (_tuplexfrag_number % curr_rows)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuples divided by %d\n", _tuplexfrag_number, curr_rows);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuple divided by %d\n", _tuplexfrag_number, curr_rows);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Create binary array
	long long elems = array_length * tuplexfrag_number;	// The whole buffer is created, but only a piece will used during the loop on tuples
	if (_oph_ioserver_nc_create_buffer(buff, transpose, 1, vartype, elems)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	if (check_for_reduce_func && _oph_ioserver_nc_create_support(buff, is_netcdf4, raw_sizeof_var)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
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
	char flag = 0;
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
			_oph_ioserver_nc_clear_buffer(buff);
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
	if (!check_for_reduce_func) {
		for (i = 0; i < ndims; i++)
			total *= count[i];
	} else
		total = check_for_reduce_func;

	if (total != _array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = 1 (fixed), TOTAL = %d\n", _array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = 1 (fixed), TOTAL = %d\n", _array_length, total);
		_oph_ioserver_nc_clear_buffer(buff);
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
					_oph_ioserver_nc_clear_buffer(buff);
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
			free(counters);
			free(src_products);
			free(limits);
		}
		_oph_ioserver_nc_clear_buffer(buff);
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
			free(counters);
			free(src_products);
			free(limits);
		}
		_oph_ioserver_nc_clear_buffer(buff);
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
				free(counters);
				free(src_products);
				free(limits);
			}
			_oph_ioserver_nc_clear_buffer(buff);
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
	int ncid = 0, varid = 0;

	char *buffer_in = NULL, *buffer_out = NULL, *_buffer_out = NULL;

	unsigned long long ii, tuplexfrag_number_1 = tuplexfrag_number - 1;
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
		// This version is not optimized in case the unlimited dimension is implicit!!!!! offset is set to 0 for this reason
		//Fill binary cache
		if (!ii
		    && _oph_ioserver_nc_read_data_v0(buff, 0, transpose, 1, vartype, ndims, src_path, measure_name, start, count, ncid, varid, tuplexfrag_number, idDim, nexp, sizemax, dims_type,
						     dims_index, dims_start, sub_operation, sub_args, fill_value, check_for_reduce_func)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
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
		//Attach shared memory segment to process
		if (!ii && _oph_ioserver_nc_get_buffer_insert(buff, &buffer_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		if (ii)
			_buffer_out += sizeof_var;
		else
			_buffer_out = buffer_out;

		if (transpose) {
#ifdef DEBUG
			//gettimeofday(&start_transpose_time, NULL);
#endif
			if (_oph_ioserver_nc_get_buffer_cache(buff, &buffer_in)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				_oph_ioserver_nc_clear_buffer(buff);
				for (i = 0; i < arg_count; i++)
					if (args[i])
						free(args[i]);
				free(args);
				free(value_list);
				if (transpose) {
					free(counters);
					free(src_products);
					free(limits);
				}
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				return OPH_IO_SERVER_MEMORY_ERROR;
			}

			oph_ioserver_nc_cache_to_buffer(nimp, counters, limits, src_products, buffer_in, _buffer_out, sizeof_type);

			//Detach shared memory segment
			_oph_ioserver_nc_release_buffer_cache(buff, buffer_in);
#ifdef DEBUG
			//gettimeofday(&end_transpose_time, NULL);
			//timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
			//timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif
		}

		args[measure_pos]->arg = (char *) _buffer_out;

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
			_oph_ioserver_nc_clear_buffer(buff);
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

		if (ii >= tuplexfrag_number_1)
			_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
	}
#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	//timeval_add(&total_read_time, &total_read_time, &intermediate_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
	if (transpose)
		printf("Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif

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
	_oph_ioserver_nc_clear_buffer(buff);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

// This version is not optimized in case the unlimited dimension is implicit!!!!! Use another version instead
int _oph_ioserver_nc_read_v0(char is_netcdf4, char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int ndims, int nimp, int nexp,
			     short int *dims_type, short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, int dim_unlim_size, unsigned long long _tuplexfrag_number, int offset,
			     oph_iostore_frag_record_set *binary_frag, unsigned long long *frag_size, unsigned long long sizeof_var, unsigned long long raw_sizeof_var, nc_type vartype, int id_dim_pos,
			     int measure_pos, unsigned long long array_length, unsigned long long _array_length, int internal_size, Buffer *buff, char is_last, char *sub_operation, char *sub_args,
			     void *fill_value, char check_for_reduce_func)
{
	if (!measure_name || !tuplexfrag_number || !frag_key_start || !ndims || !nimp || !nexp || !dims_type || !dims_index || !dims_start || !dims_end || !binary_frag || !frag_size
	    || !sizeof_var || !array_length || !_tuplexfrag_number || !_array_length || !buff) {
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
	char transpose = !dimension_ordered;

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
	char whole_explicit = 1;
	for (i = ndims - 1; i > most_extern_id; i--) {
		//Find dimension related to index
		for (j = 0; j < ndims; j++) {
			if (i == dims_index[j]) {
				break;
			}
		}

		//External explicit
		if (dims_type[j]) {
			relative_rows = (int) (_tuplexfrag_number / curr_rows);
			curr_rows *= (dims_end[j] - dims_start[j] + 1);
			if (relative_rows < (dims_end[j] - dims_start[j] + 1)) {
				whole_explicit = 0;
				break;
			}
		}
	}

	//If external explicit is not integer
	if (_tuplexfrag_number % curr_rows)
		whole_explicit = 0;

	if (!whole_explicit) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuples divided by %d\n", _tuplexfrag_number, curr_rows);
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to create fragment: internal explicit dimensions are fragmented: %d tuple divided by %d\n", _tuplexfrag_number, curr_rows);
		return OPH_IO_SERVER_EXEC_ERROR;
	}
	//Create binary array
	long long elems = array_length;
	if (_oph_ioserver_nc_create_buffer(buff, transpose, is_netcdf4, vartype, elems)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		return OPH_IO_SERVER_MEMORY_ERROR;
	}
	if (check_for_reduce_func && _oph_ioserver_nc_create_support(buff, is_netcdf4, raw_sizeof_var)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
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
	char flag = 0;
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
			_oph_ioserver_nc_clear_buffer(buff);
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
	if (!check_for_reduce_func) {
		for (i = 0; i < ndims; i++)
			total *= count[i];
	} else
		total = check_for_reduce_func;

	if (total != _array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = 1 (fixed), TOTAL = %d\n", _array_length, total);
		logging(LOG_ERROR, __FILE__, __LINE__, "ARRAY_LENGTH = %d, TUPLE = 1 (fixed), TOTAL = %d\n", _array_length, total);
		_oph_ioserver_nc_clear_buffer(buff);
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
					_oph_ioserver_nc_clear_buffer(buff);
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
			free(counters);
			free(src_products);
			free(limits);
		}
		_oph_ioserver_nc_clear_buffer(buff);
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
			free(counters);
			free(src_products);
			free(limits);
		}
		_oph_ioserver_nc_clear_buffer(buff);
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
				free(counters);
				free(src_products);
				free(limits);
			}
			_oph_ioserver_nc_clear_buffer(buff);
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
	int ncid = 0, varid = 0;
	if (!is_netcdf4) {
		int res = 0;
		if (pthread_mutex_lock(&nc_lock) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if ((res = nc_open(src_path, NC_NOWRITE, &ncid))) {
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(res));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to open netcdf file '%s': %s\n", src_path, nc_strerror(res));
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if (pthread_mutex_unlock(&nc_lock) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//Extract measured variable information
		if ((res = nc_inq_varid(ncid, measure_name, &varid))) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(res));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(res));
			pthread_mutex_lock(&nc_lock);
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			return OPH_IO_SERVER_EXEC_ERROR;
		}
	}

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
		// This version is not optimized in case the unlimited dimension is implicit!!!!! offset is set to 0 for this reason
		//Fill binary cache
		if (_oph_ioserver_nc_read_data_v0
		    (buff, 0, transpose, is_netcdf4, vartype, ndims, src_path, measure_name, start, count, ncid, varid, 1, idDim, nexp, sizemax, dims_type, dims_index, dims_start, sub_operation,
		     sub_args, fill_value, check_for_reduce_func)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling\n");
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (!is_netcdf4) {
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
			}
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
#ifdef DEBUG
		//gettimeofday(&end_read_time, NULL);
		//timeval_subtract(&intermediate_read_time, &end_transpose_time, &start_transpose_time);
		//timeval_add(&total_read_time, &total_read_time, &intermediate_read_time);
#endif
		//Attach shared memory segment to process
		char *buffer_in = NULL, *buffer_out = NULL;
		if (_oph_ioserver_nc_get_buffer_insert(buff, &buffer_out)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
			_oph_ioserver_nc_clear_buffer(buff);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (!is_netcdf4) {
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
			}
			return OPH_IO_SERVER_MEMORY_ERROR;
		}

		if (transpose) {
#ifdef DEBUG
			//gettimeofday(&start_transpose_time, NULL);
#endif
			if (_oph_ioserver_nc_get_buffer_cache(buff, &buffer_in)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_MEMORY_ALLOC_ERROR);
				_oph_ioserver_nc_clear_buffer(buff);
				for (i = 0; i < arg_count; i++)
					if (args[i])
						free(args[i]);
				free(args);
				free(value_list);
				if (transpose) {
					free(counters);
					free(src_products);
					free(limits);
				}
				free(start);
				free(count);
				free(start_pointer);
				free(sizemax);
				if (!is_netcdf4) {
					pthread_mutex_lock(&nc_lock);
					nc_close(ncid);
					pthread_mutex_unlock(&nc_lock);
				}
				return OPH_IO_SERVER_MEMORY_ERROR;
			}

			oph_ioserver_nc_cache_to_buffer(nimp, counters, limits, src_products, buffer_in, buffer_out, sizeof_type);

			//Detach shared memory segment
			_oph_ioserver_nc_release_buffer_cache(buff, buffer_in);
#ifdef DEBUG
			//gettimeofday(&end_transpose_time, NULL);
			//timeval_subtract(&intermediate_transpose_time, &end_transpose_time, &start_transpose_time);
			//timeval_add(&total_transpose_time, &total_transpose_time, &intermediate_transpose_time);
#endif
		}

		args[measure_pos]->arg = (char *) buffer_out;

		if (_oph_ioserver_query_build_row(arg_count, &row_size, binary_frag, binary_frag->field_name, value_list, args, &new_record)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IO_SERVER_LOG_QUERY_ROW_CREATE_ERROR);
			for (i = 0; i < arg_count; i++)
				if (args[i])
					free(args[i]);
			free(args);
			free(value_list);
			if (transpose) {
				free(counters);
				free(src_products);
				free(limits);
			}
			_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
			_oph_ioserver_nc_clear_buffer(buff);
			free(start);
			free(count);
			free(start_pointer);
			free(sizemax);
			if (!is_netcdf4) {
				pthread_mutex_lock(&nc_lock);
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
			}
			return OPH_IO_SERVER_MEMORY_ERROR;
		}
		//Add record to partial record set
		binary_frag->record_set[ii] = new_record;
		//Update current record size
		cumulative_size += row_size;

		new_record = NULL;
		row_size = 0;
		_oph_ioserver_nc_release_buffer_insert(buff, buffer_out);
	}
#ifdef DEBUG
	gettimeofday(&end_read_time, NULL);
	timeval_subtract(&total_read_time, &end_read_time, &start_read_time);
	//timeval_add(&total_read_time, &total_read_time, &intermediate_read_time);
	printf("Fragment %s:  Total read :\t Time %d,%06d sec\n", measure_name, (int) total_read_time.tv_sec, (int) total_read_time.tv_usec);
	if (transpose)
		printf("Fragment %s:  Total transpose :\t Time %d,%06d sec\n", measure_name, (int) total_transpose_time.tv_sec, (int) total_transpose_time.tv_usec);
#endif

	if (!is_netcdf4) {
		pthread_mutex_lock(&nc_lock);
		nc_close(ncid);
		pthread_mutex_unlock(&nc_lock);
	}
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
	_oph_ioserver_nc_clear_buffer(buff);

	*frag_size = cumulative_size;

	return OPH_IO_SERVER_SUCCESS;
}

int _oph_ioserver_nc_read(char *src_path, char *measure_name, unsigned long long tuplexfrag_number, long long frag_key_start, char compressed_flag, int dim_num, short int *dims_type,
			  short int *dims_index, int *dims_start, int *dims_end, int dim_unlim, char *sub_operation, char *sub_args, oph_iostore_frag_record_set *binary_frag,
			  unsigned long long *frag_size)
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

	char *pch = src_path, *save_pointer = NULL;
	while (pch && *pch) {
		if (*pch == OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR)
			src_paths_num++;
		pch++;
	}
	if ((src_paths_num > 1) && (dim_unlim < 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "An unlimited dimension must be present when multiple files are imported\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "An unlimited dimension must be present when multiple files are imported\n");
		return OPH_IO_SERVER_PARSE_ERROR;
	}

	int dim_unlim_size = dim_unlim < 0 ? 0 : dims_end[dim_unlim] - dims_start[dim_unlim] + 1;
	int _dims_start[dim_num], _dims_end[dim_num];
	size_t lenp = 0, sizeof_item = 0;

	unsigned long long _tuplexfrag_number;
	long long _frag_key_start, _f1, _f2;
	Buffer buff_, *buff = &buff_;
	_oph_ioserver_nc_init_buffer(buff);

	char src_paths[1 + strlen(src_path)];
	strcpy(src_paths, src_path);
	src_path = NULL;
	void *fill_value = NULL;

	while ((pch = strtok_r(src_path ? NULL : src_paths, OPH_QUERY_ENGINE_LANG_MULTI_VALUE_SEPARATOR2, &save_pointer))) {

		src_path = pch;

		memcpy(_dims_start, dims_start, dim_num * sizeof(int));
		memcpy(_dims_end, dims_end, dim_num * sizeof(int));

		if (!strstr(src_path, "http://") && !strstr(src_path, "https://") && !strstr(src_path, "file://") && !strstr(src_path, "s3://") && !strstr(src_path, "esdm://")) {
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
		//Extract measured variable information
		int varid = 0;
		if ((retval = nc_inq_varid(ncid, measure_name, &varid))) {
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		//Get information from id
		nc_type vartype;
		if ((retval = nc_inq_vartype(ncid, varid, &vartype))) {
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		sizeof_item = _oph_nc_sizeof(vartype);
		//Check ndims value
		int ndims;
		if ((retval = nc_inq_varndims(ncid, varid, &ndims))) {
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read variable information: %s\n", nc_strerror(retval));
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if (ndims != dim_num) {
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Dimension in variable not matching those provided in query\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}
		if (!fill_value) {
			fill_value = malloc(sizeof_item);
			if (fill_value)
				retval = nc_get_att(ncid, varid, NC_FILL_VALUE, fill_value);
			if (!fill_value || retval && (retval != NC_ENOTATT)) {
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Fill value cannot be retrieved\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Fill value cannot be retrieved\n");
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		}
#ifdef OPH_PAR_NC4
		//Read format metadata
		int format = 0;
		if ((retval = nc_inq_format(ncid, &format))) {
			nc_close(ncid);
			pthread_mutex_unlock(&nc_lock);
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to read format information: %s\n", nc_strerror(retval));
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to read format information: %s\n", nc_strerror(retval));
			return OPH_IO_SERVER_EXEC_ERROR;
		}
#endif
		int dim_id[dim_num];
		if (src_paths_num > 1) {
			if ((retval = nc_inq_vardimid(ncid, varid, dim_id))) {
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension ids\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension ids\n");
				return OPH_IO_SERVER_EXEC_ERROR;
			}
			if ((retval = nc_inq_dimlen(ncid, dim_id[dim_unlim], &lenp))) {
				nc_close(ncid);
				pthread_mutex_unlock(&nc_lock);
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension real size\n");
				logging(LOG_ERROR, __FILE__, __LINE__, "Unable to extract dimension real size\n");
				return OPH_IO_SERVER_EXEC_ERROR;
			}
		}

		nc_close(ncid);
		if (pthread_mutex_unlock(&nc_lock) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
			return OPH_IO_SERVER_EXEC_ERROR;
		}

		_tuplexfrag_number = tuplexfrag_number;
		_frag_key_start = frag_key_start;

		int internal_size = 1;
		if (src_paths_num > 1) {
			// Reduce subset to current file range
			if (_dims_start[dim_unlim] < offset)
				_dims_start[dim_unlim] = offset;
			if (_dims_end[dim_unlim] >= lenp + offset)
				_dims_end[dim_unlim] = lenp + offset - 1;
			char first = 1;
			for (i = dim_unlim + 1; i < ndims; ++i)
				if (dims_type[i] == dims_type[dim_unlim]) {
					if (first && dims_type[i])	// Don't consider the most external if explicit
						first = 0;
					else
						internal_size *= dims_end[i] - dims_start[i] + 1;
				}
			if (dims_type[dim_unlim]) {
				short int dims_value[ndims];
				for (i = 0; i < ndims; ++i)
					dims_value[dims_index[i]] = i;
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
					offset += lenp;
					k++;
					continue;
				}
			}
			// Rescaling
			_dims_start[dim_unlim] -= offset;
			_dims_end[dim_unlim] -= offset;
		}

		int check_for_reduce_func = _oph_nc_is_a_reduce_func(sub_operation, sub_args);

		//Compute array_length from implicit dims
		unsigned long long array_length = 1, _array_length, raw_array_length = 1;
		short int nimp = 0, nexp = 0;
		for (i = 0; i < ndims; i++) {
			if (!dims_type[i]) {
				if (!check_for_reduce_func)
					array_length *= dims_end[i] - dims_start[i] + 1;
				else if (check_for_reduce_func > 1)
					array_length *= check_for_reduce_func;
				raw_array_length *= dims_end[i] - dims_start[i] + 1;
				nimp++;
			} else {
				nexp++;
			}
		}
		if ((dim_unlim < 0) || dims_type[dim_unlim])
			_array_length = array_length;
		else
			_array_length = array_length * (_dims_end[dim_unlim] - _dims_start[dim_unlim] + 1) / dim_unlim_size;

		unsigned long long sizeof_var = array_length * sizeof_item;
		unsigned long long raw_sizeof_var = raw_array_length * sizeof_item;

		//Flag set to 1 if dimension are in the order specified in the file
		char dimension_ordered = 1;
		for (i = 0; i < ndims; i++) {
			if (dims_index[i] != i) {
				dimension_ordered = 0;
				break;
			}
		}

		//Apply proper format management
		char is_netcdf4 = 0;
#ifdef OPH_PAR_NC4
		switch (format) {
			case NC_FORMAT_CDF5:
			case NC_FORMAT_NETCDF4:
			case NC_FORMAT_NETCDF4_CLASSIC:
				is_netcdf4 = 1;
				break;
			case NC_FORMAT_CLASSIC:
			case NC_FORMAT_64BIT_OFFSET:
			default:
				is_netcdf4 = 0;
		}
#endif

#ifdef OPH_IO_SERVER_FORCE_V0
		if (0) {
#else
		if (dimension_ordered || check_for_reduce_func) {
#endif
			if (is_netcdf4)
				return_value =
				    _oph_ioserver_nc_read_v0_n4(is_netcdf4, src_path, measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ndims, nimp, nexp, dims_type, dims_index,
								_dims_start, _dims_end, dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, raw_sizeof_var,
								vartype, id_dim_pos, measure_pos, array_length, _array_length, internal_size, buff, k == src_paths_num, sub_operation, sub_args,
								fill_value, check_for_reduce_func);
			else
				return_value =
				    _oph_ioserver_nc_read_v0(is_netcdf4, src_path, measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ndims, nimp, nexp, dims_type, dims_index,
							     _dims_start, _dims_end, dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, raw_sizeof_var, vartype,
							     id_dim_pos, measure_pos, array_length, _array_length, internal_size, buff, k == src_paths_num, sub_operation, sub_args,
							     fill_value, check_for_reduce_func);
		} else
#ifdef OPH_IO_SERVER_NETCDF_BLOCK
			return_value =
			    _oph_ioserver_nc_read_v1(is_netcdf4, src_path, measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ndims, nimp, nexp, dims_type, dims_index, _dims_start,
						     _dims_end, dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, raw_sizeof_var, vartype, id_dim_pos,
						     measure_pos, array_length, _array_length, internal_size, buff, k == src_paths_num, sub_operation, sub_args, fill_value, check_for_reduce_func,
						     dimension_ordered);
#else
			return_value =
			    _oph_ioserver_nc_read_v2(is_netcdf4, src_path, measure_name, tuplexfrag_number, _frag_key_start, compressed_flag, ndims, nimp, nexp, dims_type, dims_index, _dims_start,
						     _dims_end, dim_unlim, dim_unlim_size, _tuplexfrag_number, offset, binary_frag, frag_size, sizeof_var, raw_sizeof_var, vartype, id_dim_pos,
						     measure_pos, array_length, _array_length, internal_size, buff, k == src_paths_num, sub_operation, sub_args, fill_value, check_for_reduce_func,
						     dimension_ordered);
#endif
		if (return_value) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while loading the file %s\n", src_path);
			logging(LOG_ERROR, __FILE__, __LINE__, "Error while loading the file %s\n", src_path);
			break;
		}
		// Update offset for the next loop
		offset += lenp;
		k++;
	}

	_oph_ioserver_nc_clear_buffer(buff);
	if (fill_value)
		free(fill_value);

	return return_value;
}
#endif

int _oph_ioserver_rand_data(long long tuplexfrag_number, long long frag_key_start, char compressed_flag, long long array_length, char *measure_type, char *algorithm,
			    oph_iostore_frag_record_set *binary_frag, unsigned long long *frag_size)
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
	char whole_fragment = ((tuplexfrag_number * sizeof_var) > memory_size ? 0 : 1);

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
