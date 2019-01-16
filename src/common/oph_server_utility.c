/*
    Ophidia IO Server
    Copyright (C) 2014-2019 CMCC Foundation

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

#include "oph_server_utility.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <math.h>

#include "oph-lib-binary-io.h"

#include <debug.h>
#include <sys/time.h>

extern int msglevel;
extern unsigned short disable_mem_check;

pthread_rwlock_t syslock = PTHREAD_RWLOCK_INITIALIZER;

char oph_util_get_measure_type(char *measure_type)
{
	if (!measure_type)
		return 0;

	if (!strcasecmp(measure_type, OPH_MEASURE_BYTE_TYPE))
		return OPH_MEASURE_BYTE_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_SHORT_TYPE))
		return OPH_MEASURE_SHORT_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_INT_TYPE))
		return OPH_MEASURE_INT_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_LONG_TYPE))
		return OPH_MEASURE_LONG_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_FLOAT_TYPE))
		return OPH_MEASURE_FLOAT_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_DOUBLE_TYPE))
		return OPH_MEASURE_DOUBLE_FLAG;
	else if (!strcasecmp(measure_type, OPH_MEASURE_BIT_TYPE))
		return OPH_MEASURE_BIT_FLAG;
	return 0;
}

int oph_util_build_rand_row(char *binary, int array_length, char type_flag, char rand_alg)
{

	if (!binary || !array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_SERVER_UTIL_NULL_PARAM;
	}

	int m = 0, res = 0;

	struct timeval time;
	struct drand48_data buffer;
	double val, rand_mes;
	gettimeofday(&time, NULL);
	srand48_r((long int) time.tv_sec * 1000000 + time.tv_usec, &buffer);

	if (type_flag == OPH_MEASURE_BYTE_FLAG) {
		char measure_b;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_b = (char) ceil(val * 1000.0);
				res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_b = (char) ceil(rand_mes);
			res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_b = (char) ceil(rand_mes);
				res = oph_iob_bin_array_add_b(binary, &measure_b, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_SHORT_FLAG) {
		short measure_s;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_s = (short) ceil(val * 1000.0);
				res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_s = (short) ceil(rand_mes);
			res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_s = (short) ceil(rand_mes);
				res = oph_iob_bin_array_add_s(binary, &measure_s, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_INT_FLAG) {
		int measure_i;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_i = (int) ceil(val * 1000.0);
				res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_i = (int) ceil(rand_mes);
			res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_i = (int) ceil(rand_mes);
				res = oph_iob_bin_array_add_i(binary, &measure_i, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_LONG_FLAG) {
		long long measure_l;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_l = (long long) ceil(val * 1000.0);
				res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_l = (long long) ceil(rand_mes);
			res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_l = (long long) ceil(rand_mes);
				res = oph_iob_bin_array_add_l(binary, &measure_l, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_FLOAT_FLAG) {
		float measure_f;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_f = (float) val *1000.0;
				res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_f = (float) rand_mes;
			res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_f = (float) rand_mes;
				res = oph_iob_bin_array_add_f(binary, &measure_f, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_DOUBLE_FLAG) {
		double measure_d;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_d = val * 1000.0;
				res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_d = rand_mes;
			res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_d = rand_mes;
				res = oph_iob_bin_array_add_d(binary, &measure_d, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	} else if (type_flag == OPH_MEASURE_BIT_FLAG) {
		char measure_c;
		if (rand_alg == 0) {
			for (m = 0; m < array_length; m++) {
				drand48_r(&buffer, &val);
				measure_c = (char) ceil(val * 1000.0);
				res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		} else {
			drand48_r(&buffer, &val);
			rand_mes = val * 40.0 - 5.0;
			measure_c = (char) ceil(rand_mes);
			res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) 0);

			if (res) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
				return OPH_SERVER_UTIL_ERROR;
			}

			for (m = 1; m < array_length; m++) {
				drand48_r(&buffer, &val);
				rand_mes = rand_mes * 0.9 + 0.1 * (val * 40.0 - 5.0);
				measure_c = (char) ceil(rand_mes);
				res = oph_iob_bin_array_add_c(binary, &measure_c, (long long) m);

				if (res) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error in binary array filling: %d\n", res);
					return OPH_SERVER_UTIL_ERROR;
				}
			}
		}
	}

	return OPH_SERVER_UTIL_SUCCESS;
}

void *memdup(const void *src, size_t n)
{
	void *dst = NULL;

	if (NULL == src || n <= 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return dst;
	}

	dst = malloc(n);
	if (NULL != dst)
		memcpy(dst, src, n);

	return dst;
}

int trim(char *string)
{
	if (!string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_SERVER_UTIL_NULL_PARAM;
	}

	char *p = string;
	int l = strlen(p);

	//Trim trailing spaces
	while (isspace(p[l - 1]))
		p[--l] = 0;
	//Trim initial spaces
	while (*p && isspace(*p))
		++p, --l;

	memmove(string, p, l + 1);
	return OPH_SERVER_UTIL_SUCCESS;
}

//This function works on not null-terminated strings. It simply checks it the string contains only allowed chars ([0-9...])
int is_numeric_string(int array_length, char *array, int *is_string)
{
	if (!array_length || !array || !is_string) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Null input parameter\n");
		return OPH_SERVER_UTIL_NULL_PARAM;
	}

	*is_string = 0;

	//Set of allowed charachters
	char allowed_chars[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', ',', '.', '-', 'e', 'x', ' ' };
	int i, j;
	int len = sizeof(allowed_chars);

	//Check if each charachter is admissible
	for (i = 0; i < array_length; i++) {
		//Check if charachter is in set
		for (j = 0; j < len; j++) {
			if (array[i] == allowed_chars[j])
				break;
		}
		//If charachter is not in set
		if (j == len) {
			*is_string = 0;
			return OPH_SERVER_UTIL_SUCCESS;
		}
	}

	//If each charachter has passed test
	*is_string = 1;
	return OPH_SERVER_UTIL_SUCCESS;
}

int memory_check()		// Check for memory swap
{
	if (!disable_mem_check) {
		struct sysinfo info;

		if (pthread_rwlock_wrlock(&syslock))
			return OPH_SERVER_UTIL_ERROR;

		if (sysinfo(&info)) {
			pthread_rwlock_unlock(&syslock);
			return OPH_SERVER_UTIL_ERROR;
		}

		if (pthread_rwlock_unlock(&syslock))
			return OPH_SERVER_UTIL_ERROR;

		unsigned long long min_free_mem = (unsigned long long) (OPH_MIN_MEMORY_PERC * (info.totalram < OPH_MIN_MEMORY ? OPH_MIN_MEMORY : info.totalram));

		if ((info.freeram + info.bufferram < min_free_mem)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Out of memory\n");
			return OPH_SERVER_UTIL_ERROR;
		}
	}
	return OPH_SERVER_UTIL_SUCCESS;
}
