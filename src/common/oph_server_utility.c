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

#include "oph_server_utility.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <sys/sysinfo.h>

#include <debug.h>

extern int msglevel;

pthread_rwlock_t syslock = PTHREAD_RWLOCK_INITIALIZER;

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

int memory_check() 		// Check for memory swap
{
#ifndef DISABLE_MEM_CHECK
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
#endif
	return OPH_SERVER_UTIL_SUCCESS;
}
