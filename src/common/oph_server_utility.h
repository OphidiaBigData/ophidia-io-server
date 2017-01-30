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

#ifndef OPH_SERVER_UTILITY_H
#define OPH_SERVER_UTILITY_H

#include <unistd.h>

#define OPH_SERVER_UTIL_SUCCESS                             0
#define OPH_SERVER_UTIL_NULL_PARAM                          1
#define OPH_SERVER_UTIL_ERROR                               2

#define UNUSED(x) {(void)(x);}
#define OPH_MIN_MEMORY 1073741824
#define OPH_MIN_MEMORY_PERC 0.1

#define OPH_NAME_ID "id_dim"
#define OPH_NAME_MEASURE "measure"

/**
 * \brief			        Macro used to compare two strings
 */
#define STRCMP(a,b) strncasecmp(a, b, (strlen(a) > strlen(b) ? strlen(a) : strlen(b)))

/**
 * \brief			        Function to used to duplicate a generic value (similar to strndup)
 * \param src         Pointer to memory block to be duplicated
 * \param n           Size of memory block to be duplicated
 * \return            Pointer to duplicate area or NULL if an error occured
 */
void *memdup(const void *src, size_t n);

/**
 * \brief			        Function to trim initial and trailing spaces from a string
 * \param string      String to trim
 * \return            0 if successfull, non-0 otherwise
 */
int trim(char * string);

/**
 * \brief			        This function checks if the binary array contains numeric string chars ([0-9...]).
 * \param array_length Binary array length
 * \param array       Binary array to be verified
 * \param is_string   Flag setted to 1 if array is a string
 * \return            0 if successfull, non-0 otherwise
 */
int is_numeric_string(int array_length, char *array, int *is_string);

/**
 * \brief			        This function checks if available memory is enough to process data.
 * \return            0 if successfull, non-0 otherwise
 */
int memory_check();

#endif /* OPH_SERVER_UTILITY_H */
