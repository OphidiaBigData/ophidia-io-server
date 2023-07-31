/*
    Ophidia IO Server
    Copyright (C) 2014-2023 CMCC Foundation

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

#define OPH_MEASURE_BYTE_TYPE 			"byte"
#define OPH_MEASURE_SHORT_TYPE 			"short"
#define OPH_MEASURE_INT_TYPE 			"int"
#define OPH_MEASURE_LONG_TYPE 			"long"
#define OPH_MEASURE_FLOAT_TYPE 			"float"
#define OPH_MEASURE_DOUBLE_TYPE 			"double"
#define OPH_MEASURE_STRING_TYPE 			"string"
#define OPH_MEASURE_BIT_TYPE			"bit"
#define OPH_MEASURE_DEFAULT_TYPE			OPH_MEASURE_DOUBLE_TYPE

#define OPH_MEASURE_BYTE_FLAG			'c'
#define OPH_MEASURE_SHORT_FLAG			's'
#define OPH_MEASURE_INT_FLAG			'i'
#define OPH_MEASURE_LONG_FLAG			'l'
#define OPH_MEASURE_FLOAT_FLAG			'f'
#define OPH_MEASURE_DOUBLE_FLAG			'd'
#define OPH_MEASURE_BIT_FLAG			'b'

/**
 * \brief			        Macro used to compare two strings
 */
#define STRCMP(a,b) strncasecmp(a, b, (strlen(a) > strlen(b) ? strlen(a) : strlen(b)))


/**
 * \brief			  	Function to check and convert string with type to type flag
 * \param measure_type  String with data type to check
 * \return            	Type flag or 0 in case of error
 */
char oph_util_get_measure_type(char *measure_type);


/**
 * \brief			    Function used to build an array of random values
 * \param binary        Pointer to start of memory block to be filled with random data
 * \param array_length  Number of values to generate
 * \param type_flag     Data type for random data
 * \param rand_alg      Random generation algorithm
 * \return            	0 if successfull, non-0 otherwise
 */
int oph_util_build_rand_row(char *binary, int array_length, char type_flag, char rand_alg);

/**
 * \brief			        Function used to duplicate a generic value (similar to strndup)
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
int trim(char *string);

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

#endif				/* OPH_SERVER_UTILITY_H */
