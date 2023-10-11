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

#define _GNU_SOURCE

#include "oph_iostorage_data.h"
#include "oph_iostorage_log_error_codes.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <ctype.h>

#include <debug.h>

#include <errno.h>

#include "oph_server_utility.h"

extern int msglevel;

#ifdef OPH_IO_PMEM
#include <memkind.h>
extern struct memkind *pmem_kind;
#endif

int oph_iostore_compare_id(oph_iostore_resource_id id1, oph_iostore_resource_id id2)
{
	if (!id1.id && !id2.id) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_SUCCESS;
	}

	if (id1.id_length != id2.id_length)
		return OPH_IOSTORAGE_INVALID_PARAM;

	int i;
	char *i1, *i2;
	i1 = (void *) id1.id;
	i2 = (void *) id2.id;
	for (i = 0; i < id1.id_length; i++) {
		if (i1[i] != i2[i])
			return OPH_IOSTORAGE_INVALID_PARAM;
	}

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_copy_frag_record(oph_iostore_frag_record *input_record, unsigned short input_field_num, oph_iostore_frag_record **output_record)
{
	if (!input_record || !input_field_num || !output_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
#ifdef OPH_IO_PMEM
	if (input_record->is_pmem)
		*output_record = (oph_iostore_frag_record *) memkind_malloc(pmem_kind, sizeof(oph_iostore_frag_record));
	else
#endif
		*output_record = (oph_iostore_frag_record *) malloc(sizeof(oph_iostore_frag_record));
	if (!*output_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*output_record)->field = NULL;
#ifdef OPH_IO_PMEM
	(*output_record)->is_pmem = input_record->is_pmem;
	if ((*output_record)->is_pmem) {
		(*output_record)->field_length = (unsigned long long *) memkind_malloc(pmem_kind, input_field_num * sizeof(unsigned long long));
		memcpy((*output_record)->field_length, input_record->field_length, input_field_num * sizeof(unsigned long long));
	} else
#endif
		(*output_record)->field_length = (unsigned long long *) memdup(input_record->field_length, input_field_num * sizeof(unsigned long long));
	if (!(*output_record)->field_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record(output_record, input_field_num);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
#ifdef OPH_IO_PMEM
	if ((*output_record)->is_pmem)
		(*output_record)->field = (void **) memkind_calloc(pmem_kind, input_field_num, sizeof(void *));
	else
#endif
		(*output_record)->field = (void **) calloc(input_field_num, sizeof(void *));
	if (!(*output_record)->field) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record(output_record, input_field_num);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	unsigned short i = 0;

	for (i = 0; i < input_field_num; i++) {
#ifdef OPH_IO_PMEM
		if ((*output_record)->is_pmem) {
			(*output_record)->field[i] = (unsigned long long *) memkind_malloc(pmem_kind, input_record->field_length[i] * sizeof(char));
			memcpy((*output_record)->field[i], input_record->field[i], input_record->field_length[i] * sizeof(char));
		} else
#endif
			(*output_record)->field[i] = (unsigned long long *) memdup(input_record->field[i], input_record->field_length[i] * sizeof(char));
		if (!(*output_record)->field[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record(output_record, input_field_num);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
	}

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_copy_frag_record_set(oph_iostore_frag_record_set *input_record_set, oph_iostore_frag_record_set **output_record_set)
{
	return oph_iostore_copy_frag_record_set_limit(input_record_set, output_record_set, 0, 0);
}

int oph_iostore_copy_frag_record_set_limit2(oph_iostore_frag_record_set *input_record_set, oph_iostore_frag_record_set **output_record_set, long long limit, long long offset, char force_pmem)
{
	if (!input_record_set || !output_record_set || (limit < 0) || (offset < 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}

	if (oph_iostore_copy_frag_record_set_only2(input_record_set, output_record_set, limit, offset, force_pmem)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(output_record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	long long j, total_size = 0, set_size = 0;
	oph_iostore_frag_record *tmp_record = NULL;
	if (offset) {
		tmp_record = input_record_set->record_set[0];
		while (tmp_record)
			tmp_record = input_record_set->record_set[++total_size];
	}
	if (!offset || (offset < total_size)) {
		tmp_record = input_record_set->record_set[offset];
		while (tmp_record && (!limit || (set_size < limit)))
			tmp_record = input_record_set->record_set[++set_size];
	}

	if (set_size != 0) {
		oph_iostore_frag_record **new = (*output_record_set)->record_set;
		for (j = 0; j < set_size; j++) {
			if (oph_iostore_copy_frag_record(input_record_set->record_set[j], input_record_set->field_num, &new[j]) || !new[j]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				oph_iostore_destroy_frag_record_set(output_record_set);
				return OPH_IOSTORAGE_MEMORY_ERR;
			}
		}
		new[j] = NULL;
	}
	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_copy_frag_record_set_limit(oph_iostore_frag_record_set *input_record_set, oph_iostore_frag_record_set **output_record_set, long long limit, long long offset)
{
	return oph_iostore_copy_frag_record_set_limit2(input_record_set, output_record_set, limit, offset, 0);
}

int oph_iostore_copy_frag_record_set_only2(oph_iostore_frag_record_set *input_record_set, oph_iostore_frag_record_set **output_record_set, long long limit, long long offset, char force_pmem)
{
	if (!input_record_set || !output_record_set || (limit < 0) || (offset < 0)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
	//Copy frag record set content
#ifdef OPH_IO_PMEM
	char is_pmem = force_pmem > 1 ? 1 : input_record_set->is_pmem;
	if (is_pmem)
		*output_record_set = (oph_iostore_frag_record_set *) memkind_malloc(pmem_kind, sizeof(oph_iostore_frag_record_set));
	else
#endif
		*output_record_set = (oph_iostore_frag_record_set *) malloc(sizeof(oph_iostore_frag_record_set));
	if (!*output_record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*output_record_set)->frag_name = NULL;
	(*output_record_set)->field_num = input_record_set->field_num;
	(*output_record_set)->field_type = NULL;
	(*output_record_set)->record_set = NULL;

#ifdef OPH_IO_PMEM
	(*output_record_set)->is_pmem = is_pmem;
	if (is_pmem) {
		(*output_record_set)->frag_name = (char *) memkind_calloc(pmem_kind, 1 + strlen(input_record_set->frag_name), sizeof(char));
		memcpy((*output_record_set)->frag_name, input_record_set->frag_name, strlen(input_record_set->frag_name));
	}
#endif

#ifdef OPH_IO_PMEM
	if (is_pmem)
		(*output_record_set)->field_name = (char **) memkind_calloc(pmem_kind, input_record_set->field_num, sizeof(char *));
	else
#endif
		(*output_record_set)->field_name = (char **) calloc(input_record_set->field_num, sizeof(char *));
	if (!(*output_record_set)->field_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(output_record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	unsigned long long i;
	for (i = 0; i < input_record_set->field_num; i++) {
#ifdef OPH_IO_PMEM
		if (is_pmem) {
			(*output_record_set)->field_name[i] = (char *) memkind_malloc(pmem_kind, 1 + strlen(input_record_set->field_name[i]));
			strcpy((*output_record_set)->field_name[i], input_record_set->field_name[i]);
		} else
#endif
			(*output_record_set)->field_name[i] = (char *) strdup(input_record_set->field_name[i]);
		if (!(*output_record_set)->field_name[i]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(output_record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
	}

#ifdef OPH_IO_PMEM
	if (is_pmem) {
		(*output_record_set)->field_type = (oph_iostore_field_type *) memkind_malloc(pmem_kind, input_record_set->field_num * sizeof(oph_iostore_field_type));
		memcpy((*output_record_set)->field_type, input_record_set->field_type, input_record_set->field_num * sizeof(oph_iostore_field_type));
	} else
#endif
		(*output_record_set)->field_type = (oph_iostore_field_type *) memdup(input_record_set->field_type, input_record_set->field_num * sizeof(oph_iostore_field_type));
	if (!(*output_record_set)->field_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(output_record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	long long total_size = 0, set_size = 0;
	oph_iostore_frag_record *tmp_record = NULL;
	if (offset) {
		tmp_record = input_record_set->record_set[0];
		while (tmp_record)
			tmp_record = input_record_set->record_set[++total_size];
	}
	if (!offset || (offset < total_size)) {
		tmp_record = input_record_set->record_set[offset];
		while (tmp_record && (!limit || (set_size < limit)))
			tmp_record = input_record_set->record_set[++set_size];
	}

	if (set_size != 0) {
#ifdef OPH_IO_PMEM
		if (is_pmem)
			(*output_record_set)->record_set = (oph_iostore_frag_record **) memkind_calloc(pmem_kind, set_size + 1, sizeof(oph_iostore_frag_record *));
		else
#endif
			(*output_record_set)->record_set = (oph_iostore_frag_record **) calloc(set_size + 1, sizeof(oph_iostore_frag_record *));
		if (!(*output_record_set)->record_set) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(output_record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
	}
	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_copy_frag_record_set_only(oph_iostore_frag_record_set *input_record_set, oph_iostore_frag_record_set **output_record_set, long long limit, long long offset)
{
	return oph_iostore_copy_frag_record_set_only2(input_record_set, output_record_set, limit, offset, 0);
}

int oph_iostore_create_frag_record2(oph_iostore_frag_record **record, short int field_num, char is_pmem)
{
	if (!record || !field_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
#ifdef OPH_IO_PMEM
	if (is_pmem)
		*record = (oph_iostore_frag_record *) memkind_malloc(pmem_kind, sizeof(oph_iostore_frag_record));
	else
#endif
		*record = (oph_iostore_frag_record *) malloc(sizeof(oph_iostore_frag_record));
	if (!*record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record)->field_length = NULL;
	(*record)->field = NULL;
#ifdef OPH_IO_PMEM
	(*record)->is_pmem = is_pmem;
	if (is_pmem)
		(*record)->field_length = (unsigned long long *) memkind_calloc(pmem_kind, field_num, sizeof(unsigned long long));
	else
#endif
		(*record)->field_length = (unsigned long long *) calloc(field_num, sizeof(unsigned long long));
	if (!(*record)->field_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record(record, field_num);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
#ifdef OPH_IO_PMEM
	if (is_pmem)
		(*record)->field = (void **) memkind_calloc(pmem_kind, field_num, sizeof(void *));
	else
#endif
		(*record)->field = (void **) calloc(field_num, sizeof(void *));
	if (!(*record)->field) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record(record, field_num);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_create_frag_record(oph_iostore_frag_record **record, short int field_num)
{
	return oph_iostore_create_frag_record2(record, field_num, 0);
}


int oph_iostore_destroy_frag_record(oph_iostore_frag_record **record, short int field_num)
{
	if (!*record || !field_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}

	long long j = 0;

	for (j = 0; j < field_num; j++) {
		if ((*record)->field[j]) {
#ifdef OPH_IO_PMEM
			if ((*record)->is_pmem)
				memkind_free(pmem_kind, (*record)->field[j]);
			else
#endif
				free((*record)->field[j]);
		}
	}
#ifdef OPH_IO_PMEM
	if ((*record)->is_pmem)
		memkind_free(pmem_kind, (*record)->field);
	else
#endif
		free((*record)->field);
	(*record)->field = NULL;
#ifdef OPH_IO_PMEM
	if ((*record)->is_pmem)
		memkind_free(pmem_kind, (*record)->field_length);
	else
#endif
		free((*record)->field_length);
	(*record)->field_length = NULL;
#ifdef OPH_IO_PMEM
	if ((*record)->is_pmem)
		memkind_free(pmem_kind, *record);
	else
#endif
		free(*record);
	*record = NULL;

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_destroy_frag_record_set(oph_iostore_frag_record_set **record_set)
{
	if (!*record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}

	long long i = 0;

	if ((*record_set)->record_set != NULL) {
		while ((*record_set)->record_set[i]) {
			oph_iostore_destroy_frag_record(&(*record_set)->record_set[i], (*record_set)->field_num);
			i++;
		}
	}

	oph_iostore_destroy_frag_record_set_only(record_set);

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_destroy_frag_record_set_only(oph_iostore_frag_record_set **record_set)
{
	if (!*record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}

	long long j = 0;

	if ((*record_set)->record_set) {
#ifdef OPH_IO_PMEM
		if ((*record_set)->is_pmem)
			memkind_free(pmem_kind, (*record_set)->record_set);
		else
#endif
			free((*record_set)->record_set);
		(*record_set)->record_set = NULL;
	}

	if ((*record_set)->frag_name) {
#ifdef OPH_IO_PMEM
		if ((*record_set)->is_pmem)
			memkind_free(pmem_kind, (*record_set)->frag_name);
		else
#endif
			free((*record_set)->frag_name);
		(*record_set)->frag_name = NULL;
	}

	if ((*record_set)->field_name) {
		for (j = 0; j < (*record_set)->field_num; j++) {
			if ((*record_set)->field_name[j]) {
#ifdef OPH_IO_PMEM
				if ((*record_set)->is_pmem)
					memkind_free(pmem_kind, (*record_set)->field_name[j]);
				else
#endif
					free((*record_set)->field_name[j]);
			}
		}
#ifdef OPH_IO_PMEM
		if ((*record_set)->is_pmem)
			memkind_free(pmem_kind, (*record_set)->field_name);
		else
#endif
			free((*record_set)->field_name);
		(*record_set)->field_name = NULL;
	}

	if ((*record_set)->field_type) {
#ifdef OPH_IO_PMEM
		if ((*record_set)->is_pmem)
			memkind_free(pmem_kind, (*record_set)->field_type);
		else
#endif
			free((*record_set)->field_type);
		(*record_set)->field_type = NULL;
	}
#ifdef OPH_IO_PMEM
	if ((*record_set)->is_pmem)
		memkind_free(pmem_kind, *record_set);
	else
#endif
		free(*record_set);
	*record_set = NULL;

	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_create_frag_record_set_only2(oph_iostore_frag_record_set **record_set, long long set_size, short int field_num, char is_pmem)
{
	if (!record_set || !field_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
	//Create a null-terminated record set of set_size+1 records (last one is NULL)
#ifdef OPH_IO_PMEM
	if (is_pmem)
		*record_set = (oph_iostore_frag_record_set *) memkind_malloc(pmem_kind, sizeof(oph_iostore_frag_record_set));
	else
#endif
		*record_set = (oph_iostore_frag_record_set *) malloc(sizeof(oph_iostore_frag_record_set));
	if (!*record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->frag_name = NULL;
	(*record_set)->field_num = field_num;
	(*record_set)->field_type = NULL;
	(*record_set)->record_set = NULL;
	(*record_set)->tmp_flag = 0;
#ifdef OPH_IO_PMEM
	(*record_set)->is_pmem = is_pmem;
	if (is_pmem)
		(*record_set)->field_name = (char **) memkind_calloc(pmem_kind, field_num, sizeof(char *));
	else
#endif
		(*record_set)->field_name = (char **) calloc(field_num, sizeof(char *));
	if (!(*record_set)->field_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
#ifdef OPH_IO_PMEM
	if (is_pmem)
		(*record_set)->field_type = (oph_iostore_field_type *) memkind_calloc(pmem_kind, field_num, sizeof(oph_iostore_field_type));
	else
#endif
		(*record_set)->field_type = (oph_iostore_field_type *) calloc(field_num, sizeof(oph_iostore_field_type));
	if (!(*record_set)->field_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	if (set_size != 0) {
#ifdef OPH_IO_PMEM
		if (is_pmem)
			(*record_set)->record_set = (oph_iostore_frag_record **) memkind_calloc(pmem_kind, set_size + 1, sizeof(oph_iostore_frag_record *));
		else
#endif
			(*record_set)->record_set = (oph_iostore_frag_record **) calloc(set_size + 1, sizeof(oph_iostore_frag_record *));
		if (!(*record_set)->record_set) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
	}
	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_create_frag_record_set_only(oph_iostore_frag_record_set **record_set, long long set_size, short int field_num)
{
	return oph_iostore_create_frag_record_set_only2(record_set, set_size, field_num, 0);
}

int oph_iostore_create_frag_record_set2(oph_iostore_frag_record_set **record_set, long long set_size, short int field_num, char is_pmem)
{
	if (!record_set || !field_num) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
	//Create a null-terminated record set of set_size+1 records (last one is NULL)
	if (oph_iostore_create_frag_record_set_only2(record_set, set_size, field_num, is_pmem)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	if (set_size != 0) {
		oph_iostore_frag_record **new = (*record_set)->record_set;
		long long i = 0;
		for (i = 0; i < set_size; i++) {
			if (oph_iostore_create_frag_record2(&new[i], field_num, is_pmem) || !new[i]) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				oph_iostore_destroy_frag_record_set(record_set);
				return OPH_IOSTORAGE_MEMORY_ERR;
			}
		}
		new[i] = NULL;
	}
	return OPH_IOSTORAGE_SUCCESS;
}

int oph_iostore_create_frag_record_set(oph_iostore_frag_record_set **record_set, long long set_size, short int field_num)
{
	return oph_iostore_create_frag_record_set2(record_set, set_size, field_num, 0);
}

// Note: PMEMORY is not supported for the following function
int oph_iostore_create_sample_frag(const long long row_number, const long long array_length, oph_iostore_frag_record_set **record_set)
{
	if (!record_set || !row_number || !array_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return OPH_IOSTORAGE_NULL_PARAM;
	}
	//Create a null-terminated record set of set_size+1 records (last one is NULL)
	*record_set = (oph_iostore_frag_record_set *) malloc(sizeof(oph_iostore_frag_record_set));
	if (!*record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->frag_name = NULL;
	(*record_set)->field_num = 2;
	(*record_set)->field_type = NULL;
	(*record_set)->record_set = NULL;
	(*record_set)->field_name = (char **) calloc(2, sizeof(char *));
	if (!(*record_set)->field_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->field_name[0] = strdup(OPH_NAME_ID);
	if (!(*record_set)->field_name[0]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->field_name[1] = strdup(OPH_NAME_MEASURE);
	if (!(*record_set)->field_name[1]) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->field_type = (oph_iostore_field_type *) calloc(2, sizeof(oph_iostore_field_type));
	if (!(*record_set)->field_type) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	(*record_set)->field_type[0] = OPH_IOSTORE_LONG_TYPE;
	(*record_set)->field_type[1] = OPH_IOSTORE_STRING_TYPE;
	(*record_set)->record_set = (oph_iostore_frag_record **) calloc(row_number + 1, sizeof(oph_iostore_frag_record *));
	if (!(*record_set)->record_set) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		oph_iostore_destroy_frag_record_set(record_set);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	oph_iostore_frag_record **rs = (*record_set)->record_set;
	struct timeval time;
	gettimeofday(&time, NULL);
	srand(time.tv_sec * 1000000 + time.tv_usec);
	long long r, a;
	double *measure = NULL;
	for (r = 0; r < row_number; r++) {
		rs[r] = (oph_iostore_frag_record *) malloc(sizeof(oph_iostore_frag_record));
		if (!rs[r]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
		rs[r]->field_length = NULL;
		rs[r]->field = NULL;
		rs[r]->field_length = (unsigned long long *) calloc(2, sizeof(unsigned long long));
		if (!rs[r]->field_length) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
		rs[r]->field = (void **) calloc(2, sizeof(void *));
		if (!rs[r]->field) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}

		rs[r]->field_length[0] = sizeof(unsigned long long);
		rs[r]->field_length[1] = array_length * sizeof(double);

		rs[r]->field[0] = (void *) memdup(&r, sizeof(unsigned long long));
		if (!rs[r]->field[0]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
		rs[r]->field[1] = (void *) calloc(rs[r]->field_length[1], sizeof(char));
		if (!rs[r]->field[1]) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
			oph_iostore_destroy_frag_record_set(record_set);
			return OPH_IOSTORAGE_MEMORY_ERR;
		}
		//Fill array  
		measure = (double *) rs[r]->field[1];
		for (a = 0; a < array_length; a++)
			measure[a] = ((double) rand() / RAND_MAX) * 1000.0;
	}
	rs[row_number] = NULL;

	return OPH_IOSTORAGE_SUCCESS;
}
