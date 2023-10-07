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
#include "PMEMORY_device.h"

#include "oph_server_utility.h"

#include <unistd.h>
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <memkind.h>

extern struct memkind *pmem_kind;

int _pmemory_setup(oph_iostore_handler *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}
	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_cleanup(oph_iostore_handler *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}
	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_get_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_db_record_set **db_record)
{
	if (!handle || !res_id || !res_id->id || !db_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}

	*db_record = NULL;

	//Read resource id (nothing to do)
	;

	//Get in-memory copy of DB
	*db_record = (oph_iostore_db_record_set *) malloc(sizeof(oph_iostore_db_record_set));
	if (*db_record == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		return PMEMORY_DEV_ERROR;
	}

	size_t length = res_id->id_length - strlen(handle->device);
	(*db_record)->db_name = (char *) malloc(length);
	if ((*db_record)->db_name == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		free(*db_record);
		*db_record = NULL;
		return PMEMORY_DEV_ERROR;
	}
	snprintf((*db_record)->db_name, length, "%s", (char *) res_id->id);

	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_put_db(oph_iostore_handler *handle, oph_iostore_db_record_set *db_record, oph_iostore_resource_id **res_id)
{
	if (!res_id || !db_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}

	*res_id = NULL;

	//Create in-memory copy of DB (nothing to do)
	;

	//Get resource id
	*res_id = (oph_iostore_resource_id *) malloc(sizeof(oph_iostore_resource_id));
	if (*res_id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		return PMEMORY_DEV_ERROR;
	}

	(*res_id)->id_length = strlen(db_record->db_name) + strlen(handle->device) + 1;
	(*res_id)->id = (void *) calloc((*res_id)->id_length, sizeof(char));
	if ((*res_id)->id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		free(*res_id);
		*res_id = NULL;
		return PMEMORY_DEV_ERROR;
	}
	snprintf((*res_id)->id, strlen(db_record->db_name) + strlen(handle->device) + 1, "%s%s", db_record->db_name, handle->device);

	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_delete_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle || !res_id || !res_id->id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}
	//Read resource ID (nothing to do)
	;

	//Remove db (nothing to do)
	;

	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_get_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_frag_record_set **frag_record)
{
	if (!handle || !res_id || !res_id->id || !frag_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}

	*frag_record = NULL;

	//Read resource id
	oph_iostore_frag_record_set *internal_record = *((oph_iostore_frag_record_set **) res_id->id);

	//Get in-memory copy of frag
	// In the original implementation the internal_record was copied and the new copy was assigned to *frag_record
	// This behavior is skipped even for memkind
	*frag_record = internal_record;

	return PMEMORY_DEV_SUCCESS;
}

int _pmemory_put_frag(oph_iostore_handler *handle, oph_iostore_frag_record_set *frag_record, oph_iostore_resource_id **res_id)
{
	if (!handle || !res_id || !frag_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}

	*res_id = NULL;

	//Create in-memory copy of Fragment
	oph_iostore_frag_record_set *internal_record = NULL;
	// In the original implementation the frag_record was copied and the new copy was assigned to internal_record
	//internal_record = frag_record;
	// This behavior is applied for memkind
	oph_iostore_copy_frag_record_set_only2(frag_record, &internal_record, 0, 0, 1);

	//Get resource id
	*res_id = (oph_iostore_resource_id *) malloc(sizeof(oph_iostore_resource_id));
	if (*res_id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		return PMEMORY_DEV_ERROR;
	}

	(*res_id)->id = (void *) malloc(sizeof(unsigned long long));
	if ((*res_id)->id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		free(*res_id);
		*res_id = NULL;
		return PMEMORY_DEV_ERROR;
	}
	*((unsigned long long *) (*res_id)->id) = (unsigned long long) internal_record;
	(*res_id)->id_length = sizeof(unsigned long long);

	return PMEMORY_DEV_SUCCESS;
}


int _pmemory_delete_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle || !res_id || !res_id->id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_NULL_INPUT_PARAM);
		return PMEMORY_DEV_NULL_PARAM;
	}
	//Read resource id
	oph_iostore_frag_record_set *internal_record = *((oph_iostore_frag_record_set **) res_id->id);

	//Delete in-memory frag
	if (oph_iostore_destroy_frag_record_set(&internal_record)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, PMEMORY_LOG_MEMORY_ERROR);
		return PMEMORY_DEV_ERROR;
	}

	return PMEMORY_DEV_SUCCESS;
}
