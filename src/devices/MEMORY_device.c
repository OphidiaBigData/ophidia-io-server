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
#include "MEMORY_device.h"

#include "oph_server_utility.h"

#include <unistd.h>
#include "debug.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>


int _memory_setup(oph_iostore_handler *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}
	return MEMORY_DEV_SUCCESS;
}

int _memory_cleanup(oph_iostore_handler *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}
	return MEMORY_DEV_SUCCESS;
}

int _memory_get_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_db_record_set **db_record)
{
	if (!handle || !res_id || !res_id->id || !db_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}

	*db_record = NULL;

	//Read resource id (nothing to do)
	;

	//Get in-memory copy of DB
	*db_record = (oph_iostore_db_record_set *) malloc(1 * sizeof(oph_iostore_db_record_set));
	if (*db_record == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		return MEMORY_DEV_ERROR;
	}

	(*db_record)->db_name = (char *) strndup(res_id->id, res_id->id_length - strlen(handle->device) - 1);
	if ((*db_record)->db_name == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		free(*db_record);
		*db_record = NULL;
		return MEMORY_DEV_ERROR;
	}

	return MEMORY_DEV_SUCCESS;
}

int _memory_put_db(oph_iostore_handler *handle, oph_iostore_db_record_set *db_record, oph_iostore_resource_id **res_id)
{
	if (!res_id || !db_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}

	*res_id = NULL;

	//Create in-memory copy of DB (nothing to do)
	;

	//Get resource id
	*res_id = (oph_iostore_resource_id *) malloc(1 * sizeof(oph_iostore_resource_id));
	if (*res_id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		return MEMORY_DEV_ERROR;
	}

	(*res_id)->id_length = strlen(db_record->db_name) + strlen(handle->device) + 1;
	(*res_id)->id = (void *) calloc((*res_id)->id_length, sizeof(char));
	if ((*res_id)->id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		free(*res_id);
		*res_id = NULL;
		return MEMORY_DEV_ERROR;
	}
	snprintf((*res_id)->id, strlen(db_record->db_name) + strlen(handle->device) + 1, "%s%s", db_record->db_name, handle->device);

	return MEMORY_DEV_SUCCESS;
}

int _memory_delete_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle || !res_id || !res_id->id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}
	//Read resource ID (nothing to do)
	;

	//Remove db (nothing to do)
	;

	return MEMORY_DEV_SUCCESS;
}

int _memory_get_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_frag_record_set **frag_record)
{
	if (!handle || !res_id || !res_id->id || !frag_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}

	*frag_record = NULL;

	//Read resource id
	oph_iostore_frag_record_set *internal_record = *((oph_iostore_frag_record_set **) res_id->id);

	//Get in-memory copy of frag
/*  if(_memory_copy_frag_record_set(internal_record, frag_record) || frag_record == NULL){
    pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
    logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);        
    return MEMORY_DEV_ERROR;
  }*/
	*frag_record = internal_record;

	return MEMORY_DEV_SUCCESS;
}

int _memory_put_frag(oph_iostore_handler *handle, oph_iostore_frag_record_set *frag_record, oph_iostore_resource_id **res_id)
{
	if (!handle || !res_id || !frag_record) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}

	*res_id = NULL;

	//Create in-memory copy of Fragment
	oph_iostore_frag_record_set *internal_record = NULL;
/*  if(_memory_copy_frag_record_set(frag_record, &internal_record) || internal_record == NULL){
    pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
    logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);        
    return MEMORY_DEV_ERROR;
  }*/
	internal_record = frag_record;

	//Get resource id
	*res_id = (oph_iostore_resource_id *) malloc(1 * sizeof(oph_iostore_resource_id));
	if (*res_id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		return MEMORY_DEV_ERROR;
	}

	unsigned long long addr = (unsigned long long) internal_record;
	(*res_id)->id_length = sizeof(unsigned long long);
	(*res_id)->id = (void *) memdup(&addr, sizeof(unsigned long long));
	if ((*res_id)->id == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		free(*res_id);
		*res_id = NULL;
		return MEMORY_DEV_ERROR;
	}

	return MEMORY_DEV_SUCCESS;
}


int _memory_delete_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle || !res_id || !res_id->id) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_NULL_INPUT_PARAM);
		return MEMORY_DEV_NULL_PARAM;
	}
	//Read resource id
	oph_iostore_frag_record_set *internal_record = *((oph_iostore_frag_record_set **) res_id->id);

	//Delete in-memory frag
	if (oph_iostore_destroy_frag_recordset(&internal_record)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, MEMORY_LOG_MEMORY_ERROR);
		return MEMORY_DEV_ERROR;
	}

	return MEMORY_DEV_SUCCESS;
}
