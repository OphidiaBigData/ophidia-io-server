/*
    Ophidia IO Server
    Copyright (C) 2014-2022 CMCC Foundation

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
#include "oph_metadb_interface.h"
#include "oph_metadb_auxiliary.h"
#include "oph_metadb_log_error_codes.h"

#include <stdio.h>
#include "debug.h"
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <strings.h>

#include "oph_server_utility.h"

char db_file[OPH_SERVER_CONF_LINE_LEN] = OPH_METADB_DATABASE_SCHEMA_PREFIX;
char tmp_file[OPH_SERVER_CONF_LINE_LEN] = OPH_METADB_TEMP_SCHEMA_PREFIX;
char frag_file[OPH_SERVER_CONF_LINE_LEN] = OPH_METADB_FRAGMENT_SCHEMA_PREFIX;

void oph_metadb_set_data_prefix(char *p)
{
	snprintf(db_file, OPH_SERVER_CONF_LINE_LEN, OPH_METADB_DATABASE_SCHEMA, p);
	snprintf(frag_file, OPH_SERVER_CONF_LINE_LEN, OPH_METADB_FRAGMENT_SCHEMA, p);
	snprintf(tmp_file, OPH_SERVER_CONF_LINE_LEN, OPH_METADB_TEMP_SCHEMA, p);
}

static unsigned int oph_metadb_hash_function(const char *key)
{
	/* djb2 hash function - Adapted from http://www.cse.yorku.ca/~oz/hash.html */
	unsigned int hash = 5381;
	while (*key)
		hash = ((hash << 5) + hash) + (unsigned int) *key++;

	return hash;
}

oph_metadb_frag_table *oph_metadb_frag_table_create(int size)
{
	oph_metadb_frag_table *table = (oph_metadb_frag_table *) malloc(sizeof(oph_metadb_frag_table));
	if (!table)
		return NULL;

	if (!(table->rows = (oph_metadb_frag_row **) calloc(size, sizeof(oph_metadb_frag_row *)))) {
		free(table);
		return NULL;
	}

	table->size = size;

	return table;
}

int oph_metadb_frag_table_destroy(oph_metadb_frag_table * table)
{
	if (!table) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	int i;
	oph_metadb_frag_row *curr_row, *tmp_row;

	for (i = 0; i < table->size; i++) {
		curr_row = (table->rows)[i];
		while (curr_row) {
			tmp_row = (oph_metadb_frag_row *) curr_row->next_frag;
			oph_metadb_cleanup_frag_struct(curr_row);
			curr_row = tmp_row;
		}
	}
	free(table->rows);
	free(table);

	return OPH_METADB_OK;
}


int oph_metadb_setup_db_struct(char *db_name, char *device, short unsigned int is_persistent, oph_iostore_resource_id * db_id, unsigned long long frag_number, oph_metadb_db_row ** db)
{
	if (!db || !db_name || !device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	oph_metadb_db_row *tmp_row = (oph_metadb_db_row *) malloc(1 * sizeof(oph_metadb_db_row));
	if (!tmp_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Alloc blocks
	tmp_row->db_name = (char *) calloc(strlen(db_name) + 1, sizeof(char));
	if (!tmp_row->db_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	tmp_row->device = (char *) calloc(strlen(device) + 1, sizeof(char));
	if (!tmp_row->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row->db_name);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}

	if (db_id->id_length == 0 || db_id->id == NULL) {
		tmp_row->db_id.id = NULL;
		tmp_row->db_id.id_length = 0;
	} else {
		tmp_row->db_id.id = (void *) calloc(db_id->id_length, sizeof(char));
		if (!tmp_row->db_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->db_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
		tmp_row->db_id.id_length = db_id->id_length;
		memcpy(tmp_row->db_id.id, db_id->id, db_id->id_length);
	}

	memcpy(tmp_row->db_name, db_name, strlen(db_name));
	memcpy(tmp_row->device, device, strlen(device));
	tmp_row->table = NULL;
	tmp_row->next_db = NULL;
	tmp_row->file_offset = 0;
	tmp_row->frag_number = frag_number;
	tmp_row->is_persistent = is_persistent;

	*db = tmp_row;

	return OPH_METADB_OK;
}

int oph_metadb_cleanup_db_struct(oph_metadb_db_row * db)
{
	if (db) {
		if (db->db_name)
			free(db->db_name);
		if (db->device)
			free(db->device);
		if (db->db_id.id)
			free(db->db_id.id);
		free(db);
		db = NULL;
	}

	return OPH_METADB_OK;
}

int oph_metadb_setup_frag_struct(char *frag_name, char *device, short unsigned int is_persistent, oph_iostore_resource_id * db_id, oph_iostore_resource_id * frag_id, unsigned long long frag_size,
				 oph_metadb_frag_row ** frag)
{
	if (!frag || !frag_name || !device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	oph_metadb_frag_row *tmp_row = (oph_metadb_frag_row *) malloc(1 * sizeof(oph_metadb_frag_row));
	if (!tmp_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Alloc blocks
	tmp_row->frag_name = (char *) calloc(strlen(frag_name) + 1, sizeof(char));
	if (!tmp_row->frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	tmp_row->device = (char *) calloc(strlen(device) + 1, sizeof(char));
	if (!tmp_row->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row->frag_name);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}

	if (db_id->id_length == 0 || db_id->id == NULL) {
		tmp_row->db_id.id = NULL;
		tmp_row->db_id.id_length = 0;
	} else {
		tmp_row->db_id.id = (void *) calloc(db_id->id_length, sizeof(char));
		if (!tmp_row->db_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->frag_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
		tmp_row->db_id.id_length = db_id->id_length;
		memcpy(tmp_row->db_id.id, db_id->id, db_id->id_length);
	}

	if (frag_id->id_length == 0 || frag_id->id == NULL) {
		tmp_row->frag_id.id = NULL;
		tmp_row->frag_id.id_length = 0;
	} else {
		tmp_row->frag_id.id = (void *) calloc(frag_id->id_length, sizeof(char));
		if (!tmp_row->frag_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->db_id.id);
			free(tmp_row->frag_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
		tmp_row->frag_id.id_length = frag_id->id_length;
		memcpy(tmp_row->frag_id.id, frag_id->id, frag_id->id_length);
	}

	memcpy(tmp_row->frag_name, frag_name, strlen(frag_name));
	memcpy(tmp_row->device, device, strlen(device));
	tmp_row->db_ptr = NULL;
	tmp_row->next_frag = NULL;
	tmp_row->file_offset = 0;
	tmp_row->frag_size = frag_size;
	tmp_row->is_persistent = is_persistent;

	*frag = tmp_row;

	return OPH_METADB_OK;
}

int oph_metadb_cleanup_frag_struct(oph_metadb_frag_row * frag)
{
	if (frag) {
		if (frag->frag_name)
			free(frag->frag_name);
		if (frag->device)
			free(frag->device);
		if (frag->db_id.id)
			free(frag->db_id.id);
		if (frag->frag_id.id)
			free(frag->frag_id.id);
		free(frag);
		frag = NULL;
	}

	return OPH_METADB_OK;
}

//Db schema is the head of the db record linked list (items are added at the head of the stack from below),
//fragments list are associated to each Db item (even in this case items are added as head on the bottom of the stack)
int oph_metadb_load_schema(oph_metadb_db_row ** meta_db, short unsigned int cleanup)
{
	if (!meta_db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Create file if it not exist
	if (_oph_metadb_create_file(db_file)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_CREATE_ERROR, db_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_CREATE_ERROR, db_file);
		return OPH_METADB_IO_ERR;
	}
	if (_oph_metadb_create_file(frag_file)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_CREATE_ERROR, frag_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_CREATE_ERROR, frag_file);
		return OPH_METADB_IO_ERR;
	}
	//Run delete procedure to ensure that files are clean (if cleanup flag is setted)
	if (cleanup) {
		if (_oph_metadb_delete_procedure(db_file, 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DEL_PROC_ERROR, db_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DEL_PROC_ERROR, db_file);
			return OPH_METADB_IO_ERR;
		}
		if (_oph_metadb_delete_procedure(frag_file, 1)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DEL_PROC_ERROR, frag_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DEL_PROC_ERROR, frag_file);
			return OPH_METADB_IO_ERR;
		}
	}
	//Load DB schema table
	oph_metadb_db_row *curr_db_row = NULL;
	oph_metadb_db_row *prev_db_row = NULL;
	char *line = NULL;
	unsigned int length = 0;
	unsigned long long tot_records = 0;
	unsigned long long curr_offset = 0;

	if (_oph_metadb_count_records(db_file, &tot_records)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_COUNT_RECORDS_ERROR, db_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_COUNT_RECORDS_ERROR, db_file);
		return OPH_METADB_IO_ERR;
	}
	//Build array of DB records
	*meta_db = NULL;

	unsigned long long i, j;
	for (i = 0; i < tot_records; i++) {
		//Read current record
		line = NULL;
		length = 0;
		if (_oph_metadb_read_row(db_file, curr_offset, &line, &length)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_READ_RECORD_ERROR, db_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_READ_RECORD_ERROR, db_file);
			oph_metadb_unload_schema(*meta_db);
			*meta_db = NULL;
			return OPH_METADB_IO_ERR;
		}
		//Line is not readable
		if (line != NULL) {
			//Deserialize record
			curr_db_row = NULL;
			if (_oph_metadb_deserialize_db_row(line, &curr_db_row)) {
				free(line);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DESERIAL_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DESERIAL_RECORD_ERROR);
				oph_metadb_unload_schema(*meta_db);
				*meta_db = NULL;
				return OPH_METADB_IO_ERR;
			}
			free(line);

			curr_db_row->file_offset = curr_offset;
			curr_db_row->table = NULL;
			if (prev_db_row != NULL) {
				curr_db_row->next_db = (struct oph_metadb_db_row *) prev_db_row;
			} else {
				curr_db_row->next_db = NULL;
			}

			*meta_db = curr_db_row;
			prev_db_row = curr_db_row;
		}
		curr_offset += (OPH_METADB_HEADER_LENGTH + length);
	}

	//Load Frag schema table
	oph_metadb_frag_row *curr_frag_row = NULL, *tmp_row = NULL;
	oph_metadb_db_row *db_row = NULL;
	curr_offset = 0;
	tot_records = 0;
	int hash = 0;

	if (_oph_metadb_count_records(frag_file, &tot_records)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_COUNT_RECORDS_ERROR, frag_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_COUNT_RECORDS_ERROR, frag_file);
		oph_metadb_unload_schema(*meta_db);
		*meta_db = NULL;
		return OPH_METADB_IO_ERR;
	}

	for (i = 0; i < tot_records; i++) {
		//Read current record
		line = NULL;
		length = 0;
		if (_oph_metadb_read_row(frag_file, curr_offset, &line, &length)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_READ_RECORD_ERROR, frag_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_READ_RECORD_ERROR, frag_file);
			oph_metadb_unload_schema(*meta_db);
			*meta_db = NULL;
			return OPH_METADB_IO_ERR;
		}
		//Line is not readable
		if (line != NULL) {

			//Deserialize record
			curr_frag_row = NULL;
			if (_oph_metadb_deserialize_frag_row(line, &curr_frag_row)) {
				free(line);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DESERIAL_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DESERIAL_RECORD_ERROR);
				oph_metadb_unload_schema(*meta_db);
				*meta_db = NULL;
				return OPH_METADB_IO_ERR;
			}
			free(line);

			curr_frag_row->file_offset = curr_offset;
			curr_frag_row->next_frag = NULL;
			curr_frag_row->db_ptr = NULL;

			//Find db ptr and next frag ptr
			j = 0;
			db_row = *meta_db;
			while (db_row != NULL) {
				if (oph_iostore_compare_id(db_row->db_id, curr_frag_row->db_id) == 0 && STRCMP(db_row->device, curr_frag_row->device) == 0) {
					curr_frag_row->db_ptr = db_row;
					//Update hash table

					if (db_row->table == NULL) {
						//Create hash table
						db_row->table = (oph_metadb_frag_table *) oph_metadb_frag_table_create(OPH_METADB_FRAG_TABLE_SIZE);
						if (db_row->table == NULL) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
							oph_metadb_cleanup_frag_struct(curr_frag_row);
							oph_metadb_unload_schema(*meta_db);
							*meta_db = NULL;
							return OPH_METADB_MEMORY_ERR;
						}
					}

					hash = oph_metadb_hash_function(curr_frag_row->frag_name) % db_row->table->size;

					tmp_row = db_row->table->rows[hash];
					while (tmp_row) {
						if (STRCMP(tmp_row->frag_name, curr_frag_row->frag_name) == 0) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_DUPLICATE_ERROR, curr_frag_row->frag_name);
							logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_DUPLICATE_ERROR, curr_frag_row->frag_name);
							oph_metadb_cleanup_frag_struct(curr_frag_row);
							oph_metadb_unload_schema(*meta_db);
							*meta_db = NULL;
							return OPH_METADB_IO_ERR;
						}
						tmp_row = tmp_row->next_frag;
					}

					curr_frag_row->next_frag = (struct oph_metadb_frag_row *) db_row->table->rows[hash];
					//Update db head pointer
					db_row->table->rows[hash] = (struct oph_metadb_frag_row *) curr_frag_row;
					break;
				}
				j++;
				db_row = (oph_metadb_db_row *) (db_row->next_db);
				//If even last db does not match, then exit with error
				if (db_row == NULL) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_FRAG_MATCH_ERROR, curr_frag_row->frag_name);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_FRAG_MATCH_ERROR, curr_frag_row->frag_name);
					oph_metadb_cleanup_frag_struct(curr_frag_row);
					oph_metadb_unload_schema(*meta_db);
					*meta_db = NULL;
					return OPH_METADB_IO_ERR;
				}
			}
		}
		curr_offset += (OPH_METADB_HEADER_LENGTH + length);
	}

	return OPH_METADB_OK;
}

int oph_metadb_unload_schema(oph_metadb_db_row * meta_db)
{
	oph_metadb_db_row *tmp_db_row = NULL;

	if (meta_db) {
		while (meta_db) {
			if (meta_db->table != NULL)
				oph_metadb_frag_table_destroy(meta_db->table);
			meta_db->table = NULL;
			tmp_db_row = (oph_metadb_db_row *) meta_db->next_db;
			oph_metadb_cleanup_db_struct(meta_db);
			meta_db = tmp_db_row;
		}
	}

	return OPH_METADB_OK;
}

//NOTE: new database are added as head of stack from its lower side
int oph_metadb_add_db(oph_metadb_db_row ** meta_db, oph_metadb_db_row * db)
{
	if (!meta_db || !db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	if (db->table || db->next_db || db->file_offset) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_ZEROED_WARN);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_ZEROED_WARN);
	}

	oph_metadb_db_row *db_row = NULL;
	if (oph_metadb_setup_db_struct(db->db_name, db->device, db->is_persistent, &(db->db_id), db->frag_number, &db_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_RECORD_COPY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_RECORD_COPY_ERROR);
		return OPH_METADB_DATA_ERR;
	}
	//Check if DB name already exists
	oph_metadb_db_row *tmp_row = NULL;
	if (*meta_db != NULL) {
		//If meta_db is not empty find record
		if (oph_metadb_find_db(*meta_db, db_row->db_name, db_row->device, &tmp_row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db_row->db_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db_row->db_name);
			return OPH_METADB_IO_ERR;
		}
		if (tmp_row != NULL) {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_EXIST_ERROR);
			logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_EXIST_ERROR);
			oph_metadb_cleanup_db_struct(db_row);
			return OPH_METADB_OK;
		}
	}
	//Count bytes in file
	unsigned long long byte_size = 0;
	if (_oph_metadb_count_bytes(db_file, &byte_size)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SIZE_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SIZE_ERROR);
		oph_metadb_cleanup_db_struct(db_row);
		return OPH_METADB_IO_ERR;
	}
	//Insert in file
	char *line = NULL;
	unsigned int length = 0;
	if (_oph_metadb_serialize_db_row(db_row, &line, &length)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
		oph_metadb_cleanup_db_struct(db_row);
		return OPH_METADB_IO_ERR;
	}
	//Append row
	if (_oph_metadb_write_row(line, length, db_row->is_persistent, db_file, 0, 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
		free(line);
		return OPH_METADB_IO_ERR;
	}
	free(line);

	//Insert new DB into stack
	db_row->file_offset = byte_size;
	db_row->next_db = (struct oph_metadb_db_row *) *meta_db;
	//Update meta_Db head pointer
	*meta_db = db_row;

	return OPH_METADB_OK;
}

int oph_metadb_update_db(oph_metadb_db_row * meta_db, oph_metadb_db_row * db)
{
	if (!meta_db || !db || !db->db_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	if (db->table || db->next_db || db->file_offset || db->db_id.id) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_ZEROED_WARN);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_ZEROED_WARN);
	}
	//Check if DB name exists
	oph_metadb_db_row *tmp_row = NULL;
	if (meta_db != NULL) {
		//If meta_db is not empty find record
		if (oph_metadb_find_db(meta_db, db->db_name, db->device, &tmp_row) || tmp_row == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db->db_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db->db_name);
			return OPH_METADB_IO_ERR;
		} else {
			//Update file
			char *line = NULL;
			unsigned int length = 0;
			//Update meta_db
			tmp_row->frag_number = db->frag_number;

			if (_oph_metadb_serialize_db_row(tmp_row, &line, &length)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
				return OPH_METADB_IO_ERR;
			}
			//Append row
			if (_oph_metadb_write_row(line, length, tmp_row->is_persistent, db_file, tmp_row->file_offset, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
				free(line);
				return OPH_METADB_IO_ERR;
			}
			free(line);

			return OPH_METADB_OK;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db->db_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND, db->db_name);
		return OPH_METADB_IO_ERR;
	}

	return OPH_METADB_OK;
}

int oph_metadb_remove_db(oph_metadb_db_row ** meta_db, char *db_name, char *device)
{
	if (!meta_db || !*meta_db || !db_name || !device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Find DB 
	oph_metadb_db_row *tmp_row = *meta_db;
	oph_metadb_db_row *prev_row = NULL;

	while (tmp_row) {
		if (STRCMP(tmp_row->db_name, db_name) == 0 && STRCMP(tmp_row->device, device) == 0) {
			if (tmp_row->table) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_NON_EMPTY_DB, db_name);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_NON_EMPTY_DB, db_name);
				return OPH_METADB_DATA_ERR;
			}
			//Delete row
			if (_oph_metadb_remove_row(db_file, tmp_row->file_offset)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_RECORD_ERROR, db_file);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_RECORD_ERROR, db_file);
				return OPH_METADB_IO_ERR;
			}
			//Remove row
			if (prev_row != NULL) {
				//If not first record
				prev_row->next_db = tmp_row->next_db;
			} else {
				//If first record
				*meta_db = (oph_metadb_db_row *) tmp_row->next_db;
			}
			oph_metadb_cleanup_db_struct(tmp_row);

			break;
		}
		prev_row = tmp_row;
		tmp_row = (oph_metadb_db_row *) tmp_row->next_db;
	}
	if (tmp_row == NULL) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_REMOVE_NOT_FOUND, db_name);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_REMOVE_NOT_FOUND, db_name);
	}

	return OPH_METADB_OK;
}

int oph_metadb_find_db(oph_metadb_db_row * meta_db, char *db_name, char *device, oph_metadb_db_row ** db)
{
	if (!meta_db || !db) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	*db = NULL;

	//Find DB in stack struct
	oph_metadb_db_row *tmp_row = meta_db;
	oph_metadb_db_row *found_row = NULL;

	if (db_name && device) {
		//If db is set
		while (tmp_row) {
			if (STRCMP(tmp_row->db_name, db_name) == 0 && STRCMP(tmp_row->device, device) == 0) {
				found_row = tmp_row;
				break;
			}
			tmp_row = (oph_metadb_db_row *) tmp_row->next_db;
		}
	} else {
		//Get first db
		found_row = tmp_row;
	}

	if (tmp_row == NULL) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_NOT_FOUND, db_name);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_DB_RECORD_NOT_FOUND, db_name);
		return OPH_METADB_OK;
	}
	//Return found row
	*db = found_row;

	return OPH_METADB_OK;
}

int oph_metadb_add_frag(oph_metadb_db_row * db, oph_metadb_frag_row * frag)
{
	if (!db || !frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	if (frag->db_ptr || frag->next_frag || db->file_offset) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_ZEROED_WARN);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_ZEROED_WARN);
	}
	//Simple check to verify DB/Frag matching
	if (oph_iostore_compare_id(db->db_id, frag->db_id) == 1 || STRCMP(db->device, frag->device) == 1 || db->is_persistent != frag->is_persistent) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_DB_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_RECORD_COPY_ERROR);
		return OPH_METADB_IO_ERR;
	}

	oph_metadb_frag_row *frag_row = NULL;
	if (oph_metadb_setup_frag_struct(frag->frag_name, frag->device, frag->is_persistent, &(frag->db_id), &(frag->frag_id), frag->frag_size, &frag_row)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_RECORD_COPY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_RECORD_COPY_ERROR);
		return OPH_METADB_DATA_ERR;
	}
	//Check if Frag name already exists in given DB
	oph_metadb_frag_row *tmp_row = NULL;
	if (db->table != NULL) {
		//If frag list is not empty find record
		if (oph_metadb_find_frag(db, frag_row->frag_name, &tmp_row)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag_row->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag_row->frag_name);
			return OPH_METADB_IO_ERR;
		}
		if (tmp_row != NULL) {
			pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_EXIST_ERROR);
			logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_EXIST_ERROR);
			oph_metadb_cleanup_frag_struct(frag_row);
			return OPH_METADB_OK;
		}
	}
	//Count bytes in file
	unsigned long long byte_size = 0;
	if (_oph_metadb_count_bytes(frag_file, &byte_size)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SIZE_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SIZE_ERROR);
		oph_metadb_cleanup_frag_struct(frag_row);
		return OPH_METADB_IO_ERR;
	}
	//Insert in file
	char *line = NULL;
	unsigned int length = 0;
	if (_oph_metadb_serialize_frag_row(frag_row, &line, &length)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
		oph_metadb_cleanup_frag_struct(frag_row);
		return OPH_METADB_IO_ERR;
	}
	//Append row
	if (_oph_metadb_write_row(line, length, frag_row->is_persistent, frag_file, 0, 1)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
		free(line);
		return OPH_METADB_IO_ERR;
	}
	free(line);

	if (db->table == NULL) {
		//Create hash table
		db->table = oph_metadb_frag_table_create(OPH_METADB_FRAG_TABLE_SIZE);
		if (db->table == NULL) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			return OPH_METADB_MEMORY_ERR;
		}
	}
	//Insert new Frag into stack
	frag_row->file_offset = byte_size;
	frag_row->db_ptr = db;

	int hash = oph_metadb_hash_function(frag->frag_name) % db->table->size;
	frag_row->next_frag = (struct oph_metadb_frag_row *) db->table->rows[hash];
	//Update db head pointer
	db->table->rows[hash] = (struct oph_metadb_frag_row *) frag_row;

	return OPH_METADB_OK;
}

int oph_metadb_remove_frag(oph_metadb_db_row * db, char *frag_name, oph_iostore_resource_id * frag_id)
{
	if (!db || !frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	oph_metadb_frag_row *tmp_row = NULL, *prev_row = NULL;

	if (db->table != NULL) {
		//Find Frag 
		int hash = oph_metadb_hash_function(frag_name) % db->table->size;

		tmp_row = db->table->rows[hash];
		while (tmp_row) {
			if (STRCMP(tmp_row->frag_name, frag_name) == 0) {
				//Delete row
				if (_oph_metadb_remove_row(frag_file, tmp_row->file_offset)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_RECORD_ERROR, frag_file);
					logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_REMOVE_RECORD_ERROR, frag_file);
					return OPH_METADB_IO_ERR;
				}
				//Remove row
				if (prev_row != NULL) {
					//If not first record
					prev_row->next_frag = tmp_row->next_frag;
				} else {
					//If first record
					(db->table->rows)[hash] = tmp_row->next_frag;
				}

				if (frag_id) {
					//Recover resource id
					frag_id->id = (void *) memdup(tmp_row->frag_id.id, tmp_row->frag_id.id_length);
					frag_id->id_length = tmp_row->frag_id.id_length;
				}
				oph_metadb_cleanup_frag_struct(tmp_row);

				break;
			}
			prev_row = tmp_row;
			tmp_row = (oph_metadb_frag_row *) tmp_row->next_frag;
		}

	}

	if (tmp_row == NULL) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_REMOVE_NOT_FOUND, frag_name);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_REMOVE_NOT_FOUND, frag_name);
	}

	return OPH_METADB_OK;
}

int oph_metadb_find_frag(oph_metadb_db_row * db, char *frag_name, oph_metadb_frag_row ** frag)
{
	if (!db || !frag) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	*frag = NULL;

	oph_metadb_frag_row *tmp_row = NULL, *found_row = NULL;

	if (db->table != NULL) {
		//Find Frag in DB stack struct
		if (frag_name) {
			//If frag is set
			int hash = oph_metadb_hash_function(frag_name) % db->table->size;
			tmp_row = (oph_metadb_frag_row *) db->table->rows[hash];

			while (tmp_row) {
				if (STRCMP(tmp_row->frag_name, frag_name) == 0) {
					found_row = tmp_row;
					break;
				}
				tmp_row = (oph_metadb_frag_row *) tmp_row->next_frag;
			}
		} else {
			//Get first frag
			int i;
			for (i = 0; i < db->table->size; i++) {
				tmp_row = db->table->rows[i];
				if (tmp_row) {
					found_row = tmp_row;
					break;
				}
			}
		}
	}
	if (tmp_row == NULL) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_NOT_FOUND, frag_name);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_NOT_FOUND, frag_name);
		return OPH_METADB_OK;
	}
	//Return found row
	*frag = found_row;

	return OPH_METADB_OK;
}

int oph_metadb_update_frag(oph_metadb_db_row * db, oph_metadb_frag_row * frag)
{
	if (!db || !frag->frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	if (frag->db_ptr || frag->next_frag || frag->file_offset || frag->db_id.id || frag->frag_id.id) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_ZEROED_WARN);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_ZEROED_WARN);
	}
	//Check if Frag name exists
	oph_metadb_frag_row *tmp_row = NULL;
	if (db->table != NULL) {
		//If frag list is not empty find record
		if (oph_metadb_find_frag(db, frag->frag_name, &tmp_row) || !tmp_row) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag->frag_name);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag->frag_name);
			return OPH_METADB_IO_ERR;
		} else {
			//Update file
			char *line = NULL;
			unsigned int length = 0;
			//Update meta_db
			tmp_row->frag_size = frag->frag_size;

			if (_oph_metadb_serialize_frag_row(tmp_row, &line, &length)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_SERIAL_RECORD_ERROR);
				return OPH_METADB_IO_ERR;
			}
			//Append row
			if (_oph_metadb_write_row(line, length, tmp_row->is_persistent, frag_file, tmp_row->file_offset, 0)) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_WRITE_RECORD_ERROR);
				free(line);
				return OPH_METADB_IO_ERR;
			}
			free(line);

			return OPH_METADB_OK;
		}
	} else {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag->frag_name);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND, frag->frag_name);
		return OPH_METADB_IO_ERR;
	}

	return OPH_METADB_OK;
}
