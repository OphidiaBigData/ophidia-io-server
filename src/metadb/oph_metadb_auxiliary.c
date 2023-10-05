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
#include "oph_metadb_auxiliary.h"
#include "oph_metadb_log_error_codes.h"

#include <stdio.h>
#include "debug.h"
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/file.h>

/*
SERIALIZATION DEFINITION:
RECORD_LENGTH - ACTIVE FLAG - PERSISTENT FLAG - SERIALIZED RECORD
*/
extern char tmp_file[OPH_SERVER_CONF_LINE_LEN];

//SERIALIZE STRUCTURES
int _oph_metadb_serialize_db_row(oph_metadb_db_row *row, char **line, unsigned int *line_length)
{
	if (!row || !line || !line_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Set line to NULL
	*line = NULL;
	*line_length = 0;

	//Count buffer length as sum of each struct member
	unsigned int length = 5 * sizeof(unsigned int) + strlen(row->db_name) + strlen(row->device) + sizeof(char) + (row->db_id).id_length + sizeof(row->frag_number);
	char *buffer = (char *) calloc(length, sizeof(char));
	if (!buffer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Store lenghts in buffer
	unsigned int m = 0;
	unsigned int single_length = 0;

	single_length = strlen(row->db_name);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = strlen(row->device);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = sizeof(char);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = (row->db_id).id_length;
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = sizeof(row->frag_number);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);

	//Store data
	memcpy(buffer + m, (void *) row->db_name, strlen(row->db_name));
	m += strlen(row->db_name);
	memcpy(buffer + m, (void *) row->device, strlen(row->device));
	m += strlen(row->device);
	char persist = (row->is_persistent ? 1 : 0);
	memcpy(buffer + m, (void *) &persist, sizeof(char));
	m += sizeof(char);
	if ((row->db_id).id_length != 0) {
		memcpy(buffer + m, (void *) row->db_id.id, (row->db_id).id_length);
	}
	m += (row->db_id).id_length;
	memcpy(buffer + m, (void *) &row->frag_number, sizeof(row->frag_number));
	m += sizeof(row->frag_number);

	*line = buffer;
	*line_length = length;

	return OPH_METADB_OK;
}

int _oph_metadb_serialize_frag_row(oph_metadb_frag_row *row, char **line, unsigned int *line_length)
{
	if (!row || !line || !line_length) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Set line to NULL
	*line = NULL;
	*line_length = 0;

	//Count buffer length as sum of each struct member
	unsigned int length = 6 * sizeof(unsigned int) + strlen(row->frag_name) + (row->frag_id).id_length + strlen(row->device) + sizeof(char) + (row->db_id).id_length + sizeof(row->frag_size);
	char *buffer = (char *) calloc(length, sizeof(char));
	if (!buffer) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Store lenghts in buffer
	unsigned int m = 0;
	unsigned int single_length = 0;

	single_length = strlen(row->frag_name);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = (row->frag_id).id_length;
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = strlen(row->device);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = sizeof(char);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = (row->db_id).id_length;
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);
	single_length = sizeof(row->frag_size);
	memcpy(buffer + m, (void *) &single_length, sizeof(unsigned int));
	m += sizeof(unsigned int);

	//Store data
	memcpy(buffer + m, (void *) row->frag_name, strlen(row->frag_name));
	m += strlen(row->frag_name);
	if ((row->frag_id).id_length != 0) {
		memcpy(buffer + m, (void *) &(row->frag_id).id, (row->frag_id).id_length);
	}
	m += (row->frag_id).id_length;
	memcpy(buffer + m, (void *) row->device, strlen(row->device));
	m += strlen(row->device);
	char persist = (row->is_persistent ? 1 : 0);
	memcpy(buffer + m, (void *) &persist, sizeof(char));
	m += sizeof(char);
	if ((row->db_id).id_length != 0) {
		memcpy(buffer + m, (void *) row->db_id.id, (row->db_id).id_length);
	}
	m += (row->db_id).id_length;
	memcpy(buffer + m, (void *) &row->frag_size, sizeof(row->frag_size));
	m += sizeof(row->frag_size);

	*line = buffer;
	*line_length = length;

	return OPH_METADB_OK;
}

//DESERIALIZE BUFFER
int _oph_metadb_deserialize_db_row(char *line, oph_metadb_db_row **row)
{
	if (!row || !line) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Set row to NULL
	*row = NULL;

	oph_metadb_db_row *tmp_row = (oph_metadb_db_row *) malloc(1 * sizeof(oph_metadb_db_row));
	if (!tmp_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Get lenghts from buffer
	unsigned int m = 0;
	unsigned int db_name_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int device_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int persistent_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int db_id_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int frag_number_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);

	//Alloc blocks
	tmp_row->db_name = (char *) calloc(db_name_len + 1, sizeof(char));
	if (!tmp_row->db_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	tmp_row->device = (char *) calloc(device_len + 1, sizeof(char));
	if (!tmp_row->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row->db_name);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	if (db_id_len > 0) {
		tmp_row->db_id.id = (void *) calloc(db_id_len, sizeof(char));
		if (!tmp_row->db_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->db_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
	} else
		tmp_row->db_id.id = NULL;
	tmp_row->db_id.id_length = db_id_len;
	tmp_row->frag_number = 0;

	//Save data
	memcpy(tmp_row->db_name, (void *) (line + m), db_name_len);
	m += db_name_len;
	memcpy(tmp_row->device, (void *) (line + m), device_len);
	m += device_len;
	tmp_row->is_persistent = *((char *) (line + m));
	m += persistent_len;
	if (db_id_len > 0) {
		memcpy(tmp_row->db_id.id, (void *) (line + m), db_id_len);
	}
	m += db_id_len;
	tmp_row->frag_number = *((unsigned long long *) (line + m));
	m += frag_number_len;

	*row = tmp_row;

	return OPH_METADB_OK;
}

int _oph_metadb_deserialize_frag_row(char *line, oph_metadb_frag_row **row)
{
	if (!row || !line) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}
	//Set row to NULL
	*row = NULL;

	oph_metadb_frag_row *tmp_row = (oph_metadb_frag_row *) malloc(1 * sizeof(oph_metadb_frag_row));
	if (!tmp_row) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		return OPH_METADB_MEMORY_ERR;
	}
	//Get lenghts from buffer
	unsigned int m = 0;
	unsigned int frag_name_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int frag_id_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int device_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int persistent_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int db_id_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);
	unsigned int frag_size_len = *((unsigned int *) (line + m));
	m += sizeof(unsigned int);

	//Alloc blocks
	tmp_row->frag_name = (char *) calloc(frag_name_len + 1, sizeof(char));
	if (!tmp_row->frag_name) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	tmp_row->device = (char *) calloc(device_len + 1, sizeof(char));
	if (!tmp_row->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		free(tmp_row->frag_name);
		free(tmp_row);
		return OPH_METADB_MEMORY_ERR;
	}
	if (db_id_len > 0) {
		tmp_row->db_id.id = (void *) calloc(db_id_len, sizeof(char));
		if (!tmp_row->db_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->frag_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
	} else
		tmp_row->db_id.id = NULL;
	tmp_row->db_id.id_length = db_id_len;
	tmp_row->frag_id.id = (void *) calloc(frag_id_len, sizeof(char));
	if (frag_id_len > 0) {
		if (!tmp_row->frag_id.id) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
			free(tmp_row->db_id.id);
			free(tmp_row->frag_name);
			free(tmp_row->device);
			free(tmp_row);
			return OPH_METADB_MEMORY_ERR;
		}
	} else
		tmp_row->frag_id.id = NULL;
	tmp_row->frag_id.id_length = frag_id_len;
	tmp_row->frag_size = 0;

	//Save data
	memcpy(tmp_row->frag_name, (void *) (line + m), frag_name_len);
	m += frag_name_len;
	if (frag_id_len > 0) {
		memcpy(tmp_row->frag_id.id, (void *) (line + m), frag_id_len);
	}
	m += frag_id_len;
	memcpy(tmp_row->device, (void *) (line + m), device_len);
	m += device_len;
	tmp_row->is_persistent = *((char *) (line + m));
	m += persistent_len;
	if (db_id_len > 0) {
		memcpy(tmp_row->db_id.id, (void *) (line + m), db_id_len);
	}
	m += db_id_len;
	tmp_row->frag_size = *((unsigned long long *) (line + m));
	m += frag_size_len;

	*row = tmp_row;

	return OPH_METADB_OK;
}

//NOTE: a row is composed by RECORD LENGTH - ACTIVE FLAG - PERSISTEN FLAG - RECORD
int _oph_metadb_write_row(char *line, unsigned int line_length, unsigned short int persistent_flag, char *schema_file, unsigned long long file_offset, unsigned short int append_flag)
{
	if (!line || !line_length || !schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	FILE *fp = fopen(schema_file, "r+b");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	flock(fd, LOCK_EX);
	if (append_flag == 0) {
		if (fseek(fp, file_offset, SEEK_SET)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			flock(fd, LOCK_UN);
			fclose(fp);
			return OPH_METADB_IO_ERR;
		}
	} else {
		if (fseek(fp, 0, SEEK_END)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			flock(fd, LOCK_UN);
			fclose(fp);
			return OPH_METADB_IO_ERR;
		}
	}


	char active_f = 1;
	char persistent_f = (persistent_flag ? 1 : 0);

	//Write Length - active flag - persistent flag and buffer line to file
	fwrite(&line_length, sizeof(unsigned int), 1, fp);
	fwrite(&active_f, sizeof(char), 1, fp);
	fwrite(&persistent_f, sizeof(char), 1, fp);
	if (line_length <= 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_WRITE_ERROR, line_length, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_WRITE_ERROR, line_length, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	fwrite(line, sizeof(char), line_length, fp);

	flock(fd, LOCK_UN);
	fclose(fp);

	return OPH_METADB_OK;
}

//NOTE: flag a row as removed
int _oph_metadb_remove_row(char *schema_file, unsigned long long file_offset)
{
	if (!schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	FILE *fp = fopen(schema_file, "r+b");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	flock(fd, LOCK_EX);
	if (fseek(fp, file_offset + sizeof(unsigned int), SEEK_SET)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	char flag = 0;

	//Write active flag to file
	fwrite(&flag, sizeof(char), 1, fp);

	flock(fd, LOCK_UN);
	fclose(fp);

	return OPH_METADB_OK;
}


//NOTE: a row is composed by RECORD LENGTH - ACTIVE FLAG - RECORD
int _oph_metadb_read_row(char *schema_file, unsigned long long file_offset, char **line, unsigned int *line_length)
{
	if (!line || !line_length || !schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	FILE *fp = fopen(schema_file, "rb");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	flock(fd, LOCK_EX);
	if (fseek(fp, file_offset, SEEK_SET)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	//Read Length, active flag and buffer line from file
	unsigned int tmp_len = 0;
	char tmp_active_f = 1;
	char tmp_persistent_f = 0;


	if (fread(&tmp_len, sizeof(unsigned int), 1, fp) != 1 || fread(&tmp_active_f, sizeof(char), 1, fp) != 1 || fread(&tmp_persistent_f, sizeof(char), 1, fp) != 1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, sizeof(unsigned int), schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, sizeof(unsigned int), schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}


	if (tmp_active_f == 0) {
		pmesg(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FILE_DEL_READ_ERROR, schema_file);
		logging(LOG_DEBUG, __FILE__, __LINE__, OPH_METADB_LOG_FILE_DEL_READ_ERROR, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		*line_length = tmp_len;
		*line = NULL;
		return OPH_METADB_OK;
	}
	if (tmp_len <= 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_len, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_len, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	char *tmp_line = (char *) malloc(tmp_len * sizeof(char));
	if (!tmp_line) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_MEMORY_ERR;
	}

	if (fread(tmp_line, sizeof(char), tmp_len, fp) != tmp_len) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_len, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_len, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	flock(fd, LOCK_UN);
	fclose(fp);

	*line_length = tmp_len;
	*line = tmp_line;

	return OPH_METADB_OK;
}

//NOTE: count byte number in file
int _oph_metadb_count_bytes(char *schema_file, unsigned long long *byte_size)
{
	if (!schema_file || !byte_size) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	FILE *fp = fopen(schema_file, "rb");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	//Get file total length
	flock(fd, LOCK_EX);
	if (fseek(fp, 0, SEEK_END)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
		flock(fd, LOCK_UN);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	long int tot_length = ftell(fp);

	flock(fd, LOCK_UN);
	fclose(fp);

	*byte_size = tot_length;

	return OPH_METADB_OK;
}

int _oph_metadb_create_file(char *schema_file)
{
	if (!schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	struct stat buffer;
	int res = stat(schema_file, &buffer);

	//If file does not exist then create it
	if (res != 0) {
		FILE *fp = fopen(schema_file, "wb");
		if (!fp) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
			return OPH_METADB_IO_ERR;
		}
		fclose(fp);
	}

	return OPH_METADB_OK;
}

//Removes all record with active flag set to false
int _oph_metadb_delete_procedure(char *schema_file, short int clean_all)
{
	if (!schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	FILE *fp = fopen(schema_file, "rb");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}
	FILE *fp_tmp = fopen(tmp_file, "wb");
	if (!fp_tmp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, tmp_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, tmp_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}
	//Read records from file
	unsigned int tmp_length = 0;
	char tmp_active_f = 1;
	char tmp_persistent_f = 1;
	//Track current treshold
	unsigned int curr_threshold = OPH_METADB_RECORD_LENGTH;
	//Alloc to OPH_METADB_RECORD_LENGTH
	char *tmp_line = (char *) malloc(curr_threshold * sizeof(char));
	if (!tmp_line) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
		fclose(fp);
		fclose(fp_tmp);
		return OPH_METADB_MEMORY_ERR;
	}

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		free(tmp_line);
		fclose(fp);
		fclose(fp_tmp);
		return OPH_METADB_IO_ERR;
	}
	int fd_tmp = fileno(fp_tmp);
	if (fd_tmp == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, tmp_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, tmp_file);
		free(tmp_line);
		fclose(fp);
		fclose(fp_tmp);
		return OPH_METADB_IO_ERR;
	}


	flock(fd, LOCK_EX);
	flock(fd_tmp, LOCK_EX);

	while (fread(&tmp_length, sizeof(unsigned int), 1, fp) > 0) {
		if (tmp_length <= 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			free(tmp_line);
			flock(fd, LOCK_UN);
			flock(fd_tmp, LOCK_UN);
			fclose(fp);
			fclose(fp_tmp);
			return OPH_METADB_IO_ERR;
		}
		//Resize buffer if record length is bigger than threshold
		if (tmp_length > curr_threshold) {
			free(tmp_line);
			curr_threshold = tmp_length;
			tmp_line = (char *) malloc(curr_threshold * sizeof(char));
			if (!tmp_line) {
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_MEMORY_ALLOC_ERROR);
				flock(fd, LOCK_UN);
				flock(fd_tmp, LOCK_UN);
				fclose(fp);
				fclose(fp_tmp);
				return OPH_METADB_MEMORY_ERR;
			}
		}

		if (fread(&tmp_active_f, sizeof(char), 1, fp) != 1 || fread(&tmp_persistent_f, sizeof(char), 1, fp) != 1 || fread(tmp_line, sizeof(char), tmp_length, fp) != tmp_length) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			flock(fd, LOCK_UN);
			flock(fd_tmp, LOCK_UN);
			fclose(fp);
			fclose(fp_tmp);
			return OPH_METADB_IO_ERR;
		}
		//If record is active and it is persistent, then copy it to new file
		if (clean_all) {
			if (tmp_active_f == 1 && tmp_persistent_f == 1) {
				//Write record line to file
				fwrite(&tmp_length, sizeof(unsigned int), 1, fp_tmp);
				fwrite(&tmp_active_f, sizeof(char), 1, fp_tmp);
				fwrite(&tmp_persistent_f, sizeof(char), 1, fp_tmp);
				fwrite(tmp_line, sizeof(char), tmp_length, fp_tmp);
			}
		} else {
			if (tmp_active_f == 1) {
				//Write record line to file
				fwrite(&tmp_length, sizeof(unsigned int), 1, fp_tmp);
				fwrite(&tmp_active_f, sizeof(char), 1, fp_tmp);
				fwrite(&tmp_persistent_f, sizeof(char), 1, fp_tmp);
				fwrite(tmp_line, sizeof(char), tmp_length, fp_tmp);
			}
		}
	}

	free(tmp_line);
	flock(fd, LOCK_UN);
	flock(fd_tmp, LOCK_UN);
	fclose(fp);
	fclose(fp_tmp);

	remove(schema_file);
	rename(tmp_file, schema_file);

	return OPH_METADB_OK;
}

//Count number of active records
int _oph_metadb_count_records(char *schema_file, unsigned long long *record_number)
{
	if (!schema_file) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_NULL_INPUT_PARAM);
		return OPH_METADB_NULL_ERR;
	}

	*record_number = 0;

	FILE *fp = fopen(schema_file, "rb");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		return OPH_METADB_IO_ERR;
	}
	//Read records from file
	unsigned int tmp_length = 0;
	char tmp_flag = 1;
	unsigned long long active_counter = 0;

	int fd = fileno(fp);
	if (fd == -1) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_OPEN_ERROR, errno, schema_file);
		fclose(fp);
		return OPH_METADB_IO_ERR;
	}

	flock(fd, LOCK_EX);

	while (fread(&tmp_length, sizeof(unsigned int), 1, fp) > 0) {
		if (tmp_length <= 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, tmp_length, schema_file);
			flock(fd, LOCK_UN);
			fclose(fp);
			return OPH_METADB_IO_ERR;
		}


		if (fread(&tmp_flag, sizeof(char), 1, fp) != 1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, 1, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_READ_ERROR, 1, schema_file);
			flock(fd, LOCK_UN);
			fclose(fp);
			return OPH_METADB_IO_ERR;
		}

/*    //If record is active, then count it
    if(tmp_flag == 1){
*/
		active_counter++;
/*    }  
*/
		if (fseek(fp, tmp_length + 1, SEEK_CUR)) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_METADB_LOG_FILE_SEEK_ERROR, errno, schema_file);
			flock(fd, LOCK_UN);
			fclose(fp);
			return OPH_METADB_IO_ERR;
		}
	}

	*record_number = active_counter;

	flock(fd, LOCK_UN);
	fclose(fp);

	return OPH_METADB_OK;
}
