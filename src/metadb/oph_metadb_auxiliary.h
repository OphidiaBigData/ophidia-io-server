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

#ifndef __OPH_METADB_AUX_H
#define __OPH_METADB_AUX_H

#include "oph_metadb_interface.h"

#define OPH_METADB_RECORD_LENGTH 4096
#define OPH_METADB_HEADER_LENGTH sizeof(char) + sizeof(char) + sizeof(unsigned int)

/**
 * \brief           Auxiliar function to serialize structure into binary string.
 * \param row       Row to be serialized 
 * \param line      Line that will contain serialized struct
 * \param line_length  Length of line that will contain serialized struct
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_serialize_db_row(oph_metadb_db_row * row, char **line, unsigned int *line_length);

/**
 * \brief           Auxiliar function to deserialize binary string into structure.
 * \param line      record to be deserialized
 * \param row       Row that will contain deserialized record
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_deserialize_db_row(char *line, oph_metadb_db_row ** row);

/**
 * \brief           Auxiliar function to serialize structure into binary string.
 * \param row       Row to be serialized 
 * \param line      Line that will contain serialized struct
 * \param line_length  Length of line that will contain serialized struct
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_serialize_frag_row(oph_metadb_frag_row * row, char **line, unsigned int *line_length);

/**
 * \brief           Auxiliar function to deserialize binary string into structure.
 * \param line      record to be deserialized
 * \param row       Row that will contain deserialized record
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_deserialize_frag_row(char *line, oph_metadb_frag_row ** row);

/**
 * \brief           Auxiliar function to write a row into the file.
 * \param line      record to be inserted
 * \param line_length Length of record
 * \param parsistent_flag Flag to indicate if record is persistent (1) or transient (0)
 * \param schema_file File where the record will be stored
 * \param file_offset Offset inside file where the record will be stored.
 * \param append_flag If setted then record will be appended at end of file.
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_write_row(char *line, unsigned int line_length, unsigned short int persistent_flag, char *schema_file, unsigned long long file_offset, unsigned short int append_flag);

/**
 * \brief           Auxiliar function to remove a row from file (actually it sets active flag to false).
 * \param schema_file File where the record is be stored
 * \param file_offset Offeset inside file where the record is be stored
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_remove_row(char *schema_file, unsigned long long file_offset);

/**
 * \brief           Auxiliar function to count number of bytes into the file.
 * \param schema_file File where the record are stored
 * \param byte_size   Size of file in bytes
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_count_bytes(char *schema_file, unsigned long long *byte_size);


/**
 * \brief           Auxiliar function to count number of records in the file.
 * \param schema_file File where the record are stored
 * \param byte_size   Number of records
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_count_records(char *schema_file, unsigned long long *record_number);

/**
 * \brief           Auxiliar function to read a row from file.
 * \param schema_file File where the record is be stored
 * \param file_offset Offeset inside file where the record is be stored
 * \param line      record to be read
 * \param line_length Length of record
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_read_row(char *schema_file, unsigned long long file_offset, char **line, unsigned int *line_length);

/**
 * \brief           Auxiliar function to phisycally create an empty file.
 * \param schema_file File to create
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_create_file(char *schema_file);

/**
 * \brief           Auxiliar function to phisycally delete unactive and/or transient rows from file.
 * \param schema_file File to clean
 * \param clean_all Remove also transient record
 * \return          0 if successfull, non-0 otherwise
 */
int _oph_metadb_delete_procedure(char *schema_file, short int clean_all);

#endif				/* OPH_METADB_AUX_H */
