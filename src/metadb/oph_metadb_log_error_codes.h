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

#ifndef __OPH_METADB_LOG_ERROR_CODES_H
#define __OPH_METADB_LOG_ERROR_CODES_H

#define OPH_METADB_LOG_NULL_INPUT_PARAM       "Missing input argument\n"
#define OPH_METADB_LOG_MEMORY_ALLOC_ERROR     "Memory allocation error\n"
#define OPH_METADB_LOG_FILE_OPEN_ERROR        "Error %d while opening file %s\n"
#define OPH_METADB_LOG_FILE_SEEK_ERROR        "Error %d while seeking position in file %s\n"
#define OPH_METADB_LOG_FILE_WRITE_ERROR       "Unable to write %d bytes in %s\n"
#define OPH_METADB_LOG_FILE_READ_ERROR        "Unable to read %d bytes from %s\n"
#define OPH_METADB_LOG_FILE_DEL_READ_ERROR    "Unable to read deleted record from %s\n"

#define OPH_METADB_LOG_FILE_CREATE_ERROR      "Unable to create empty file %s\n"
#define OPH_METADB_LOG_DEL_PROC_ERROR         "Unable to apply delete procedure to %s\n"

#define OPH_METADB_LOG_COUNT_RECORDS_ERROR    "Unable to count records of file %s\n"
#define OPH_METADB_LOG_READ_RECORD_ERROR      "Unable to read record from file %s\n"
#define OPH_METADB_LOG_DESERIAL_RECORD_ERROR  "Unable to deserialize record\n"
#define OPH_METADB_LOG_DB_FRAG_MATCH_ERROR    "Unable to find db associated to fragment %s\n"
#define OPH_METADB_LOG_DB_RECORD_UPDATE_NOT_FOUND   "Unable to find record to be updated for DB %s\n"
#define OPH_METADB_LOG_DB_RECORD_REMOVE_NOT_FOUND   "Unable to find record to be removed for DB %s\n"
#define OPH_METADB_LOG_FRAG_RECORD_UPDATE_NOT_FOUND "Unable to find record to be updated for Frag %s\n"
#define OPH_METADB_LOG_FRAG_RECORD_REMOVE_NOT_FOUND "Unable to find record to be removed for Frag %s\n"
#define OPH_METADB_LOG_DB_RECORD_NOT_FOUND    "Unable to find record for DB %s\n"
#define OPH_METADB_LOG_FRAG_RECORD_NOT_FOUND  "Unable to find record for Frag %s\n"
#define OPH_METADB_LOG_SERIAL_RECORD_ERROR    "Unable to serialize record\n"
#define OPH_METADB_LOG_DB_RECORD_ZEROED_WARN  "Warning: DB record pointers, offset and db_id are zeroed out\n"
#define OPH_METADB_LOG_FRAG_RECORD_ZEROED_WARN "Warning: Frag record pointers, offset, db_id and frag_id are zeroed out\n"
#define OPH_METADB_LOG_RECORD_COPY_ERROR      "Unable to create copy of given record\n"
#define OPH_METADB_LOG_FILE_SIZE_ERROR        "Unable to get file size\n"
#define OPH_METADB_LOG_WRITE_RECORD_ERROR     "Unable to write record to MetaDB file\n"
#define OPH_METADB_LOG_REMOVE_RECORD_ERROR    "Unable to remove record from MetaDB file %s\n"
#define OPH_METADB_LOG_DB_EXIST_ERROR         "DB already exists\n"
#define OPH_METADB_LOG_FRAG_EXIST_ERROR       "Frag already exists\n"
#define OPH_METADB_LOG_REMOVE_NON_EMPTY_DB    "Unable to remove non-empty database %s\n"
#define OPH_METADB_LOG_FRAG_DB_ERROR          "Given DB does not match with fragment. Corrupted record!\n"
#define OPH_METADB_LOG_FRAG_DUPLICATE_ERROR    "Fragment %s already inserted. Corrupted record!\n"

#endif				//__OPH_METADB_LOG_ERROR_CODES_H
