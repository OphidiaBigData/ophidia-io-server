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

#ifndef __OPH_IOSTORAGE_DATA_H
#define __OPH_IOSTORAGE_DATA_H

/**
 * \brief			          Enum with possible field types (also used as plugin return types)
 */
typedef enum {
	OPH_IOSTORE_LONG_TYPE,
	OPH_IOSTORE_REAL_TYPE,
	OPH_IOSTORE_STRING_TYPE
} oph_iostore_field_type;

/**
 * \brief			          Structure for storing information about a fragment record (a single table row)
 * \param field_length 	Array containing the length for each cell in the record
 * \param field			    Array containing the cell values
 */
typedef struct {
	unsigned long long *field_length;
	void **field;
} oph_iostore_frag_record;

/**
 * \brief			          Structure containing information about a fragment record set (entire table)
 * \param frag_name		  Name of Fragment
 * \param field_num 	  Number of fields contained in records
 * \param field_name		Array with field (columns) names
 * \param field_type		Array containing type of each cell
 * \param record_set		NULL terminated array with pointers to actual records
 * \param tmp_flag			Flag set to 1 if the table is considered as a temporary one (deleted at the end of the operation)
 */
typedef struct {
	char *frag_name;
	unsigned short field_num;
	char **field_name;
	oph_iostore_field_type *field_type;
	oph_iostore_frag_record **record_set;
	char tmp_flag;
} oph_iostore_frag_record_set;

/**
 * \brief			          Structure containing information about a DB record set
 * \param db_name		    Name of DB
 */
typedef struct {
	char *db_name;
} oph_iostore_db_record_set;

/**
 * \brief			        Structure to contain a DB or Fragment resource ID
 * \param id		      ID of resource (can be NULL)
 * \param id_length   Length of ID
 */
typedef struct {
	void *id;
	unsigned short id_length;
} oph_iostore_resource_id;

//Internal functions
/**
 * \brief           Function to compare two resource ID.
 * \param id1       First ID 
 * \param id2       Second ID
 * \return          0 if successfull, non-0 otherwise
 */
int oph_iostore_compare_id(oph_iostore_resource_id id1, oph_iostore_resource_id id2);

/**
 * \brief			          Copy a fragment record
 * \param input_record  Record to be copied
 * \param field_num     Number of fields in input record
 * \param input_record  Record copied
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_copy_frag_record(oph_iostore_frag_record * input_record, unsigned short input_field_num, oph_iostore_frag_record ** output_record);

/**
 * \brief			              Copy a fragment record_set (it does not copy the frag_name)
 * \param input_record_set  Record to be copied
 * \param output_record_set  Record copied
 * \return                  0 if successfull, non-0 otherwise
 */
int oph_iostore_copy_frag_record_set(oph_iostore_frag_record_set * input_record_set, oph_iostore_frag_record_set ** output_record_set);

/**
 * \brief			              Copy a fragment record_set by specifying a limit (it does not copy the frag_name)
 * \param input_record_set  Record to be copied
 * \param output_record_set  Record copied
 * \param limit Copy up to 'limit' rows; all the rows are extracted using '0'
 * \param limit Discard the first 'offset' rows
 * \return                  0 if successfull, non-0 otherwise
 */
int oph_iostore_copy_frag_record_set_limit(oph_iostore_frag_record_set * input_record_set, oph_iostore_frag_record_set ** output_record_set, long long limit, long long offset);

/**
 * \brief			              Copy a fragment record_set structure only by specifying a limit (it does not copy the frag_name and the internal record set)
 * \param input_record_set  Record to be copied
 * \param output_record_set  Record copied
 * \param limit Copy up to 'limit' rows; all the rows are extracted using '0'
 * \param limit Discard the first 'offset' rows
 * \return                  0 if successfull, non-0 otherwise
 */
int oph_iostore_copy_frag_record_set_only(oph_iostore_frag_record_set * input_record_set, oph_iostore_frag_record_set ** output_record_set, long long limit, long long offset);

/**
 * \brief			        Destroy a record and release resources
 * \param record      Record to be freed
 * \param field_num   Number of fields in a record
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_destroy_frag_record(oph_iostore_frag_record ** record, short int field_num);

/**
 * \brief			        Create an empty record
 * \param record      Record to be allocated
 * \param field_num   Number of fields in a record
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_create_frag_record(oph_iostore_frag_record ** record, short int field_num);

/**
 * \brief			        Destroy a record set and release resources
 * \param record_set  Record set to be freed
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_destroy_frag_recordset(oph_iostore_frag_record_set ** record_set);

/**
 * \brief			        Destroy a record set and release resources (it does not destroy internal record set)
 * \param record_set  Record set to be freed
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_destroy_frag_recordset_only(oph_iostore_frag_record_set ** record_set);

/**
 * \brief			        Create an empty recordset
 * \param record_set  Record set to be allocated
 * \param set_size    Number of rows in record set
 * \param field_num   Number of fields in each record
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_create_frag_recordset(oph_iostore_frag_record_set ** record_set, long long set_size, short int field_num);

/**
 * \brief             Create an empty recordset. It does not create internal record structures.
 * \param record_set  Record set to be allocated
 * \param set_size    Number of rows in record set
 * \param field_num   Number of fields in each record
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_create_frag_recordset_only(oph_iostore_frag_record_set ** record_set, long long set_size, short int field_num);

/**
 * \brief			        Create a sample recordset (for test purposes). It does not set the frag_name.
 * \param row_number  Number of rows in record set
 * \param array_length Length of each measure array
 * \param record_set  Record set to be allocated
 * \return            0 if successfull, non-0 otherwise
 */
int oph_iostore_create_sample_frag(const long long row_number, const long long array_length, oph_iostore_frag_record_set ** record_set);

#endif				/* __OPH_IOSTORAGE_DATA_H */
