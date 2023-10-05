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

#ifndef __PMEMORY_DEVICE_H
#define __PMEMORY_DEVICE_H

#include "oph_iostorage_interface.h"

#define PMEMORY_DEV_ERROR -1
#define PMEMORY_DEV_SUCCESS 0
#define PMEMORY_DEV_NULL_PARAM -2
#define PMEMORY_DEV_MEMORY_ERROR -3

#define PMEMORY_LOG_NULL_INPUT_PARAM "Null input parameter\n"
#define PMEMORY_LOG_MEMORY_ERROR	"Memory allocation error\n"


/**
 * \brief               Function to initialize memory device library. 
 * \param handle        Address to pointer for dynamic device plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_setup(oph_iostore_handler * handle);

/**
 * \brief               Function to finalize library of memory device and release all dynamic loading resources.
 * \param handle        Dynamic I/O storage plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_cleanup(oph_iostore_handler * handle);

/**
 * \brief               Function to retrieve a DB record from memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource being fetched
 * \param db_record     Record containing a copy of a DB (it should be deleted)
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_get_db(oph_iostore_handler * handle, oph_iostore_resource_id * res_id, oph_iostore_db_record_set ** db_record);

/**
 * \brief               Function to insert a DB record into memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param db_record     Record containing a DB (it should be copied into the memory device)
 * \param res_id        ID of resource created
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_put_db(oph_iostore_handler * handle, oph_iostore_db_record_set * db_record, oph_iostore_resource_id ** res_id);

/**
 * \brief               Function to delete a DB from a memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource to delete
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_delete_db(oph_iostore_handler * handle, oph_iostore_resource_id * res_id);

/**
 * \brief               Function to retrieve a fragment record from memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource being fetched
 * \param db_record     Record containing a copy of a fragment (it should be deleted)
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_get_frag(oph_iostore_handler * handle, oph_iostore_resource_id * res_id, oph_iostore_frag_record_set ** frag_record);

/**
 * \brief               Function to insert a fragment record into memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param db_record     Record containing a fragment (it should be copied into the memory device)
 * \param res_id        ID of resource created
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_put_frag(oph_iostore_handler * handle, oph_iostore_frag_record_set * frag_record, oph_iostore_resource_id ** res_id);

/**
 * \brief               Function to delete a fragment from a memory device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource to delete
 * \return              0 if successfull, non-0 otherwise
 */
int _pmemory_delete_frag(oph_iostore_handler * handle, oph_iostore_resource_id * res_id);

#endif				//__PMEMORY_DEVICE_H
