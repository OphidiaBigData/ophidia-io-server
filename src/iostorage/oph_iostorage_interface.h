/*
    Ophidia IO Server
    Copyright (C) 2014-2016 CMCC Foundation

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

#ifndef __OPH_IOSTORAGE_INTERFACE_H
#define __OPH_IOSTORAGE_INTERFACE_H

#include "oph_iostorage_data.h"

//*****************Defines***************//

#define OPH_IOSTORAGE_BUFLEN            1024


#define OPH_IOSTORAGE_PERSISTENT_DEV    "persistent"
#define OPH_IOSTORAGE_TRANSIENT_DEV     "transient"

#define OPH_IOSTORAGE_SETUP_FUNC        "_%s_setup"
#define OPH_IOSTORAGE_CLEANUP_FUNC      "_%s_cleanup"
#define OPH_IOSTORAGE_GET_DB_FUNC       "_%s_get_db"
#define OPH_IOSTORAGE_PUT_DB_FUNC       "_%s_put_db"
#define OPH_IOSTORAGE_DELETE_DB_FUNC    "_%s_delete_db"
#define OPH_IOSTORAGE_GET_FRAG_FUNC     "_%s_get_frag"
#define OPH_IOSTORAGE_PUT_FRAG_FUNC     "_%s_put_frag"
#define OPH_IOSTORAGE_DELETE_FRAG_FUNC  "_%s_delete_frag"

//****************Handle******************//

/**
 * \brief                 Handle structure with dynamic storage device library parameters
 * \param device          Name of storage device used within the server
 * \param is_persistent   Flag that indicates if the device is persistent or transient
 * \param lib             Dynamic library path
 * \param dlh             Libtool handler to dynamic library
 * \param connection      Variable to hold generic storage device connection status info
 */
typedef struct{
	char 	*device;
  short unsigned int is_persistent;
	char	*lib;
	void	*dlh;
  void  *connection;
} oph_iostore_handler;

//****************Plugin Interface******************//

//Function to initialize storage library.
int (*_DEVICE_setup) (oph_iostore_handler *handle);

//Function to finalize the storage library and release all dynamic loading resources.
int (*_DEVICE_cleanup) (oph_iostore_handler* handle);

//Function to retrieve a DB record from storage device
int (*_DEVICE_get_db) (oph_iostore_handler* handle, oph_iostore_resource_id *res_id, oph_iostore_db_record_set **db_record); 

//Function to insert a DB record into storage device
int (*_DEVICE_put_db) (oph_iostore_handler* handle, oph_iostore_db_record_set *db_record, oph_iostore_resource_id **res_id); 

//Function to delete a DB from a storage device
int (*_DEVICE_delete_db) (oph_iostore_handler* handle, oph_iostore_resource_id *res_id); 

//Function to retrieve a fragment record from storage device
int (*_DEVICE_get_frag) (oph_iostore_handler* handle, oph_iostore_resource_id *res_id, oph_iostore_frag_record_set **frag_record); 

//Function to insert a fragment record into storage device
int (*_DEVICE_put_frag) (oph_iostore_handler* handle, oph_iostore_frag_record_set *frag_record, oph_iostore_resource_id **res_id); 

//Function to delete a fragment from a storage device
int (*_DEVICE_delete_frag) (oph_iostore_handler* handle, oph_iostore_resource_id *res_id); 

//*****************Internal Functions (used by query engine library)***************//

/**
 * \brief               Function to initialize data storage library. This function should be called before any other function to initialize the dynamic library.
 * \param device        String with the name of storage device plugin to use
 * \param handle        Address to pointer for dynamic device plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_setup(const char *device, oph_iostore_handler **handle);

/**
 * \brief               Function to finalize library of data storage and release all dynamic loading resources.
 * \param handle        Dynamic I/O storage plugin handle
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_cleanup(oph_iostore_handler* handle);

/**
 * \brief               Function to retrieve a DB record from storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource being fetched
 * \param db_record     Record contains a copy of a DB if device is persisten (it should be deleted outside), or a pointer to the record if device is transient
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_get_db(oph_iostore_handler* handle, oph_iostore_resource_id *res_id, oph_iostore_db_record_set **db_record); 

/**
 * \brief               Function to insert a DB record into storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param db_record     Record containing a DB: if device is persistent it will be freed outside otherwise if device is transient it will be added into the device 
 * \param res_id        ID of resource created
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_put_db(oph_iostore_handler* handle, oph_iostore_db_record_set *db_record, oph_iostore_resource_id **res_id); 

/**
 * \brief               Function to delete a DB from a storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource to delete
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_delete_db(oph_iostore_handler* handle, oph_iostore_resource_id *res_id); 

/**
 * \brief               Function to retrieve a fragment record from storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource being fetched
 * \param frag_record   Record contains a copy of a frag if device is persisten (it should be deleted outside), or a pointer to the record if device is transient
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_get_frag(oph_iostore_handler* handle, oph_iostore_resource_id *res_id, oph_iostore_frag_record_set **frag_record); 

/**
 * \brief               Function to insert a fragment record into storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param db_record     Record containing a fragment: if device is persistent it will be freed outside otherwise if device is transient it will be added into the device 
 * \param res_id        ID of resource created
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_put_frag(oph_iostore_handler* handle, oph_iostore_frag_record_set *frag_record, oph_iostore_resource_id **res_id); 

/**
 * \brief               Function to delete a fragment from a storage device
 * \param handle        Dynamic I/O storage plugin handle
 * \param res_id        ID of resource to delete
 * \return              0 if successfull, non-0 otherwise
 */
int oph_iostore_delete_frag(oph_iostore_handler* handle, oph_iostore_resource_id *res_id); 

#endif //__OPH_IOSTORAGE_INTERFACE_H
