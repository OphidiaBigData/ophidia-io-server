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

#include "oph_iostorage_interface.h"

#include <stdlib.h>
#include <stdio.h>
#include <ltdl.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "debug.h"
#include "oph_iostorage_log_error_codes.h"
#include "oph_server_confs.h"
#include "oph_server_utility.h"

#include <pthread.h>

extern int msglevel;
extern pthread_mutex_t libtool_lock;

static int oph_iostore_find_device(const char *device, char **dyn_lib, unsigned short int *is_persitent);

int (*_DEVICE_setup)(oph_iostore_handler * handle);
int (*_DEVICE_cleanup)(oph_iostore_handler * handle);
int (*_DEVICE_get_db)(oph_iostore_handler * handle, oph_iostore_resource_id * res_id, oph_iostore_db_record_set ** db_record);
int (*_DEVICE_put_db)(oph_iostore_handler * handle, oph_iostore_db_record_set * db_record, oph_iostore_resource_id ** res_id);
int (*_DEVICE_delete_db)(oph_iostore_handler * handle, oph_iostore_resource_id * res_id);
int (*_DEVICE_get_frag)(oph_iostore_handler * handle, oph_iostore_resource_id * res_id, oph_iostore_frag_record_set ** frag_record);
int (*_DEVICE_put_frag)(oph_iostore_handler * handle, oph_iostore_frag_record_set * frag_record, oph_iostore_resource_id ** res_id);
int (*_DEVICE_delete_frag)(oph_iostore_handler * handle, oph_iostore_resource_id * res_id);

int oph_iostore_setup(const char *device, oph_iostore_handler **handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}
	//If already executed don't procede further
	if ((*handle) && (*handle)->dlh)
		return OPH_IOSTORAGE_SUCCESS;

	oph_iostore_handler *internal_handle = (oph_iostore_handler *) malloc(sizeof(oph_iostore_handler));
	if (internal_handle == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}

	internal_handle->device = NULL;
	internal_handle->lib = NULL;
	internal_handle->dlh = NULL;

	//Set storage device type
	internal_handle->device = (char *) strndup(device, strlen(device));
	if (internal_handle->device == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
		free(internal_handle);
		return OPH_IOSTORAGE_MEMORY_ERR;
	}
	//Convert device name to lower case
	int i = 0;
	while (device[i]) {
		internal_handle->device[i] = tolower(device[i]);
		i++;
	}

	//LTDL_SET_PRELOADED_SYMBOLS();
	pthread_mutex_lock(&libtool_lock);
	if (lt_dlinit() != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLINIT_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLINIT_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		free(internal_handle->device);
		free(internal_handle);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	if (oph_iostore_find_device(internal_handle->device, &internal_handle->lib, &internal_handle->is_persistent)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LIB_NOT_FOUND);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LIB_NOT_FOUND);
		free(internal_handle->device);
		free(internal_handle);
		return OPH_IOSTORAGE_LIB_NOT_FOUND;
	}

	pthread_mutex_lock(&libtool_lock);
	if (!(internal_handle->dlh = (lt_dlhandle) lt_dlopen(internal_handle->lib))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLOPEN_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLOPEN_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		free(internal_handle->lib);
		free(internal_handle->device);
		free(internal_handle);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_SETUP_FUNC, internal_handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_setup = (int (*)(oph_iostore_handler *)) lt_dlsym(internal_handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	*handle = internal_handle;
	return _DEVICE_setup(*handle);
}

int oph_iostore_cleanup(oph_iostore_handler *handle)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_CLEANUP_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_cleanup = (int (*)(oph_iostore_handler *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	//Release device resources
	int res;
	if ((res = _DEVICE_cleanup(handle))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_RELEASE_RES_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_RELEASE_RES_ERROR);
		return res;
	}
	//Release handle resources
	if (handle->device) {
		free(handle->device);
		handle->device = NULL;
	}
	if (handle->lib) {
		free(handle->lib);
		handle->lib = NULL;
	}

	pthread_mutex_lock(&libtool_lock);
	if ((lt_dlclose(handle->dlh))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLCLOSE_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLCLOSE_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLCLOSE_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);
	handle->dlh = NULL;

	pthread_mutex_lock(&libtool_lock);
	if (lt_dlexit()) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLEXIT_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_DLEXIT_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLEXIT_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	free(handle);
	handle = NULL;

	return res;
}

int oph_iostore_get_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_db_record_set **db_record)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_GET_DB_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_get_db = (int (*)(oph_iostore_handler *, oph_iostore_resource_id *, oph_iostore_db_record_set **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_get_db(handle, res_id, db_record);
}

int oph_iostore_put_db(oph_iostore_handler *handle, oph_iostore_db_record_set *db_record, oph_iostore_resource_id **res_id)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_PUT_DB_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_put_db = (int (*)(oph_iostore_handler *, oph_iostore_db_record_set *, oph_iostore_resource_id **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_put_db(handle, db_record, res_id);
}

int oph_iostore_delete_db(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_DELETE_DB_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_delete_db = (int (*)(oph_iostore_handler *, oph_iostore_resource_id *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_delete_db(handle, res_id);
}

int oph_iostore_get_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id, oph_iostore_frag_record_set **frag_record)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_GET_FRAG_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_get_frag = (int (*)(oph_iostore_handler *, oph_iostore_resource_id *, oph_iostore_frag_record_set **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_get_frag(handle, res_id, frag_record);
}

int oph_iostore_put_frag(oph_iostore_handler *handle, oph_iostore_frag_record_set *frag_record, oph_iostore_resource_id **res_id)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_PUT_FRAG_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_put_frag = (int (*)(oph_iostore_handler *, oph_iostore_frag_record_set *, oph_iostore_resource_id **)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_put_frag(handle, frag_record, res_id);
}

int oph_iostore_delete_frag(oph_iostore_handler *handle, oph_iostore_resource_id *res_id)
{
	if (!handle) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_HANDLE);
		return OPH_IOSTORAGE_NULL_HANDLE;
	}

	if (!handle->dlh || !handle->device) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_PLUGIN_ERROR);
		return OPH_IOSTORAGE_DLOPEN_ERR;
	}

	char func_name[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	snprintf(func_name, OPH_IOSTORAGE_BUFLEN, OPH_IOSTORAGE_DELETE_FRAG_FUNC, handle->device);

	pthread_mutex_lock(&libtool_lock);
	if (!(_DEVICE_delete_frag = (int (*)(oph_iostore_handler *, oph_iostore_resource_id *)) lt_dlsym(handle->dlh, func_name))) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_LOAD_FUNC_ERROR, lt_dlerror());
		pthread_mutex_unlock(&libtool_lock);
		return OPH_IOSTORAGE_DLSYM_ERR;
	}
	pthread_mutex_unlock(&libtool_lock);

	return _DEVICE_delete_frag(handle, res_id);
}

static int oph_iostore_find_device(const char *device, char **dyn_lib, unsigned short int *is_persistent)
{
	FILE *fp = NULL;
	char line[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	char value[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	char dyn_lib_str[OPH_IOSTORAGE_BUFLEN] = { '\0' };
	char *res_string = NULL;

	if (!device || !dyn_lib) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_NULL_INPUT_PARAM);
		return -1;
	}

	snprintf(dyn_lib_str, sizeof(dyn_lib_str), OPH_SERVER_DEVICE_FILE_PATH);

	fp = fopen(dyn_lib_str, "r");
	if (!fp) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_FILE_NOT_FOUND, dyn_lib_str);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_FILE_NOT_FOUND, dyn_lib_str);
		return -1;	// driver not found
	}

	while (!feof(fp)) {
		res_string = fgets(line, OPH_IOSTORAGE_BUFLEN, fp);
		if (!res_string) {
			fclose(fp);
			pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_READ_LINE_ERROR);
			logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_READ_LINE_ERROR);
			return -1;
		}
		sscanf(line, "[%[^]]", value);

		//If value matches device then read library
		if (!strcasecmp(value, device)) {
			res_string = NULL;
			res_string = fgets(line, OPH_IOSTORAGE_BUFLEN, fp);
			if (!res_string) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_READ_LINE_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_READ_LINE_ERROR);
				return -1;
			}
			sscanf(line, "%[^\n]", value);
			*dyn_lib = (char *) strndup(value, OPH_IOSTORAGE_BUFLEN);
			if (!*dyn_lib) {
				fclose(fp);
				pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_MEMORY_ERROR);
				return -2;
			}
			sscanf(line, "%[^\n]", value);
			*is_persistent = !STRCMP(value, OPH_IOSTORAGE_PERSISTENT_DEV);
			fclose(fp);
			return 0;
		}
	}
	fclose(fp);
	pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_FILE_NOT_FOUND, dyn_lib_str);
	logging(LOG_ERROR, __FILE__, __LINE__, OPH_IOSTORAGE_LOG_FILE_NOT_FOUND, dyn_lib_str);

	return -2;		// driver not found
}
