/*
    Ophidia IO Server
    Copyright (C) 2014-2021 CMCC Foundation

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

#ifndef __OPH_SERVER_CONFS_H
#define __OPH_SERVER_CONFS_H

#include "hashtbl.h"

//IO server defines

#define OPH_SERVER_PREFIX                                    OPH_IO_SERVER_PREFIX

#define OPH_SERVER_CONF_FILE_PATH                      OPH_SERVER_PREFIX"/etc/oph_ioserver.conf"

#define OPH_SERVER_PLUGIN_FILE_PATH                     OPH_SERVER_PREFIX"/etc/oph_primitives_list"

#define OPH_SERVER_DEVICE_FILE_PATH                     OPH_SERVER_PREFIX"/etc/oph_device_list"

//Instance Macros

#define OPH_SERVER_DATABASE_SCHEMA_PREFIX         OPH_SERVER_PREFIX"/var/database.db"
#define OPH_SERVER_FRAGMENT_SCHEMA_PREFIX         OPH_SERVER_PREFIX"/var/fragment.db"
#define OPH_SERVER_TEMP_SCHEMA_PREFIX             OPH_SERVER_PREFIX"/var/tmp.db"
#define OPH_SERVER_DATABASE_SCHEMA                "%s/var/database.db"
#define OPH_SERVER_FRAGMENT_SCHEMA                "%s/var/fragment.db"
#define OPH_SERVER_TEMP_SCHEMA                    "%s/var/tmp.db"

#define OPH_SERVER_LOG_PATH                 OPH_SERVER_PREFIX"/log/server.log"
#define OPH_SERVER_LOG_PATH_PREFIX          "%s/log/server.log"


//Library defines

#define OPH_SERVER_CONF_LINE_LEN                            1024
#define OPH_SERVER_CONF_LOAD_SIZE                           7

#define OPH_SERVER_CONF_SUCCESS                             0
#define OPH_SERVER_CONF_NULL_PARAM                          1
#define OPH_SERVER_CONF_ERROR                               2

#define OPH_SERVER_CONF_INSTANCE                            "instance"

#define OPH_SERVER_CONF_COMMENT                            '#'

/**
 * \brief			        Array with possible configuration parameters used into conf file
 */
#define OPH_SERVER_CONF_HOSTNAME          "SERVER_HOSTNAME"
#define OPH_SERVER_CONF_PORT              "SERVER_PORT"
#define OPH_SERVER_CONF_DIR               "SERVER_DIR"
#define OPH_SERVER_CONF_MPL               "MAX_PACKET_LEN"
#define OPH_SERVER_CONF_TTL               "CLIENT_TTL"
#define OPH_SERVER_CONF_OMP_THREADS       "OPENMP_THREADS"
#define OPH_SERVER_CONF_MEMORY_BUFFER     "MEMORY_BUFFER"
#define OPH_SERVER_CONF_CACHE_LINE_SIZE	  "CACHE_LINE_SIZE"
#define OPH_SERVER_CONF_CACHE_SIZE     	  "CACHE_SIZE"


static const char *const oph_server_conf_params[] =
    { OPH_SERVER_CONF_HOSTNAME, OPH_SERVER_CONF_PORT, OPH_SERVER_CONF_DIR, OPH_SERVER_CONF_MPL, OPH_SERVER_CONF_TTL, OPH_SERVER_CONF_OMP_THREADS, OPH_SERVER_CONF_MEMORY_BUFFER,
	OPH_SERVER_CONF_CACHE_LINE_SIZE, OPH_SERVER_CONF_CACHE_SIZE, NULL
};

/**
 * \brief			        Function to load parameters from configuration file
 * \param instance    Server instance to be loaded (if 0 then the first instance will be selected)
 * \param hashtbl     Pointer to hash table containing param list
 * \return            0 if successfull, non-0 otherwise
 */
int oph_server_conf_load(short unsigned int instance, HASHTBL ** hashtbl);

/**
 * \brief			        Function to get a param from configuration hash table
 * \param hashtbl     Hash table containing param list
 * \param param       Param to be found
 * \param value       Value found
 * \return            0 if successfull, non-0 otherwise
 */
int oph_server_conf_get_param(HASHTBL * hashtbl, const char *param, char **value);

/**
 * \brief			        Function to unload parameters from configuration file
 * \param hashtbl     Pointer to hash table containing param list
 * \return            0 if successfull, non-0 otherwise
 */
int oph_server_conf_unload(HASHTBL ** hashtbl);

#endif				//__OPH_SERVER_CONFS_H
