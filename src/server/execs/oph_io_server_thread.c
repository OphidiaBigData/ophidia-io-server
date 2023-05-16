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

#include "oph_io_server_thread.h"

#include <poll.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include "debug.h"
#include "taketime.h"

#include "hashtbl.h"
#include "oph_server_utility.h"
#include "oph_io_server_query_manager.h"

#include "oph_iostorage_data.h"
#include "oph_query_parser.h"
#include "oph_metadb_interface.h"
#include "oph_network.h"

extern int msglevel;

//TODO thread management operations
//TODO save info related to threads
//TODO save connection status

//Global server variables (read-only)
extern unsigned long long max_packet_length;
extern unsigned short omp_threads;
extern unsigned short client_ttl;
extern HASHTBL *plugin_table;

extern pthread_rwlock_t rwlock;
extern oph_metadb_db_row *db_table;

//#define DEBUG

int oph_io_server_free_status(oph_io_server_thread_status *status)
{

	if (status->curr_stmt != NULL) {
		if (status->curr_stmt->partial_result_set != NULL)
			oph_iostore_destroy_frag_recordset(&status->curr_stmt->partial_result_set);
		if (status->curr_stmt->device != NULL)
			free(status->curr_stmt->device);
		if (status->curr_stmt->frag != NULL)
			free(status->curr_stmt->frag);
		free(status->curr_stmt);
	}
	if (status->current_db != NULL)
		free(status->current_db);
	if (status->device != NULL)
		free(status->device);

	if (status->last_result_set != NULL) {
		if (status->delete_only_rs)
			oph_iostore_destroy_frag_recordset_only(&(status->last_result_set));
		else
			oph_iostore_destroy_frag_recordset(&(status->last_result_set));
	}
	status->last_result_set = NULL;
	status->delete_only_rs = 0;

	return 0;
}


int oph_io_server_send_error(int sockfd)
{

	if (write(sockfd, (void *) OPH_IO_SERVER_REQ_ERROR, strlen(OPH_IO_SERVER_REQ_ERROR)) != strlen(OPH_IO_SERVER_REQ_ERROR)) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
		return -1;
	}
	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
	logging(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");

	return 0;
}


int oph_io_server_free_query_args(oph_query_arg **args, unsigned int arg_count)
{

	unsigned int n;

	if (args) {
		for (n = 0; n < arg_count; n++) {
			if (args[n]) {
				if (args[n]->arg)
					free(args[n]->arg);
				free(args[n]);
			}
		}
		free(args);
	}
	return 0;
}

void oph_io_server_thread(int sockfd, pthread_t tid)
{
	char *line = (char *) calloc(max_packet_length, sizeof(char));
	if (!line) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
		return;
	}
	char *result = (char *) calloc(max_packet_length, sizeof(char));
	if (!result) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
		logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
		free(result);
		return;
	}

	char header[OPH_IO_SERVER_MSG_TYPE_LEN + 1], buffer[OPH_IO_SERVER_MAX_DOUBLE_LEN];
	int res;
	int m = 0;

#ifdef DEBUG
	//Total Exec time evaluate
	struct timeval start_time, end_time, total_time;
	//Partial Exec time evaluate
	struct timeval s_time, e_time, t_time;
#endif

	//Status of thread
	oph_io_server_thread_status global_status;
	global_status.current_db = NULL;
	global_status.last_result_set = NULL;
	global_status.delete_only_rs = 0;
	global_status.device = NULL;
	global_status.curr_stmt = NULL;

	oph_metadb_db_row *db_row = NULL;

	//Poll socket 
	int rv;
	struct pollfd ufds[1];

	ufds[0].fd = sockfd;
	ufds[0].events = POLLIN;	// check for normal or out-of-band

	unsigned long long current_threshold = 0;
	char *result_buffer = NULL;
	char *tmp_buffer = NULL;
	unsigned long long k = 0, i = 0;
	unsigned int j = 0, n = 0;
	unsigned long long size = 0;
	unsigned int num_fields = 0;
	unsigned long long payload_len = 0;
	unsigned int arg_count = 0;
	unsigned long long tot_run = 0, curr_run = 0;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Waiting for a query from socket %d...\n", sockfd);
	logging(LOG_DEBUG, __FILE__, __LINE__, "Waiting for a query from socket %d...\n", sockfd);
	for (;;) {
#ifdef DEBUG
		//Get time from first call
		gettimeofday(&start_time, NULL);
#endif
		// wait for events on the sockets, ttl second timeout
		rv = poll(ufds, 1, client_ttl * 1000);

		if (rv == -1) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Error in polling socket: %d\n", errno);
			logging(LOG_WARNING, __FILE__, __LINE__, "Error in polling socket: %d\n", errno);
			break;
		} else if (rv == 0) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Timeout occured\n");
			logging(LOG_WARNING, __FILE__, __LINE__, "Timeout occured\n");
			break;
		}
		if (ufds[0].revents != POLLIN) {
			pmesg(LOG_WARNING, __FILE__, __LINE__, "Connection closed with error\n");
			logging(LOG_WARNING, __FILE__, __LINE__, "Connection closed with error\n");
			break;
		}

		res = oph_net_readn(sockfd, line, OPH_IO_SERVER_MSG_TYPE_LEN);
		if (res > 0) {
			//Request Manager section: handle request and call the correct function

			m = 0;
			payload_len = 0;

			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Received %d bytes\n", res);
			logging(LOG_DEBUG, __FILE__, __LINE__, "Received %d bytes\n", res);
			line[OPH_IO_SERVER_MSG_TYPE_LEN] = 0;
			pmesg(LOG_DEBUG, __FILE__, __LINE__, "Received query '%s'\n", line);
			logging(LOG_DEBUG, __FILE__, __LINE__, "Received query '%s'\n", line);

			//Decode message and find payload length
			snprintf(header, OPH_IO_SERVER_MSG_TYPE_LEN + 1, "%s", line);

			//Select operation
			if (STRCMP(header, OPH_IO_SERVER_MSG_PING) == 0) {
				//Answer to ping
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sending ping answer...\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Sending ping answer...\n");
				if (write(sockfd, (void *) OPH_IO_SERVER_MSG_PING, strlen(OPH_IO_SERVER_MSG_PING)) != strlen(OPH_IO_SERVER_MSG_PING)) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					break;
				}
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
			} else if (STRCMP(header, OPH_IO_SERVER_MSG_USE_DB) == 0) {
				//Set database
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Setting default database...\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Setting default database...\n");
				//Read payload len
				res = oph_net_readn(sockfd, line, OPH_IO_SERVER_MSG_LONG_LEN);
				if (res <= 0)
					break;
				line[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
				payload_len = *((unsigned long long *) line);
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Db name length: %llu\n", payload_len);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Db name length: %llu\n", payload_len);

				//Read database name
				res = oph_net_readn(sockfd, line, payload_len);
				if (res <= 0)
					break;
				line[payload_len] = 0;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Database name: %s\n", line);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Database name: %s\n", line);

				//Read payload len
				res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
				if (res <= 0)
					break;
				result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
				payload_len = *((unsigned long long *) result);
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Device length: %llu\n", payload_len);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Device length: %llu\n", payload_len);

				//Read device name
				res = oph_net_readn(sockfd, result, payload_len);
				if (res <= 0)
					break;
				result[payload_len] = 0;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Device name: %s\n", result);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Device name: %s\n", result);

				//TODO perform coerence check to verify device existance
				if (global_status.device)
					free(global_status.device);
				global_status.device = (char *) strndup(result, strlen(result));
				if (global_status.device == NULL) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
					break;
				}
				//Set thread status structure using MetaDB
				if (pthread_rwlock_rdlock(&rwlock) != 0) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Unable to lock mutex\n");
					oph_io_server_send_error(sockfd);
					break;
				}

				if (db_table != NULL) {
					if (oph_metadb_find_db(db_table, line, global_status.device, &db_row) || db_row == NULL) {
						if (pthread_rwlock_unlock(&rwlock) != 0) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
							logging(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
							oph_io_server_send_error(sockfd);
							break;
						}
						pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to find DB: %s\n", line);
						logging(LOG_WARNING, __FILE__, __LINE__, "Unable to find DB: %s\n", line);
						//Build response packet TYPE
						m = snprintf(result, strlen(OPH_IO_SERVER_REQ_ERROR) + 1, OPH_IO_SERVER_REQ_ERROR);
					} else {
						if (pthread_rwlock_unlock(&rwlock) != 0) {
							pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
							logging(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
							oph_io_server_send_error(sockfd);
							break;
						}
						//Set current db name
						if (global_status.current_db)
							free(global_status.current_db);
						global_status.current_db = (char *) strndup(line, strlen(line));
						if (global_status.current_db == NULL) {
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
							logging(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
							oph_io_server_send_error(sockfd);
							break;
						}
						//Build response packet TYPE
						m = snprintf(result, strlen(OPH_IO_SERVER_MSG_USE_DB) + 1, OPH_IO_SERVER_MSG_USE_DB);
					}
				} else {
					if (pthread_rwlock_unlock(&rwlock) != 0) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
						logging(LOG_ERROR, __FILE__, __LINE__, "Unable to unlock mutex\n");
						oph_io_server_send_error(sockfd);
						break;
					}
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to find DB: %s\n", line);
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to find DB: %s\n", line);
					//Build response packet TYPE
					m = snprintf(result, strlen(OPH_IO_SERVER_REQ_ERROR) + 1, OPH_IO_SERVER_REQ_ERROR);
				}

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", m);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", m);
				if (write(sockfd, (void *) result, m) != m) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					break;
				}
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
			} else if (STRCMP(header, OPH_IO_SERVER_MSG_RESULT) == 0) {
				//Get resultset
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Retrieving result set...\n");

				if (global_status.last_result_set == NULL) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Result set of last query is corrupted\n");
					logging(LOG_WARNING, __FILE__, __LINE__, "Result set of last query is corrupted\n");
					oph_io_server_send_error(sockfd);
					break;
				}
				//Build request packet TYPE|PAYLOAD_LENGTH|NUM_ROWS|NUM_FIELDS|PAYLOAD
				current_threshold = max_packet_length;
				result_buffer = (char *) calloc(current_threshold, sizeof(char));
				if (!result_buffer) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
					oph_io_server_send_error(sockfd);
					break;
				}
				tmp_buffer = NULL;

				m = snprintf(result_buffer, strlen(OPH_IO_SERVER_MSG_RESULT) + 1, OPH_IO_SERVER_MSG_RESULT);
				k = m + sizeof(unsigned long long) + sizeof(unsigned long long) + sizeof(unsigned int);

				i = 0;
				j = 0;
				size = 0;
				num_fields = global_status.last_result_set->field_num;

				if (k >= current_threshold) {
					current_threshold *= 2;
					tmp_buffer = (char *) realloc(result_buffer, current_threshold * sizeof(char));
					if (!tmp_buffer) {
						pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
						logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
						oph_io_server_send_error(sockfd);
						free(result_buffer);
						break;
					}
					result_buffer = tmp_buffer;
				}
				//If non-empty record set
				if (global_status.last_result_set->record_set != NULL) {

					while (global_status.last_result_set->record_set[i]) {
						//Send each row of result set

						//TODO send also field name
						for (j = 0; j < num_fields; j++) {
							//Check field type
							if (global_status.last_result_set->field_type[j] != OPH_IOSTORE_STRING_TYPE) {
								//Convert to string
								if (global_status.last_result_set->field_type[j] == OPH_IOSTORE_LONG_TYPE) {
									snprintf(buffer, OPH_IO_SERVER_MAX_LONG_LEN, "%llu",
										 *((unsigned long long *) global_status.last_result_set->record_set[i]->field[j]));
								} else {
									snprintf(buffer, OPH_IO_SERVER_MAX_DOUBLE_LEN, "%f", *((double *) global_status.last_result_set->record_set[i]->field[j]));
								}
								size = strlen(buffer) + 1;

								//Check current size
								if ((k + size + sizeof(unsigned long long)) >= current_threshold) {
									current_threshold *= 2;
									tmp_buffer = (char *) realloc(result_buffer, current_threshold * sizeof(char));
									if (!tmp_buffer) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
										logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
										oph_io_server_send_error(sockfd);
										free(result_buffer);
										break;
									}
									result_buffer = tmp_buffer;
								}

								memcpy(result_buffer + k, (void *) &size, sizeof(unsigned long long));
								k += sizeof(unsigned long long);
								memcpy(result_buffer + k, (void *) buffer, size);
								k += size;
							} else {
								//String and binary values are already char*

								//Check current size
								if ((k + global_status.last_result_set->record_set[i]->field_length[j] + sizeof(unsigned long long)) >= current_threshold) {
									current_threshold *= 2;
									tmp_buffer = (char *) realloc(result_buffer, current_threshold * sizeof(char));
									if (!tmp_buffer) {
										pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
										logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
										oph_io_server_send_error(sockfd);
										free(result_buffer);
										break;
									}
									result_buffer = tmp_buffer;
								}

								memcpy(result_buffer + k, (void *) &(global_status.last_result_set->record_set[i]->field_length[j]), sizeof(unsigned long long));
								k += sizeof(unsigned long long);
								memcpy(result_buffer + k, (void *) (global_status.last_result_set->record_set[i]->field[j]),
								       global_status.last_result_set->record_set[i]->field_length[j]);
								k += global_status.last_result_set->record_set[i]->field_length[j];
							}
							pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg[%d] progressive length is: %lld\n", j, k);
						}
						i++;
					}
				}
				payload_len = k - (m + sizeof(unsigned long long) + sizeof(unsigned long long) + sizeof(unsigned int));
				memcpy(result_buffer + m, (void *) &payload_len, sizeof(unsigned long long));
				m += sizeof(unsigned long long);
				memcpy(result_buffer + m, (void *) &i, sizeof(unsigned long long));
				m += sizeof(unsigned long long);
				memcpy(result_buffer + m, (void *) &num_fields, sizeof(unsigned int));
				//Transfer result set

				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", k);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", k);

				if (write(sockfd, (void *) result_buffer, k) != (ssize_t) k) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					free(result_buffer);
					break;
				}
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");

				free(result_buffer);
			} else if (STRCMP(header, OPH_IO_SERVER_MSG_EXEC_QUERY) == 0) {

#ifdef DEBUG
				gettimeofday(&s_time, NULL);
#endif
				//Execute query
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Setup query...\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Setup query...\n");
				//Read payload len
				res = oph_net_readn(sockfd, line, OPH_IO_SERVER_MSG_SHORT_LEN);
				if (res <= 0)
					break;
				line[OPH_IO_SERVER_MSG_SHORT_LEN] = 0;
				arg_count = *((unsigned int *) line) - 1;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Number of args: %u\n", arg_count);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Number of args: %u\n", arg_count);

				//Read payload len
				res = oph_net_readn(sockfd, line, OPH_IO_SERVER_MSG_LONG_LEN);
				if (res <= 0)
					break;
				line[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
				payload_len = *((unsigned long long *) line);
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Query length: %llu\n", payload_len);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Query length: %llu\n", payload_len);

				//Read query
				res = oph_net_readn(sockfd, line, payload_len);
				if (res <= 0)
					break;
				line[payload_len] = 0;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Query is: %s - threadID: %lu\n", line, tid);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Query is: %s\n", line);

				//Read payload len
				res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
				if (res <= 0)
					break;
				result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
				payload_len = *((unsigned long long *) result);
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Device length: %llu\n", payload_len);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Device length: %llu\n", payload_len);

				if (payload_len >= max_packet_length) {
					//Request too long
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Request length is too big ...\n");
					logging(LOG_WARNING, __FILE__, __LINE__, "Request length is too big ...\n");
					oph_io_server_send_error(sockfd);
					break;
				}
				//Read device name
				res = oph_net_readn(sockfd, result, payload_len);
				if (res <= 0)
					break;
				result[payload_len] = 0;
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Device name: %s\n", result);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Device name: %s\n", result);

				//TODO perform coerence check to verify device existance
				if (global_status.device)
					free(global_status.device);
				global_status.device = (char *) strndup(result, strlen(result));
				if (global_status.device == NULL) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to set default device: %s\n", result);
					break;
				}

				oph_query_arg **args = NULL;

				n = 0;

				//If complex query (with prepared statement) parse following arguments and save query status
				if (arg_count > 0) {
					//Set status (save current/total runs + alloc argument array) + status flag + alloc result set container 
					//Decode complex part of message ...|N_RUN|CURR_RUN|ARG1_LEN|ARG1_TYPE|ARG1|...

					//Read number of runs
					res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
					if (res <= 0)
						break;
					result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
					tot_run = *((unsigned long long *) result);
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "Total runs: %llu\n", tot_run);
					logging(LOG_DEBUG, __FILE__, __LINE__, "Total runs: %llu\n", tot_run);

					//Read number of runs
					res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
					if (res <= 0)
						break;
					result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
					curr_run = *((unsigned long long *) result);
					pmesg(LOG_DEBUG, __FILE__, __LINE__, "Current run: %llu\n", curr_run);
					logging(LOG_DEBUG, __FILE__, __LINE__, "Current run: %llu\n", curr_run);

					//Check if current statement is already in progress
					if (global_status.curr_stmt != NULL) {
						if (curr_run > tot_run) {
							//Corrupted section then exit
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
							logging(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
							oph_io_server_send_error(sockfd);
							break;
						} else {
							//Decode message and update statement status
							global_status.curr_stmt->curr_run = curr_run;
							global_status.curr_stmt->tot_run = tot_run;

							args = (oph_query_arg **) calloc(arg_count + 1, sizeof(oph_query_arg *));
							if (!args) {
								pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
								logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
								oph_io_server_send_error(sockfd);
								break;
							}

							for (n = 0; n < arg_count; n++) {

								args[n] = (oph_query_arg *) malloc(1 * sizeof(oph_query_arg));
								if (!args[n]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
									logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
									oph_io_server_send_error(sockfd);
									break;
								}
								//Read arg length
								res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
								if (res <= 0)
									break;
								result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
								payload_len = *((unsigned long long *) result);
								pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg %d len: %d\n", n, payload_len);
								logging(LOG_DEBUG, __FILE__, __LINE__, "Arg %d len: %d\n", n, payload_len);

								args[n]->arg_length = payload_len;
								args[n]->arg_is_null = 0;

								//Read arg type
								res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_TYPE_LEN);
								if (res <= 0)
									break;
								result[OPH_IO_SERVER_MSG_TYPE_LEN] = 0;
								pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);
								logging(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);

								if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_LONG)) {
									args[n]->arg_type = OPH_QUERY_TYPE_LONG;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_DOUBLE)) {
									args[n]->arg_type = OPH_QUERY_TYPE_DOUBLE;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_NULL)) {
									args[n]->arg_type = OPH_QUERY_TYPE_NULL;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_VARCHAR)) {
									args[n]->arg_type = OPH_QUERY_TYPE_VARCHAR;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_BLOB)) {
									args[n]->arg_type = OPH_QUERY_TYPE_BLOB;
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Argument type not recognized\n");
									logging(LOG_ERROR, __FILE__, __LINE__, "Argument type not recognized\n");
									break;
								}

								//Read arg
								res = oph_net_readn(sockfd, result, payload_len);
								if (res <= 0)
									break;
								result[payload_len] = 0;

								args[n]->arg = (void *) memdup(result, payload_len);

							}

							//Check termination condition
							if (n < arg_count) {
								//Corrupted section then exit
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
								logging(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
								oph_io_server_send_error(sockfd);
								oph_io_server_free_query_args(args, arg_count);
								break;
							}
						}
					}
					//Create curr_stmt
					else {
						if (curr_run > 1) {
							//Corrupted section then exit
							pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
							logging(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
							oph_io_server_send_error(sockfd);
							break;
						} else {
							//Create first struct 
							global_status.curr_stmt = (oph_io_server_running_stmt *) malloc(1 * sizeof(oph_io_server_running_stmt));
							global_status.curr_stmt->tot_run = tot_run;
							global_status.curr_stmt->curr_run = curr_run;
							global_status.curr_stmt->partial_result_set = NULL;
							global_status.curr_stmt->device = NULL;
							global_status.curr_stmt->frag = NULL;
							global_status.curr_stmt->size = 0;
							global_status.curr_stmt->mi_prev_rows = 0;

							args = (oph_query_arg **) calloc(arg_count + 1, sizeof(oph_query_arg *));

							for (n = 0; n < arg_count; n++) {
								args[n] = (oph_query_arg *) malloc(1 * sizeof(oph_query_arg));
								if (!args[n]) {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
									logging(LOG_ERROR, __FILE__, __LINE__, "Unable to allocate buffer for communications\n");
									oph_io_server_send_error(sockfd);
									break;
								}
								//Read arg length
								res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_LONG_LEN);
								if (res <= 0)
									break;
								result[OPH_IO_SERVER_MSG_LONG_LEN] = 0;
								payload_len = *((unsigned long long *) result);
								pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg %d len: %d\n", n, payload_len);
								logging(LOG_DEBUG, __FILE__, __LINE__, "Arg %d len: %d\n", n, payload_len);

								args[n]->arg_length = payload_len;
								args[n]->arg_is_null = 0;

								//Read arg type
								res = oph_net_readn(sockfd, result, OPH_IO_SERVER_MSG_TYPE_LEN);
								if (res <= 0)
									break;
								result[OPH_IO_SERVER_MSG_TYPE_LEN] = 0;
								pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);
								logging(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);

								if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_LONG)) {
									args[n]->arg_type = OPH_QUERY_TYPE_LONG;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_DOUBLE)) {
									args[n]->arg_type = OPH_QUERY_TYPE_DOUBLE;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_NULL)) {
									args[n]->arg_type = OPH_QUERY_TYPE_NULL;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_VARCHAR)) {
									args[n]->arg_type = OPH_QUERY_TYPE_VARCHAR;
								} else if (STRCMP(result, OPH_IO_SERVER_MSG_ARG_DATA_BLOB)) {
									args[n]->arg_type = OPH_QUERY_TYPE_BLOB;
								} else {
									pmesg(LOG_ERROR, __FILE__, __LINE__, "Argument type not recognized\n");
									logging(LOG_ERROR, __FILE__, __LINE__, "Argument type not recognized\n");
									break;
								}

								//Read arg
								res = oph_net_readn(sockfd, result, payload_len);
								if (res <= 0)
									break;
								result[payload_len] = 0;
								pmesg(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);
								logging(LOG_DEBUG, __FILE__, __LINE__, "Arg %d type: %d\n", n, result);

								args[n]->arg = (void *) memdup(result, payload_len);

							}
							//Check termination condition
							if (n < arg_count) {
								//Corrupted section then exit
								pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
								logging(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
								oph_io_server_send_error(sockfd);
								oph_io_server_free_query_args(args, arg_count);
								break;
							}
						}
					}
				}
#ifdef DEBUG
				gettimeofday(&e_time, NULL);
				timeval_subtract(&t_time, &e_time, &s_time);
				pmesg(LOG_INFO, __FILE__, __LINE__, "Setup query:\t Time %d,%06d sec\n", (int) t_time.tv_sec, (int) t_time.tv_usec);
				gettimeofday(&s_time, NULL);
#endif
				//Define global variables
				HASHTBL *query_args = NULL;

				if (oph_query_parser(line, &query_args)) {
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to run query\n");
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to run query\n");
					oph_io_server_send_error(sockfd);
					oph_io_server_free_query_args(args, arg_count);
					break;
				}
#ifdef DEBUG
				gettimeofday(&e_time, NULL);
				timeval_subtract(&t_time, &e_time, &s_time);
				pmesg(LOG_INFO, __FILE__, __LINE__, "Parse query %s:\t Time %d,%06d sec\n", line, (int) t_time.tv_sec, (int) t_time.tv_usec);
				gettimeofday(&s_time, NULL);
#endif

				oph_iostore_handler *dev_handle = NULL;

				if (oph_iostore_setup(global_status.device, &dev_handle) != 0) {
					hashtbl_destroy(query_args);
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to setup iostorage\n");
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to setup iostorage\n");
					oph_io_server_send_error(sockfd);
					oph_io_server_free_query_args(args, arg_count);
					break;
				}
				//TODO if query is SELECT then set globally last result set
				if (oph_io_server_dispatcher(&db_table, dev_handle, &global_status, args, query_args, plugin_table)) {

					oph_iostore_cleanup(dev_handle);
					hashtbl_destroy(query_args);
					pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to run query\n");
					logging(LOG_WARNING, __FILE__, __LINE__, "Unable to run query\n");
					oph_io_server_send_error(sockfd);
					oph_io_server_free_query_args(args, arg_count);
					break;
				}

				oph_iostore_cleanup(dev_handle);

#ifdef DEBUG
				gettimeofday(&e_time, NULL);
				timeval_subtract(&t_time, &e_time, &s_time);
				pmesg(LOG_INFO, __FILE__, __LINE__, "Exec query %s:\t Time %d,%06d sec\n", line, (int) t_time.tv_sec, (int) t_time.tv_usec);
				gettimeofday(&s_time, NULL);
#endif
				hashtbl_destroy(query_args);
				//Delete temp result set
				oph_io_server_free_query_args(args, arg_count);

				//Build response packet TYPE
				m = snprintf(line, strlen(OPH_IO_SERVER_MSG_EXEC_QUERY) + 1, OPH_IO_SERVER_MSG_EXEC_QUERY);
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", m);
				logging(LOG_DEBUG, __FILE__, __LINE__, "Sending %d bytes\n", m);
				if (write(sockfd, (void *) line, m) != m) {
					pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					logging(LOG_ERROR, __FILE__, __LINE__, "Error while writing to socket\n");
					break;
				}
				pmesg(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");
				logging(LOG_DEBUG, __FILE__, __LINE__, "Result sent\n");

#ifdef DEBUG
				gettimeofday(&e_time, NULL);
				timeval_subtract(&t_time, &e_time, &s_time);
				pmesg(LOG_INFO, __FILE__, __LINE__, "Reply:\t Time %d,%06d sec\n", (int) t_time.tv_sec, (int) t_time.tv_usec);
#endif

			} else {
				pmesg(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
				logging(LOG_WARNING, __FILE__, __LINE__, "Unable to understand request '%s'...\n", header);
				oph_io_server_send_error(sockfd);
				break;
			}
		} else if (res <= 0)
			break;

#ifdef DEBUG
		gettimeofday(&end_time, NULL);
		timeval_subtract(&total_time, &end_time, &start_time);
		pmesg(LOG_INFO, __FILE__, __LINE__, "Total reply:\t Time %d,%06d sec\n", (int) total_time.tv_sec, (int) total_time.tv_usec);
#endif
	}

	oph_io_server_free_status(&global_status);
	free(result);
	free(line);

}
