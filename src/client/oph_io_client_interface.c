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

#include "debug.h"
#include "oph_io_client_interface.h"

#include "oph_server_utility.h"

#include <string.h>
#include <unistd.h>

#include "oph_network.h"

//TODO poll the tcp socket to discover if the connection was lost

int _oph_io_client_ping_connection(oph_io_client_connection *connection){
	if (!connection)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

	char reply[strlen(OPH_IO_CLIENT_MSG_PING) + 1];
  int res = 0;

  if(connection->socket){
    //Check connection state sending a short ping string
  	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Sending %d bytes\n",strlen(OPH_IO_CLIENT_MSG_PING));
	  if (write(connection->socket, (void*)OPH_IO_CLIENT_MSG_PING, strlen(OPH_IO_CLIENT_MSG_PING)) != strlen(OPH_IO_CLIENT_MSG_PING)){
      pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while writing to socket\n");
      return OPH_IO_CLIENT_INTERFACE_IO_ERR;
    }

    //Get answer
	  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Waiting for answer...\n");
	  res = oph_net_readn(connection->socket, reply, strlen(OPH_IO_CLIENT_MSG_PING));
	  if (!res)
	  {
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
		  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
	  }
    reply[strlen(OPH_IO_CLIENT_MSG_PING)] = 0;

    //Check message
    if(STRCMP(OPH_IO_CLIENT_MSG_PING, reply) != 0 )
	  {
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"Error pinging server\n");
		  return OPH_IO_CLIENT_INTERFACE_QUERY_ERR;
	  }
	  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Ping answer received\n");
    
	  return OPH_IO_CLIENT_INTERFACE_OK;
  }
  else
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
}

int oph_io_client_setup (){
	  return OPH_IO_CLIENT_INTERFACE_OK;
}

//Connect or reconnect
int oph_io_client_connect (const char *hostname, const char *port, const char *db_name, const char *device, oph_io_client_connection **connection){
	if (!hostname || !port || !connection)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

	int fd=0;

  if(*connection){
    if((*connection)->socket){
      //Check connection state
      if(!_oph_io_client_ping_connection(*connection)){
        //Connection established
		    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection already established\n");
		    return OPH_IO_CLIENT_INTERFACE_OK;
      }
      else{
        //Try to close connection
	      if(close((*connection)->socket) == -1){
        	pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while closing connection!\n");
      		return OPH_IO_CLIENT_INTERFACE_IO_ERR;
        }
        free(*connection);
      }
    }
    else
      free(*connection);
  }

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connecting to %s:%s...\n",hostname,port);
	
	if (oph_net_connect(hostname, port, &fd) != 0)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Connection error\n");
		return OPH_IO_CLIENT_INTERFACE_IO_ERR;
	}

  //Create connection struct
  *connection = malloc(sizeof(oph_io_client_connection));
  if(!*connection)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to allocate connection struct\n");
		return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
	}

  strncpy((*connection)->port, port, OPH_IO_CLIENT_PORT_LEN );
  strncpy((*connection)->host, hostname, OPH_IO_CLIENT_HOST_LEN );
  memset((*connection)->db_name, 0, OPH_IO_CLIENT_DB_LEN);
  (*connection)->port[OPH_IO_CLIENT_PORT_LEN-1] = 0;
  (*connection)->host[OPH_IO_CLIENT_HOST_LEN-1] = 0;
  (*connection)->socket = fd;

	//Set default db
	if(db_name){
		if(oph_io_client_use_db (db_name, device, *connection))
		{
			pmesg(LOG_ERROR,__FILE__,__LINE__,"Connection error\n");
			return OPH_IO_CLIENT_INTERFACE_IO_ERR;
		}
		strncpy((*connection)->db_name, db_name, OPH_IO_CLIENT_DB_LEN );	
	}

	return OPH_IO_CLIENT_INTERFACE_OK;
}


int oph_io_client_use_db (const char *db_name, const char *device, oph_io_client_connection *connection){
	if (!db_name || !connection || !device)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  if(connection->socket){
    //Check connection state
/*    if(_oph_io_client_ping_connection(connection)){
      pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
      return OPH_IO_CLIENT_INTERFACE_OK;    
    }*/
  }
  else{
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
    return OPH_IO_CLIENT_INTERFACE_OK;    
  }

  //set default db
	char *request = NULL;
  unsigned int m = 0;
  int res = 0;

  unsigned long long message_len = strlen(OPH_IO_CLIENT_MSG_USE_DB) + 1 + 2*sizeof(unsigned long long) + strlen(db_name) +1 + strlen(device) +1;
  request = (char *)calloc(message_len ,sizeof(char));
  if (!request)
  {
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }

  //Build request packet TYPE|DB_NAME_LEN|DB_NAME|DEVICE_LEN|DEVICE
  m = snprintf(request, strlen(OPH_IO_CLIENT_MSG_USE_DB) +1, OPH_IO_CLIENT_MSG_USE_DB);
  unsigned long long payload_len = strlen(db_name);
  memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
  m += sizeof(unsigned long long);
  m += snprintf(request+m, strlen(db_name)+1, "%s", db_name);
  payload_len = strlen(device);
  memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
  m += sizeof(unsigned long long);
  m += snprintf(request+m, strlen(device)+1, "%s", device);

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Sending %d bytes\n",m);
	if (write(connection->socket, (void*)request, m) != m){
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while writing to socket\n");
    free(request);
    return OPH_IO_CLIENT_INTERFACE_IO_ERR;
  }

  free(request);

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Waiting for answer...\n");
  //Decode response
	char reply[strlen(OPH_IO_CLIENT_MSG_USE_DB) + 1];
  res = oph_net_readn(connection->socket, reply, strlen(OPH_IO_CLIENT_MSG_USE_DB));
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  reply[strlen(OPH_IO_CLIENT_MSG_USE_DB)] = 0;
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Response received: %s\n",reply);

  if(STRCMP(OPH_IO_CLIENT_MSG_USE_DB, reply) != 0 )
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error setting default database\n");
		return OPH_IO_CLIENT_INTERFACE_QUERY_ERR;
	}
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Default database %s on device %s set correctly\n",db_name, device);

	return OPH_IO_CLIENT_INTERFACE_OK;
} 

int oph_io_client_setup_query (oph_io_client_connection *connection, const char *operation, const char *device, unsigned long long tot_run, oph_io_client_query_arg **args, oph_io_client_query **query){

	if (!connection || !query || !operation || !device)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  *query = (oph_io_client_query *)malloc(1*sizeof(oph_io_client_query));
  if(!*query){
    pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }
  (*query)->query = NULL;
  (*query)->args = NULL;
  (*query)->args_count = 0;
  (*query)->fixed_length = 0;
  (*query)->args_length = 0;
  (*query)->tot_run = 0;
  (*query)->curr_run = 0;

  unsigned int m = 0, n = 0;
  unsigned long long message_len = 0;
  char *request = NULL;
  unsigned int arg_number = 0;
  unsigned long long payload_len = 0;

  if(!args ){
    //Build simple request message TYPE|ARG_NUMBER|QUERY_LEN|QUERY|DEVICE_LEN|DEVICE

    message_len = strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY) + 1 + sizeof(unsigned int) + 2*sizeof(unsigned long long) + strlen(operation) +1 + strlen(operation) +1;
	  request = (char *)calloc(message_len ,sizeof(char));
    if (!request)
    {
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
      free(*query);
      return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
    }

    m = snprintf(request, strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY) +1, OPH_IO_CLIENT_MSG_EXEC_QUERY);
    arg_number = 1;
    memcpy(request+m,(void *)&arg_number,sizeof(unsigned int));
    m += sizeof(unsigned int);
    payload_len = strlen(operation);
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);
    m += snprintf(request+m, strlen(operation)+1, "%s", operation);
    payload_len = strlen(device);
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);
    m += snprintf(request+m, strlen(device)+1, "%s", device);

    (*query)->query = (void *)request;
    (*query)->fixed_length = m;
  }
  else{
    //Build complex request message TYPE|ARG_NUMBER|QUERY_LEN|QUERY|DEVICE_LEN|DEVICE|N_RUN|CURR_RUN|ARG1_LEN|ARG1_TYPE|ARG1|...

    unsigned int arg_count = 0;
    oph_io_client_query_arg *arg_ptr = args[0];
    //Reach null-termination
    while(arg_ptr) arg_ptr = args[++arg_count];

	  //TODO check if number of ? in query is the same as number of arguments

    //First part of message has fixed size
    message_len = strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY) + 1 + sizeof(unsigned int) + 2*sizeof(unsigned long long) + strlen(operation) +1 + strlen(operation) +1 + 2*sizeof(unsigned long long);
    
    //Pre-calculate variable part length
    unsigned long long arg_len = arg_count * (strlen(OPH_IO_CLIENT_MSG_ARG_DATA_LONG) + 1 + sizeof(unsigned long long)); 
    //Get size of variable args   
    for(n = 0; n < arg_count; n++){
      arg_ptr = args[n];
      arg_len += arg_ptr->arg_length;
    }

	  request = (char *)calloc(message_len + arg_len ,sizeof(char));
    if (!request)
    {
      pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
      return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
    }

    m = snprintf(request, strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY) +1, OPH_IO_CLIENT_MSG_EXEC_QUERY);
    arg_number = 1 + arg_count;
    memcpy(request+m,(void *)&arg_number,sizeof(unsigned int));
    m += sizeof(unsigned int);
    payload_len = strlen(operation);
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);
    m += snprintf(request+m, strlen(operation)+1, "%s", operation);
    payload_len = strlen(device);
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);
    m += snprintf(request+m, strlen(device)+1, "%s", device);
    payload_len = tot_run;
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);
    payload_len = 0;
    memcpy(request+m,(void *)&payload_len,sizeof(unsigned long long));
    m += sizeof(unsigned long long);

    (*query)->args = args;
    (*query)->query = (void *)request;
    (*query)->fixed_length = m;
    (*query)->args_count = arg_count;
    (*query)->args_length = arg_len;
    (*query)->tot_run = tot_run;
    (*query)->curr_run = 1;
  }
  
	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_execute_query (oph_io_client_connection *connection, oph_io_client_query *query)
{

	if (!connection || !query || !query->query )
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}
  
  if(connection->socket){
    //Check connection state
/*    if(_oph_io_client_ping_connection(connection)){
      pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
      return OPH_IO_CLIENT_INTERFACE_DATA_ERR;    
    }*/
  }
  else{
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
    return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
  }

  unsigned int n = 0, m = 0;
  int res = 0;

  if(!query->args){
	  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Sending %d bytes\n", query->fixed_length);
	  if (write(connection->socket, (void*)query->query, query->fixed_length) != query->fixed_length){
	    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while writing to socket\n");
      return OPH_IO_CLIENT_INTERFACE_IO_ERR;
    }
  }
  else {
    //Re-calculate variable part length
    unsigned long long arg_len = query->args_count * (strlen(OPH_IO_CLIENT_MSG_ARG_DATA_LONG) + 1 + sizeof(unsigned long long)); 
    //Get size of variable args   
    for(n = 0; n < query->args_count; n++){
      arg_len += query->args[n]->arg_length;
    }
    char *query_ptr = NULL;
    if(arg_len > query->args_length){
      //Realloc args array 
      query_ptr = realloc(query->query, arg_len);
      if(!query_ptr)
      {
        pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
        return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
      }
      query->query = query_ptr;
      query->args_length = arg_len;
    }

    m = query->fixed_length - sizeof(unsigned long long);
    //Store current row number into buffer
    memcpy(query->query + m,(void *)&(query->curr_run),sizeof(unsigned long long));
    m += sizeof(unsigned long long);

    for(n = 0; n < query->args_count; n++){
      memcpy(query->query + m,(void *)&(query->args[n]->arg_length),sizeof(unsigned long long));
      m += sizeof(unsigned long long);

      //Switch on type
      switch(query->args[n]->arg_type){
          case OPH_IO_CLIENT_TYPE_DECIMAL: case  OPH_IO_CLIENT_TYPE_LONG: case OPH_IO_CLIENT_TYPE_LONGLONG:
            m += snprintf(query->query + m, strlen(OPH_IO_CLIENT_MSG_ARG_DATA_LONG) +1, OPH_IO_CLIENT_MSG_ARG_DATA_LONG);
            break;
          case OPH_IO_CLIENT_TYPE_FLOAT: case OPH_IO_CLIENT_TYPE_DOUBLE:
            m += snprintf(query->query + m, strlen(OPH_IO_CLIENT_MSG_ARG_DATA_DOUBLE) +1, OPH_IO_CLIENT_MSG_ARG_DATA_DOUBLE);
            break;
          case OPH_IO_CLIENT_TYPE_NULL: 
            m += snprintf(query->query + m, strlen(OPH_IO_CLIENT_MSG_ARG_DATA_NULL) +1, OPH_IO_CLIENT_MSG_ARG_DATA_NULL);
            break;
          case OPH_IO_CLIENT_TYPE_VARCHAR: 
            m += snprintf(query->query + m, strlen(OPH_IO_CLIENT_MSG_ARG_DATA_VARCHAR) +1, OPH_IO_CLIENT_MSG_ARG_DATA_VARCHAR);
            break;
          case OPH_IO_CLIENT_TYPE_LONG_BLOB: case OPH_IO_CLIENT_TYPE_BLOB: case OPH_IO_CLIENT_TYPE_BIT:
            m += snprintf(query->query + m, strlen(OPH_IO_CLIENT_MSG_ARG_DATA_BLOB) +1, OPH_IO_CLIENT_MSG_ARG_DATA_BLOB);
            break;
          default :
            pmesg(LOG_ERROR, __FILE__, __LINE__, "Argument type not recognized\n");
            return OPH_IO_CLIENT_INTERFACE_QUERY_ERR;
      }
      memcpy(query->query + m,(void *)(query->args[n]->arg),query->args[n]->arg_length);
      m += query->args[n]->arg_length;
    }
	  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Sending %d bytes\n", m);
	  if (write(connection->socket, (void*)query->query, m) != m){
	    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while writing to socket\n");
      return OPH_IO_CLIENT_INTERFACE_IO_ERR;
    }

    query->curr_run++;
  }

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Waiting for answer...\n");

  //Decode response
	char reply[strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY) + 1];
  res = oph_net_readn(connection->socket, reply, strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY));
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  reply[strlen(OPH_IO_CLIENT_MSG_EXEC_QUERY)] = 0;
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Response received: %s\n",reply);

  if(STRCMP(OPH_IO_CLIENT_MSG_EXEC_QUERY, reply) != 0 )
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error executing query\n");
		return OPH_IO_CLIENT_INTERFACE_QUERY_ERR;
	}
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Query executed correctly\n");

	return OPH_IO_CLIENT_INTERFACE_OK;
} 

int oph_io_client_free_query (oph_io_client_query *query){
	if (!query)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  if(query->query) free(query->query);
  free(query);
  
	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_get_result(oph_io_client_connection *connection,  oph_io_client_result **result_set){
	if (!result_set || !connection)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  if(connection->socket){
    //Check connection state
/*    if(_oph_io_client_ping_connection(connection)){
      pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
      return OPH_IO_CLIENT_INTERFACE_OK;    
    }*/
  }
  else{
    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection was closed\n");
    return OPH_IO_CLIENT_INTERFACE_OK;    
  }

	char request[strlen(OPH_IO_CLIENT_MSG_RESULT) +1];
  unsigned int m = 0;
  int res = 0;

  //Build request packet TYPE
  m = snprintf(request, strlen(OPH_IO_CLIENT_MSG_RESULT) +1, OPH_IO_CLIENT_MSG_RESULT);

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Sending %d bytes\n",m);
	if (write(connection->socket, (void*)request, m) != m){
    pmesg(LOG_ERROR,__FILE__,__LINE__,"Error while writing to socket\n");
    return OPH_IO_CLIENT_INTERFACE_IO_ERR;
  }

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Waiting for answer...\n");
  //transfer result + build structure

	char reply_type[strlen(OPH_IO_CLIENT_MSG_RESULT) + 1];
  res = oph_net_readn(connection->socket, reply_type, strlen(OPH_IO_CLIENT_MSG_RESULT));
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  reply_type[strlen(OPH_IO_CLIENT_MSG_RESULT)] = 0;
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Response received: %s\n",reply_type);

  if(STRCMP(OPH_IO_CLIENT_MSG_RESULT, reply_type) != 0 )
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Error transfering result\n");
		return OPH_IO_CLIENT_INTERFACE_QUERY_ERR;
	}
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Transfer executed\n");

  //Read payload len
	char reply_info[sizeof(unsigned long long)];
  unsigned long long payload_len = 0;
  memset(reply_info, 0, sizeof(unsigned long long));
  res = oph_net_readn(connection->socket, reply_info, OPH_IO_CLIENT_MSG_LONG_LEN);
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  memcpy(&payload_len, reply_info, sizeof(unsigned long long));
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Response length: %llu\n",payload_len);

  unsigned long long num_rows = 0;
  res = oph_net_readn(connection->socket, reply_info, OPH_IO_CLIENT_MSG_LONG_LEN);
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  memcpy(&num_rows, reply_info, sizeof(unsigned long long));
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Number of rows: %llu\n",num_rows);

  unsigned int num_fields = 0;
  res = oph_net_readn(connection->socket, reply_info, OPH_IO_CLIENT_MSG_SHORT_LEN);
  if (!res)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
	  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
  }
  memcpy(&num_fields, reply_info, sizeof(unsigned long long));
  pmesg(LOG_DEBUG,__FILE__,__LINE__,"Number of fields: %u\n",num_fields);

  //Rebuild result set struct
  *result_set = (oph_io_client_result *)calloc(1,sizeof(oph_io_client_result));
  if(!(*result_set))  
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
    *result_set = NULL;
	  return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }
  (*result_set)->num_rows = num_rows;
  (*result_set)->num_fields = num_fields;
  (*result_set)->current_row = 0;
  (*result_set)->result_set = 0;
  (*result_set)->max_field_length = (unsigned long long *)calloc(num_fields,sizeof(unsigned long long));

  if(!(*result_set)->max_field_length)
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
    oph_io_client_free_result(*result_set);
    *result_set = NULL;
	  return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }
  (*result_set)->result_set = (oph_io_client_record **)calloc(num_rows+1,sizeof(oph_io_client_record *));

  if(!((*result_set)->result_set))
  {
	  pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
    oph_io_client_free_result(*result_set);
    *result_set = NULL;
	  return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }
  unsigned long long i = 0, j = 0;

  //If result set does contain rows
  if(num_rows > 0){
	  //Read payload (binary format)
	  char *reply = (char *)calloc(payload_len ,sizeof(char));
	  if (!reply)
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
		return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
	  }
	  res = oph_net_readn(connection->socket, reply, payload_len);
	  if (!res)
	  {
		  pmesg(LOG_ERROR,__FILE__,__LINE__,"No reply\n");
		free(reply);
		  return OPH_IO_CLIENT_INTERFACE_CONN_ERR;
	  }

	  char *sub_reply = (char *)calloc(payload_len ,sizeof(char));
	  if (!sub_reply)
	  {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error allocation memory\n");
		free(reply);
		return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
	  }
	  unsigned long long string_head = 0;

  	//Setup each row
	  for(i = 0; i < num_rows; i++){
		  (*result_set)->result_set[i] = (oph_io_client_record *)calloc(1, sizeof(oph_io_client_record));
		  if(!((*result_set)->result_set[i]))
		  {
		    pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
		    free(reply);
		    free(sub_reply);
		    oph_io_client_free_result(*result_set);
		    *result_set = NULL;
		    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
		  }

		  (*result_set)->result_set[i]->field_length = (unsigned long *)calloc(num_fields, sizeof(unsigned long));
		  if(!((*result_set)->result_set[i]->field_length))
		  {
		    pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
		    free(reply);
		    free(sub_reply);
		    oph_io_client_free_result(*result_set);
		    *result_set = NULL;
		    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
		  }

		  (*result_set)->result_set[i]->field = (char **)calloc(num_fields+1, sizeof(char *)); 
		  if(!((*result_set)->result_set[i]->field))
		  {
		    pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
		    free(reply);
		    free(sub_reply);
		    oph_io_client_free_result(*result_set);
		    *result_set = NULL;
		    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
		  }

		  for(j = 0; j < num_fields; j++){
		    //Setup each field
	 
		   //Extract field length
		    memcpy(sub_reply, reply + string_head, sizeof(unsigned long));
		    (*result_set)->result_set[i]->field_length[j] = *((unsigned long*)sub_reply);
		    string_head += sizeof(unsigned long);
		    pmesg(LOG_DEBUG,__FILE__,__LINE__,"Field %llu, row %llu length is: %lu\n",j,i,(*result_set)->result_set[i]->field_length[j]);

		    (*result_set)->result_set[i]->field[j] = (char *)calloc((*result_set)->result_set[i]->field_length[j],sizeof(char));
		    if(!((*result_set)->result_set[i]->field[j]))
		    {
		      pmesg(LOG_ERROR,__FILE__,__LINE__,"Unable to alloc memory\n");
		      free(reply);
		      free(sub_reply);
		      oph_io_client_free_result(*result_set);
		      *result_set = NULL;
		      return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
		    }
		    memcpy((*result_set)->result_set[i]->field[j], reply + string_head, (*result_set)->result_set[i]->field_length[j]);
		    string_head += (*result_set)->result_set[i]->field_length[j];

		    //Set max field length
		    if((*result_set)->max_field_length[j] < (*result_set)->result_set[i]->field_length[j]) (*result_set)->max_field_length[j] = (*result_set)->result_set[i]->field_length[j];
		  }
	}
	free(sub_reply);
	free(reply);
  }  

	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_fetch_row(oph_io_client_result *result_set,  oph_io_client_record **current_row){
	if (!result_set || !current_row)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  if(result_set->current_row <= result_set->num_rows){
    *current_row = result_set->result_set[result_set->current_row];
    result_set->current_row++;
  }
  else
  {
    *current_row = NULL;
    pmesg(LOG_ERROR,__FILE__,__LINE__,"No row available!\n");
    return OPH_IO_CLIENT_INTERFACE_MEMORY_ERR;
  }

	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_free_result(oph_io_client_result *result){
	if (!result)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

  if(result->max_field_length) free(result->max_field_length);
  
  unsigned long long i, j;

  if(result->result_set){
    for(i = 0; i < result->num_rows; i++){
      if(result->result_set[i]){ 
        if(result->result_set[i]->field_length) free(result->result_set[i]->field_length);          
        for(j = 0; j < result->num_fields; j++){
          if(result->result_set[i]->field[j]) free(result->result_set[i]->field[j]);         
        }
        if(result->result_set[i]->field) free(result->result_set[i]->field);          
        free(result->result_set[i]);
      }
    }
    free(result->result_set);
  }

  free(result);

	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_close (oph_io_client_connection *connection){
	if (!connection)
	{
		pmesg(LOG_ERROR,__FILE__,__LINE__,"Parameters are not given\n");
		return OPH_IO_CLIENT_INTERFACE_DATA_ERR;
	}

	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Closing the connection...\n");
	if(close(connection->socket) == -1){
  	pmesg(LOG_WARNING,__FILE__,__LINE__,"Error while closing connection!\n");
  }
	pmesg(LOG_DEBUG,__FILE__,__LINE__,"Connection closed\n");

  free(connection);

	return OPH_IO_CLIENT_INTERFACE_OK;
}

int oph_io_client_cleanup (){
	return OPH_IO_CLIENT_INTERFACE_OK;
}


