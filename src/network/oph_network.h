/*
    Ophidia IO Server
    Copyright (C) 2014-2019 CMCC Foundation

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

#ifndef OPH_NETWORK_H
#define OPH_NETWORK_H

// error codes
#define OPH_NETWORK_SUCCESS                             0
#define OPH_NETWORK_ERROR                              -1

#include <netdb.h>

// Prototypes

/**
 * \brief               Function to read up to n bytes from socket
 * \param fd            Socket being read
 * \param buffer        Buffer for result read
 * \param n             Max number of bytes being read
 * \return              number of bytes read if successfull, -1 otherwise
 */
ssize_t oph_net_readn(int fd, void *buffer, size_t n);

/**
 * \brief               Function to connect to hostname:port 
 * \param host          Server hostname
 * \param port          Server port
 * \param fd            Fd descriptor of the new socket
 * \return              0 if successfull, -1 otherwise
 */
int oph_net_connect(const char *host, const char *port, int *fd);

/**
 * \brief               Function to accept a connection on a socket
 * \param in_fd         Descriptor of socket being listened
 * \param sa            Pointer to socket address
 * \param salenptr      Pointer to size of socket address
 * \param out_fd        Descriptor of socket for accepted socket
 * \return              0 if successfull, -1 otherwise
 */
int oph_net_accept(int in_fd, struct sockaddr *sa, socklen_t * salenptr, int *out_fd);

/**
 * \brief               Function to listen to a socket
 * \param host          Server hostname
 * \param port          Server port
 * \param addrelenp     Pointer to size of socket address
 * \param fd            Descriptor of socket for listened socket
 * \return              0 if successfull, -1 otherwise
 */
int oph_net_listen(const char *host, const char *port, socklen_t * addrlenp, int *fd);

/**
 * \brief               Function used to set a handler function for a signal
 * \param signo         Signal to be catched
 * \param func          Function to be executed
 * \return              0 if successfull, -1 otherwise
 */
int oph_net_signal(int signo, void *func);

#endif				/* OPH_SERVER_INTERFACE_H */
