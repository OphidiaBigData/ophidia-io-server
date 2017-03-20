/*
    Ophidia IO Server
    Copyright (C) 2014-2017 CMCC Foundation

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

#include "oph_network.h"

#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <strings.h>

#include "debug.h"
#include <errno.h>
#include <signal.h>

#define	OPH_NET_LISTEN_QUEUE		512	/* 2nd argument to listen() */

/* Read "n" bytes from a descriptor. */
ssize_t oph_net_readn(int fd, void *buffer, size_t n)
{
	/* Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html */

	size_t nleft;
	ssize_t nread;
	char *ptr;

	ptr = buffer;
	nleft = n;
	while (nleft > 0) {
		if ((nread = read(fd, ptr, nleft)) < 0) {
			if (errno == EINTR)
				nread = 0;	/* and call read() again */
			else
				return OPH_NETWORK_ERROR;
		} else if (nread == 0)
			break;	/* EOF */

		nleft -= nread;
		ptr += nread;
	}
	return (n - nleft);	/* return >= 0 */
}

int oph_net_connect(const char *host, const char *port, int *fd)
{
	/* Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html */

	int sockfd, n;
	struct addrinfo hints, *res, *ressave;
	*fd = 0;

	bzero(&hints, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((n = getaddrinfo(host, port, &hints, &res)) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "tcp_connect error for %s, %s: %s\n", host, port, gai_strerror(n));
		return OPH_NETWORK_ERROR;
	}
	ressave = res;

	do {
		sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (sockfd < 0)
			continue;	/* ignore this one */

		if (connect(sockfd, res->ai_addr, res->ai_addrlen) == 0)
			break;	/* success */

		if (close(sockfd) == -1) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while closing connection!\n");
			return OPH_NETWORK_ERROR;
		}		/* ignore this one */
	} while ((res = res->ai_next) != NULL);

	if (res == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "tcp_connect error for %s, %s\n", host, port);
		return OPH_NETWORK_ERROR;
	}

	freeaddrinfo(ressave);

	*fd = sockfd;
	return OPH_NETWORK_SUCCESS;
}

int oph_net_accept(int in_fd, struct sockaddr *sa, socklen_t * salenptr, int *out_fd)
{
	/* Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html */

	int n;
	*out_fd = 0;

      again:
	if ((n = accept(in_fd, sa, salenptr)) < 0) {
#ifdef	EPROTO
		if (errno == EPROTO || errno == ECONNABORTED)
#else
		if (errno == ECONNABORTED)
#endif
			goto again;
		else {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Accept connection error!\n");
			return OPH_NETWORK_ERROR;
		}
	}
	*out_fd = n;
	return OPH_NETWORK_SUCCESS;
}

int oph_net_listen(const char *host, const char *port, socklen_t * addrlenp, int *out_fd)
{
	/* Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html */

	int listenfd, n;
	const int on = 1;
	struct addrinfo hints, *res, *ressave;
	*out_fd = 0;

	bzero(&hints, sizeof(struct addrinfo));
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((n = getaddrinfo(host, port, &hints, &res)) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "tcp_listen error for %s, %s: %s\n", host, port, gai_strerror(n));
		return OPH_NETWORK_ERROR;
	}
	ressave = res;

	do {
		listenfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
		if (listenfd < 0)
			continue;	/* error, try next one */

		if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
			pmesg(LOG_ERROR, __FILE__, __LINE__, "Tcp socket option error\n");
			return OPH_NETWORK_ERROR;
		}

		if (bind(listenfd, res->ai_addr, res->ai_addrlen) == 0)
			break;	/* success */

		close(listenfd);	/* bind error, close and try next one */
	} while ((res = res->ai_next) != NULL);

	if (res == NULL) {	/* errno from final socket() or bind() */
		pmesg(LOG_ERROR, __FILE__, __LINE__, "tcp_listen error for %s, %s\n", host, port);
		return OPH_NETWORK_ERROR;
	}

	if (listen(listenfd, OPH_NET_LISTEN_QUEUE) != 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Error while listening socket!\n");
		return OPH_NETWORK_ERROR;
	}

	if (addrlenp)
		*addrlenp = res->ai_addrlen;	/* return size of protocol address */

	freeaddrinfo(ressave);
	*out_fd = listenfd;

	return OPH_NETWORK_SUCCESS;
}

int oph_net_signal(int signo, void *func)
{
	/* Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html */

	struct sigaction new_act, old_act;

	new_act.sa_handler = func;
	sigemptyset(&new_act.sa_mask);
	new_act.sa_flags = 0;

	if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
		new_act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
	} else {
#ifdef	SA_RESTART
		new_act.sa_flags |= SA_RESTART;	/* SVR4, 44BSD */
#endif
	}

	int res;
	if ((res = sigaction(signo, &new_act, &old_act)) < 0) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, "Unable to setup signal\n");
		return OPH_NETWORK_ERROR;
	}
	return res;
}
