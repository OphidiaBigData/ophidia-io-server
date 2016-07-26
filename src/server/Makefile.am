#
#    Ophidia IO Server
#    Copyright (C) 2014-2016 CMCC Foundation
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

if DEBUG 
OPT+=-DDEBUG
endif

bin_PROGRAMS= oph_io_server 
if DEBUG
bin_PROGRAMS+=oph_io_server_test 
endif
bindir=${prefix}/bin

oph_io_server_SOURCES = oph_io_server_query.c oph_io_server_thread.c oph_io_server.c
oph_io_server_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common -I../iostorage  -fPIC -I. -I../query_engine -I../metadb -I../network @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_LDADD = -L. -L../common -ldebug -lpthread -loph_server_conf -L../metadb  -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser  -L../iostorage -loph_iostorage_data  -loph_iostorage_interface -L../network -loph_network 
oph_io_server_LDFLAGS= -Wl,-R -Wl,. 


if DEBUG
oph_io_server_test_SOURCES = oph_io_server_query.c oph_io_server_thread.c oph_io_server_test.c
oph_io_server_test_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common -I../iostorage  -fPIC -I. -I../query_engine -I../metadb -I../network  @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_test_LDADD = -L. -L../common -ldebug -lpthread -loph_server_conf -L../metadb  -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser  -L../iostorage -loph_iostorage_data  -loph_iostorage_interface  -L../network -loph_network
oph_io_server_test_LDFLAGS= -Wl,-R -Wl,. 
endif
