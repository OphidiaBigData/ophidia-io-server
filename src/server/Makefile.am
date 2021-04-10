#
#    Ophidia IO Server
#    Copyright (C) 2014-2021 CMCC Foundation
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

noinst_LTLIBRARIES=liboph_io_server_query_manager.la

additional_FILES =
additional_CFLAGS =
additional_LIBS =

if HAVE_NETCDF
additional_FILES += oph_io_server_nc.c
additional_CFLAGS += $(NETCDF_CFLAGS) -DOPH_IO_SERVER_NETCDF
additional_LIBS += $(NETCDF_LIBS) -lm
endif

if HAVE_ESDM
additional_FILES += oph_io_server_esdm.c
additional_CFLAGS += $(ESDM_CFLAGS) -DOPH_IO_SERVER_ESDM
additional_LIBS += $(ESDM_LIBS)
endif

liboph_io_server_query_manager_la_SOURCES = oph_io_server_query_blocks.c oph_io_server_query_engine.c oph_io_server_query_procedures.c oph_io_server_query.c ${additional_FILES}
liboph_io_server_query_manager_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) ${additional_CFLAGS} -I../metadb -I../common -I../iostorage -I../query_engine -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_io_server_query_manager_la_LIBADD = @LIBLTDL@ ${additional_LIBS} -L../common -ldebug -lhashtbl -loph_binary_io -loph_server_util -L../metadb -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data -loph_iostorage_interface
liboph_io_server_query_manager_la_LDFLAGS = -module -static

bin_PROGRAMS= oph_io_server 
bindir=${prefix}/bin

oph_io_server_SOURCES = oph_io_server_thread.c oph_io_server.c
oph_io_server_CFLAGS = ${OPENMP_CFLAGS} $(OPT) ${additional_CFLAGS} -I../common -I../iostorage -I../query_engine -fPIC -I. -I../metadb -I../network @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_LDADD = ${additional_LIBS} -L. -L../common -ldebug -lpthread -loph_binary_io -loph_server_conf -L../metadb -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data -loph_iostorage_interface -L../network -loph_network -loph_io_server_query_manager
oph_io_server_LDFLAGS= -Wl,-R -Wl,. 

