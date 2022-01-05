#
#    Ophidia IO Server
#    Copyright (C) 2014-2022 CMCC Foundation
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

if HAVE_NETCDF
liboph_io_server_query_manager_la_SOURCES = oph_io_server_query_blocks.c oph_io_server_query_engine.c  oph_io_server_nc.c  oph_io_server_query_procedures.c oph_io_server_query.c  
if PAR_NC4
liboph_io_server_query_manager_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) $(NETCDF_CFLAGS) -I../metadb -I../common -I../iostorage -I../query_engine -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\" -DOPH_IO_SERVER_NETCDF -DOPH_PAR_NC4
else
liboph_io_server_query_manager_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) $(NETCDF_CFLAGS) -I../metadb -I../common -I../iostorage -I../query_engine -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\" -DOPH_IO_SERVER_NETCDF
endif
liboph_io_server_query_manager_la_LIBADD = @LIBLTDL@ $(NETCDF_LIBS) -lm -L../common -ldebug -lhashtbl -loph_binary_io -loph_server_util -L../metadb  -loph_metadb  -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data  -loph_iostorage_interface 
liboph_io_server_query_manager_la_LDFLAGS = -module -static 

else

liboph_io_server_query_manager_la_SOURCES = oph_io_server_query_blocks.c oph_io_server_query_engine.c oph_io_server_query_procedures.c oph_io_server_query.c  
liboph_io_server_query_manager_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../metadb -I../common -I../iostorage -I../query_engine -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_io_server_query_manager_la_LIBADD = @LIBLTDL@ -L../common -ldebug -lhashtbl -loph_server_util -L../metadb  -loph_metadb  -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data  -loph_iostorage_interface 
liboph_io_server_query_manager_la_LDFLAGS = -module -static 
endif

bin_PROGRAMS= oph_io_server
if PAR_NC4
bin_PROGRAMS+= oph_io_server_nc_load
endif
bindir=${prefix}/bin

oph_io_server_SOURCES = oph_io_server_thread.c oph_io_server.c
if HAVE_NETCDF
oph_io_server_CFLAGS = ${OPENMP_CFLAGS} $(OPT) $(NETCDF_CFLAGS) -I../common -I../iostorage -I../query_engine -fPIC -I. -I../metadb -I../network @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_LDADD = $(NETCDF_LIBS) -L. -L../common -ldebug -lpthread -loph_binary_io -loph_server_conf -L../metadb  -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data  -loph_iostorage_interface -L../network -loph_network -loph_io_server_query_manager
else
oph_io_server_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common -I../iostorage -I../query_engine -fPIC -I. -I../metadb -I../network @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_LDADD = -L. -L../common -ldebug -lpthread -loph_binary_io -loph_server_conf -L../metadb  -loph_metadb -L../query_engine -loph_query_engine -loph_query_parser -L../iostorage -loph_iostorage_data  -loph_iostorage_interface -L../network -loph_network -loph_io_server_query_manager
endif
oph_io_server_LDFLAGS= -Wl,-R -Wl,. 

if PAR_NC4
oph_io_server_nc_load_SOURCES = oph_io_server_nc_load.c
oph_io_server_nc_load_CFLAGS = $(OPT) $(NETCDF_CFLAGS) -I../common -I../iostorage -I../query_engine -fPIC -I. -I../metadb -I../network @INCLTDL@ ${MYSQL_CFLAGS} -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
oph_io_server_nc_load_LDADD = $(NETCDF_LIBS) -L. -L../common -ldebug -loph_server_conf -loph_io_server_query_manager
oph_io_server_nc_load_LDFLAGS= -Wl,-R -Wl,.  
endif
