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

SUBDIRS = expression_evaluator .

if DEBUG 
OPT+=-DDEBUG
endif

noinst_LTLIBRARIES=liboph_query_parser.la liboph_query_engine.la

liboph_query_parser_la_SOURCES = oph_query_parser.c
liboph_query_parser_la_CFLAGS = $(OPT) -I../common -I..  -I../iostorage -fPIC @INCLTDL@ -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_query_parser_la_LIBADD = @LIBLTDL@ -L../common -ldebug -lhashtbl -loph_server_util
liboph_query_parser_la_LDFLAGS = -module -static 

liboph_query_engine_la_SOURCES = oph_query_engine.c oph_query_plugin_executor.c oph_query_plugin_loader.c
if HAVE_OPENMP
liboph_query_engine_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common -I.. -I../metadb  -I../iostorage -fPIC @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\" -DOPH_OMP
else
liboph_query_engine_la_CFLAGS = $(OPT) -I../common -I.. -I../metadb  -I../iostorage -fPIC @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
endif
liboph_query_engine_la_LIBADD = @LIBLTDL@ -L../common -ldebug -lhashtbl -loph_server_util -L../metadb -loph_metadb
liboph_query_engine_la_LDFLAGS = -module -static 

bindir=${prefix}/bin


if DEBUG
bin_PROGRAMS=oph_query_client

oph_query_client_SOURCES = oph_query_client.c
if HAVE_OPENMP
oph_query_client_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common  -I../iostorage -I.. -I../metadb @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
else
oph_query_client_CFLAGS = $(OPT) -I../common  -I../iostorage -I.. -I../metadb @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
endif
oph_query_client_LDADD = -L. -loph_query_engine -loph_query_parser -L../common -ldebug -loph_server_util -L../iostorage -loph_iostorage_data
endif
