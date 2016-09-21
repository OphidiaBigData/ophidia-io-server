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
OPT+=-DDEBUG -DOPH_WITH_VALGRIND 
endif

noinst_LTLIBRARIES=liboph_query_parser.la liboph_query_engine.la


BUILT_SOURCES = oph_query_expression_parser.h oph_query_expression_parser.c oph_query_expression_lexer.h oph_query_expression_lexer.c
AM_YFLAGS = -d --name-prefix=ee
AM_LFLAGS = --prefix=ee

#To build lexer header file
.l.c:
	$(LEXCOMPILE) oph_query_expression_lexer.l

liboph_query_parser_la_SOURCES = oph_query_parser.c
liboph_query_parser_la_CFLAGS = $(OPT) -I../common -I..  -I../iostorage -fPIC @INCLTDL@ -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_query_parser_la_LIBADD = @LIBLTDL@ -L../common -ldebug -lhashtbl -loph_server_util
liboph_query_parser_la_LDFLAGS = -module -static 

liboph_query_engine_la_SOURCES = oph_query_plugin_executor.c oph_query_plugin_loader.c oph_query_expression_functions.c oph_query_expression_parser.y oph_query_expression_lexer.l oph_query_expression_evaluator.c
if HAVE_OPENMP
liboph_query_engine_la_CFLAGS = ${OPENMP_CFLAGS} $(OPT) -I../common -I../metadb  -I../iostorage -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\" -DOPH_OMP
else
liboph_query_engine_la_CFLAGS = $(OPT) -I../common -I../metadb  -I../iostorage -I. -fPIC @INCLTDL@ ${MYSQL_CFLAGS}  -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
endif
liboph_query_engine_la_LIBADD = @LIBLTDL@ -L../common -ldebug -lhashtbl -loph_server_util -lm -L../metadb -loph_metadb
liboph_query_engine_la_LDFLAGS = -module -static 

bindir=${prefix}/bin

if DEBUG
bin_PROGRAMS=oph_query_expression_client

oph_query_expression_client_SOURCES = oph_query_expression_client.c
oph_query_expression_client_CFLAGS = $(OPT) -I../common -I../iostorage -I. @INCLTDL@  -DOPH_IO_SERVER_PREFIX=\"${prefix}\" ${MYSQL_CFLAGS}
oph_query_expression_client_LDADD = -L. -loph_query_engine -loph_query_parser -L../common -ldebug -lpthread

endif
