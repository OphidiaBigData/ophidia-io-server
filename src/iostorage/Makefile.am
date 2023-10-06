#
#    Ophidia IO Server
#    Copyright (C) 2014-2023 CMCC Foundation
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

lib_LTLIBRARIES=liboph_iostorage_data.la liboph_iostorage_interface.la
libdir=${prefix}/lib

include_HEADERS=oph_iostorage_interface.h oph_iostorage_data.h

if HAVE_MEMKIND
ADDITIONAL_CFLAGS=$(MEMKIND_INCLUDE) -DOPH_IO_PMEM
ADDITIONAL_LIBS=$(MEMKIND_LIB)
else
ADDITIONAL_CFLAGS=
ADDITIONAL_LIBS=
endif

liboph_iostorage_data_la_SOURCES = oph_iostorage_data.c
liboph_iostorage_data_la_CFLAGS = $(OPT) -I. -I../common -I.. -I../.. -fPIC @INCLTDL@ $(ADDITIONAL_CFLAGS)
liboph_iostorage_data_la_LIBADD = @LIBLTDL@ -L../common -ldebug -loph_server_util $(ADDITIONAL_LIBS)
liboph_iostorage_data_la_LDFLAGS = -module -static

liboph_iostorage_interface_la_SOURCES = oph_iostorage_interface.c
liboph_iostorage_interface_la_CFLAGS = $(OPT) -I. -I.. -I../.. -I../common  -fPIC @INCLTDL@ -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_iostorage_interface_la_LIBADD= @LIBLTDL@ -L../common -ldebug -loph_server_util -lpthread
liboph_iostorage_interface_la_LDFLAGS = -module -static

bindir=${prefix}/bin

if DEBUG
bin_PROGRAMS= oph_iostorage_client

oph_iostorage_client_SOURCES = oph_iostorage_client.c
oph_iostorage_client_CFLAGS = $(OPT)  -I../common  -I. -fPIC -DOPH_IO_SERVER_PREFIX=\"${prefix}\" $(ADDITIONAL_CFLAGS)
oph_iostorage_client_LDADD = -L../common -ldebug -loph_server_util -L. -loph_iostorage_interface -loph_iostorage_data
oph_iostorage_client_LDFLAGS= 
endif
