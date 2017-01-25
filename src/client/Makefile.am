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

lib_LTLIBRARIES = liboph_io_client_interface.la

include_HEADERS=oph_io_client_interface.h

bin_PROGRAMS= 
if DEBUG
bin_PROGRAMS+=oph_io_client_show oph_io_client_test oph_io_client  
endif
bindir=${prefix}/bin

liboph_io_client_interface_la_SOURCES = oph_io_client_interface.c
liboph_io_client_interface_la_CFLAGS = $(OPT) -I../common  -I../network -fPIC
liboph_io_client_interface_la_LIBADD = -L../common -ldebug -L../network -loph_network 
liboph_io_client_interface_la_LDFLAGS = -module -avoid-version -shared

if DEBUG
oph_io_client_SOURCES = oph_io_client.c
oph_io_client_CFLAGS= $(OPT)  -I../common 
oph_io_client_LDADD= -L. -L../common -ldebug -loph_io_client_interface

oph_io_client_show_SOURCES = oph_io_client_show.c
oph_io_client_show_CFLAGS= $(OPT) -I../common 
oph_io_client_show_LDADD= -L. -L../common -ldebug -loph_io_client_interface

oph_io_client_test_SOURCES = oph_io_client_test.c
oph_io_client_test_CFLAGS= $(OPT) -I../common 
oph_io_client_test_LDADD= -L. -L../common -ldebug -loph_io_client_interface
endif
