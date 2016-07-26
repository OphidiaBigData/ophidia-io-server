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


lib_LTLIBRARIES=libdebug.la 
libdir=${prefix}/lib

noinst_LTLIBRARIES=libhashtbl.la liboph_server_util.la liboph_server_conf.la

libdebug_la_SOURCES = debug.c
libdebug_la_CFLAGS = $(OPT) -I. -I.. -I../.. -fPIC -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
libdebug_la_LIBADD=
libdebug_la_LDFLAGS = -module -static

libhashtbl_la_SOURCES = hashtbl.c
libhashtbl_la_CFLAGS = $(OPT) -I. -I.. -I../.. -fPIC
libhashtbl_la_LIBADD=
libhashtbl_la_LDFLAGS = -module -static

liboph_server_conf_la_SOURCES = oph_server_confs.c
liboph_server_conf_la_CFLAGS = $(OPT) -I. -I.. -I../.. -fPIC -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
liboph_server_conf_la_LIBADD= -L. -ldebug -lhashtbl
liboph_server_conf_la_LDFLAGS = -module -static

liboph_server_util_la_SOURCES = oph_server_utility.c
liboph_server_util_la_CFLAGS = $(OPT) -I. -I.. -I../.. -fPIC
liboph_server_util_la_LIBADD= -L. -ldebug
liboph_server_util_la_LDFLAGS = -module -static
