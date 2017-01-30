#
#    Ophidia IO Server
#    Copyright (C) 2014-2017 CMCC Foundation
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

lib_LTLIBRARIES=libmemory_device.la
libdir=${DEVICE_PATH}

libmemory_device_la_SOURCES = MEMORY_device.c
libmemory_device_la_CFLAGS = $(OPT) -I. -I.. -I../.. -I../common -I../iostorage -DOPH_IO_SERVER_PREFIX=\"${prefix}\"
libmemory_device_la_LIBADD= -L../common -ldebug  -loph_server_util -L../iostorage -loph_iostorage_data
libmemory_device_la_LDFLAGS = -module -avoid-version -no-undefined
