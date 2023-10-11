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

lib_LTLIBRARIES=libmemory_device.la
if HAVE_MEMKIND
lib_LTLIBRARIES+=libpmemory_device.la
endif
libdir=${DEVICE_PATH}

if HAVE_MEMKIND
ADDITIONAL_CFLAGS=$(MEMKIND_INCLUDE) -DOPH_IO_PMEM
ADDITIONAL_LIBS=$(MEMKIND_LIB)
else
ADDITIONAL_CFLAGS=
ADDITIONAL_LIBS=
endif

libmemory_device_la_SOURCES = MEMORY_device.c
libmemory_device_la_CFLAGS = $(OPT) -I. -I.. -I../.. -I../common -I../iostorage -DOPH_IO_SERVER_PREFIX=\"${prefix}\" $(ADDITIONAL_CFLAGS)
libmemory_device_la_LIBADD= -L../common -ldebug  -loph_server_util -L../iostorage -loph_iostorage_data $(ADDITIONAL_LIBS)
libmemory_device_la_LDFLAGS = -module -avoid-version -no-undefined

if HAVE_MEMKIND
libpmemory_device_la_SOURCES = PMEMORY_device.c
libpmemory_device_la_CFLAGS = $(OPT) -I. -I.. -I../.. -I../common -I../iostorage -DOPH_IO_SERVER_PREFIX=\"${prefix}\" $(ADDITIONAL_CFLAGS)
libpmemory_device_la_LIBADD= -L../common -ldebug  -loph_server_util -L../iostorage -loph_iostorage_data $(ADDITIONAL_LIBS)
libpmemory_device_la_LDFLAGS = -module -avoid-version -no-undefined
endif

