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

ACLOCAL_AMFLAGS = -I m4

SUBDIRS=
if HAVE_ESDM_PAV_KERNELS
SUBDIRS+= esdm-pav-analytical-kernels
endif
SUBDIRS+= . libltdl src

sysconf_DATA = etc/oph_primitives_list etc/oph_ioserver.conf etc/oph_device_list 

vardir = $(prefix)/var/
dist_var_DATA =

logdir = $(prefix)/log/
dist_log_DATA =

include_HEADERS=
