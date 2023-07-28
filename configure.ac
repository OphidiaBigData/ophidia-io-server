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

AC_PREREQ([2.60])
AC_INIT([ophidia-io-server], [1.11.0], [ophidia-info@cmcc.it])

AC_CONFIG_AUX_DIR([config])

AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_SRCDIR([src])

AC_CONFIG_MACRO_DIR([m4])

LT_CONFIG_LTDL_DIR([libltdl])
LT_INIT[dlopen]
LTDL_INIT

AC_SUBST(INCLTDL)
AC_SUBST(LIBLTDL)

AC_CANONICAL_HOST

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_STDC
AM_PROG_CC_C_O
AC_PROG_MAKE_SET
AC_PROG_INSTALL
AC_PROG_AWK

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS(sys/types.h sys/socket.h time.h netinet/in.h errno.h fcntl.h netdb.h signal.h stdio.h stdlib.h string.h sys/stat.h unistd.h sys/wait.h sys/un.h poll.h strings.h pthread.h)
AC_CHECK_HEADERS([stddef.h],  ,AC_MSG_ERROR([Cannot find stddef.h]))
AC_CHECK_HEADERS([sys/time.h],  ,AC_MSG_ERROR([Cannot find sys/time.h]))

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_TIME
AC_TYPE_UINT8_T
AC_TYPE_INT16_T
AC_TYPE_UINT16_T
AC_TYPE_INT32_T
AC_TYPE_UINT32_T
AC_TYPE_SIZE_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SSIZE_T

AC_CHECK_TYPES([socklen_t sa_family_t])
AC_CHECK_TYPES([struct addrinfo], [], [], [#include <netdb.h>])
AC_CHECK_TYPES([struct sockaddr], [], [], [#include <sys/socket.h>])

# Checks for library functions.
AC_CHECK_FUNCS([floor])
AC_CHECK_FUNCS([strncasecmp strndup strcasecmp strchr strerror strtol strtoll strtod snprintf])
AC_CHECK_FUNCS([gettimeofday])
AC_CHECK_FUNCS([memset memcpy])
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([select socket bzero getaddrinfo poll])
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MMAP

AC_CHECK_HEADER([dlfcn.h],  ,AC_MSG_ERROR([Cannot find dlfcn.h]))
case "$host_os" in
	*bsd*)
		AC_CHECK_LIB(c, dlopen,,AC_MSG_ERROR([Cannot find -lc]))
	;; *)
		AC_CHECK_LIB(dl, dlopen,,AC_MSG_ERROR([Cannot find -ldl]))
	;;
esac

CFLAGS=""
CPPFLAGS=""
CXXFLAGS=""

# Checks for library.
AC_CHECK_LIB(nsl, t_open)
AC_CHECK_LIB(socket, socket)
AC_CHECK_LIB([m],[floor])

AC_CHECK_LIB(pthread, pthread_create)
if test "$ac_cv_lib_pthread_pthread_create" = yes ; then
   CFLAGS="$CFLAGS -D_REENTRANT"
else
   AC_CHECK_LIB(pthreads, pthread_create)
   if test "$ac_cv_lib_pthreads_pthread_create" = yes ; then
      CFLAGS="$CFLAGS -D_REENTRANT"
   fi
fi

AC_ARG_ENABLE([openmp], [AS_HELP_STRING([--enable-openmp], [enable OpenMP multi-threading [[default=no]]])])
if test "x$enable_openmp" = xyes; then
  AX_OPENMP
fi
AM_CONDITIONAL([HAVE_OPENMP], [test -n "$OPENMP_CFLAGS"])
AC_SUBST(OPENMP_CFLAGS)

AC_MSG_CHECKING(for Bison)
AC_PROG_YACC
if test "$YACC" != "bison -y"; then
	AC_MSG_ERROR([No bison found])
else
	AC_PATH_PROG([BISON],[bison])
	AX_PROG_BISON_VERSION([2.3],[ENABLE_YACCLEX=yes],AC_MSG_ERROR([Bison version 2.3 is the minimum required]))
fi

AC_MSG_CHECKING(for Flex)
AC_PROG_LEX
if test "x$LEX" != xflex; then
	LEX="$SHELL $missing_dir/missing flex"
else 
	AC_PATH_PROG([FLEX],[flex])
	AX_PROG_FLEX_VERSION([2.5],[ENABLE_YACCLEX=yes],AC_MSG_ERROR([Flex version 2.5 is the minimum required]))
fi

# Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html
AC_MSG_CHECKING(for IPv4 support)
AC_CACHE_VAL(ac_cv_ipv4,
	AC_TRY_RUN([
#	include <sys/types.h>
#	include <sys/socket.h>
#	include <netinet/in.h>
	/* Make sure the definitions for AF_INET and struct sockaddr_in
	 * are defined, and that we can actually create an IPv4 TCP socket.
	 */
	main()
	{
		int fd;
		struct sockaddr_in foo;
		fd = socket(AF_INET, SOCK_STREAM, 0);
		exit(fd >= 0 ? 0 : 1);
	}],
	ac_cv_ipv4=yes,
	ac_cv_ipv4=no,
	ac_cv_ipv4=no))
if test -n $ac_cv_ipv4; then
  AC_MSG_NOTICE([System supports IPv4])
else
	AC_MSG_ERROR([System does not support IPv4])
fi

# Adapted from Stevens et al. UNP Vol. 1, 3rd Ed. source code - http://www.unpbook.com/src.html
AC_MSG_CHECKING(for IPv6 support)
AC_CACHE_VAL(ac_cv_ipv6,
	AC_TRY_RUN([
#	include <sys/types.h>
#	include <sys/socket.h>
#	include <netinet/in.h>
	/* Make sure the definitions for AF_INET6 and struct sockaddr_in6
	 * are defined, and that we can actually create an IPv6 TCP socket.
	 */
	main()
	{
		int fd;
		struct sockaddr_in6 foo;
		fd = socket(AF_INET6, SOCK_STREAM, 0);
		exit(fd >= 0 ? 0 : 1);
	}],
	ac_cv_ipv6=yes,
	ac_cv_ipv6=no,
	ac_cv_ipv6=no))
if test -n $ac_cv_ipv6; then
  AC_MSG_NOTICE([System supports IPv6])
else
	AC_MSG_ERROR([System does not support IPv6])
fi

#Check MySQL
AX_LIB_MYSQL([5.6])
if test -n $HAVE_MYSQL; then
  AC_MSG_NOTICE([MySQL libraries available])
else
	AC_MSG_ERROR([MySQL libraries not found! Aborting configure!])
fi

have_netcdf=no
NETCDF_CONFIG=
AC_ARG_WITH(netcdf_path,
           [  --with-netcdf-path=PATH  Set the netcdf root dir],
           [
                netcdf_path="$withval"
		have_netcdf=yes
		NETCDF_CONFIG="${withval}/bin/nc-config"
           ],
           [
			AC_PATH_PROG([NETCDF_CONFIG], nc-config)
			if test -n "$NETCDF_CONFIG" ; then
				have_netcdf=yes
                	fi
           ]
           )
if test "x${have_netcdf}" = "xyes"; then
	NETCDF_CFLAGS="`$NETCDF_CONFIG --cflags`"
	NETCDF_LIBS="`$NETCDF_CONFIG --libs`"
	if test "x${NETCDF_LIBS}" != "x"; then
		AC_SUBST(NETCDF_CFLAGS)
		AC_SUBST(NETCDF_LIBS)
		AC_MSG_NOTICE([NetCDF support enabled])
	else
		AC_MSG_WARN([NetCDF not found! NetCDF support automatically disabled!])
		have_netcdf=no
	fi
fi
AM_CONDITIONAL([HAVE_NETCDF], [test "x$have_netcdf" = "xyes"])


have_esdm=no
ESDM_CFLAGS=
ESDM_LIBS=
AC_ARG_WITH(esdm_path,
           [  --with-esdm-path=PATH  Set the esdm root dir],
           [
				have_esdm=yes
				ESDM_CFLAGS="-I$withval/include"
				ESDM_LIBS="-L$withval/lib -lesdm -Wl,-rpath -Wl,$withval/lib"
				AC_SUBST(ESDM_CFLAGS)
				AC_SUBST(ESDM_LIBS)
				AC_MSG_NOTICE([ESDM support enabled])
           ],
           [
				have_esdm=no
           ]
           )
AM_CONDITIONAL([HAVE_ESDM], [test "x$have_esdm" = "xyes"])

#Check if ESDM PAV kernels are installed
have_esdm_pav_kernels="no"
AC_ARG_ENABLE(esdm_pav_kernels,
        [  --enable-esdm-pav-kernels          
            Turn on ESDM PAV kernels],
        [have_esdm_pav_kernels="yes" 
            AC_MSG_NOTICE(ESDM PAV kernels enabled)],
        [have_esdm_pav_kernels="no"
            AC_MSG_NOTICE(ESDM PAV kernels disabled)]
        )
AM_CONDITIONAL([HAVE_ESDM_PAV_KERNELS], [test "x$have_esdm_pav_kernels" = "xyes"])

#Enable parallel nc4 support
parallel_nc4="no"
AC_ARG_ENABLE(parallel_nc4,
        [  --enable-parallel-nc4          turn on support for parallel nc4 loading],
        [parallel_nc4="yes"],
        [parallel_nc4="no"]
        )
AM_CONDITIONAL([PAR_NC4], [test "x$parallel_nc4" = "xyes" -a "x$have_netcdf" = "xyes"])
AM_COND_IF([PAR_NC4], [AC_MSG_NOTICE(NetCDF4 parallel support enabled)], [AC_MSG_NOTICE(NetCDF4 parallel support disabled)])


#Enable optimization
optimization="no"
AC_ARG_ENABLE(optimization,
        [  --enable-optimization          turn on optimization flags],
        [optimization="yes"
        AC_MSG_WARN("Optimization activated")],
        [optimization="no"]
        )

#Enable debug mode
AC_ARG_ENABLE(debug, 
		[  --enable-debug 				Enable debug. (Disabled by default)], 
		[debug="yes"        
    AC_MSG_WARN("Debug activated")],
    [debug="no"]
		)
AM_CONDITIONAL(DEBUG, test "x$debug" = "xyes")

#Set executables path 
AC_ARG_WITH(exec_path,
	   [  --with-exec-path=PATH  Set the executable directory (default :$prefix/bin)],
	   [exec_path="$withval"],
	   [
		  if test "x${prefix}" != "xNONE"; then
			  exec_path="${prefix}/bin"
		  else  exec_path="/usr/local/bin"
		  fi
     ]
	   )
EXEC_PATH=$exec_path
AC_SUBST(EXEC_PATH)

#Set etc path 
AC_ARG_WITH(etc_path,
	   [  --with-etc-path=PATH  Set the etc directory (default :$prefix/etc)],
	   [etc_path="$withval"],
	   [
		  if test "x${prefix}" != "xNONE"; then
			  etc_path="${prefix}/etc"
		  else  etc_path="/usr/local/etc"
		  fi
     ]
	   )
ETC_PATH=$etc_file_path
AC_SUBST(ETC_PATH)

#Set UDF plugin path 
AC_ARG_WITH(plugin_path,
	   [  --with-plugin-path=PATH  Set the plugin directory (default :/usr/local/ophidia/oph-cluster/oph-primitives/lib)],
	   [plugin_path="$withval"],
	   [ plugin_path="/usr/local/ophidia/oph-cluster/oph-primitives/lib"]
	   )
PLUGIN_PATH=$plugin_path
AC_SUBST(PLUGIN_PATH)

#Set storage device path
AC_ARG_WITH(device_file_path,
	   [  --with-device-file-path=PATH  Set the driver path (default :$prefix/lib/devices)],
	   [device_file_path="$withval"],
	   [
		if test "x${prefix}" != "xNONE"; then
			device_file_path="${prefix}/lib/devices"
		else  device_file_path="/usr/local/lib/devices"
		fi
	   ]
	   )
DEVICE_PATH=$device_file_path
AC_SUBST(DEVICE_PATH)

OPT=""
case "${host}" in
        *-*-solaris*)   PLATFORM=SUN_OS
                        SAMPLE_EXTRA_LIBS="-lxnet -lsocket -lnsl"
                        ;;
        *-*-sysv5*)     PLATFORM=UNIXWARE ;;
        *-*-UnixWare*)  PLATFORM=UNIXWARE ;;
        *-*-unixware*)  PLATFORM=UNIXWARE ;;
        *-*-sco3.2v5*)  PLATFORM=OPENSERVER
                        SAMPLE_EXTRA_LIBS="-lsocket"
                        ;;
        *-*-linux*)     PLATFORM=LINUX
                        if test "x$debug" = "xyes"; then
							            OPT+="-g3 -O0 -Wall -Wextra -rdynamic"
						            else
			                    if test "x$optimization" = "xyes" ; then
			                         OPT+="-Wall -Wextra -O3 -g0 -rdynamic -finline-functions -funswitch-loops -fgcse-after-reload -ftree-loop-linear -ftree-loop-im -ftree-loop-ivcanon -fivopts -ftree-vectorize -ftracer -fgcse-sm -fgcse-las -fgcse-after-reload -funroll-loops -freorder-blocks-and-partition -fweb"
			                    else
                            OPT+="-O3 -rdynamic"
                          fi						            
                        fi
			            ;;
                *-*-cygwin*)    PLATFORM=CYGWIN
                        ;;
        *-*-freebsd*)   PLATFORM=FREEBSD ;;
        *-*-irix*)      PLATFORM=IRIX
                        ;;
        *-*-aix*)       PLATFORM=AIX
                      case "${host}" in
                         *-*-aix4.1*)   osver=AIX41;;
                         *-*-aix4.2*)   osver=AIX42;;
                         *-*-aix4.3*)   osver=AIX43;;
                         *-*-aix4.*)    osver=AIX43;;
                      esac;;
        *-sequent-*)    PLATFORM=PTX ;;
        *-*-hp*)        PLATFORM=HP_UX ;
                        case "${host}" in
                            *-*-hpux11*)  osver=HPUX11;;
                            *-*-hpux10*)  osver=HPUX10;;
                            *)            osver=HPUX11;;
                        esac;;
        *-*-mvs*)       PLATFORM=OS390 ;;
        *-*-os400*)     PLATFORM=OS400 ;;
        *-*-OS400*)     PLATFORM=OS400 ;;
        *-*-osf*)       PLATFORM=TRU64
				        OPT="-g3"
				        REENTRANT="1"
                               ;;
        *-apple-*)      PLATFORM=__APPLE__
				        OPT="-g3"
				        ;;
        *)              PLATFORM=UNKNOWN ;;
esac

AC_SUBST(OPT)

if test "x${have_esdm_pav_kernels}" = "xyes" && test -d "$srcdir/esdm-pav-analytical-kernels"; then
    AC_CONFIG_SUBDIRS([esdm-pav-analytical-kernels])
fi

AC_OUTPUT([Makefile
           src/Makefile
           src/common/Makefile
           src/iostorage/Makefile
           src/devices/Makefile
           src/metadb/Makefile
           src/metadb/execs/Makefile           
           src/network/Makefile           
           src/query_engine/Makefile
           src/client/Makefile      
           src/server/Makefile
           src/server/execs/Makefile
           etc/oph_primitives_list
           etc/oph_device_list
           ])
