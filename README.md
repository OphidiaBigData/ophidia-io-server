# Ophidia IO Server

### Description

The Ophidia IO Server is the Ophidia native IO server component.

### Requirements

In order to compile and run the IO Server, make sure you have the following packages properly installed:

1. mysql-community-devel
2. nectdf and netcdf-devel
3. optionally bison and flex 

### How to Install

If you are building from git, you also need automake, autoconf and libtool. To prepare the code for building run:

```
$ ./bootstrap 
```

The source code has been packaged with GNU Autotools, so to install simply type:

```
$ ./configure --prefix=prefix
$ make
$ make install
```

Type:

```
$ ./configure --help
```

to see all available options.

If you want to use the program system-wide, remember to add its installation directory to your PATH.

Additional details on installation and configuration are available in the [documentation](http://ophidia.cmcc.it/documentation/admin/install/components/install_ioserver.html)

### How to Launch

```
$ oph_io_server -i 1
```

Type:

```
$ oph_io_server -h
```

to see all other available options.

