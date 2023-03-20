
## Next release

### Changed:

- Option list
- Configuration argument 'SERVER_HOSTNAME' is optional

## v1.7.3 - 2023-01-20

### Fixed:

- Bug in handling unlimited dimensions

### Added:

- Support for CentOS 9 [#25](https://github.com/OphidiaBigData/ophidia-io-server/pull/25)

### Changed:

- Update reference to ESDM-PAV kernels package 

## v1.7.2 - 2022-10-20

### Changed:

- Folder structure for executables building [#24](https://github.com/OphidiaBigData/ophidia-io-server/pull/24)

## v1.7.1 - 2022-07-28

### Fixed:

- Bug in bootstrap module

### Added:

- Support for new analytical kernel 'stat'

## v1.7.0 - 2022-07-01

### Fixed:

- Bug in debug printing function

### Added:

- Include ESDM-PAV kernels package [#23](https://github.com/OphidiaBigData/ophidia-io-server/pull/23)
- New configuration argument 'WORKING_DIR'
- Support for OPH_CONCATESDM2 [#22](https://github.com/OphidiaBigData/ophidia-io-server/pull/22)
- Support for OPH_IMPORTNCS operator to load multiple files in a single fragment [#21](https://github.com/OphidiaBigData/ophidia-io-server/pull/21)
- Support for ESDM-based operators (OPH_IMPORTESDM, OPH_IMPORTESDM2, OPH_EXPORTESDM, OPH_EXPORTESDM2, OPH_CONCATESDM) [#21](https://github.com/OphidiaBigData/ophidia-io-server/pull/21)
- Support for oph_filter [#19](https://github.com/OphidiaBigData/ophidia-io-server/pull/19), oph_operation_array [#20](https://github.com/OphidiaBigData/ophidia-io-server/pull/20) primitives

### Changed:

- Improved parallel access to NetCDFv4 files [#21](https://github.com/OphidiaBigData/ophidia-io-server/pull/21)

## v1.6.0 - 2021-03-05

### Added:

- Optimizations to query engine and nested plugin execution [#18](https://github.com/OphidiaBigData/ophidia-io-server/pull/18)

### Changed:

- Improved nc file import core functions and execution flow [#17](https://github.com/OphidiaBigData/ophidia-io-server/pull/17)

### Fixed:

- Bug [#16](https://github.com/OphidiaBigData/ophidia-io-server/issues/16)


## v1.5.0 - 2019-01-24

### Fixed:

- Minor memory leaks

### Added:

- New query and functionalities to concatenate data from NetCDF files to fragments [#15](https://github.com/OphidiaBigData/ophidia-io-server/pull/15) 
- New query and functionalities to create a fragment from random data [#14](https://github.com/OphidiaBigData/ophidia-io-server/pull/14) 

## v1.4.0 - 2018-07-27

### Fixed:

- Bug causing server to crash when high number of variables is used in queries [#13](https://github.com/OphidiaBigData/ophidia-io-server/issues/13)

## v1.3.0 - 2018-06-18

### Fixed:

- Bug [#8](https://github.com/OphidiaBigData/ophidia-io-server/issues/8)
- Bug [#7](https://github.com/OphidiaBigData/ophidia-io-server/issues/7)

### Added:

- New parameter 'MEMORY_BUFFER' in oph_ioserver.conf 
- New query and functionalities to create a fragment with data imported directly from NetCDF files [#12](https://github.com/OphidiaBigData/ophidia-io-server/pull/12) 
- Option to specify user-defined path for configuration file [#11](https://github.com/OphidiaBigData/ophidia-io-server/pull/11)
- Support for oph_sequence primitive [#10](https://github.com/OphidiaBigData/ophidia-io-server/pull/10)

### Changed:

- Configure for new building dependency (NetCDF library)
- Main IO Server thread to avoid memset on whole network buffer [#9](https://github.com/OphidiaBigData/ophidia-io-server/pull/9)

## v1.2.0 - 2018-02-16

### Fixed:

- Bug in create and insert blocks to properly evaluate fragment name in case of hierarchical notation
- Reference to oph_get_index_array
- Bug in metadb reader and when loading a non-empty metadb schema

### Added:

- Support for oph_normalize, oph_padding and oph_replace primitives
- Option to disable memory check at server startup

## v1.1.0 - 2017-07-28

### Fixed:

- Bug in usage of primitives argument number from global function symtable

### Added:

- Test mode for debugging without memory check constraints
- Support for oph_append and oph_concat2 primitives

## v1.0.0 - 2017-03-23

### Changed:

- Code indentation style

## v0.11.0 - 2017-01-31

- Initial public release including support for most Ophidia operators
