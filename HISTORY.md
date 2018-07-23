
## Next release

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
