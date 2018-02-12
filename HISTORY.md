
## v1.2.0 - 2018-02-12

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
