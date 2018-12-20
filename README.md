# Package for dbf reading in [GO](https://golang.org)

## A fork to extend the original work by eentzel

[![Build Status](https://travis-ci.org/squeeze69/dbf.svg?branch=master)](https://travis-ci.org/squeeze69/dbf)

So far, I've added some field types and made some minor tweak changes.
But I'd like to extend them and add support for more field types (adding an ad hoc handling for Clipper, FoxPro and so on).

See [DBF File Format](http://www.clicketyclick.dk/databases/xbase/format/index.html)

## Notes

- added SetFlags : to set binary flags, actually, only: FlagDateAssql to represent in a simplified SQL format the Date Field
- added ReadOrdered - it's a wrapper around Read to read the record in an array with tha same field order as the dbf fields

Most of the changes are made for use in [dbfgo2mysql](https://github.com/squeeze69/dbfgo2mysql), a simple dbf 2 mysql converter (not so simple, actually)

- Flags (they should be combined with "or" (|)), set using dbf.SetFlags(...):
    - FlagDateAssql : see above
    - FlagSkipWeird : I've got a malformed dbf with a 0x1a instead of a delete marker, with this flag, it's treated as a deleted record. More "weird" cases could follow, returns a SkipError (you could use type assertion to identify it, _,ok := err.(*SkipError) and so on
    - FlagSkipDeleted : Skip deleted records instead of aborting with an error (this should be changed, sooner or later,maybe something like a "scanner" for sequential reading) returns the same "SkipError"
    - FlagEmptyDateAsZero : set an empty date as zero "0000-00-00"