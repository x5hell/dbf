A fork to extend the original work by eentzel.

So far, I've added some field types and made some minor tweak changes.
But I'd like to extend them and add support for more field types (adding an ad hoc handling form Clipper, FoxPro and so on).

- added SetFlags : to set binary flags, actually, only: FlagDateAssql to represent in a simplified SQL format the Date Field
- added ReadOrdered - it's a wrapper around Read to read the record in an array with tha same field order as the dbf fields