A fork to extend the original work by eentzel.

So far, I've added some field types and made some minor tweak changes.
But I'd like to extend them and add support for more field types (adding an ad hoc handling form Clipper, FoxPro and so on).

- added SetFlags : to set binary flags, actually, only: FlagDateAssql to represent in a simplified SQL format the Date Field
- added ReadOrdered - it's a wrapper around Read to read the record in an array with tha same field order as the dbf fields


- Flags (they should be "orred"):
	- FlagDateAssql : see above
	- FlagSkipWeird : I've got a malformed dbf with a 0x1a instead of a delete marker, with this flag, it's treated as a deleted record.
		more "weird" cases could follow, returns a SkipError (you could use type assertion to identify it, _,ok := err.(*SkipError) and so on
	- FlagSkipDeleted : Skip deleted records instead of aborting with an error (this should be changed, sooner or later, maybe something like
		a "scanner" for sequential reading) returns the same "SkipError"
