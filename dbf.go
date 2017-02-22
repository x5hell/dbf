package dbf

// reference implementation:
//     http://dbf.berlios.de/ - broken link
//     info on xbase files: http://www.clicketyclick.dk/databases/xbase/format/index.html

// test data: http://abs.gov.au/AUSSTATS/abs@.nsf/DetailsPage/2923.0.30.0012006?OpenDocument

// a dbf.Reader should have some metadata, and a Read() method that returns
// table rows, one at a time

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

//FlagDateAssql : read date in a near good sql format
const (
	FlagDateAssql = 1
)

//Reader structure
type Reader struct {
	r                io.ReadSeeker
	year, month, day int
	Length           int // number of records
	fields           []Field
	headerlen        uint16 // in bytes
	recordlen        uint16 // length of each record, in bytes
	flags            int32  //general purpose flags - see constant
	sync.Mutex
}

type header struct {
	// documented at: http://www.clicketyclick.dk/databases/xbase/format/index.html
	Version    byte
	Year       uint8 // stored as offset from (decimal) 1900
	Month, Day uint8
	Nrec       uint32
	Headerlen  uint16 // in bytes
	Recordlen  uint16 // length of each record, in bytes
}

//NewReader - create a new reader
func NewReader(r io.ReadSeeker) (*Reader, error) {
	var h header
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	err := binary.Read(r, binary.LittleEndian, &h)
	if err != nil {
		return nil, err
	} else if h.Version != 0x03 {
		return nil, fmt.Errorf("unexepected file version: %d", h.Version)
	}
	if _, err = r.Seek(0x20, io.SeekStart); err != nil {
		return nil, err
	}
	var nfields = int(h.Headerlen/32 - 1)
	fields := make([]Field, 0, nfields)
	for offset := 0; offset < nfields; offset++ {
		f := Field{}
		binary.Read(r, binary.LittleEndian, &f)
		if f.Name[1] == '\x0d' { //0x0d (aka: \r) is the official field list terminator
			break
		}
		if err = f.validate(); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	br := bufio.NewReader(r)
	if eoh, err := br.ReadByte(); err != nil {
		return nil, err
	} else if eoh != 0x0D {
		return nil, fmt.Errorf("Header was supposed to be %d bytes long, but found byte %#x at that offset instead of expected byte 0x0D", h.Headerlen, eoh)
	}

	return &Reader{r, 1900 + int(h.Year),
		int(h.Month), int(h.Day), int(h.Nrec), fields,
		h.Headerlen, h.Recordlen, 0, *new(sync.Mutex)}, nil
}

//ModDate - modification date
func (r *Reader) ModDate() (int, int, int) {
	return r.year, r.month, r.day
}

//tillzeo - strcpy like function
func tillzero(s []byte) (name string) {
	for _, val := range string(s) {
		if val == 0 {
			return
		}
		name = name + string(val)
	}
	return
}

//FieldName retrieves field name - check for NULL (0x00) termination
// specs says that it should be 0x00 padded, but it's not always true
func (r *Reader) FieldName(i int) (name string) {
	for _, val := range string(r.fields[i].Name[:]) {
		if val == 0 {
			return
		}
		name = name + string(val)
	}
	return
}

//FieldNames get an array with the fields' names
func (r *Reader) FieldNames() []string {
	//pre allocate array - to reduce risk of re-allocation with append
	names := make([]string, 0, int(r.headerlen/32-1))
	for i := range r.fields {
		names = append(names, r.FieldName(i))
	}
	return names
}

//SetFlags - set flags to alter behaviour - binary, should be "orred"
//returns: previous flags
func (r *Reader) SetFlags(flags int32) int32 {
	oldflags := r.flags
	r.flags = flags
	return oldflags
}

//validate - check if it's a valid field type
func (f *Field) validate() error {
	switch f.Type {
	case 'C', 'N', 'F', 'L', 'D':
		return nil
	}
	return fmt.Errorf("Sorry, dbf library doesn't recognize field type '%c', Field: '%s'", f.Type, tillzero(f.Name[:]))
}

//Field - field description
type Field struct {
	Name          [11]byte // 0x0 terminated or 11 bytes long (it SHOULD be 0x00 padded)
	Type          byte
	Offset        uint32
	Len           uint8
	DecimalPlaces uint8 // ?
	_             [14]byte
}

//Record http://play.golang.org/p/-CUbdWc6zz
type Record map[string]interface{}

//Read - read record i
func (r *Reader) Read(i int) (rec Record, err error) {
	var tm time.Time
	r.Lock()
	defer r.Unlock()

	offset := int64(r.headerlen) + int64(r.recordlen)*int64(i)
	r.r.Seek(offset, io.SeekStart)
	var deleted byte
	if err = binary.Read(r.r, binary.LittleEndian, &deleted); err != nil {
		return nil, err
	} else if deleted == '*' {
		return nil, fmt.Errorf("record %d is deleted", i)
	} else if deleted != ' ' {
		return nil, fmt.Errorf("record %d contained an unexpected value in the deleted flag: %x", i, deleted)
	}
	rec = make(Record)
	for i, f := range r.fields {
		buf := make([]byte, f.Len)
		if err = binary.Read(r.r, binary.LittleEndian, &buf); err != nil {
			return nil, err
		}
		fieldVal := strings.TrimSpace(string(buf))
		switch f.Type {
		case 'F': //Float
			rec[r.FieldName(i)], err = strconv.ParseFloat(fieldVal, 64)
		case 'N': //Numeric
			rec[r.FieldName(i)], err = strconv.Atoi(fieldVal)
		case 'L': //Logical, T,F or Space (ternary) - sorry, you've got to rune
			switch {
			case fieldVal == "Y" || fieldVal == "T":
				rec[r.FieldName(i)] = 'T'
			case fieldVal == "N" || fieldVal == "F":
				rec[r.FieldName(i)] = 'F'
				err = nil
			case fieldVal == "?" || fieldVal == "":
				rec[r.FieldName(i)] = ' '
				err = nil
			default:
				err = fmt.Errorf("Invalid Logical Field: %s", r.FieldName(i))
			}
		case 'D': //Date - YYYYYMMDD - use time.Parse (reference date Jan 2, 2006)

			tm, err = time.Parse("20060102", fieldVal)
			if err != nil {
				if fieldVal == "" {
					err = nil
					if r.flags&FlagDateAssql != 0 {
						rec[r.FieldName(i)] = ""
					} else {
						//this is the zero time, as far the package time, states
						rec[r.FieldName(i)] = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
					}
				}
			} else {
				if r.flags&FlagDateAssql != 0 {
					rec[r.FieldName(i)] = tm.Format("2006-01-02")
				} else {
					rec[r.FieldName(i)] = tm
				}
			}
		default: //String value (C, padded with blanks) -Notice: blanks removed by the trim above
			rec[r.FieldName(i)] = fieldVal
		}
		if err != nil {
			return nil, err
		}
	}
	return rec, nil
}

//OrderedRecord : it's an ordered (0 based) record, instead of map
type OrderedRecord []interface{}

//ReadOrdered - read record in an ordered manner - integer index (0 based)
func (r *Reader) ReadOrdered(i int) (orec OrderedRecord, err error) {
	rec, err := r.Read(i)
	if err != nil {
		return nil, err
	}
	orec = make([]interface{}, 0, r.headerlen/32)
	fns := r.FieldNames()
	for i := range fns {
		orec = append(orec, rec[fns[i]])
	}
	return orec, nil
}
