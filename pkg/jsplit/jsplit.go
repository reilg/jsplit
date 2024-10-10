package jsplit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/reilg/jsplit/pkg/cloud"
)

const (
	Escape  = byte('\\')
	OpenCB  = byte('{')
	CloseCB = byte('}')
	OpenSB  = byte('[')
	CloseSB = byte(']')
	QM      = byte('"')
	COLON   = byte(':')
	SPACE   = byte(' ')
	TAB     = byte('\t')
	CR      = byte('\r')
	LF      = byte('\n')
	COMMA   = byte(',')
)

var (
	isOpen       []bool
	isWhitespace []bool
)

func init() {
	isWhitespace = make([]bool, 256)
	isWhitespace[SPACE] = true
	isWhitespace[TAB] = true
	isWhitespace[CR] = true
	isWhitespace[LF] = true

	isOpen = make([]bool, 256)
	isOpen[OpenCB] = true
	isOpen[OpenSB] = true
	isOpen[QM] = true
}

// SkipWhitespace moves the iterator skipping over any whitespace. The next call to itr.Next will return the first
// non-whitespace character or 0 if the iterator has been exhausted
func SkipWhitespace(itr *BufferedByteStreamIter) {
	var ch byte

	for {
		ch = itr.Next()
		if !isWhitespace[ch] {
			break
		}
	}

	if ch != 0 {
		itr.Advance(-1)
	}

	itr.Value()
}

// IsNext returns an error if the first non-whitespace character is different than expected.
func IsNext(itr *BufferedByteStreamIter, expected byte) error {
	SkipWhitespace(itr)

	ch := itr.Next()
	if ch != expected {
		return fmt.Errorf("expected '%v' found '%v'", rune(ch), rune(expected))
	}

	return nil
}

// ParseUntil reads from the iterator until the specified byte is found
func ParseUntil(itr *BufferedByteStreamIter, findCh byte) ([]byte, error) {
	var prev byte

	for {
		ch := itr.Next()
		if ch == 0 {
			return nil, fmt.Errorf("unexpected eof found while looking for '%v'", string(ch))
		} else if ch == findCh && prev != Escape {
			return itr.Value(), nil
		}

		prev = ch
	}
}

// ParseKey will parse a json key from the iterator
func ParseKey(itr *BufferedByteStreamIter) ([]byte, error) {
	err := IsNext(itr, QM)
	if err != nil {
		return nil, err
	}

	key, err := ParseUntil(itr, QM)
	if err != nil {
		return nil, err
	}

	err = IsNext(itr, COLON)
	if err != nil {
		return nil, err
	}

	return key, nil
}

var parseObjBuffer = make([]byte, 128*1024)

// ParseObject parses a json struct or list. It is non-reentrant and not thread safe due to it's use of parseObjBuffer.
// Could swap for a sync pool to make it so.  Also a reference to the returned data should not be stored as the data may
// change when ParseObject is called again
func ParseObject(itr *BufferedByteStreamIter) ([]byte, error) {
	SkipWhitespace(itr)

	var (
		closeCh  byte
		lastOpen byte
		prev     byte
	)

	ch := itr.Next()
	switch ch {
	case OpenCB:
		closeCh = CloseCB
	case OpenSB:
		closeCh = CloseSB
	default:
		return nil, fmt.Errorf("unexpected char '%v' found while looking for '{'", string(ch))
	}

	parseObjBuffer = parseObjBuffer[:1]
	parseObjBuffer[0] = ch

	openStack := NewByteStack()

	for {
		ch := itr.Next()
		if ch == 0 {
			return nil, errors.New("unexpected EOF found while parsing object")
		}

		if isWhitespace[ch] {
			if lastOpen == QM {
				switch ch {
				case CR:
					parseObjBuffer = append(parseObjBuffer, Escape, Escape, byte('r'))
				case LF:
					parseObjBuffer = append(parseObjBuffer, Escape, Escape, byte('n'))
				default:
					parseObjBuffer = append(parseObjBuffer, ch)
				}
			}

			continue
		}

		parseObjBuffer = append(parseObjBuffer, ch)

		switch lastOpen {
		case 0:
			if ch == closeCh {
				return parseObjBuffer, nil
			} else if isOpen[ch] {
				openStack.Push(ch)
				lastOpen = ch
			}

		case QM:
			if ch == QM && prev != Escape {
				openStack.Pop()
				lastOpen = openStack.Peek()
			}

		case OpenCB:
			if ch == CloseCB {
				openStack.Pop()
				lastOpen = openStack.Peek()
			} else if isOpen[ch] {
				openStack.Push(ch)
				lastOpen = ch
			}

		case OpenSB:
			if ch == CloseSB {
				openStack.Pop()
				lastOpen = openStack.Peek()
			} else if isOpen[ch] {
				openStack.Push(ch)
				lastOpen = ch
			}

		default:
			return nil, fmt.Errorf("unknown opening character '%v'", string(ch))
		}

		prev = ch
	}
}

type ListAddFunc func(item []byte) error

type ParentType int

const (
	None ParentType = iota
	List
)

// ParseVal parses a json value
func ParseVal(itr *BufferedByteStreamIter, addFn ListAddFunc, parentType ParentType) (bool, []byte, error) {
	SkipWhitespace(itr)

	ch := itr.Next()
	switch ch {
	case 0:
		return false, nil, errors.New("reached EOF while parsing value")

	case QM:
		val, err := ParseUntil(itr, QM)
		return false, val, err

	case OpenSB:
		itr.Advance(-1)
		itr.Skip()

		if parentType == None {
			return true, nil, ParseList(itr, addFn)
		}

		listObj, err := ParseObject(itr)

		return true, listObj, err

	case OpenCB:
		itr.Advance(-1)
		itr.Skip()
		val, err := ParseObject(itr)

		return false, val, err

	default:
		if ch == CloseSB && parentType == List {
			itr.Advance(-1)
			return true, nil, nil
		}

		for {
			ch = itr.Next()
			if ch == COMMA || ch == CloseSB || ch == CloseCB {
				itr.Advance(-1)
				return false, itr.Value(), nil
			} else if ch == 0 {
				return false, nil, errors.New("reached EOF while parsing value")
			}
		}
	}
}

// ParseList parses a json list calling addFn for each list item
func ParseList(itr *BufferedByteStreamIter, addFn func(item []byte) error) error {
	SkipWhitespace(itr)

	ch := itr.Next()
	if ch != OpenSB {
		return fmt.Errorf("unexpected char '%v' found while looking for '['", string(ch))
	}

	itr.Skip()

	for {
		_, newVal, err := ParseVal(itr, nil, List)
		if err != nil {
			return err
		}

		if newVal != nil {
			err = addFn(newVal)
			if err != nil {
				return err
			}
		}

		SkipWhitespace(itr)
		ch = itr.Next()
		itr.Skip()

		if ch == CloseSB {
			return nil
		} else if ch != COMMA {
			return fmt.Errorf("unexpected token '%v' found. Expecting ','", rune(ch))
		}
	}
}

// SplitStream processes a json byte stream reading it and sending json lists in the root of the json document to jsonl
// files sharded based on the size of the data written. Non-List root level objects are written to a file named root.json
func SplitStream(ctx context.Context, rd ByteStream, dir string) error {
	itr := NewBufferedStreamIter(ctx, rd)

	SkipWhitespace(itr)

	ch := itr.Next()
	if ch != '{' {
		return fmt.Errorf("invalid format. only json objects are supported")
	}

	itr.Skip()

	start := time.Now()
	rootItems := make([]byte, 0, 128*1024)
	rootItems = append(rootItems, []byte("{\n")...)
	initialLen := len(rootItems)

	for {
		key, err := ParseKey(itr)
		if err != nil {
			return err
		}

		fileFactory := NewBufferedWriterFactory(dir, string(key[1:len(key)-1]), 256*1024)
		wr := NewSplittingJsonlWriter(fileFactory.CreateWriter, 4*1024*1024*1024)

		_, val, err := ParseVal(itr, wr.Add, None)
		if err != nil {
			return err
		}

		err = wr.Close()
		if err != nil {
			return err
		}

		if val != nil {
			if len(rootItems) != initialLen {
				rootItems = append(rootItems, []byte(",\n")...)
			}

			rootItems = append(rootItems, '\t')
			rootItems = append(rootItems, key...)
			rootItems = append(rootItems, ':')
			rootItems = append(rootItems, val...)
		}

		SkipWhitespace(itr)

		ch := itr.Next()
		if ch == COMMA {
			itr.Skip()
		} else if ch == CloseCB {
			break
		}
	}

	rootItems = append(rootItems, []byte("\n}")...)

	var rootFile string

	if cloud.IsCloudURI(dir) {
		rootFile = strings.TrimSuffix(dir, "/") + "/root.json"

		w, err := cloud.NewWriter(context.TODO(), rootFile)
		if err != nil {
			return err
		}

		_, err = w.Write(rootItems)
		if err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}
	} else {
		rootFile = filepath.Join(dir, "root.json")
		err := os.WriteFile(rootFile, rootItems, os.ModePerm)
		if err != nil {
			return err
		}
	}

	fmt.Printf("%s written successfully\n", rootFile)

	elapsed := time.Since(start)
	fmt.Printf("Completed in %f seconds", elapsed.Seconds())

	return nil
}

// SplitFile processes a json file reading it and sending json lists in the root of the json document to jsonl
func Split(filename, outputPath string, overwrite bool) error {
	var (
		fi    os.FileInfo
		err   error
		rd    *AsyncReader
		perms os.FileMode = 0o755
	)

	// create output path if it doesn't exist
	// or if it does exist, remove it if overwrite is true
	if !cloud.IsCloudURI(outputPath) {
		fi, err = os.Stat(outputPath)

		switch {
		// if we ecountered an error and it's not a "file does not exist" error, exit
		case err != nil && !os.IsNotExist(err):
			return err
		// if the file exists and it's a directory, exit unless overwrite is true
		case err == nil && fi.IsDir() && !overwrite:
			return fmt.Errorf("error: %s already exists", outputPath)
		// if the file exists and it's a directory, remove it if overwrite is true
		case err == nil && fi.IsDir() && overwrite:
			err = os.RemoveAll(outputPath)
			if err != nil {
				return err
			}
		}

		err = os.MkdirAll(outputPath, perms)
		if err != nil {
			return err
		}
	}

	rd, err = AsyncReaderFromFile(filename, 1024*1024)
	if err != nil {
		return err
	}

	fmt.Printf("Reading %s\n", filename)

	ctx := context.Background()
	ctx = rd.Start(ctx)

	err = SplitStream(ctx, rd, outputPath)
	if err != nil {
		return err
	}

	return nil
}
