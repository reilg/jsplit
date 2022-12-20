package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

type ListAddFunc func(item []byte) error

func errExit(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func main() {
	var (
		filename   string
		outputPath string
		rd         *AsyncReader
		err        error
	)

	flag.StringVar(&filename, "file", "", "Source JSON file")
	flag.StringVar(&outputPath, "output", "", "Output path for parsed JSON files (optional)")
	flag.Parse()

	if filename == "" {
		fmt.Println("Usage: jsplit -file <json_file> -output <output_path>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if len(outputPath) > 0 {
		if !IsGcStorageURI(outputPath) {
			if _, err = os.Stat(outputPath); err == nil {
				errExit(fmt.Errorf("error: %s already exists", filename))
			} else if !os.IsNotExist(err) {
				errExit(err)
			}

			err = os.Mkdir(outputPath, os.ModePerm)
			errExit(err)
		}
	} else {
		outputPath = strings.ReplaceAll(filename, ".", "_")
	}

	if IsGcStorageURI(filename) {
		rd, err = AsyncReaderFromGCStorage(filename, 1024*1024)
		errExit(err)
	} else {
		rd, err = AsyncReaderFromFile(filename, 1024*1024)
		errExit(err)
	}

	fmt.Printf("Reading %s\n", filename)

	ctx := context.Background()
	ctx = rd.Start(ctx)

	err = SplitStream(ctx, rd, outputPath)
	errExit(err)
}
