package main

import (
	"github.com/lillianhealth/jsplit/pkg/cloud"
	"github.com/lillianhealth/jsplit/pkg/jserror"
	"github.com/lillianhealth/jsplit/pkg/jsplit"

	"context"
	"flag"
	"fmt"
	"os"
)

func main() {
	var (
		filename   string
		outputPath string
		overwrite  bool
		rd         *jsplit.AsyncReader
		err        error
		fi         os.FileInfo
		perms      os.FileMode = 0o755
	)

	flag.StringVar(&filename, "file", "", "Source JSON file")
	flag.StringVar(&outputPath, "output", "", "Output path for parsed JSON files (can be an s3:// or gs:// URI")
	flag.BoolVar(&overwrite, "overwrite", false, "Overwrite local filesystem output path if it exists")
	flag.Parse()

	if filename == "" || outputPath == "" {
		fmt.Println("Usage: jsplit -file <json_file> -output <output_path>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// create output path if it doesn't exist
	// or if it does exist, remove it if overwrite is true
	if !cloud.IsCloudURI(outputPath) {
		fi, err = os.Stat(outputPath)

		switch {
		// if we ecountered an error and it's not a "file does not exist" error, exit
		case err != nil && !os.IsNotExist(err):
			jserror.ErrExit(err)
		// if the file exists and it's a directory, exit unless overwrite is true
		case err == nil && fi.IsDir() && !overwrite:
			jserror.ErrExit(fmt.Errorf("error: %s already exists", outputPath))
		// if the file exists and it's a directory, remove it if overwrite is true
		case err == nil && fi.IsDir() && overwrite:
			err = os.RemoveAll(outputPath)
			jserror.ErrExit(err)
		}

		err = os.MkdirAll(outputPath, perms)
		jserror.ErrExit(err)
	}

	rd, err = jsplit.AsyncReaderFromFile(filename, 1024*1024)
	jserror.ErrExit(err)

	fmt.Printf("Reading %s\n", filename)

	ctx := context.Background()
	ctx = rd.Start(ctx)

	err = jsplit.SplitStream(ctx, rd, outputPath)
	jserror.ErrExit(err)
}
