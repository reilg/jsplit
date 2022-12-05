package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
)

func GetGCStorageObject(uri string) (*storage.ObjectHandle, context.Context, error) {
	bucket, objectName, err := ParseGCStorageUri(uri)
	if err != nil {
		return nil, nil, err
	}

	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	bkt := client.Bucket(bucket)

	return bkt.Object(objectName), ctx, nil
}

// Parse GC Storage Uri and return bucket and object name
func ParseGCStorageUri(uri string) (bucket string, object string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	if u.Scheme != "gs" {
		return "", "", fmt.Errorf("invalid scheme for Google Cloud Storage: %s", u.Scheme)
	}

	return u.Host, strings.TrimLeft(u.Path, "/"), nil

}

var IsGcStorageUri = func(uri string) bool {
	return strings.HasPrefix(uri, "gs:")
}
