package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsGcStorageUriTrue(t *testing.T) {
	var uri = "gs://bucket/path/to/file.json"
	var expected = true
	var actual = IsGcStorageUri(uri)
	require.Equal(t, expected, actual)
}

func TestIsGcStorageUriFalse(t *testing.T) {
	var uri = "/path/to/file.json"
	var expected = false
	var actual = IsGcStorageUri(uri)
	require.Equal(t, expected, actual)
}

// test that ParseGCStorageUri returns the correct GC Storage bucket and object name
func TestParseGCStorageUri(t *testing.T) {
	var uri = "gs://bucket/path/to/file.json"
	var expectedBucket = "bucket"
	var expectedObject = "path/to/file.json"
	var actualBucket, actualObject, err = ParseGCStorageUri(uri)
	require.Nil(t, err)
	require.Equal(t, expectedBucket, actualBucket)
	require.Equal(t, expectedObject, actualObject)
}
