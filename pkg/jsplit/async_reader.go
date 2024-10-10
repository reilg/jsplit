package jsplit

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/reilg/jsplit/pkg/cloud"
)

// AsyncReader reads an io.Reader asynchronously
type AsyncReader struct {
	rd         io.Reader
	readCh     chan []byte
	bufferSize int
	isClosed   int32
} // reordered to pack better

// AsyncReaderFromFile creates an AsyncReader for reading from a Google Cloud Storage object
func AsyncReaderFromFile(uri string, bufferSize int) (*AsyncReader, error) {
	var (
		r   io.Reader
		err error
	)

	switch {
	case strings.HasPrefix(uri, "http"):
		r, err = HTTPReader(uri)
		if err != nil {
			return nil, err
		}
	case cloud.IsCloudURI(uri):
		r, err = cloud.NewReader(context.TODO(), uri)
		if err != nil {
			return nil, err
		}
	default:
		r, err = os.Open(uri)
		if err != nil {
			return nil, err
		}
	}

	// if gzipped, wrap in gzip reader
	if strings.HasSuffix(uri, ".gz") {
		gr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}

		return AsyncReaderFromReader(gr, bufferSize)
	}

	return AsyncReaderFromReader(r, bufferSize)
}

// AsyncReaderFromReader returns an AsyncReader for reading the supplied io.Reader
func AsyncReaderFromReader(rd io.Reader, bufferSize int) (*AsyncReader, error) {
	return &AsyncReader{
		readCh:     make(chan []byte, 16),
		rd:         rd,
		bufferSize: bufferSize,
	}, nil
}

// Start starts the background reading of the io.Reader
func (afr *AsyncReader) Start(ctx context.Context) context.Context {
	errCtx, cancelFunc := NewErrContextWithCancel(ctx)

	go func() {
		for {
			buf := make([]byte, afr.bufferSize)
			n, err := afr.rd.Read(buf)

			if err != nil && err != io.EOF {
				cancelFunc(err)
				return
			}

			if n > 0 {
				afr.readCh <- buf[:n]
			}

			if err == io.EOF {
				close(afr.readCh)
				atomic.StoreInt32(&afr.isClosed, 1)

				return
			}
		}
	}()

	return errCtx
}

// Read gets the next chunk which has been read from the file.
func (afr *AsyncReader) Read(ctx context.Context) ([]byte, error) {
	select {
	case buf, ok := <-afr.readCh:
		if !ok {
			return nil, io.EOF
		}

		return buf, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// IsClosed is used for testing to verify that the reader and associated channel has been closed.
func (afr *AsyncReader) IsClosed() bool {
	return atomic.LoadInt32(&afr.isClosed) == 1
}

func HTTPReader(uri string) (io.Reader, error) {
	const HTTPTimeOut = 10 * time.Minute

	var (
		err error
		r   *http.Response
	)

	httpClient := &http.Client{
		Timeout: HTTPTimeOut,
	}

	r, err = httpClient.Get(uri)
	if err != nil {
		return nil, err
	}

	return r.Body, nil
}
