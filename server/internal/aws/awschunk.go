package aws

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	xhttp "github.com/mageg-x/dedups3/internal/http"
)

type AwsChunkedReader struct {
	reader   *bufio.Reader
	trailers http.Header
	buffer   []byte
	offset   int
	err      error
	debug    bool
}

const (
	MaxChunkSize = 1 << 20
	AwsChunked   = "aws-chunked"
)

var (
	ErrMalformedEncoding = fmt.Errorf("malformed chunked encoding")
	ErrChunkTooBig       = fmt.Errorf("chunk too big")
)

// NewReader 返回一个解码后的 ReadCloser
func NewReader(req *http.Request) (io.ReadCloser, error) {
	// 只有 Content-Encoding == aws-chunked 才启用
	if req.Header.Get(xhttp.ContentEncoding) != AwsChunked {
		return req.Body, nil
	}

	trailer := req.Header.Get(xhttp.AmzTrailer) != ""

	if trailer {
		req.Trailer = make(http.Header)
		trailers := req.Header.Values(xhttp.AmzTrailer)
		for _, key := range trailers {
			req.Trailer.Add(key, "")
		}
	} else {
		req.Trailer = nil
	}

	return &AwsChunkedReader{
		trailers: req.Trailer,
		reader:   bufio.NewReader(req.Body),
		buffer:   make([]byte, 64*1024),
	}, nil
}

func (acr *AwsChunkedReader) Close() error {
	return acr.err
}

func (acr *AwsChunkedReader) Read(buf []byte) (n int, err error) {
	if acr.offset > 0 {
		n = copy(buf, acr.buffer[acr.offset:])
		if n == len(buf) {
			acr.offset += n
			return n, nil
		}
		acr.offset = 0
		buf = buf[n:]
	}

	mustRead := func(b ...byte) error {
		for _, want := range b {
			got, err := acr.reader.ReadByte()
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			if got != want {
				if acr.debug {
					fmt.Printf("mustread: want: %q got: %q\n", string(want), string(got))
				}
				return ErrMalformedEncoding
			}
			if err != nil {
				return err
			}
		}
		return nil
	}

	var size int
	for {
		b, err := acr.reader.ReadByte()
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			acr.err = err
			return n, acr.err
		}
		if b == '\r' {
			err := mustRead('\n')
			if err != nil {
				acr.err = err
				return n, acr.err
			}
			break
		}

		switch {
		case b >= '0' && b <= '9':
			size = size<<4 | int(b-'0')
		case b >= 'a' && b <= 'f':
			size = size<<4 | int(b-('a'-10))
		case b >= 'A' && b <= 'F':
			size = size<<4 | int(b-('A'-10))
		default:
			if acr.debug {
				fmt.Printf("err size: %v\n", string(b))
			}
			acr.err = ErrMalformedEncoding
			return n, acr.err
		}
		if size > MaxChunkSize {
			acr.err = ErrChunkTooBig
			return n, acr.err
		}
	}

	if cap(acr.buffer) < size {
		acr.buffer = make([]byte, size)
	} else {
		acr.buffer = acr.buffer[:size]
	}

	_, err = io.ReadFull(acr.reader, acr.buffer)
	if err == io.EOF && size != 0 {
		err = io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF {
		acr.err = err
		return n, acr.err
	}

	if len(acr.buffer) == 0 {
		if acr.trailers != nil {
			err = acr.readTrailers()
			if err != nil {
				acr.err = err
				return 0, err
			}
		}
		acr.err = io.EOF
		return n, acr.err
	}

	err = mustRead('\r', '\n')
	if err != nil && err != io.EOF {
		acr.err = err
		return n, acr.err
	}

	acr.offset = copy(buf, acr.buffer)
	n += acr.offset
	return n, err
}

func (acr *AwsChunkedReader) readTrailers() error {
	var valueBuffer bytes.Buffer
	for {
		v, err := acr.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if v != '\r' {
			valueBuffer.WriteByte(v)
			continue
		}
		var tmp [3]byte
		_, err = io.ReadFull(acr.reader, tmp[:])
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
		}
		if !bytes.Equal(tmp[:], []byte{'\n', '\r', '\n'}) {
			if acr.debug {
				fmt.Printf("got %q, want %q\n", string(tmp[:]), "\n\r\n")
			}
			return ErrMalformedEncoding
		}
		break
	}

	wantTrailers := make(map[string]struct{})
	for k := range acr.trailers {
		wantTrailers[strings.ToLower(k)] = struct{}{}
	}

	input := bufio.NewScanner(bytes.NewReader(valueBuffer.Bytes()))
	for input.Scan() {
		line := strings.TrimSpace(input.Text())
		if line == "" {
			continue
		}
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			if acr.debug {
				fmt.Printf("Could not find separator, got %q\n", line)
			}
			return ErrMalformedEncoding
		}
		key := strings.ToLower(strings.TrimSpace(line[:idx]))
		value := strings.TrimSpace(line[idx+1:])
		if _, ok := wantTrailers[key]; !ok {
			if acr.debug {
				fmt.Printf("Unknown key %q\n", key)
			}
			return ErrMalformedEncoding
		}
		acr.trailers.Set(key, value)
		delete(wantTrailers, key)
	}

	if len(wantTrailers) > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}
