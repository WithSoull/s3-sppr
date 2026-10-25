package httpapi

import "io"

type countReader struct {
	reader io.Reader
	count  *int64
}

func (c *countReader) Read(buf []byte) (int, error) {
	n, err := c.reader.Read(buf)
	*c.count += int64(n)
	return n, err
}

type countWriter struct {
	writer io.Writer
	count  *int64
}

func (c *countWriter) Write(buf []byte) (int, error) {
	n, err := c.writer.Write(buf)
	*c.count += int64(n)
	return n, err
}
