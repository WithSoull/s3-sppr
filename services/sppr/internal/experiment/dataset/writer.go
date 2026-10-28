package dataset

// RowWriter streams the existing dataset format without imposing an event
// source. Experiments use it for minute-aggregate snapshots and causal labels.
type RowWriter interface {
	Write(Row) error
	Close() error
	Names() []string
}

func OpenRowWriter(dir, format string) (RowWriter, error) { return openRows(dir, format) }
