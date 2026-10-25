package logger

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsUnsyncableStdout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "macOS terminal ENOTTY",
			err:  &os.PathError{Op: "sync", Path: "/dev/stdout", Err: syscall.ENOTTY},
			want: true,
		},
		{
			name: "Linux terminal EINVAL",
			err:  &os.PathError{Op: "sync", Path: "/dev/stdout", Err: syscall.EINVAL},
			want: true,
		},
		{
			name: "closed stdout EBADF",
			err:  &os.PathError{Op: "sync", Path: "/dev/stdout", Err: syscall.EBADF},
			want: true,
		},
		{
			name: "real I/O error is not ignored",
			err:  syscall.EIO,
			want: false,
		},
		{
			name: "unrelated error is not ignored",
			err:  errors.New("x"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, isUnsyncableStdout(tt.err))
		})
	}
}
