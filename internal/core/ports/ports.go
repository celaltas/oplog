package ports

import "context"

type OplogReader interface {
	ReadOplog(string) ([]byte, error)
}

type OplogWriter interface {
	WriteOplog(context.Context, string) error
}
