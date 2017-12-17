package desync

import "io"

type Node struct{}

type NodeHandler func(Node) error

func DecodeCatar(r io.Reader, f NodeHandler) error {

	return nil
}
