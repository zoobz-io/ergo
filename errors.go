package ergo

import "errors"

// Sentinel errors for mesh configuration and lifecycle.
var (
	// ErrMeshStarted is returned when registering channels after Start has been called.
	ErrMeshStarted = errors.New("ergo: mesh already started")

	// ErrMeshClosed is returned when operating on a closed mesh.
	ErrMeshClosed = errors.New("ergo: mesh is closed")

	// ErrDuplicateStream is returned when the same stream is registered twice in the same direction.
	ErrDuplicateStream = errors.New("ergo: duplicate stream registration")

	// ErrEmptyStream is returned when an empty stream name is provided.
	ErrEmptyStream = errors.New("ergo: empty stream name")
)
