package desync

func blocksizeOfFile(name string) uint64 {
	// TODO: Not that it really matters for reflink cloning of files on windows
	// but it would be nice to determine the actual blocksize here anyway.
	return DefaultBlockSize
}
