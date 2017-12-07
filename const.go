package desync

const (
	// Format identifiers
	CaFormatIndex  = 0x96824D9C7B129FF9
	CaFormatHeader = 0xE75B9E112F17417D

	// Protocol message types
	CaProtocolHello      = 0x3c71d0948ca5fbee
	CaProtocolIndex      = 0xb32a91dd2b3e27f8
	CaProtocolIndexEOF   = 0x4f0932f1043718f5
	CaProtocolArchive    = 0x95d6428a69eddcc5
	CaProtocolArchiveEOF = 0x450bef663f24cbad
	CaProtocolRequest    = 0x8ab427e0f89d9210
	CaProtocolChunk      = 0x5213dd180a84bc8c
	CaProtocolMissing    = 0xd010f9fac82b7b6c
	CaProtocolGoodbye    = 0xad205dbf1a3686c3
	CaProtocolAbort      = 0xe7d9136b7efea352

	// Provided services
	CaProtocolReadableStore   = 0x1
	CaProtocolWritableStore   = 0x2
	CaProtocolReadableIndex   = 0x4
	CaProtocolWritableIndex   = 0x8
	CaProtocolReadableArchive = 0x10
	CaProtocolWritableArchive = 0x20

	// Wanted services
	CaProtocolPullChunks      = 0x40
	CaProtocolPullIndex       = 0x80
	CaProtocolPullArchive     = 0x100
	CaProtocolPushChunks      = 0x200
	CaProtocolPushIndex       = 0x400
	CaProtocolPushIndexChunks = 0x800
	CaProtocolPushArchive     = 0x1000

	// Protocol request flags
	CaProtocolRequestHighPriority = 1

	// Chunk properties
	CaProtocolChunkCompressed = 1
)
