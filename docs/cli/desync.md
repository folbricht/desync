## desync

Content-addressed binary distribution system

### Synopsis

desync is a content-addressed binary distribution system. It chunks files
into reusable, compressed pieces kept in chunk stores, and reassembles them
efficiently using indexes, seeds and caches. It is compatible with casync
archives, indexes and stores.

Store locations, used with options like -s/--store and -c/--cache, can be:
  /path/to/store              local directory store
  http(s)://host/path/        chunk/index server (see chunk-server command)
  s3+http(s)://host/bucket    S3-compatible object store
  gs://bucket/prefix          Google Cloud Storage bucket
  sftp://user@host/path       SFTP store
  ssh://user@host/path        casync protocol over SSH (read-only)

Commands that accept multiple stores try them in the order given. Several
stores can also be combined into one failover group by separating them with
'|', for example -s "http://server1/store|http://server2/store".

### Options

```
      --config string   config file (default $HOME/.config/desync/config.json)
      --digest string   digest algorithm, sha512-256 or sha256 (default "sha512-256")
  -h, --help            help for desync
      --verbose         verbose mode
      --version         print the version and exit
```

### SEE ALSO

* [desync cache](desync_cache.md)	 - Read indexes and copy the referenced chunks
* [desync cat](desync_cat.md)	 - Stream a blob to stdout or a file-like object
* [desync chop](desync_chop.md)	 - Read chunks from a file according to an index
* [desync chunk](desync_chunk.md)	 - Chunk input file and print chunk boundaries and IDs
* [desync chunk-server](desync_chunk-server.md)	 - Server for chunks over HTTP(S)
* [desync completion](desync_completion.md)	 - Generate the autocompletion script for the specified shell
* [desync config](desync_config.md)	 - Show or write config file
* [desync extract](desync_extract.md)	 - Read an index and build a blob from it
* [desync index-server](desync_index-server.md)	 - Server for indexes over HTTP(S)
* [desync info](desync_info.md)	 - Show information about an index
* [desync inspect-chunks](desync_inspect-chunks.md)	 - Inspect chunks from an index and an optional local store
* [desync list-chunks](desync_list-chunks.md)	 - List chunk IDs from an index
* [desync make](desync_make.md)	 - Chunk input file and create index
* [desync manpage](desync_manpage.md)	 - Generate manpages for desync
* [desync mount-index](desync_mount-index.md)	 - FUSE mount an index file
* [desync mtree](desync_mtree.md)	 - Print the content of a catar, caidx or local directory in mtree format
* [desync prune](desync_prune.md)	 - Remove unreferenced chunks from a store
* [desync pull](desync_pull.md)	 - Serve chunks via casync protocol over SSH
* [desync tar](desync_tar.md)	 - Store a directory tree in a catar archive or index
* [desync untar](desync_untar.md)	 - Extract a directory tree from a catar archive or index
* [desync verify](desync_verify.md)	 - Read chunks in a store and verify their integrity
* [desync verify-index](desync_verify-index.md)	 - Verify an index matches a file
* [desync version](desync_version.md)	 - Show the desync version

