# Chunking Algorithms

The chunking algorithm decides where one chunk ends and the next begins. desync defaults to `buzhash`, the rolling hash casync uses, and offers alternatives through `--chunker` on the commands that create indexes: `make`, `tar -i` and `chunk`.

```text
desync chunkers
```

lists the available algorithms with their help texts.

| Chunker | Parallel chunking | Notes |
| --- | --- | --- |
| `buzhash` | yes | Default. The rolling hash casync uses. |
| `fastcdc` | no | Gear hash with chunk size normalization: a stricter boundary condition below the average size and a looser one above it. |
| `fastcdc4stadia` | no | FastCDC variant from Google's [cdc-file-transfer](https://github.com/google/cdc-file-transfer). Uses a single threshold and falls back to the best candidate seen, which gives a flatter chunk size distribution. |
| `segmentaware` | no | Places boundaries on the boundaries of segments in the input, such as the elements of a catar archive. See [Segment-aware chunking](#segment-aware-chunking). |

## Choosing a chunker

`buzhash` is the right choice unless there's a reason to change it. It's the only chunker that chunks in parallel (see [Parallel chunking](concepts.md#parallel-chunking)), and the one casync uses. With any other chunker, `make` finds the chunk boundaries using a single core, whatever `-n` is set to. Storing the chunks still uses the configured concurrency.

The alternatives are worth trying when deduplication between versions matters more than chunking speed. How much they help depends on the data. Measure it on two real versions before committing to one. Store the chunks of the first version, index the second without storing it, and let `desync info` count how many of its chunks are already in the store, as in [Update size estimation](cookbook.md#update-size-estimation). Repeat with a new store for each candidate.

```text
desync make --chunker fastcdc -s /tmp/fastcdc-store v1.caibx image-v1.img
desync make --chunker fastcdc v2.caibx image-v2.img
desync info -s /tmp/fastcdc-store v2.caibx
```

`chunk` prints the boundaries without storing anything, which is a quick way to see what an algorithm does with a file:

```text
desync chunk --chunker fastcdc4stadia image.img
```

## Keeping chunking settings consistent

Two versions of a file only share chunks if they were cut the same way. Chunks from different chunkers can share a store, since chunks are identified by their content, but an image chunked with `buzhash` and the next version chunked with `fastcdc` will have few chunks in common. The same applies to [seeds](concepts.md#seeds-and-reflinks): a seed only provides chunks if its index was made with the same algorithm and sizes as the index being extracted.

An index records the min, avg and max chunk sizes, but not the algorithm or its options. Keep them alongside the build that creates the indexes, and use them for every version.

This matters when a seed's index has to be regenerated with `extract --regenerate-invalid-seeds`. The chunk sizes are taken from the seed's index, but the algorithm comes from `--index-regeneration-chunker`, which defaults to `buzhash`. Set it to the chunker the seed was made with. Chunker options can't be passed for regeneration, so only seeds made with the default options of their chunker can be regenerated to match. For `segmentaware`, that also means the seed file has to supply its own segments, as a catar file does.

Reading an index doesn't depend on how it was chunked. `extract`, `cat`, `mount-index` and `untar` work with indexes from any of these chunkers.

## Chunk sizes

`-m` sets the min, avg and max chunk size in KB for every chunker (default `16:64:256`), but each algorithm uses them differently:

- `buzhash` is tuned for sizes that differ by a factor of 4. Other ratios skew the average chunk size.
- `fastcdc` derives its boundary conditions from the average size and switches between them at the average.
- `fastcdc4stadia` derives its threshold from the difference between the average and minimum size.

The min size has to be at least the window size of the rolling hash: 48 bytes for `buzhash` and 64 bytes for `fastcdc` and `fastcdc4stadia`. `segmentaware` uses the window size of its subchunker.

## Segment-aware chunking

Content-defined chunking knows nothing about what the bytes mean. A chunk can start in the middle of one file in an archive and end in the middle of the next, so a change to one file also changes the chunks around it. `segmentaware` takes a list of segments describing the input, such as the elements of a catar archive, and places chunk boundaries on segment boundaries.

It runs another chunker, the subchunker, to decide which segment boundaries to use:

- **Small segments** are merged into a buffer of up to the max chunk size. The subchunker finds a boundary in that buffer, and the boundary is moved forward to the next segment boundary. Each chunk therefore consists of whole segments.
- **Big segments**, those larger than `smallSegmentBreakpoint`, are chunked on their own by the subchunker. Their start and end are always chunk boundaries, and their last chunk is often smaller than the min size.

Chunks smaller than the min size are normal with this chunker.

### Options

Options are passed with `--chunker-options` as a comma-separated list of `key=value` pairs. `segmentaware` is the only chunker that takes options; passing them to another chunker is an error.

| Option | Description |
| --- | --- |
| `segmentsSource` | File to read the segment sizes from. Defaults to the file being chunked, which only works for formats that describe themselves, like catar. |
| `segmentsSourceFormat` | `csv` or `catar`. Taken from the extension of the segments source if not set. |
| `subchunker` | Chunker used to find boundaries, default `fastcdc`. |
| `mergeSmallSegments` | Merge small segments before chunking, `true` (default) or `false`. With `false`, every segment is chunked on its own and segments smaller than the min size become chunks of their own. |
| `smallSegmentBreakpoint` | Largest segment, in KB, that is merged with others. Defaults to twice the avg chunk size, and is never larger than the max chunk size. |

Paths in `segmentsSource` can't contain commas, since they separate the options.

### Segment sources

**catar.** Every element of the archive (entry, filename, payload and so on) is a segment. A catar file can supply its own segments:

```text
desync make --chunker segmentaware -s /some/store app.caidx app.catar
```

**CSV.** One segment size in bytes per line, in order. The sizes have to add up to the size of the input exactly, or chunking fails:

```text
desync make --chunker segmentaware --chunker-options segmentsSource=layout.csv,subchunker=fastcdc4stadia -s /some/store disk.caibx disk.img
```

When chunking starts at an offset, as with `chunk --start`, the segments still describe the whole file, and the ones before the offset are skipped.

`tar -i` chunks the archive as it's being created, so the archive can't supply its own segments there. Create a catar file with `tar` and chunk it with `make` instead. The resulting index can be extracted with `untar -i` like one made by `tar -i`:

```text
desync tar app.catar /some/dir
desync make --chunker segmentaware -s /some/store app.caidx app.catar
desync untar -s /some/store -i app.caidx /some/target
```
