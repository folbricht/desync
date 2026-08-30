# OCI Registry Stores

OCI container registries (ghcr.io, Docker Hub, Harbor, [zot](https://zothub.io), the [distribution registry](https://github.com/distribution/distribution), ...) can be used as chunk stores. Registries are widely available, often free or low cost, and typically sit behind a CDN, which makes them an attractive way to distribute chunks publicly without running any server infrastructure. Use the `oci+https` scheme when pointing at a registry, or `oci+http` for registries without TLS. The URL contains the registry host followed by the repository name, which must be lowercase:

```text
oci+https://ghcr.io/myuser/mystore
oci+http://127.0.0.1:5000/test/store
```

An OCI store supports reading, writing, pruning, and use as a cache, so it works with `make`, `extract`, `chop`, `cache`, `info`, `prune` and every other command that takes a `-s` store. Indexes can be kept in a registry too, so a registry can hold everything needed to distribute a file — see [Indexes in a registry](#indexes-in-a-registry) below.

## How chunks are stored

Every chunk is stored as its own small OCI artifact consisting of a blob holding the chunk data — compressed and/or encrypted according to the store options — and a manifest referencing that blob. The manifest is tagged with the chunk ID in hex followed by the storage extension, the same naming used for chunk files in other stores, e.g. `<id>.cacnk` for a compressed store or `<id>.cacnk.xchacha20-poly1305-<keyid>` for a compressed and encrypted one (see [Chunk Encryption](encryption.md)). Chunks in different formats or with different encryption keys can therefore coexist in one repository, and `prune` only considers chunks matching the store's own configuration. A chunk artifact looks like this in the registry:

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "artifactType": "application/vnd.desync.chunk.v1",
  "config": {
    "mediaType": "application/vnd.oci.empty.v1+json",
    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    "size": 2
  },
  "layers": [
    {
      "mediaType": "application/octet-stream",
      "digest": "sha256:759129e4f837302163925898441ed7a7ce728e9340dc20c1109f7461a878dc39",
      "size": 34806
    }
  ],
  "annotations": {
    "org.opencontainers.image.title": "cf16af015e294eb73277987c0f08f9c8157cb05425e52f5e8023749ac1279948.cacnk"
  }
}
```

This layout has a few important properties:

- The chunk ID appears only in the tag, never as a blob digest. OCI blob digests must be SHA256, so this is what allows the store to work with desync's default SHA512/256 digest algorithm and lets it be combined freely with other stores, caches, and existing indexes. It is also what makes encryption possible: the registry digests and verifies the ciphertext.
- Registries garbage-collect blobs that are not referenced by a manifest (and some, like ghcr.io, won't serve them at all). The tagged manifest keeps every chunk referenced and therefore safe.
- Identical chunks pushed again reuse the same blob — registries deduplicate blobs by digest within a repository.

Be aware that registry UIs show one tag (or "package version") per chunk, so a store with many chunks makes for a noisy listing. Using a repository dedicated to the chunk store is recommended, although unrelated artifacts sharing the repository are safe, even from `prune`.

Reading a chunk takes two requests (manifest by tag, then blob), an existence check takes one, and writing takes three. desync requests chunks concurrently, which hides much of the added round-trip latency, but combining a registry store with a local cache (`-c`) is still more worthwhile than for most other store types.

## Indexes in a registry

Indexes can be stored in a registry as well, which means a registry can hold everything needed to distribute a file, with no other server or file transfer involved. The index location is the repository followed by the index name, and the name becomes the tag:

```sh
# Chunk the file, storing chunks and the index in the registry
desync make -s oci+https://ghcr.io/myuser/mystore \
  oci+https://ghcr.io/myuser/mystore/file.iso.caibx file.iso

# On another machine, reassemble it from the registry alone
desync extract -s oci+https://ghcr.io/myuser/mystore \
  oci+https://ghcr.io/myuser/mystore/file.iso.caibx file.iso
```

Indexes are stored as their own artifact, a blob holding the index referenced by a manifest tagged with the index name and carrying the artifact type `application/vnd.desync.index.v1`. The manifest also records the number of chunks, the size of the indexed blob, and the chunk sizes as annotations, so the shape of an index can be read without fetching it.

Indexes and chunks can share a repository. `prune` only ever deletes chunk artifacts, so indexes in the same repository are left alone, and a chunk lookup is never satisfied by an index. Keeping them in separate repositories is still tidier, since registry UIs list one version per tag:

```sh
desync make -s oci+https://ghcr.io/myuser/chunks \
  oci+https://ghcr.io/myuser/indexes/file.iso.caibx file.iso
```

Because the index name is used as a tag it has to fit the OCI tag grammar: word characters, dots and dashes, not starting with a dot or dash, at most 128 characters. `file.iso.caibx` and `rootfs-v2.caidx` are fine, a name containing `/` is not. Note also that pushing an index that already exists overwrites the tag, which registries configured for immutable tags will reject, and that removing an index needs the same manifest deletion support described under [Pruning](#pruning).

Storing an index in a registry does not keep its chunks alive there. It doesn't need to: every chunk already has its own tagged manifest holding a reference to its blob.

Two things to be aware of when indexes and chunks share a repository. Store options are looked up by location, so a `store-options` entry for the repository applies to the index as well as the chunks; if that entry enables encryption, index operations fail, as described under [Chunk Encryption](encryption.md). Keep the indexes in a separate repository in that case, or add a more specific entry without encryption for them. The `timeout` option is applied differently too: for indexes it bounds how long a transfer may make no progress rather than how long it may take, so a large index isn't cut off part way through by the one minute default while a registry that stops responding mid-transfer still fails.

Indexes over 32 MiB, which is about 840,000 chunks, roughly a 50 GB blob at the default chunk size, are streamed to the registry rather than serialized into memory first. The request body can't be rewound in that case, so `error-retry` doesn't apply to the upload and an access token that expires part way through surfaces as a `401` instead of being renewed. Registry tokens are often short-lived, so for indexes that big prefer credentials that don't expire mid-push, or split the input across several smaller indexes.

## Authentication

Credentials are looked up in the following order, first match wins:

1. The `DESYNC_OCI_USERNAME` and `DESYNC_OCI_PASSWORD` environment variables. Convenient in CI, e.g. with the ephemeral `GITHUB_TOKEN` in GitHub Actions.
2. The `oci-credentials` section of the config file (see [Configuration](configuration.md)), keyed by the full store URL with glob support, the same format as `store-options` keys.
3. The Docker credential store. If you're logged in with `docker login ghcr.io` or `oras login ghcr.io`, desync picks those credentials up without any configuration.

Anonymous access works for public repositories, so extracting from a public store needs no credentials at all. For ghcr.io, pushing requires a token with the `write:packages` scope, and note that new ghcr.io packages default to private visibility.

## Store options

The usual [store options](configuration.md#configuration-reference) apply, keyed by the full store URL: `uncompressed` to store chunks in plain form, `timeout`, `error-retry`, `error-retry-base-interval`, `trust-insecure`, and `ca-cert`/`client-cert`/`client-key` for custom or mutual TLS.

```json
{
  "store-options": {
    "oci+https://registry.internal/desync/store": {
      "uncompressed": true,
      "error-retry": 2,
      "ca-cert": "/path/to/internal-ca.crt"
    }
  }
}
```

## Pruning

`desync prune` deletes the manifests of all chunks not referenced by the given indexes. Only tags that parse as chunk IDs are considered, and of those only manifests with the desync chunk artifact type are deleted, so an unrelated artifact tagged with a chunk-ID-shaped hex string is safe. Reclaiming the space held by the then-unreferenced blobs is left to the registry's own garbage collection. Manifest deletion is not supported by every registry: the distribution registry requires it to be enabled with `REGISTRY_STORAGE_DELETE_ENABLED=true`, and ghcr.io only supports deletion through the GitHub Packages API, not the registry API — for ghcr.io, use a cleanup action for untagged/unwanted versions instead.

## Examples

Publish a file to ghcr.io, using credentials from a previous `docker login`:

```sh
desync make -s oci+https://ghcr.io/myuser/mystore file.iso.caibx file.iso
```

Reassemble the file on another machine, keeping a local cache of chunks:

```sh
desync extract -s oci+https://ghcr.io/myuser/mystore -c /var/tmp/cache file.iso.caibx file.iso
```

The same, in CI with credentials from the environment:

```sh
DESYNC_OCI_USERNAME=myuser DESYNC_OCI_PASSWORD=$GITHUB_TOKEN \
  desync make -s oci+https://ghcr.io/myuser/mystore file.iso.caibx file.iso
```

See how much of an updated image is already covered by the chunks in the registry:

```sh
desync info -s oci+https://ghcr.io/myuser/mystore file-v2.iso.caibx
```

Remove all chunks no longer referenced by the current set of indexes:

```sh
desync prune -s oci+https://registry.internal/desync/store file-v2.iso.caibx
```

Run a local throwaway registry for testing:

```sh
podman run -d --rm -p 5000:5000 -e REGISTRY_STORAGE_DELETE_ENABLED=true docker.io/library/registry:2
desync make -s oci+http://127.0.0.1:5000/test/store file.iso.caibx file.iso
```
