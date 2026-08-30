# Examples

## Extraction

Re-assemble somefile.tar using a remote chunk store and a blob index file.

```text
desync extract -s ssh://192.168.1.1/path/to/casync.store/ -c /tmp/store somefile.tar.caibx somefile.tar
```

Use multiple stores, specify the local one first to improve performance.

```text
desync extract -s /some/local/store -s ssh://192.168.1.1/path/to/casync.store/ somefile.tar.caibx somefile.tar
```

Extract version 3 of a disk image using the previous 2 versions as seed for cloning (if supported), or copying. Note, when providing a seed like `--seed <file>.ext.caibx`, it is assumed that `<file>.ext` is available next to the index file, and matches the index.

```text
desync extract -s /local/store \
  --seed image-v1.qcow2.caibx \
  --seed image-v2.qcow2.caibx \
  image-v3.qcow2.caibx image-v3.qcow2
```

Extract an image using several seeds present in a directory. Each of the `.caibx` files in the directory needs to have a matching blob of the same name. It is possible for the source index file to be in the same directory also (it'll be skipped automatically).

```text
desync extract -s /local/store --seed-dir /path/to/images image-v3.qcow2.caibx image-v3.qcow2
```

Mix and match remote stores and use a local cache store to improve performance. Also group two identical HTTP stores with `|` to provide failover in case of errors on one.

```text
desync extract \
       -s "http://192.168.1.101/casync.store/|http://192.168.1.102/casync.store/" \
       -s ssh://192.168.1.1/path/to/casync.store/ \
       -s https://192.168.1.3/ssl.store/ \
       -c /path/to/cache \
       somefile.tar.caibx somefile.tar
```

Extract a file in-place (`-k` option). If this operation fails, the file will remain partially complete and can be restarted without the need to re-download chunks from the remote SFTP store. Use `-k` when a local cache is not available and the extract may be interrupted.

```text
desync extract -k -s sftp://192.168.1.1/path/to/store file.caibx file.tar
```

Extract an image directly onto a block device. The `-k` or `--in-place` option is needed.

```text
desync extract -k -s /mnt/store image.caibx /dev/sdc
```

Extract a file using a remote index stored in an HTTP index store.

```text
desync extract -k -s sftp://192.168.1.1/path/to/store http://192.168.1.2/file.caibx file.tar
```

## Chunking

Split a blob, store the chunks and create an index file.

```text
desync make -s /some/local/store index.caibx /some/blob
```

Split a blob, create an index file and store the chunks in an S3 bucket named `store`.

```text
S3_ACCESS_KEY=mykey S3_SECRET_KEY=mysecret desync make -s s3+http://127.0.0.1:9000/store index.caibx /some/blob
```

Index an existing local file without creating chunks.

```text
desync make image.raw.caibx /tmp/image.raw
```

Verify the index you just created.

```text
desync verify-index image.raw.caibx /tmp/image.raw
```

## Cache and Store Management

Verify a local cache. Errors will be reported to STDOUT, since `-r` is not given, nothing invalid will be removed.

```text
desync verify -s /some/local/store
```

Cache the chunks used in a couple of index files in a local store without actually writing the blob.

```text
desync cache -s ssh://192.168.1.1/path/to/casync.store/ -c /local/cache somefile.tar.caibx other.file.caibx
```

Copy all chunks referenced in an index file from a remote HTTP store to a remote SFTP store.

```text
desync cache -s ssh://192.168.1.2/store -c sftp://192.168.1.3/path/to/store /path/to/index.caibx
```

Cache chunks from remote locally with non-standard port. Ignore existing files that are available locally from seed(s). This will only download chunks from the remote if they do not exist in the seed. Works with multiple seeds.

```text
desync cache -s http://cdn:9876 -c /tmp/chunkstore --ignore /tmp/indices/existing-image.raw.caibx /tmp/images/existing-image.raw
```

List the chunks referenced in a caibx.

```text
desync list-chunks somefile.tar.caibx
```

Chop an existing file according to an existing caibx and store the chunks in a local store. This can be used to populate a local cache from a possibly large blob that already exists on the target system.

```text
desync chop -s /some/local/store somefile.tar.caibx somefile.tar
```

Chop a blob according to an existing index, while ignoring any chunks that are referenced in another index. This can be used to improve performance when it is known that all chunks referenced in `image-v1.caibx` are already present in the target store and can be ignored when chopping `image-v2.iso`.

```text
desync chop -s /some/local/store --ignore image-v1.iso.caibx image-v2.iso.caibx image-v2.iso
```

Prune a store to only contain chunks that are referenced in the provided index files. Possible data loss.

```text
desync prune -s /some/local/store index1.caibx index2.caibx
```

## Archives

Pack a directory tree into a catar file.

```text
desync tar archive.catar /some/dir
```

Pack a directory tree into an archive and chunk the archive, producing an index file.

```text
desync tar -i -s /some/local/store archive.caidx /some/dir
```

Unpack a catar file.

```text
desync untar archive.catar /some/dir
```

Unpack a directory tree using an index file referencing a chunked archive.

```text
desync untar -i -s /some/local/store archive.caidx /some/dir
```

Pack a directory tree currently available as tar archive into a catar. The tar input stream can also be read from STDIN by providing `-` instead of the file name.

```text
desync tar --input-format=tar archive.catar /path/to/archive.tar
```

Process a tar stream into a catar. Since catar don't support hardlinks, we need to make sure those are dereferenced in the input stream.

```text
tar --hard-dereference -C /path/to/dir -c . | desync tar --input-format tar archive.catar -
```

Unpack a directory tree from an index file and store the output filesystem in a GNU tar file rather than the local filesystem. Instead of an archive file, the output can be given as `-` which will write to STDOUT.

```text
desync untar -i -s /some/local/store --output-format=gnu-tar archive.caidx /path/to/archive.tar
```

## Server Examples

Start a chunk server serving up a local store via port 80.

```text
desync chunk-server -s /some/local/store
```

Start a chunk server on port 8080 acting as proxy for other remote HTTP and SSH stores and populate a local cache.

```text
desync chunk-server -s http://192.168.1.1/ -s ssh://192.168.1.2/store -c cache -l :8080
```

Start a chunk server with a store-file, this allows the configuration to be re-read on SIGHUP without restart.

```text
# Create store file
echo '{"stores": ["http://192.168.1.1/"], "cache": "/tmp/cache"}' > stores.json

# Start the server
desync chunk-server --store-file stores.json -l :8080

# Modify
echo '{"stores": ["http://192.168.1.2/"], "cache": "/tmp/cache"}' > stores.json

# Reload
killall -1 desync
```

Start a writable index server, chunk a file and store the index.

```text
server# desync index-server -s /mnt/indexes --writable -l :8080

client# desync make -s /some/store http://192.168.1.1:8080/file.vmdk.caibx file.vmdk
```

Start a TLS chunk server on port 443 acting as proxy for a remote chunk store in AWS with local cache. The credentials for AWS are expected to be in the config file under key `https://s3-eu-west-3.amazonaws.com`.

```text
desync chunk-server -s s3+https://s3-eu-west-3.amazonaws.com/desync.bucket/prefix -c cache -l 127.0.0.1:https --cert cert.pem --key key.pem
```

FUSE mount an index file. This will make the indexed blob available as file underneath the mount point. The filename in the mount matches the name of the index with the extension removed. In this example `/some/mnt/` will contain one file `index`.

```text
desync mount-index -s /some/local/store index.caibx /some/mnt
```

FUSE mount a chunked and remote index file. First a (small) index file is read from the index-server which is used to re-assemble a larger index file and pipe it into the 2nd command that then mounts it.

```text
desync cat -s http://192.168.1.1/store http://192.168.1.2/small.caibx | desync mount-index -s http://192.168.1.1/store - /mnt/point
```

Long-running FUSE mount that may need to have its store setup changed without unmounting. This can be done by using the `--store-file` option rather than specifying store+cache in the command line. The process will then reload the file when a SIGHUP is sent.

```text
# Create the store file
echo '{"stores": ["http://192.168.1.1/"], "cache": "/tmp/cache"}' > stores.json

# Start the mount
desync mount-index --store-file stores.json index.caibx /some/mnt

# Modify the store setup
echo '{"stores": ["http://192.168.1.2/"], "cache": "/tmp/cache"}' > stores.json

# Reload
killall -1 desync
```

Show information about an index file to see how many of its chunks are present in a local store or an S3 store. The local store is queried first, S3 is only queried if the chunk is not present in the local store. The output will be in JSON format (`--format=json`) for easier processing in scripts.

```text
desync info --format=json -s /tmp/store -s s3+http://127.0.0.1:9000/store /path/to/index
```

Start an HTTP chunk server that will store uncompressed chunks locally, configured via JSON config file, and serve uncompressed chunks over the network (`-u` option). This chunk server could be used as a cache, minimizing latency by storing and serving uncompressed chunks. Clients will need to be configured to request uncompressed chunks from this server.

```text
# Chunk server
echo '{"store-options": {"/path/to/store/":{"uncompressed": true}}}' > /path/to/server.json

desync --config /path/to/server.json chunk-server -w -u -s /path/to/store/ -l :8080

# Client
echo '{"store-options": {"http://store.host:8080/":{"uncompressed": true}}}' > /path/to/client.json

desync --config /path/to/client.json cache -s sftp://remote.host/store -c http://store.host:8080/ /path/to/blob.caibx
```

HTTP chunk server using basic authorization. The server is configured to expect an `Authorization` header with the correct value in every request. The client configuration defines what the value should be on a per-server basis. The client config could be added to the default `$HOME/.config/desync/config.json` instead.

```text
# Server
DESYNC_HTTP_AUTH="Bearer abcabcabc" desync chunk-server -s /path/to/store -l :8080

# Client
echo '{"store-options": {"http://127.0.0.1:8080/":{"http-auth": "Bearer abcabcabc"}}}' > /path/to/client.json

desync --config /path/to/client.json extract -s http://127.0.0.1:8080/ /path/to/blob.caibx /path/to/blob
```

HTTPS chunk server using key and certificate signed by custom CA.

```text
# Building the CA and server certificate
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr (Common Name should be the server name)
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256

# Chunk server
desync chunk-server -s /path/to/store --key server.key --cert server.crt -l :8443

# Client
desync extract --ca-cert ca.crt -s https://hostname:8443/ image.iso.caibx image.iso
```

HTTPS chunk server with client authentication (mutual-TLS).

```text
# Building the CA, server and client certificates
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr (Common Name should be the server name)
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650 -sha256

# Chunk server
desync chunk-server -s /path/to/store --key server.key --cert server.crt --mutual-tls --client-ca ca.crt -l :8443

# Client
desync extract --client-key client.key --client-cert client.crt --ca-cert ca.crt -s https://hostname:8443/ image.iso.caibx image.iso
```


## Update Size Estimation

Get the size of the chunks that are required for an update, when using *compressed* chunks (default). I.e. how much data a client needs to download.

```text
# Server
## Create the update index file
desync make --store /some/local/store update.caibx /some/blob

## Create a detailed JSON info file for the chunks
desync inspect-chunks --store /some/local/store update.caibx update_chunks_details.json

# Client
## Download the update_chunks_details.json file
## Get the update info
desync info --seed local_index.caibx --chunks-info update_chunks_details.json --format=json update.caibx

## The value in 'dedup-size-not-in-seed-nor-cache-compressed' will hold the size in bytes that needs to be downloaded
```

Get the size of the chunks that are required for an update, when using *uncompressed* chunks.

```text
# Server
## Create the update index file
desync make --store /some/local/store update.caibx /some/blob

# Client
## Get the update info
desync info --seed local_index.caibx --format=json update.caibx

## The value 'dedup-size-not-in-seed-nor-cache' will hold the size in bytes that needs to be downloaded
```
