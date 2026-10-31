/*
Package desync implements data structures, protocols and features of
https://github.com/systemd/casync in order to allow support for additional
platforms and improve performance by way of concurrency and caching.

Supports the following casync data structures: catar archives, caibx/caidx index
files, castr stores (local or remote).

Local, HTTP and SSH stores are part of this package. Stores that depend on
large client libraries are in their own packages, so a program only links the
ones it imports: S3 in store/s3, Google Cloud Storage in store/gcs, SFTP in
store/sftp, and OCI registries in store/oci.

See desync/cmd for reference implementations of the available features.
*/
package desync
