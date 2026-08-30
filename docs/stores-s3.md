# S3 Store URLs

desync supports reading from and writing to chunk stores that offer an S3 API, for example hosted in AWS or running on a local server. When using such a store, credentials are passed into the tool either via environment variables `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_SESSION_TOKEN` (if needed) or, if multiples are required, in the config file. Care is required when building those URLs. Below a few examples:

## AWS

This store is hosted in `eu-west-3` in AWS. `s3` signals that the S3 protocol is to be used, `https` should be specified for SSL connections. The first path element of the URL contains the bucket, `desync.bucket` in this example. Note, when using AWS, no port should be given in the URL!

```text
s3+https://s3-eu-west-3.amazonaws.com/desync.bucket
```

It's possible to use prefixes (or "directories") to object names like so:

```text
s3+https://s3-eu-west-3.amazonaws.com/desync.bucket/prefix
```

## Other service with S3 API

This is a store running on the local machine on port 9000 without SSL.

```text
s3+http://127.0.0.1:9000/store
```

## Setting S3 bucket addressing style

desync uses [minio](https://github.com/minio/minio-go) as an S3 client library. It has an auto-detection mechanism for determining the addressing style of the buckets which should work for Amazon and Google S3 services but could potentially fail for your custom implementation. You can manually specify the addressing style by appending the "lookup" query parameter to the URL.

By default, the value of `?lookup=auto` is implied.

```text
s3+http://127.0.0.1:9000/bucket/prefix?lookup=path
s3+https://s3.internal.company/bucket/prefix?lookup=dns
s3+https://example.com/bucket/prefix?lookup=auto
```
