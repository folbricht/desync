# desync documentation

Start with the [README](../README.md) for what desync is and a quick start.

| | |
| --- | --- |
| [Concepts](concepts.md) | Chunking, seeds and reflinks, how the pieces fit together |
| [Store backends](stores.md) | Capabilities, chaining, caching, failover groups |
| [S3 stores](stores-s3.md) | Bucket URLs, addressing styles, credentials |
| [OCI registry stores](stores-oci.md) | Chunks and indexes in a container registry |
| [Chunk encryption](encryption.md) | Encrypting a store at rest |
| [Configuration](configuration.md) | Config file, store options, dynamic reload |
| [CLI reference](cli/) | Every command and flag |
| [Cookbook](cookbook.md) | Worked examples for extraction, chunking, servers, archives |

The pages under [cli/](cli/) are generated from the command definitions by
`desync gendocs docs/cli` and checked by CI. Everything else here is written by
hand.
