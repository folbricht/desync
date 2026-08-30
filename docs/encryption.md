# Chunk Encryption

Chunks can be encrypted with a symmetric algorithm on a per-store basis. To use encryption, it has to be enabled in the [configuration](configuration.md) file by setting `encryption` to `true` and providing a 256-bit encryption key. The key is expected hex-encoded and should be randomly generated, for example with:

```text
openssl rand -hex 32
```

A single instance of desync can use multiple stores at the same time, each with a different (or the same) encryption algorithm and key. Encrypted chunks are stored with file extensions containing the algorithm and a key identifier (the leading 4 bytes of the SHA256 hash of the key). If the key for a store is changed, all existing chunks in it will become "invisible" since the extension no longer matches. To change the key, chunks have to be re-encrypted with the new key, ideally into a new store. Create a new store, then either re-chunk the data, or use `desync cache -c <new-store> -s <old-store> <index>` to decrypt the chunks from the old store and re-encrypt them with the new key in the new store.

Encryption nonces are generated randomly per chunk which can weaken encryption in some modes when used on very large chunk stores, see notes below.

| ID | Algorithm | Key | Nonce/IV | Notes |
|:---:|:---:|:---:|:---:|:---:|
| `xchacha20-poly1305` | XChaCha20-Poly1305 (AEAD) | 256bit | 192bit | Default |
| `aes-256-gcm` | AES 256bit Galois Counter Mode (AEAD) | 256bit | 96bit | Don't use for large chunk stores (>2<sup>32</sup> chunks) |

Chunk extensions in stores are chosen based on compression or encryption settings as follows:

| Compressed | Encrypted | Extension | Example |
|:---:|:---:|:---:|:---:|
| no | no | n/a | `fbef/fbef1a00ced..9280ce78` |
| yes | no | `.cacnk` | `fbef/fbef1a00ced..9280ce78.cacnk` |
| no | yes | `.<algorithm>-<keyID>` | `fbef/fbef1a00ced..9280ce78.aes-256-gcm-635af003` |
| yes | yes | `.cacnk.<algorithm>-<keyID>` | `fbef/fbef1a00ced..9280ce78.cacnk.aes-256-gcm-635af003` |

Note that encryption only protects the chunk data itself. Chunk file names, which are the content hashes of the plain data, remain visible to anyone with access to the store. An observer that already knows the plain content of a chunk can therefore confirm its presence in the store.

Encryption applies to chunk stores only. Index files are always stored in plain form, and index stores reject encryption options rather than silently ignoring them. If a config entry enabling encryption matches a location that is also used to store indexes, index operations will fail with an error — keep indexes in a separate location, or use a more specific config entry without encryption for them. Keep in mind that a plain index reveals metadata about the encrypted content: the IDs, sizes and offsets of all its chunks.

Encryption provides confidentiality, while integrity comes from desync's regular chunk validation: every chunk is identified by the hash of its plain content, which is verified when chunks are read (unless `skip-verify` is set). The AEAD ciphertext is authenticated under the key, but it is not bound to the chunk's name — someone with write access to the store could swap two encrypted chunk files and decryption alone would not detect it. Such a swap is caught by the content validation of the final consumer, e.g. during `extract`, which is enabled by default. Only disable verification (`skip-verify`, `--skip-verify-read`) for intermediate proxies or caches where a downstream reader still validates the chunks.
