# Cryptography

This chapter describes the at-rest cryptography used in MuroDB and the rationale behind each choice.

## Encryption Suites

Defined in `src/crypto/suite.rs`:

- `aes256-gcm-siv` (suite id `1`)
- `plaintext` (suite id `0`, explicit opt-in)

The selected suite id is stored in the main DB header.

## Key Derivation

Password-based opening uses Argon2id (`src/crypto/kdf.rs`):

- input: passphrase + 16-byte random salt
- output: 32-byte `MasterKey`
- salt is stored in DB header

`MasterKey` is zeroized on drop (`ZeroizeOnDrop`) in `src/crypto/aead.rs`.

## Page Encryption Format

For encrypted mode, each page payload is:

`nonce(12) || ciphertext || tag(16)`

implemented by `PageCrypto` (`src/crypto/aead.rs`).

AAD (additional authenticated data):

- `page_id` (u64 LE)
- `epoch` (u64 LE)

This binds ciphertext to logical page identity and epoch.

## WAL Encryption

WAL frames use the same `PageCipher` abstraction (`src/wal/writer.rs`):

- encryption nonce context uses `(lsn, 0)` as AEAD inputs
- payload before encryption is `record_bytes || crc32(record_bytes)`

## FTS Term Blinding

Full-text term ids are derived by HMAC-SHA256 (`src/crypto/hmac_util.rs`):

- `term_id = HMAC(term_key, token_bytes)`

Goal: avoid storing raw token bytes directly in FTS index structures.

## Selection Rationale

### Why AES-256-GCM-SIV?

Implementation comments explicitly call out nonce-misuse resistance.
For storage engines, this is useful because nonce management bugs are high impact; a misuse-resistant AEAD provides a safer failure envelope than nonce-sensitive modes.

### Why Argon2id for password KDF?

Argon2id is memory-hard and designed for password hashing/KDF use.
This raises offline brute-force cost when database files are stolen.

### Why AAD includes page_id/epoch?

Without AAD binding, page swapping/replay between locations can be harder to detect.
Binding to `(page_id, epoch)` ensures authentication fails if encrypted bytes are moved across logical page identities.

### Why keep plaintext mode?

Operational flexibility:

- test/development setups
- environments where external encryption layers are already enforced

It is explicit opt-in so encrypted mode remains the default posture.

## Key Rotation (Rekey)

`ALTER DATABASE REKEY WITH PASSWORD 'new_password'` re-encrypts all database pages with a new key derived from the new password.

### Flow

1. Reject if inside an active transaction or if the database is plaintext.
2. Checkpoint WAL to flush all pending writes to the data file.
3. Generate a new 16-byte random salt and derive a new `MasterKey` via Argon2id.
4. Write a `.rekey` marker file (`<db_path>.rekey`) containing: magic (`REKY`), new salt, new epoch, and CRC32 checksum.
5. For each page in the data file:
   - Decrypt with the current key and epoch.
   - Re-encrypt with the new key and incremented epoch.
   - Write back to disk.
6. `fsync` all page data.
7. Update the file header with the new salt and epoch.
8. `fsync` the header.
9. Delete the `.rekey` marker file.
10. Recreate the WAL writer with the new key.

### Crash Recovery

On `Database::open_with_password`, before normal open, the system checks for a `.rekey` marker file:

- **Marker exists, header salt matches marker salt**: Rekey completed successfully but marker was not deleted. The marker is simply removed.
- **Marker exists, header salt does not match**: Rekey was interrupted mid-operation. The system derives both old and new keys, identifies which pages were already re-encrypted (by attempting decryption with the new key first), re-encrypts remaining pages, updates the header, and deletes the marker.

The marker file format is 36 bytes: `magic(4) || new_salt(16) || new_epoch(8) || reserved(4) || crc32(4)`.

## Non-goals

- Traffic encryption (no network protocol layer here)
- Access-pattern hiding (no ORAM)
- HSM/KMS integration abstraction (today key material is process-local)
