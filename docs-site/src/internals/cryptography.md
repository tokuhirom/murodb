# Cryptography

MuroDB encrypts all data at rest. This chapter explains the full encryption pipeline — from a user's password to bytes on disk — and the rationale behind each design choice.

## Overview

```
User's passphrase
    │
    ↓  Argon2id (memory-hard KDF) + salt
256-bit MasterKey (zeroized on drop)
    │
    ├──→ Page encryption  (AES-256-GCM-SIV, AAD = page_id || epoch)
    └──→ WAL encryption   (AES-256-GCM-SIV, AAD = lsn || 0)

FTS term blinding (HMAC-SHA256, compile-time constant key)
```

The encryption system has four layers:

1. **Key derivation (KDF)** — Derive a cryptographic key from the user's passphrase
2. **Page encryption** — Encrypt and authenticate each data page
3. **WAL encryption** — Encrypt write-ahead log records
4. **FTS term blinding** — Hide full-text search tokens on disk (separate from passphrase-based encryption; see below)

## Encryption Suites

Defined in `src/crypto/suite.rs`:

| Suite ID | Name | Purpose |
|---|---|---|
| 1 | `aes256-gcm-siv` | **Default.** Production use |
| 0 | `plaintext` | Testing/development. Explicit opt-in |

The suite ID is stored in plaintext in the DB header (bytes 68..72), allowing MuroDB to determine the encryption mode before the user provides a passphrase.

## Key Derivation (KDF)

`src/crypto/kdf.rs`

### Flow

```
passphrase (arbitrary-length bytes) + salt (16-byte random)
    │
    ↓  Argon2id
256-bit MasterKey
```

- **Argon2id**: A memory-hard KDF. If a database file is stolen, the attacker must spend significant memory and CPU per password guess, making offline brute-force expensive.
- **Salt**: Generated randomly at database creation, stored in plaintext in the DB header (bytes 12..28). The salt must be readable before the passphrase is provided.
- **MasterKey**: Implements `ZeroizeOnDrop` — key material is securely erased from memory when dropped.

### Why Argon2id?

Argon2id is the NIST-recommended password hashing function. Its memory-hard design provides strong resistance against GPU/ASIC-accelerated brute-force attacks, outperforming bcrypt and PBKDF2 in this regard.

## Page Encryption

`src/crypto/aead.rs`

### On-Disk Format

Each 4096-byte page is individually encrypted and stored as:

```
nonce (12B) || ciphertext (4096B) || auth tag (16B)
──────────────────────────────────────────────────
                  total: 4124B
```

- **Nonce**: 12 bytes, randomly generated for each encryption operation
- **Authentication tag**: 16 bytes, detects tampering of both ciphertext and AAD

### Why AES-256-GCM-SIV?

MuroDB uses **AES-256-GCM-SIV** rather than standard AES-GCM.

With standard AES-GCM, reusing a nonce completely breaks authentication. AES-GCM-SIV, on the other hand, degrades gracefully under nonce reuse — it only leaks whether two plaintexts are identical, without compromising authentication. For a storage engine where nonce management bugs can have catastrophic consequences, this **nonce-misuse resistance** provides a significantly safer failure envelope.

### AAD (Additional Authenticated Data)

```rust
fn build_aad(page_id: PageId, epoch: u64) -> [u8; 16] {
    aad[0..8]  = page_id.to_le_bytes()   // 8 bytes
    aad[8..16] = epoch.to_le_bytes()      // 8 bytes
}
```

AAD is data that is **not encrypted** but is **included in the authentication tag computation**. If the AAD provided during decryption does not match what was used during encryption, authentication fails and decryption is rejected.

By including `page_id` and `epoch` in the AAD, MuroDB detects the following attacks:

| Attack | Detected by |
|---|---|
| Page swapping (inserting page 5's data at page 10's location) | `page_id` mismatch |
| Downgrade (injecting a page from an older backup) | `epoch` mismatch |

Even if the ciphertext itself is untouched, decryption is rejected when the **context** (which page, which generation) does not match.

## WAL Encryption

`src/wal/writer.rs`

WAL (Write-Ahead Log) frames are encrypted using the same `PageCipher`.

### Frame Format

```
[frame_len: u32] [encrypted payload]
```

The payload before encryption is:

```
record_bytes || CRC32(record_bytes)
```

### AAD Differences from Page Encryption

For WAL frames, the AEAD parameters differ:

- `page_id` parameter → **LSN** (Log Sequence Number)
- `epoch` parameter → always **0**

Since LSN increases monotonically within a WAL session, this prevents swapping ciphertext between different WAL frames.

Note: After a checkpoint, the WAL is truncated and LSN resets to 0. This means a new WAL session may reuse the same LSN values as a previous session. This is not a vulnerability because each encryption uses a fresh random nonce, and AES-GCM-SIV's nonce-misuse resistance provides an additional safety margin.

## FTS Term Blinding

`src/crypto/hmac_util.rs`

Full-text search (FTS) stores bigram tokens in a B-tree index. Storing tokens as plaintext would directly expose search terms on disk. MuroDB blinds tokens with HMAC-SHA256:

```
term_id = HMAC-SHA256(term_key, "tokyo")  →  32-byte hash
```

- **Deterministic**: The same token always produces the same `term_id`, enabling search
- **One-way**: Recovering the original token from a `term_id` is computationally infeasible

Only hash values are stored on disk — no plaintext search terms ever reach the storage layer.

**Important caveat**: The current `term_key` is a compile-time constant (`[0x55u8; 32]`) embedded in the binary, **not** derived from the user's passphrase or `MasterKey`. This means an attacker with access to MuroDB's source code (or binary) can compute term IDs for candidate tokens. The blinding prevents casual inspection of on-disk data but does not provide resistance against a determined attacker who knows the key. Deriving `term_key` from `MasterKey` is a potential future improvement.

## Key Rotation (Epoch)

When the user changes their passphrase, MuroDB re-encrypts all pages with the new key. The **epoch** — a `u64` counter stored in the DB header — coordinates this process.

### Rotation Flow

```
1. Derive a new MasterKey from the new passphrase
2. Increment epoch (e.g., 2 → 3)
3. Re-encrypt every page with the new key and new epoch
4. Update salt and epoch in the DB header
```

### Why Full Re-encryption Instead of KEK?

Large-scale databases often use a KEK (Key Encryption Key) architecture: a master key encrypts per-page Data Encryption Keys (DEKs), and rotation only re-wraps the DEKs without touching the data. This avoids the cost of re-encrypting all data.

MuroDB is an embedded database. Typical data sizes range from a few MB to a few hundred MB. Full re-encryption completes in seconds, so the complexity of KEK — an extra DEK management layer, more complex crash recovery, increased attack surface — is not justified.

Full re-encryption also provides a security advantage that KEK lacks: **all data is actually protected by the new key**. If the old key is compromised, no data encrypted under it remains on disk.

### Epoch-Based Attack Detection

Because epoch is included in the AAD, an attacker who extracts a page from an epoch=2 backup and inserts it into an epoch=3 database will trigger an authentication tag verification failure. The page is rejected without revealing any data.

## In-Memory Key Protection

- `MasterKey` implements `ZeroizeOnDrop`: memory is zeroed on drop
- Key material is process-local (no external KMS/HSM dependency)

## Non-Goals

The following are explicitly out of scope for MuroDB's encryption design:

- **Traffic encryption**: MuroDB is an embedded database with no network protocol
- **Access-pattern hiding**: No ORAM or similar oblivious access mechanisms
- **HSM/KMS integration**: Key material is process-local
