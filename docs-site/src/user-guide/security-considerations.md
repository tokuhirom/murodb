# Security Considerations

This page summarizes the current security model, known risks, and operational guidance.

## Threat Model (Current)

- MuroDB is an embedded database library/CLI, not a network server.
- Encrypted mode (`aes256-gcm-siv`) targets at-rest confidentiality and tamper detection for DB/WAL pages.
- Plaintext mode (`--encryption off`) is explicit opt-in and provides no cryptographic protection.

## Known Risks

| Risk | Impact | Status |
|---|---|---|
| Malformed page/cell metadata can currently trigger panic paths instead of clean corruption errors | Process abort (availability) when opening/querying corrupted files, especially relevant in plaintext mode | Tracked: [#182](https://github.com/tokuhirom/murodb/issues/182) |
| Plaintext mode has no confidentiality/integrity guarantees | Data can be read/modified offline without cryptographic checks | By design |
| No built-in user authentication/authorization layer | Access control depends on host process + filesystem permissions | By design |

## Operational Guidance

- Prefer encrypted mode for production data.
- Avoid passing secrets via CLI args (`--password`) when possible; use interactive prompt.
- Treat database files as trusted inputs only until #182 is addressed.
- Apply OS-level controls: file permissions, disk encryption, process isolation, and secrets management.
