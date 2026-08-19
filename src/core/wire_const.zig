//! Shared wire chunk constant — imported by limits and wire_pump without cycles.
/// Drain-turn packing cap. Matches `emit_batch.max_plaintext` and the TLS 1.3
/// application plaintext limit; HTTP/2 never sees ciphertext.
pub const TLS_PLAINTEXT_SCRATCH_SIZE: usize = 16 * 1024;
/// One max TLS record. Asserted in `edge/tls.zig` against the BioPair
/// capacity so a boring bump cannot silently undersize the bound.
pub const TLS_STREAM_BUFFER_SIZE: usize = 16 * 1024 + 5;
/// Ciphertext chunks posted by the TLS read task. One max record each.
pub const TLS_CIPHER_CHUNK_SIZE: usize = TLS_STREAM_BUFFER_SIZE;
/// tcp in + tcp out + BioPair (ssl write buf + transport write buf).
pub const TLS_CONN_BUFFER_BYTES: usize = TLS_STREAM_BUFFER_SIZE * 4;

/// One max HTTP/2 frame (16 KiB payload + 9-byte header), at least a TLS
/// stream buffer, plus a little writer-buffer headroom.
pub const WIRE_CHUNK_SIZE: usize = @max(16 * 1024 + 9, TLS_STREAM_BUFFER_SIZE) + 64;
