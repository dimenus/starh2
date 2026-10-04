//! Shared wire chunk constant — imported by limits and wire_pump without cycles.
/// Drain-turn packing cap. Matches `emit_batch.max_plaintext` and the TLS 1.3
/// application plaintext limit; HTTP/2 never sees ciphertext.
pub const TLS_PLAINTEXT_SCRATCH_SIZE: usize = 16 * 1024;
/// A 16 KiB TLS plaintext record plus its 5-byte header. Sizes the TLS read
/// buffer and the wire chunk; not a ciphertext bound (see
/// `TLS_RECORD_BUFFER_SIZE`).
pub const TLS_STREAM_BUFFER_SIZE: usize = 16 * 1024 + 5;
/// Ciphertext chunks posted by the TLS read task. One max record each.
pub const TLS_CIPHER_CHUNK_SIZE: usize = TLS_STREAM_BUFFER_SIZE;
/// One maximum TLS 1.3 record on the wire: 5-byte header, 2^14 plaintext
/// bytes, and up to 256 bytes of expansion (RFC 8446 5.2). Asserted in
/// `edge/tls.zig` against tls.zig's own input buffer length.
pub const TLS_RECORD_BUFFER_SIZE: usize = 5 + 16 * 1024 + 256;
/// `Conn.in_buf` + `Conn.out_buf`, one maximum record each.
pub const TLS_CONN_BUFFER_BYTES: usize = TLS_RECORD_BUFFER_SIZE * 2;

/// One max HTTP/2 frame (16 KiB payload + 9-byte header), at least one TLS
/// record payload, plus a little writer-buffer headroom. The TLS pump
/// decrypts into one wire chunk, and tls.zig only decrypts into the
/// caller's buffer when it can take the whole record payload (up to 2^14 +
/// 256 bytes); a smaller buffer makes it keep the plaintext inside the
/// ciphertext buffer, which `Conn` reuses.
pub const WIRE_CHUNK_SIZE: usize = @max(16 * 1024 + 9, TLS_RECORD_BUFFER_SIZE - 5) + 64;
