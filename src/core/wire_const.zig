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

/// The most chunks one scheduler pop can push before the scheduler asks its
/// pause hooks again (see `tls.Pump.pushes_per_pop`). The drain sink flushes a
/// non-empty packed batch and then queues an unbatchable frame on its own: two
/// pushes for one pop.
pub const POP_PUSHES: usize = 2;

/// TLS write stash slots beyond `inbound_wire_chunks_per_connection`. They
/// hold the one push a drain makes after its last scheduler pop (see
/// `tls.Pump.tail_pushes_per_drain`), so the pause rule leaves the pops the
/// same room they had before the tail push was counted.
pub const TLS_STASH_TAIL_SLOTS: usize = 1;

/// TLS write stash (`tls.Pump.write_ch`) slots beyond
/// `inbound_wire_chunks_per_connection`: the tail slots, plus one so that a
/// one-chunk stash can still pop. A pop needs `tls.Pump.pushes_per_pop` +
/// `TLS_STASH_TAIL_SLOTS` free slots, which is 3 when there is one chunk.
pub const TLS_STASH_EXTRA_SLOTS: usize = 1 + TLS_STASH_TAIL_SLOTS;

/// One max HTTP/2 frame (16 KiB payload + 9-byte header), at least one TLS
/// record payload, plus a little writer-buffer headroom. The TLS pump
/// decrypts into one wire chunk, and tls.zig only decrypts into the
/// caller's buffer when it can take the whole record payload (up to 2^14 +
/// 256 bytes); a smaller buffer makes it keep the plaintext inside the
/// ciphertext buffer, which `Conn` reuses.
pub const WIRE_CHUNK_SIZE: usize = @max(16 * 1024 + 9, TLS_RECORD_BUFFER_SIZE - 5) + 64;
