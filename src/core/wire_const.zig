//! Shared wire chunk constant — imported by limits and wire_pump without cycles.
/// TLS 1.3 decrypts the inner content-type byte alongside up to 16 KiB
/// application plaintext.
pub const TLS_PLAINTEXT_SCRATCH_SIZE: usize = 16 * 1024 + 1;

pub const WIRE_CHUNK_SIZE: usize = blk: {
    const tls_in = 16_645;
    const tls_ct = 16_469;
    const h2 = 16_384 + 9;
    break :blk @max(tls_in, @max(tls_ct, @max(TLS_PLAINTEXT_SCRATCH_SIZE, h2)));
};
