//! Actor-owned nonblocking TLS adapter (starh2-nonblock-v1 ABI).
//!
//! Every function here is a pure byte transform: give it input and a scratch
//! buffer, and it reports what it consumed and what it produced. It owns no
//! socket and it never blocks. That is required, because the socket belongs to
//! the pumps and the cipher belongs to the actor. A TLS library that read and
//! wrote a socket itself would have to become a third owner of the transport.
//!
//! A patched fork of `tls.zig` is pinned for exactly that reason — the upstream
//! API drives the socket. The pin is a URL plus a content hash in
//! `build.zig.zon`, and the patch is kept readable at
//! `vendor/tls-zig-nonblock-v1.patch`. The `comptime` check below is the guard
//! that a wrong or unpatched fork breaks the BUILD rather than failing at the
//! first handshake.
//!
//! Concurrency, and this is the rule that has already cost a production crash:
//! ONE task at a time may drive one cipher. `Connection.session_mu` is what
//! enforces it, and t-538 is what happens without it — a read task and a
//! handler encrypted concurrently, one side's reset left the other's output
//! pointer undefined, and the process took a SIGSEGV.
const std = @import("std");
const tls = @import("tls");

comptime {
    if (tls.nonblock.starh2_nonblock_abi != 1) {
        @compileError("tls.zig missing starh2-nonblock-v1 ABI");
    }
}

pub const DriveStatus = tls.nonblock.DriveStatus;
pub const serverDrive = tls.nonblock.serverDrive;
pub const serverAlpnProtocol = tls.nonblock.serverAlpnProtocol;
pub const serverTakeCipher = tls.nonblock.serverTakeCipher;
pub const connectionDecrypt = tls.nonblock.connectionDecrypt;
pub const connectionEncrypt = tls.nonblock.connectionEncrypt;
pub const connectionClose = tls.nonblock.connectionClose;

pub const alpn_h2 = "h2";
pub const alpn_list = [_][]const u8{alpn_h2};
pub const starh2_nonblock_abi = tls.nonblock.starh2_nonblock_abi;

/// Limit each application-data decrypt drive to one TLS record. The patched
/// tls.zig decryptor can otherwise consume a small record and then fail when
/// the remaining plaintext scratch cannot hold a following maximum-size
/// record, after its cipher sequence has already advanced.
pub fn firstRecord(input: []const u8) []const u8 {
    const header_len = 5;
    if (input.len < header_len) return input;
    const payload_len = (@as(usize, input[3]) << 8) | input[4];
    const record_len = header_len + payload_len;
    return input[0..@min(input.len, record_len)];
}

pub fn requireH2(server: *const tls.nonblock.Server) bool {
    const proto = serverAlpnProtocol(server) orelse return false;
    return std.mem.eql(u8, proto, alpn_h2);
}
