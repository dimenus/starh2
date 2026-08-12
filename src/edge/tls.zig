//! Actor-owned nonblocking TLS adapter (starh2-nonblock-v1 ABI).
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
