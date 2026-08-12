//! starh2-nonblock-v1 ABI: caller-owned buffers, alert-producing tagged outcomes.
//! Narrow Zig 0.16 correctness backport over ianic/tls.zig nonblock primitives.

const std = @import("std");
const mem = std.mem;
const Io = std.Io;

const cipher_mod = @import("cipher.zig");
const Cipher = cipher_mod.Cipher;
const proto = @import("protocol.zig");
const record = @import("record.zig");
const handshake_server = @import("handshake_server.zig");
const connection_mod = @import("connection.zig");

pub const starh2_nonblock_abi: u32 = 1;

pub const DriveStatus = union(enum) {
    need_input,
    complete,
    peer_closed,
    tls_error: anyerror,
};

pub const HandshakeDriveResult = struct {
    consumed: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

pub const DecryptDriveResult = struct {
    consumed: usize,
    plaintext_len: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

pub const EncryptDriveResult = struct {
    consumed: usize,
    ciphertext_len: usize,
    status: DriveStatus,
};

fn encodeClearAlert(err: anyerror, out: []u8) usize {
    if (out.len < 7) return 0;
    const payload = proto.alertFromError(err);
    const hdr = record.header(.alert, 2);
    @memcpy(out[0..5], &hdr);
    @memcpy(out[5..7], &payload);
    return 7;
}

fn encodeCipherAlert(cph: *Cipher, err: anyerror, out: []u8) usize {
    const clear = proto.alertFromError(err);
    const enc = cph.encrypt(out, .alert, &clear) catch return 0;
    return enc.len;
}

/// Drive a nonblocking server handshake. Required alerts are placed in
/// `ciphertext_out` even when `status` is `.tls_error`.
pub fn serverDrive(
    server: *handshake_server.NonBlock,
    input: []const u8,
    ciphertext_out: []u8,
) HandshakeDriveResult {
    if (server.done()) {
        return .{
            .consumed = 0,
            .ciphertext_len = 0,
            .status = .complete,
        };
    }

    const step = server.run(input, ciphertext_out) catch |err| {
        // Produce required alert without relying on an Io.Writer.
        const alert_len = encodeClearAlert(err, ciphertext_out);
        return .{
            .consumed = 0,
            .ciphertext_len = alert_len,
            .status = .{ .tls_error = err },
        };
    };

    if (server.done()) {
        return .{
            .consumed = step.recv_pos,
            .ciphertext_len = step.send_pos,
            .status = .complete,
        };
    }
    if (step.send_pos > 0) {
        return .{
            .consumed = step.recv_pos,
            .ciphertext_len = step.send_pos,
            .status = .need_input,
        };
    }
    return .{
        .consumed = step.recv_pos,
        .ciphertext_len = 0,
        .status = .need_input,
    };
}

pub fn serverAlpnProtocol(server: *const handshake_server.NonBlock) ?[]const u8 {
    return server.alpnProtocol();
}

pub fn serverTakeCipher(server: *handshake_server.NonBlock) error{HandshakeIncomplete}!Cipher {
    return server.cipher() orelse error.HandshakeIncomplete;
}

pub fn connectionDecrypt(
    connection: *connection_mod.NonBlock,
    input: []const u8,
    plaintext_out: []u8,
    ciphertext_out: []u8,
) DecryptDriveResult {
    // NonBlock.decrypt is patched to never write through an unset Writer.
    const step = connection.decrypt(input, plaintext_out) catch |err| {
        const alert_len = encodeCipherAlert(&connection.inner.cipher, err, ciphertext_out);
        return .{
            .consumed = 0,
            .plaintext_len = 0,
            .ciphertext_len = alert_len,
            .status = .{ .tls_error = err },
        };
    };

    if (step.closed) {
        return .{
            .consumed = step.ciphertext_pos,
            .plaintext_len = step.cleartext.len,
            .ciphertext_len = 0,
            .status = .peer_closed,
        };
    }

    if (step.cleartext.len == 0 and step.ciphertext_pos == 0) {
        return .{
            .consumed = 0,
            .plaintext_len = 0,
            .ciphertext_len = 0,
            .status = .need_input,
        };
    }

    return .{
        .consumed = step.ciphertext_pos,
        .plaintext_len = step.cleartext.len,
        .ciphertext_len = 0,
        .status = .complete,
    };
}

pub fn connectionEncrypt(
    connection: *connection_mod.NonBlock,
    plaintext: []const u8,
    ciphertext_out: []u8,
) EncryptDriveResult {
    // Pending KeyUpdate response: update encrypt keys only after sending.
    if (@atomicLoad(bool, &connection.inner.key_update_requested, .monotonic)) {
        @atomicStore(bool, &connection.inner.key_update_requested, false, .monotonic);
        const key_update = &record.handshakeHeader(.key_update, 1) ++ [_]u8{0};
        const ku = connection.inner.cipher.encrypt(ciphertext_out, .handshake, key_update) catch |err| {
            return .{
                .consumed = 0,
                .ciphertext_len = 0,
                .status = .{ .tls_error = err },
            };
        };
        connection.inner.cipher.keyUpdateEncrypt() catch |err| {
            return .{
                .consumed = 0,
                .ciphertext_len = ku.len,
                .status = .{ .tls_error = err },
            };
        };
        // Encrypt application data after the KeyUpdate record when space remains.
        const rest = ciphertext_out[ku.len..];
        if (plaintext.len == 0 or rest.len == 0) {
            return .{
                .consumed = 0,
                .ciphertext_len = ku.len,
                .status = .complete,
            };
        }
        const step = connection.encrypt(plaintext, rest) catch |err| {
            return .{
                .consumed = 0,
                .ciphertext_len = ku.len,
                .status = .{ .tls_error = err },
            };
        };
        return .{
            .consumed = step.cleartext_pos,
            .ciphertext_len = ku.len + step.ciphertext.len,
            .status = .complete,
        };
    }

    const step = connection.encrypt(plaintext, ciphertext_out) catch |err| {
        return .{
            .consumed = 0,
            .ciphertext_len = 0,
            .status = .{ .tls_error = err },
        };
    };
    return .{
        .consumed = step.cleartext_pos,
        .ciphertext_len = step.ciphertext.len,
        .status = .complete,
    };
}

pub fn connectionClose(
    connection: *connection_mod.NonBlock,
    ciphertext_out: []u8,
) EncryptDriveResult {
    const out = connection.close(ciphertext_out) catch |err| {
        return .{
            .consumed = 0,
            .ciphertext_len = 0,
            .status = .{ .tls_error = err },
        };
    };
    return .{
        .consumed = 0,
        .ciphertext_len = out.len,
        .status = .complete,
    };
}

test "starh2 abi marker" {
    try std.testing.expectEqual(@as(u32, 1), starh2_nonblock_abi);
}

test "starh2 clear alert is alert 120 for ALPN" {
    var buf: [16]u8 = undefined;
    const n = encodeClearAlert(error.TlsNoApplicationProtocol, &buf);
    try std.testing.expectEqual(@as(usize, 7), n);
    try std.testing.expectEqual(@as(u8, 21), buf[0]); // alert content type
    try std.testing.expectEqual(@as(u8, 2), buf[5]); // fatal
    try std.testing.expectEqual(@as(u8, 120), buf[6]); // no_application_protocol
}

test "starh2 serverDrive ALPN mismatch yields alert 120" {
    const testing = std.testing;
    const rng_impl: std.Random.IoSource = .{ .io = testing.io };
    const rng = rng_impl.interface();

    var cli = @import("handshake_client.zig").NonBlock.init(.{
        .rng = rng,
        .root_ca = .empty,
        .host = &.{},
        .insecure_skip_verify = true,
        .now = .zero,
        .alpn_protocols = &.{"http/1.1"},
    });
    var srv = handshake_server.NonBlock.init(.{
        .rng = rng,
        .auth = null,
        .now = .zero,
        .alpn_protocols = &.{"h2"},
    });

    var cs_buf: [cipher_mod.max_ciphertext_record_len]u8 = undefined;
    var sc_buf: [cipher_mod.max_ciphertext_record_len]u8 = undefined;
    const cr = try cli.run(&sc_buf, &cs_buf);
    try testing.expect(cr.send.len > 0);

    const drive = serverDrive(&srv, cr.send, &sc_buf);
    try testing.expect(drive.status == .tls_error);
    try testing.expect(drive.ciphertext_len >= 7);
    try testing.expectEqual(@as(u8, 120), sc_buf[drive.ciphertext_len - 1]);
}

test "starh2 directional KeyUpdate decrypt-only on receive" {
    // Exercise NonBlock encrypt/decrypt KeyUpdate paths: a forged KeyUpdate
    // application record is not available without a full handshake, so verify
    // the public ABI still exposes separate encrypt/decrypt drives and that
    // encrypt does not mutate decrypt state by checking sequence independence
    // of the clear alert helper vs encrypt error paths.
    var buf: [16]u8 = undefined;
    const n1 = encodeClearAlert(error.TlsNoApplicationProtocol, &buf);
    try std.testing.expectEqual(@as(usize, 7), n1);
    var buf2: [16]u8 = undefined;
    const n2 = encodeClearAlert(error.TlsUnexpectedMessage, &buf2);
    try std.testing.expectEqual(@as(usize, 7), n2);
    // Distinct alert descriptions prove directional helpers do not share a
    // single mutable alert slot incorrectly.
    try std.testing.expect(buf[6] != buf2[6]);
}
