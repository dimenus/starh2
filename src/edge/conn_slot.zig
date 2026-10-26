//! Connection slots: the memory a connection boots from, kept and reused.
//!
//! A `ConnSlot` is a heap object that never moves, holding one `Connection`
//! and one boot buffer. Everything `Connection.init` allocates at boot, and for
//! TLS the handshake `Conn` before it, comes out of that buffer through a
//! `std.heap.FixedBufferAllocator`. When the connection ends the slot goes
//! back on the server's free list and the allocator is rewound, so the next
//! connection boots from the same bytes and makes no call to the general
//! allocator for them. What a request allocates (the request arena, bodies,
//! HPACK, frame and write-chunk fallbacks, response headers) still comes from
//! the server's allocator.
//!
//! # Lifecycle
//!
//! 1. `SlotPool.take` in the accept loop, after admission, before the spawn.
//! 2. TLS only: `createTlsConn` takes the `Conn` first, at offset 0.
//! 3. `serveAccepted` calls `Connection.init` into `slot.conn` (state
//!    `booted`), runs it, and `quiesce`s it: the connection gives back
//!    everything it borrowed from the server (streams, slabs, reaper tokens,
//!    the socket) but frees none of its own boot memory.
//! 4. `connEntry` calls `recycle`, which checks the slot is clean, rewinds the
//!    buffer, and `SlotPool.put` pushes it. Only then does it release the
//!    connection's admission. The order is load-bearing: releasing admission
//!    first lets the accept loop see room, find the free list empty, and create
//!    a second slot for a connection that could have reused this one.
//!
//! # Reuse is a rebuild, not a patch
//!
//! A reused slot runs the whole of `Connection.init` again over rewound
//! memory. Every one-shot field (`dead`, `wire_started`, the closed channels,
//! the read free list, the preface queue) is therefore set by the same code
//! that sets it for a first connection; there is no second list of "what to
//! re-arm" to forget an entry from. What the rebuild does not do is clear the
//! buffer: a fresh slot's bytes are undefined and a reused slot's bytes are the
//! last connection's, so no boot code may rely on zeroed memory. Debug builds
//! poison the buffer on recycle so that a read of stale boot memory fails
//! loudly in tests.
//!
//! # A dirty slot is leaked, not reused
//!
//! `recycle` checks, in safe builds, that the ended connection left nothing
//! behind that a stranger could still touch (`Connection.dirtiness`). A slot
//! that fails is not rewound and not pushed: it is counted in
//! `SlotPool.leaked` and reported, because something may still point into it
//! and reusing it would hand that something a new connection's memory. The
//! slot stays out of use for the life of the server and is freed by
//! `SlotPool.deinit`, when no connection can be running.
//!
//! Do not read this as a promise that real leaks end up here. `quiesce`
//! panics or asserts on most of what the check looks for, before `recycle`
//! runs, so for those the check is a second net. Four conditions (request
//! bytes, admitted streams, a held handler join, a queued completion) are not
//! checked by `quiesce`; only this sees them, and no known path sets them. The
//! list, with which is which, is on `Connection.dirtiness`.
const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const boot_plan = @import("../core/boot_plan.zig");
const connection = @import("connection.zig");
const tls_edge = @import("tls.zig");

pub const ConnSlot = struct {
    buffer: []align(boot_plan.slot_align) u8,
    fba: std.heap.FixedBufferAllocator,
    /// Valid when `state == .booted`. The slot never moves, so interior
    /// pointers `Connection.finishBoot` takes stay good across reuse.
    conn: connection.Connection,
    state: State,

    pub const State = enum {
        /// Nothing allocated from the buffer.
        empty,
        /// The TLS `Conn` is allocated (and is the buffer's first allocation);
        /// no `Connection` has booted. It is also where a TLS connection that
        /// ALPN sends to HTTP/1.1 stays.
        tls_conn,
        /// `conn` booted and ran.
        booted,
    };

    pub const Recycle = union(enum) {
        clean,
        /// The slot is dirty: the reason, and the slot is untouched.
        dirty: []const u8,
    };

    pub fn create(gpa: std.mem.Allocator, boot_bytes: usize) error{OutOfMemory}!*ConnSlot {
        const self = try gpa.create(ConnSlot);
        errdefer gpa.destroy(self);
        const buffer = try gpa.alignedAlloc(u8, .fromByteUnits(boot_plan.slot_align), boot_bytes);
        self.* = .{
            .buffer = buffer,
            .fba = .init(buffer),
            .conn = undefined,
            .state = .empty,
        };
        return self;
    }

    pub fn destroy(self: *ConnSlot, gpa: std.mem.Allocator) void {
        gpa.free(self.buffer);
        gpa.destroy(self);
    }

    pub fn bootAllocator(self: *ConnSlot) std.mem.Allocator {
        return self.fba.allocator();
    }

    /// The handshake `Conn`. It must be the buffer's first allocation, because
    /// `Limits.slotBootBytes` plans it first.
    pub fn createTlsConn(self: *ConnSlot) error{OutOfMemory}!*tls_edge.Conn {
        std.debug.assert(self.state == .empty);
        std.debug.assert(self.fba.end_index == 0);
        const conn = try self.bootAllocator().create(tls_edge.Conn);
        self.state = .tls_conn;
        return conn;
    }

    /// End of a connection's use of the slot. Returns `.clean` after rewinding
    /// the buffer, ready for `SlotPool.put`; `.dirty` leaves the slot as it is
    /// and the caller must not reuse it.
    pub fn recycle(self: *ConnSlot) Recycle {
        if (std.debug.runtime_safety and self.state == .booted) {
            if (self.conn.dirtiness()) |why| return .{ .dirty = why };
        }
        if (builtin.mode == .debug) @memset(self.buffer, 0xdd);
        self.fba.reset();
        self.state = .empty;
        return .clean;
    }
};

/// The server's free list of slots: a LIFO stack, so the slot reused is the
/// most recently used and its pages are the warmest. Slots are created when
/// the stack is empty, never ahead of need, so resident memory follows the
/// peak number of concurrent connections and not `max_connections`.
pub const SlotPool = struct {
    gpa: std.mem.Allocator,
    mu: zio.Mutex = .init,
    /// Capacity `max_connections`, and the most slots that can exist: `take`
    /// refuses to make one past it. A slot is pushed before its admission is
    /// released, so the stack cannot be asked for more than it can hold.
    stack: []*ConnSlot,
    len: usize = 0,
    /// Slots in existence: made and not yet freed. A quarantined slot is still
    /// counted, because it holds memory and no longer holds admission. `take`
    /// never lets this pass `stack.len` (`max_connections`), so the slots the
    /// bound counts are all there can be.
    created: usize = 0,
    /// Connections refused because every slot in existence was in use or
    /// quarantined and the cap was reached.
    refused: usize = 0,
    /// Slots that failed the clean check and were kept out of reuse.
    leaked: usize = 0,
    /// The leaked slots, so `deinit` can free them. Sized once at `init` to
    /// `capacity` and never grown. At most `capacity` slots exist, so it cannot
    /// fill past it, and keeping a dirty slot cannot fail.
    quarantine: std.ArrayList(*ConnSlot),
    boot_bytes: usize,

    pub fn init(gpa: std.mem.Allocator, capacity: usize, boot_bytes: usize) error{OutOfMemory}!SlotPool {
        const stack = try gpa.alloc(*ConnSlot, capacity);
        errdefer gpa.free(stack);
        var quarantine: std.ArrayList(*ConnSlot) = .empty;
        try quarantine.ensureTotalCapacityPrecise(gpa, capacity);
        return .{
            .gpa = gpa,
            .stack = stack,
            .quarantine = quarantine,
            .boot_bytes = boot_bytes,
        };
    }

    /// The counters, read under the lock. The fields are written by connection
    /// tasks on other threads, so a test or an operator reads them here and
    /// not directly.
    pub const Snapshot = struct {
        /// Slots on the free list.
        len: usize,
        /// Slots made and not yet freed.
        created: usize,
        /// Slots held back by the clean check.
        leaked: usize,
        /// Connections `take` refused at the cap.
        refused: usize,
    };

    pub fn snapshot(self: *SlotPool) Snapshot {
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        return .{ .len = self.len, .created = self.created, .leaked = self.leaked, .refused = self.refused };
    }

    /// What `recycle` did with a slot.
    pub const Returned = enum {
        /// Rewound and pushed: the next connection can take it.
        free,
        /// Failed the clean check: kept out of reuse until `deinit`.
        quarantined,
    };

    /// Every slot must be back, or leaked on purpose, before the pool goes.
    /// A slot still in use here is a connection that outlived the server.
    /// Each slot is freed exactly once: the free list and the quarantine hold
    /// disjoint slots, and the count check below would fail if one were in
    /// neither.
    pub fn deinit(self: *SlotPool) void {
        std.debug.assert(self.len + self.leaked == self.created);
        std.debug.assert(self.quarantine.items.len == self.leaked);
        for (self.stack[0..self.len]) |slot| slot.destroy(self.gpa);
        for (self.quarantine.items) |slot| slot.destroy(self.gpa);
        self.quarantine.deinit(self.gpa);
        self.gpa.free(self.stack);
        self.* = undefined;
    }

    /// The most recently returned slot, or a new one. `error.SlotsExhausted`
    /// when the free list is empty and `max_connections` slots already exist:
    /// without it, each slot the clean check holds back would let admission
    /// make another beside it, and the server could hold twice the slots
    /// `resourceUpperBound` counts. The accept loop closes the connection.
    pub fn take(self: *SlotPool) error{ OutOfMemory, SlotsExhausted }!*ConnSlot {
        self.mu.lockUncancelable();
        if (self.len > 0) {
            self.len -= 1;
            const slot = self.stack[self.len];
            self.mu.unlock();
            return slot;
        }
        if (self.created >= self.stack.len) {
            self.refused += 1;
            self.mu.unlock();
            return error.SlotsExhausted;
        }
        // Counted before the slot exists so two takers cannot both pass a
        // limit; undone if the allocation fails.
        self.created += 1;
        self.mu.unlock();
        return ConnSlot.create(self.gpa, self.boot_bytes) catch |err| {
            self.mu.lockUncancelable();
            self.created -= 1;
            self.mu.unlock();
            return err;
        };
    }

    /// Return a slot `recycle` reported clean.
    pub fn put(self: *SlotPool, slot: *ConnSlot) void {
        std.debug.assert(slot.state == .empty);
        self.mu.lockUncancelable();
        defer self.mu.unlock();
        // Not an assert: past capacity the write below would be out of bounds
        // in a build without safety checks. Reaching it means a slot was made
        // for a connection that could have reused one, i.e. a slot pushed after
        // its admission was released.
        if (self.len >= self.stack.len) @panic("connection slot free list over capacity");
        self.stack[self.len] = slot;
        self.len += 1;
    }

    /// Give a slot back whatever its state: rewind and push it, or keep it out
    /// of reuse and say why.
    pub fn recycle(self: *SlotPool, slot: *ConnSlot) Returned {
        switch (slot.recycle()) {
            .clean => {
                self.put(slot);
                return .free;
            },
            .dirty => |why| {
                self.mu.lockUncancelable();
                defer self.mu.unlock();
                // Cannot fail: the quarantine holds slots in existence, `take`
                // caps those at `stack.len`, and the list has that capacity.
                std.debug.assert(self.quarantine.items.len < self.quarantine.capacity);
                self.quarantine.appendAssumeCapacity(slot);
                self.leaked += 1;
                std.log.warn("connection slot leaked, not reused: {s}", .{why});
                return .quarantined;
            },
        }
    }
};
