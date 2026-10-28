//! The type of a test hook global (`test_*` in the edge files): its real
//! type in observe builds (`-Dobserve=true`, and every Debug build), `void`
//! in the others. A hook read or written outside `if (comptime observe)` then
//! does not compile in a release build, which `./zb build ci` compiles (the
//! release targets), so a test hook cannot reach a production hot path by
//! accident. Declare one as
//!
//!     pub var test_x: Hook(T) = if (on) <initial T> else {};
//!
//! and use it only behind `comptime test_hook.on` (or the file's own
//! `observe` / `test_observe`, which is the same build option).
pub const on = @import("build_options").observe;

pub fn Hook(comptime T: type) type {
    return if (on) T else void;
}
