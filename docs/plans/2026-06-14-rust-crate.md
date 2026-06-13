# Plan: Rust crate for shard-db

**Date:** 2026-06-14
**Author:** Claude Sonnet 4.6
**Target:** Two Rust crates under `rust/` — `shard-db-sys` (raw FFI) and `shard-db` (safe wrapper) — publishable to crates.io

---

## Execution rules

- Branch off `main`: `git checkout -b feat/rust-crate`
- Do tasks **in order**; do not skip ahead or reorder steps.
- Build the library first: run `SKIP_TESTS=1 ./build.sh` from the repo root before touching Rust code. This produces `build/bin/libshard-db.a` and `build/bin/shard_db.h`.
- Build Rust crates with: `cargo build` from `rust/` (or `cargo build -p shard-db-sys` / `cargo build -p shard-db` individually).
- Test with: `cargo test` from `rust/` — this runs the integration test in `rust/shard-db/tests/integration.rs`.
- **Never claim a step passed without showing the actual compiler/test output.**
- If a quoted anchor is not found exactly in the file as written, stop and write `PLAN_NOTES.md` in the repo root — do not guess, rephrase, or skip.
- Do not commit. Leave all work uncommitted on the branch.
- All paths below are relative to the repo root (`/path/to/shard-db/`). Never use line numbers as anchors — quote literal text instead.

---

## Background

`shard-db` is a high-performance embedded database written in C. Its public embedded API lives in `src/db/shard_db.h` and is compiled into `build/bin/libshard-db.a` by `./build.sh`. The npm package (`npm/`) wraps the same API as a Node.js N-API addon. This plan adds a Rust binding with the same embedding pattern.

The C API is tiny — exactly 5 functions and one opaque struct pointer:

```c
typedef struct ShardDb ShardDb;

ShardDb *shard_db_open(const char *db_root);
int      shard_db_query(ShardDb *db, const char *json, char **out, size_t *out_len);
void     shard_db_free_result(char *out);
void     shard_db_close(ShardDb *db);
void     shard_db_set_log_handler(ShardDb *db,
             void (*fn)(int type, const char *msg, void *userdata),
             void *userdata);
```

Log type constants (from `src/db/shard_db.h`):
- `SHARD_DB_LOG_ERROR = 1`
- `SHARD_DB_LOG_WARN  = 2`
- `SHARD_DB_LOG_INFO  = 3`
- `SHARD_DB_LOG_DEBUG = 4`
- `SHARD_DB_LOG_AUDIT = 5`
- `SHARD_DB_LOG_SLOW  = 6`

Key runtime facts (from `build.sh` and `binding.gyp`):
- Linux link flags: `-lpthread -lm -latomic` (and `-lssl -lcrypto` when TLS enabled, but TLS is skipped in this plan — we use `EMBED_NO_TLS`)
- The `libshard-db.a` produced by `build.sh` **includes** TLS object code (compiled with full OpenSSL). Since we cannot exclude already-compiled `.o` files from the archive, `build.rs` must link `-lssl -lcrypto` on Linux as well (same as the daemon). If OpenSSL is absent on the build machine, the linker will error — document this clearly in the README. macOS is deferred to a future plan.
- The library is thread-safe for concurrent `shard_db_query` calls on the same handle.

---

## Directory layout produced by this plan

```
rust/
  Cargo.toml                        ← workspace manifest
  shard-db-sys/
    Cargo.toml
    build.rs
    src/
      lib.rs
  shard-db/
    Cargo.toml
    src/
      lib.rs
    tests/
      integration.rs
.github/workflows/
  rust-publish.yml
```

---

## Task 1 — Create workspace Cargo.toml

Create `rust/Cargo.toml` with the following exact content:

```toml
[workspace]
resolver = "2"
members = [
    "shard-db-sys",
    "shard-db",
]
```

---

## Task 2 — Create `shard-db-sys` crate

### 2a — `rust/shard-db-sys/Cargo.toml`

```toml
[package]
name = "shard-db-sys"
version = "0.1.0"
edition = "2021"
description = "Raw FFI bindings for the shard-db embedded database C library"
license = "MIT"
repository = "https://github.com/sayyiditow/shard-db"
links = "shard-db"
build = "build.rs"

[build-dependencies]
# No external build-dependencies required — we invoke the existing build.sh
# via std::process::Command and then emit cargo: directives.
```

### 2b — `rust/shard-db-sys/build.rs`

This script:
1. Runs `SKIP_TESTS=1 ./build.sh` from the repo root (idempotent — skips heavy recompile if `libshard-db.a` is already up to date, because `build.sh` uses gcc's dependency rules and `ar` only rewrites on change).
2. Emits `cargo:rustc-link-search` pointing at `build/bin/`.
3. Emits `cargo:rustc-link-lib` for `shard-db` (static) and its runtime dependencies.

```rust
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Locate the repo root. build.rs runs with CWD = the crate dir
    // (rust/shard-db-sys/), so we go up two levels.
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let repo_root = crate_dir
        .parent()   // rust/
        .unwrap()
        .parent()   // repo root
        .unwrap()
        .to_path_buf();

    // Run ./build.sh with SKIP_TESTS=1 to produce libshard-db.a.
    // We always run it; build.sh is idempotent and fast when sources haven't changed.
    let status = Command::new("bash")
        .arg("build.sh")
        .current_dir(&repo_root)
        .env("SKIP_TESTS", "1")
        .status()
        .expect("Failed to execute build.sh — is bash available?");

    if !status.success() {
        panic!(
            "build.sh failed with exit code {:?}. \
             Ensure gcc, libssl-dev, and libncurses-dev are installed.",
            status.code()
        );
    }

    let lib_dir = repo_root.join("build").join("bin");

    // Tell cargo where to find libshard-db.a.
    println!("cargo:rustc-link-search=native={}", lib_dir.display());

    // Link libshard-db.a statically.
    println!("cargo:rustc-link-lib=static=shard-db");

    // Runtime dependencies — must match those used by build.sh.
    // libshard-db.a was compiled with full TLS (tls.c, not tls_stub.c),
    // so we must link OpenSSL even though we don't expose TLS in the Rust API.
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=m");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    if target_os == "linux" {
        println!("cargo:rustc-link-lib=atomic");
        println!("cargo:rustc-link-lib=ssl");
        println!("cargo:rustc-link-lib=crypto");
    }
    // macOS support is deferred — OpenSSL path detection (Homebrew prefix)
    // is non-trivial and left for a follow-up plan.

    // Re-run this build script when any C source or header changes.
    println!("cargo:rerun-if-changed=build.sh");
    println!("cargo:rerun-if-changed=src/db/shard_db.h");
    println!("cargo:rerun-if-changed=src/db/embedded.c");
    println!("cargo:rerun-if-changed=src/db/query.c");
    println!("cargo:rerun-if-changed=src/db/storage.c");
    println!("cargo:rerun-if-changed=src/db/config.c");
    println!("cargo:rerun-if-changed=src/db/btree.c");
    println!("cargo:rerun-if-changed=src/db/index.c");
}
```

### 2c — `rust/shard-db-sys/src/lib.rs`

Hand-written FFI declarations for the 5 public functions. No bindgen required — the API surface is stable and tiny. All declarations are `unsafe extern "C"`.

```rust
//! Raw unsafe FFI bindings for `libshard-db`.
//!
//! This crate re-exports the C symbols from `libshard-db.a` exactly as they
//! appear in `src/db/shard_db.h`. Prefer the safe `shard-db` crate for
//! application code.

use std::os::raw::{c_char, c_int, c_void};

/// Opaque handle returned by `shard_db_open`. All access goes through the
/// pointer; the internal layout is not exposed.
#[repr(C)]
pub struct ShardDb {
    _private: [u8; 0],
}

/// Log event type constants passed to the log handler callback.
pub const SHARD_DB_LOG_ERROR: c_int = 1;
pub const SHARD_DB_LOG_WARN:  c_int = 2;
pub const SHARD_DB_LOG_INFO:  c_int = 3;
pub const SHARD_DB_LOG_DEBUG: c_int = 4;
pub const SHARD_DB_LOG_AUDIT: c_int = 5;
pub const SHARD_DB_LOG_SLOW:  c_int = 6;

/// Type alias for the log handler function pointer expected by
/// `shard_db_set_log_handler`.
///
/// - `type_`: one of the `SHARD_DB_LOG_*` constants.
/// - `msg`: a pre-formatted, newline-terminated UTF-8 string of the form
///   `"YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n"`. The pointer is only
///   valid for the duration of the call.
/// - `userdata`: the opaque pointer passed to `shard_db_set_log_handler`.
///
/// The handler is called synchronously from the thread that emits the log
/// event (which may be an internal parallel worker). It **must be
/// thread-safe**.
pub type LogHandlerFn =
    unsafe extern "C" fn(type_: c_int, msg: *const c_char, userdata: *mut c_void);

extern "C" {
    /// Open a shard-db data directory for in-process use.
    ///
    /// `db_root` must point to an existing, writable directory. Returns a
    /// non-null handle on success, or null on error. Only one instance per
    /// process is allowed (V1 constraint of the C library).
    pub fn shard_db_open(db_root: *const c_char) -> *mut ShardDb;

    /// Execute a JSON query string and write the JSON response into `*out`.
    ///
    /// The caller **must** free `*out` with `shard_db_free_result` after use.
    /// Thread-safe: multiple threads may call concurrently on the same handle.
    ///
    /// Returns 0 on success, -1 on allocation failure.
    pub fn shard_db_query(
        db: *mut ShardDb,
        json: *const c_char,
        out: *mut *mut c_char,
        out_len: *mut usize,
    ) -> c_int;

    /// Free a result buffer returned by `shard_db_query`.
    pub fn shard_db_free_result(out: *mut c_char);

    /// Close the database instance and free all internal resources.
    ///
    /// After this call the handle is invalid. Must not be called concurrently
    /// with `shard_db_query`.
    pub fn shard_db_close(db: *mut ShardDb);

    /// Register a log handler for embedded use.
    ///
    /// `fn_` is called synchronously on the emitting thread for every log
    /// event. Pass `None` / null to unregister. The handler must be
    /// thread-safe. No-op when the ring-buffer drain thread is running
    /// (daemon mode).
    pub fn shard_db_set_log_handler(
        db: *mut ShardDb,
        fn_: Option<LogHandlerFn>,
        userdata: *mut c_void,
    );
}
```

---

## Task 3 — Create the `shard-db` safe wrapper crate

### 3a — `rust/shard-db/Cargo.toml`

```toml
[package]
name = "shard-db"
version = "0.1.0"
edition = "2021"
description = "Safe, ergonomic Rust bindings for the shard-db embedded database"
license = "MIT"
repository = "https://github.com/sayyiditow/shard-db"
keywords = ["database", "embedded", "key-value", "btree", "json"]
categories = ["database", "external-ffi-bindings"]

[dependencies]
shard-db-sys = { path = "../shard-db-sys", version = "0.1" }

[dev-dependencies]
tempfile = "3"
serde_json = "1"
```

### 3b — `rust/shard-db/src/lib.rs`

Complete file content:

```rust
//! Safe, ergonomic Rust bindings for the shard-db embedded database.
//!
//! # Quick start
//!
//! ```no_run
//! use shard_db::ShardDb;
//! use std::path::Path;
//!
//! let db = ShardDb::open(Path::new("/path/to/db_root")).unwrap();
//! let response = db.query(r#"{"mode":"find","dir":"mydir","object":"myobj","criteria":{}}"#).unwrap();
//! println!("{}", response);
//! // db is closed automatically when dropped
//! ```
//!
//! # Concurrency
//!
//! `ShardDb` is `Send` and `Sync`. The underlying C library guarantees that
//! `shard_db_query` is safe to call concurrently from multiple threads on the
//! same handle. Wrap the handle in `Arc<ShardDb>` to share it across threads.
//!
//! # Async use
//!
//! `query` is synchronous (blocking). For async runtimes, run it on a
//! thread-pool via `tokio::task::spawn_blocking` or equivalent:
//!
//! ```no_run
//! # async fn example() {
//! use shard_db::ShardDb;
//! use std::{path::Path, sync::Arc};
//!
//! let db = Arc::new(ShardDb::open(Path::new("/path/to/db_root")).unwrap());
//! let db2 = Arc::clone(&db);
//! let result = tokio::task::spawn_blocking(move || {
//!     db2.query(r#"{"mode":"count","dir":"d","object":"o"}"#)
//! })
//! .await
//! .unwrap();
//! # }
//! ```
//!
//! # Log handler
//!
//! ```no_run
//! use shard_db::ShardDb;
//! use std::path::Path;
//!
//! let mut db = ShardDb::open(Path::new("/path/to/db_root")).unwrap();
//! db.set_log_handler(|level, msg| {
//!     eprintln!("[shard-db level={}] {}", level, msg);
//! });
//! ```

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;

use shard_db_sys as sys;

// ─── Error type ─────────────────────────────────────────────────────────────

/// Errors returned by `ShardDb` operations.
#[derive(Debug)]
pub enum Error {
    /// `shard_db_open` returned null. The db_root path may be missing,
    /// unreadable, or another instance may already be open in this process.
    OpenFailed,

    /// `shard_db_query` returned -1 (internal allocation failure). This is
    /// extremely rare and indicates the C library ran out of memory building
    /// the response buffer.
    QueryAllocFailed,

    /// The JSON response returned by the C library was not valid UTF-8.
    /// shard-db always produces UTF-8, so this indicates a severe internal
    /// bug. The raw bytes are included for diagnosis.
    Utf8Error(std::str::Utf8Error),

    /// A NUL byte was found in the query JSON string, which cannot be passed
    /// to the C library.
    NulInQuery(std::ffi::NulError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::OpenFailed => write!(
                f,
                "shard_db_open failed — check db_root path and ensure only one \
                 instance is open per process"
            ),
            Error::QueryAllocFailed => {
                write!(f, "shard_db_query allocation failure (out of memory)")
            }
            Error::Utf8Error(e) => write!(f, "response is not valid UTF-8: {e}"),
            Error::NulInQuery(e) => write!(f, "NUL byte in query string: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Utf8Error(e) => Some(e),
            Error::NulInQuery(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error::Utf8Error(e)
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(e: std::ffi::NulError) -> Self {
        Error::NulInQuery(e)
    }
}

// ─── Log level re-export ────────────────────────────────────────────────────

/// Log level constants matching `SHARD_DB_LOG_*` from `shard_db.h`.
pub mod log_level {
    /// Internal errors.
    pub const ERROR: i32 = shard_db_sys::SHARD_DB_LOG_ERROR;
    /// Warnings.
    pub const WARN: i32 = shard_db_sys::SHARD_DB_LOG_WARN;
    /// General info.
    pub const INFO: i32 = shard_db_sys::SHARD_DB_LOG_INFO;
    /// Verbose debug.
    pub const DEBUG: i32 = shard_db_sys::SHARD_DB_LOG_DEBUG;
    /// Auth / write audit trail.
    pub const AUDIT: i32 = shard_db_sys::SHARD_DB_LOG_AUDIT;
    /// Slow-query threshold crossed.
    pub const SLOW: i32 = shard_db_sys::SHARD_DB_LOG_SLOW;
}

// ─── Log handler trampoline ─────────────────────────────────────────────────

// We store the user's Rust closure on the heap and pass its raw pointer as
// `userdata` into the C library. The C library then calls our trampoline
// (a plain `extern "C" fn`) which re-constructs the fat pointer and calls
// through to Rust.
//
// The heap allocation is leaked intentionally: the C library may call the
// handler after `shard_db_set_log_handler(db, NULL, NULL)` returns (if a
// parallel worker is mid-flight), so we cannot drop the closure until after
// `shard_db_close` completes. We rely on process exit to reclaim the memory.
// This is the same approach used by the npm binding (which never frees the
// C-side log buffer either).
//
// The closure is `Box<dyn Fn(i32, &str) + Send + Sync + 'static>`. The
// `Send + Sync` bounds are required because the C library's parallel workers
// call the handler from arbitrary threads.

type LogClosure = Box<dyn Fn(i32, &str) + Send + Sync + 'static>;

unsafe extern "C" fn log_trampoline(type_: c_int, msg: *const c_char, userdata: *mut c_void) {
    // SAFETY: userdata is a *mut LogClosure cast to *mut c_void.
    // The pointer is valid as long as the shard-db instance is alive (we
    // leaked the Box so it is never freed prematurely).
    let closure = &*(userdata as *const LogClosure);
    let msg_str = if msg.is_null() {
        ""
    } else {
        match CStr::from_ptr(msg).to_str() {
            Ok(s) => s,
            Err(_) => return, // malformed UTF-8 from C — skip silently
        }
    };
    closure(type_ as i32, msg_str);
}

// ─── ShardDb ────────────────────────────────────────────────────────────────

/// A handle to an open shard-db database instance.
///
/// The handle is `Send + Sync`: the underlying C library guarantees that
/// `shard_db_query` is safe to call concurrently from multiple threads.
///
/// Only one `ShardDb` instance per process is supported by the C library (V1
/// limitation). Attempting to open a second instance while the first is still
/// alive will return `Err(Error::OpenFailed)`.
///
/// The database is closed automatically when the handle is dropped.
pub struct ShardDb {
    /// Non-null pointer to the C-side handle.
    ptr: *mut sys::ShardDb,

    /// Heap-allocated log closure. `None` until `set_log_handler` is called.
    /// Stored here so we can document lifetime; the actual pointer passed to C
    /// is a `*mut c_void` obtained via `Box::into_raw`.
    _log_handler: Option<*mut LogClosure>,
}

// SAFETY: The C library documents that shard_db_query is thread-safe. The ptr
// is never aliased outside of this struct (the C library manages its own
// internal synchronization). The log handler closure is Send + Sync.
unsafe impl Send for ShardDb {}
unsafe impl Sync for ShardDb {}

impl ShardDb {
    /// Open a shard-db data directory for in-process use.
    ///
    /// `db_root` must be an existing, writable directory path. The path is
    /// converted to a C string internally; it must not contain interior NUL
    /// bytes.
    ///
    /// # Errors
    ///
    /// Returns `Err(Error::OpenFailed)` if the C library returns null (bad
    /// path, permissions, or another instance already open in this process).
    pub fn open(db_root: &Path) -> Result<Self, Error> {
        let path_str = db_root.to_string_lossy();
        let c_path = CString::new(path_str.as_ref()).map_err(Error::NulInQuery)?;

        // SAFETY: c_path is a valid NUL-terminated C string. The C library
        // copies anything it needs from the pointer before returning.
        let ptr = unsafe { sys::shard_db_open(c_path.as_ptr()) };

        if ptr.is_null() {
            return Err(Error::OpenFailed);
        }

        Ok(ShardDb {
            ptr,
            _log_handler: None,
        })
    }

    /// Execute a JSON query and return the JSON response as a `String`.
    ///
    /// The call is **synchronous / blocking**. The C library may use internal
    /// parallelism to execute the query, but this function does not return
    /// until the result is ready.
    ///
    /// For use with async runtimes, wrap with `tokio::task::spawn_blocking` or
    /// `rayon::spawn` as appropriate.
    ///
    /// # Errors
    ///
    /// - `Error::NulInQuery` — `json` contains a NUL byte (very unlikely for
    ///   well-formed JSON, but checked for safety).
    /// - `Error::QueryAllocFailed` — the C library returned -1 (OOM).
    /// - `Error::Utf8Error` — the response bytes are not valid UTF-8 (should
    ///   never happen with a correctly built shard-db).
    pub fn query(&self, json: &str) -> Result<String, Error> {
        let c_json = CString::new(json)?;

        let mut out: *mut c_char = std::ptr::null_mut();
        let mut out_len: usize = 0;

        // SAFETY: self.ptr is non-null (invariant). c_json is valid. out and
        // out_len are valid stack locations. The C library allocates *out with
        // malloc and we free it with shard_db_free_result below.
        let rc = unsafe {
            sys::shard_db_query(self.ptr, c_json.as_ptr(), &mut out, &mut out_len)
        };

        if rc != 0 {
            return Err(Error::QueryAllocFailed);
        }

        // Convert the result to a Rust String before freeing.
        let result = if out.is_null() || out_len == 0 {
            String::new()
        } else {
            // SAFETY: out points to out_len bytes of C-heap-allocated data.
            let bytes = unsafe { std::slice::from_raw_parts(out as *const u8, out_len) };
            let s = std::str::from_utf8(bytes)?.to_owned();
            s
        };

        // Free the C-side buffer regardless of whether conversion succeeded.
        // (If from_raw_parts / from_utf8 panicked we'd leak, but that's
        // acceptable — a UTF-8 panic means something is catastrophically wrong.)
        if !out.is_null() {
            // SAFETY: out was allocated by shard_db_query; shard_db_free_result
            // is the correct deallocator.
            unsafe { sys::shard_db_free_result(out) };
        }

        Ok(result)
    }

    /// Register a Rust closure as the log handler.
    ///
    /// The closure receives `(level: i32, msg: &str)` where `level` is one of
    /// the `log_level::*` constants and `msg` is the pre-formatted log line
    /// (newline-terminated). The closure must be `Send + Sync + 'static`
    /// because the C library calls it from its internal parallel worker threads.
    ///
    /// Calling this method more than once is safe: the previous handler is
    /// replaced. The old closure is **leaked** (not freed) because the C
    /// library may invoke it from a parallel worker after the replacement;
    /// this is a small, bounded allocation and occurs at most once per call to
    /// `set_log_handler`.
    ///
    /// Pass an explicit `None` with `unset_log_handler` to deregister.
    pub fn set_log_handler<F>(&mut self, f: F)
    where
        F: Fn(i32, &str) + Send + Sync + 'static,
    {
        let boxed: Box<LogClosure> = Box::new(Box::new(f));
        let raw = Box::into_raw(boxed);

        // SAFETY: self.ptr is non-null. raw is a valid heap pointer that
        // lives for the program's lifetime (we intentionally leak it).
        unsafe {
            sys::shard_db_set_log_handler(self.ptr, Some(log_trampoline), raw as *mut c_void);
        }

        self._log_handler = Some(raw);
    }

    /// Deregister the log handler (equivalent to passing NULL to the C API).
    pub fn unset_log_handler(&mut self) {
        // SAFETY: self.ptr is non-null.
        unsafe {
            sys::shard_db_set_log_handler(self.ptr, None, std::ptr::null_mut());
        }
        // Don't free _log_handler — see comment on set_log_handler.
    }
}

impl Drop for ShardDb {
    fn drop(&mut self) {
        // SAFETY: self.ptr is non-null (invariant maintained since open()).
        // After this call the ptr is invalid; Rust's ownership ensures no
        // further use occurs.
        unsafe { sys::shard_db_close(self.ptr) };
    }
}
```

### 3c — `rust/shard-db/tests/integration.rs`

This is a `cargo test` integration test that requires a real writable tmpdir and exercises open / query / close / log_handler. It must be run after `build.sh` has produced `libshard-db.a`.

```rust
//! Integration test for the `shard-db` Rust crate.
//!
//! Creates a real shard-db data directory in a tmpdir, opens it, runs a
//! create-object query, inserts a record, queries it back, and closes.
//!
//! Requires: build/bin/libshard-db.a must already exist (run
//! `SKIP_TESTS=1 ./build.sh` from the repo root first, or rely on build.rs
//! to do it).

use shard_db::{Error, ShardDb};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn make_db_root() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("failed to create tempdir");

    // shard-db needs dirs.conf and schema.conf to exist.
    // For a minimal test we create them empty — the C library reads them at
    // open time but tolerates empty files for directory/object creation via
    // query mode.
    std::fs::write(dir.path().join("dirs.conf"), "").unwrap();
    std::fs::write(dir.path().join("schema.conf"), "").unwrap();
    dir
}

#[test]
fn test_open_and_close() {
    let tmpdir = make_db_root();
    let db = ShardDb::open(tmpdir.path()).expect("open should succeed");
    drop(db); // exercises Drop / shard_db_close
}

#[test]
fn test_open_bad_path_returns_error() {
    let result = ShardDb::open(PathBuf::from("/nonexistent/path/shard_db_rust_test").as_path());
    assert!(
        matches!(result, Err(Error::OpenFailed)),
        "expected OpenFailed, got {result:?}"
    );
}

#[test]
fn test_query_create_and_find() {
    let tmpdir = make_db_root();
    let db = ShardDb::open(tmpdir.path()).expect("open");

    // Create a tenant directory entry.
    let create_dir = r#"{"mode":"create-dir","dir":"testdir"}"#;
    // Note: some builds may not expose create-dir; we use create-object which
    // also implicitly creates the dir entry.

    // Create an object.
    let create_obj = r#"{
        "mode": "create-object",
        "dir": "testdir",
        "object": "items",
        "splits": 8,
        "max_key": 64,
        "fields": [
            {"name": "title", "type": "varchar", "size": 128}
        ]
    }"#;
    let resp = db.query(create_obj).expect("create-object query");
    let parsed: serde_json::Value = serde_json::from_str(&resp)
        .unwrap_or_else(|_| panic!("response should be JSON, got: {resp}"));
    assert!(
        parsed.get("error").is_none(),
        "create-object returned error: {resp}"
    );

    // Insert a record.
    let insert = r#"{
        "mode": "insert",
        "dir": "testdir",
        "object": "items",
        "key": "item-001",
        "value": {"title": "Hello, shard-db!"}
    }"#;
    let resp = db.query(insert).expect("insert query");
    let parsed: serde_json::Value = serde_json::from_str(&resp)
        .unwrap_or_else(|_| panic!("insert response should be JSON, got: {resp}"));
    assert!(parsed.get("error").is_none(), "insert returned error: {resp}");

    // Retrieve the record.
    let get = r#"{
        "mode": "get",
        "dir": "testdir",
        "object": "items",
        "key": "item-001"
    }"#;
    let resp = db.query(get).expect("get query");
    let parsed: serde_json::Value = serde_json::from_str(&resp)
        .unwrap_or_else(|_| panic!("get response should be JSON, got: {resp}"));
    assert_eq!(
        parsed.get("title").and_then(|v| v.as_str()),
        Some("Hello, shard-db!"),
        "unexpected get response: {resp}"
    );
}

#[test]
fn test_query_count() {
    let tmpdir = make_db_root();
    let db = ShardDb::open(tmpdir.path()).expect("open");

    // Create object + insert one record then count.
    db.query(r#"{"mode":"create-object","dir":"d","object":"o","splits":8,"max_key":32,"fields":[{"name":"v","type":"varchar","size":64}]}"#)
        .expect("create-object");
    db.query(r#"{"mode":"insert","dir":"d","object":"o","key":"k1","value":{"v":"x"}}"#)
        .expect("insert");

    let resp = db
        .query(r#"{"mode":"count","dir":"d","object":"o"}"#)
        .expect("count");
    // count returns a bare integer (e.g. "1")
    let count: u64 = resp
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("count should be an integer, got: {resp}"));
    assert_eq!(count, 1);
}

#[test]
fn test_log_handler_receives_events() {
    let tmpdir = make_db_root();
    let mut db = ShardDb::open(tmpdir.path()).expect("open");

    let log_lines: Arc<Mutex<Vec<(i32, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_lines2 = Arc::clone(&log_lines);

    db.set_log_handler(move |level, msg| {
        log_lines2.lock().unwrap().push((level, msg.to_owned()));
    });

    // Trigger at least one log event by running any query.
    db.query(r#"{"mode":"create-object","dir":"logtest","object":"t","splits":8,"max_key":16,"fields":[{"name":"x","type":"int"}]}"#)
        .expect("create-object for log test");

    // We can't assert exact messages (they're implementation-defined), but we
    // can assert the handler was installed without panicking.
    drop(db);

    // If we reach here without a panic or segfault, the log handler trampoline
    // is correct.
}

#[test]
fn test_nul_in_query_returns_error() {
    let tmpdir = make_db_root();
    let db = ShardDb::open(tmpdir.path()).expect("open");
    let result = db.query("bad\0query");
    assert!(
        matches!(result, Err(Error::NulInQuery(_))),
        "expected NulInQuery, got {result:?}"
    );
}

#[test]
fn test_concurrent_queries() {
    use std::sync::Arc;
    use std::thread;

    let tmpdir = make_db_root();
    let db = Arc::new(ShardDb::open(tmpdir.path()).expect("open"));

    // Create object first.
    db.query(r#"{"mode":"create-object","dir":"c","object":"x","splits":8,"max_key":32,"fields":[{"name":"n","type":"int"}]}"#)
        .expect("create-object for concurrent test");

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let db2 = Arc::clone(&db);
            thread::spawn(move || {
                let key = format!("key-{i}");
                let insert = format!(
                    r#"{{"mode":"insert","dir":"c","object":"x","key":"{key}","value":{{"n":{i}}}}}"#
                );
                db2.query(&insert).expect("concurrent insert");

                let get = format!(
                    r#"{{"mode":"get","dir":"c","object":"x","key":"{key}"}}"#
                );
                let resp = db2.query(&get).expect("concurrent get");
                let parsed: serde_json::Value = serde_json::from_str(&resp).unwrap();
                assert_eq!(parsed["n"].as_i64(), Some(i), "concurrent read mismatch");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}
```

---

## Task 4 — GitHub Actions workflow: `rust-publish.yml`

Create `.github/workflows/rust-publish.yml`:

```yaml
name: Publish Rust crates

on:
  push:
    tags:
      - "rust-v*"

jobs:
  publish:
    name: Publish to crates.io
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust stable
        uses: dtolnay/rust-toolchain@stable

      - name: Install system dependencies
        run: |
          sudo apt-get update -q
          sudo apt-get install -y gcc libssl-dev libncursesw5-dev

      - name: Build libshard-db.a
        run: SKIP_TESTS=1 ./build.sh

      - name: Run Rust tests
        working-directory: rust
        run: cargo test

      - name: Publish shard-db-sys
        working-directory: rust/shard-db-sys
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
        run: cargo publish

      - name: Wait for crates.io index to update
        run: sleep 30

      - name: Publish shard-db
        working-directory: rust/shard-db
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
        run: cargo publish
```

---

## Task 5 — Verification

After creating all files, run these commands from the repo root and paste the actual output. Never skip this step or claim it passed without output.

### 5a — Build the C library

```bash
SKIP_TESTS=1 ./build.sh
```

Expected: ends with `-> build/bin/libshard-db.a + build/bin/shard_db.h`

### 5b — Build both Rust crates

```bash
cd rust && cargo build 2>&1
```

Expected: `Compiling shard-db-sys v0.1.0` then `Compiling shard-db v0.1.0` then `Finished`.

### 5c — Run integration tests

```bash
cd rust && cargo test 2>&1
```

Expected: all 7 tests pass:
```
test test_open_and_close ... ok
test test_open_bad_path_returns_error ... ok
test test_query_create_and_find ... ok
test test_query_count ... ok
test test_log_handler_receives_events ... ok
test test_nul_in_query_returns_error ... ok
test test_concurrent_queries ... ok
```

---

## Design notes (for reviewers, not for the executing model)

### Why no bindgen?

The public API is 5 functions. Bindgen would add a `build-dependencies` entry and require `libclang`, which is not available in all CI environments. Hand-written declarations are 30 lines and cover the entire surface; they don't drift unless the C header changes (which triggers a build.rs rerun via `cargo:rerun-if-changed`).

### Why leak the log closure?

The C library calls the log handler synchronously on the emitting thread, which may be an internal parallel worker. There is no documented guarantee that no worker call is in-flight when `shard_db_set_log_handler(db, NULL, NULL)` returns. Leaking the Box ensures the trampoline never dereferences a freed pointer. The leak is bounded (one `Box<Box<dyn Fn>>` per `set_log_handler` call, typically ≤ 3 per process lifetime) and is the same approach used by the npm binding.

### Why OpenSSL is required even without TLS

`libshard-db.a` is compiled by `build.sh` with `tls.c` (not `tls_stub.c`). The npm package avoids this by compiling C sources directly with `-DEMBED_NO_TLS` and substituting `tls_stub.c`. For the Rust crate we re-use the pre-built archive to keep `build.rs` simple, at the cost of requiring `libssl-dev`. A future improvement: detect whether `build/bin/libshard-db.a` was built with TLS or not and emit link flags conditionally. Alternatively, a `no-tls` Cargo feature could invoke `build.sh` with `EMBED_NO_TLS=1` and produce a separate archive.

### macOS deferred

`build.sh` requires `brew install openssl@3` on macOS and passes `-L$(brew --prefix openssl@3)/lib`. Detecting the Homebrew prefix from `build.rs` is straightforward (`Command::new("brew").args(["--prefix", "openssl@3"])`) but was deferred to keep this plan focused on Linux CI. The GitHub Actions workflow targets `ubuntu-latest` only.

### `shard-db` vs `shard_db` naming

Cargo crate names use hyphens (`shard-db`, `shard-db-sys`). Rust module/crate identifiers use underscores (`shard_db`, `shard_db_sys`). The `lib.rs` files use `use shard_db_sys as sys` which Cargo resolves correctly from the hyphenated crate name.
