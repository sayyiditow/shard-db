# Plan: Python (PyPI) Package for shard-db

**Date:** 2026-06-14
**Branch:** `feat/python-package`
**Author:** Claude Sonnet 4.6

---

## Execution Rules

Read these before touching any file.

1. Branch off `main`: `git checkout -b feat/python-package`
2. Execute tasks in order. Do not skip ahead.
3. Build the C library first with `SKIP_TESTS=1 ./build.sh` from the repo root before doing any Python work.
4. Build the Python extension with `pip install -e python/` (from repo root). Never run it from inside `python/`.
5. Test with `python python/test/test_basic.py` (from repo root).
6. Never claim a step passed without showing the actual output.
7. If a quoted anchor is not found exactly in a file, stop and write `PLAN_NOTES.md` — do not guess or adapt.
8. Leave work **uncommitted**. The user commits after Sonnet's review.
9. `libshard-db.a` is produced by `./build.sh` at `build/bin/libshard-db.a`. The Python `setup.py` must locate it via the repo root, not an absolute path.
10. The `EMBED_NO_TLS` define must be passed to the C extension compilation so it links without OpenSSL (same as the npm binding).

---

## Overview

Add a `python/` directory to the shard-db repo that produces a `shard-db` PyPI package. The package exposes a CPython C extension (`shard_db._binding`) that wraps the five public C API functions from `src/db/shard_db.h`. A thin `shard_db/__init__.py` provides the public Python API.

The pattern mirrors the npm N-API binding (`npm/src/binding.c`), adapted for CPython's extension API instead of N-API. The key design differences from the npm binding:

- **Synchronous** `query()` that releases the GIL during the C call, rather than N-API async work items. Python users wanting async call `asyncio.to_thread()` themselves.
- **Log handler** invoked on the calling thread: the C callback fires during `shard_db_query` on whatever thread called `query()`. The callback acquires the GIL with `PyGILState_Ensure` before calling the Python callable.
- **`ShardDbObject`** Python type (not an opaque external like N-API) — a proper `PyObject` subtype with `tp_new`, `tp_init`, `tp_dealloc`, methods table.

Wheel distribution via `cibuildwheel` on GitHub Actions, triggered by `python-v*` tags, published to PyPI.

---

## File Tree to Create

```
python/
├── pyproject.toml
├── setup.py
├── src/
│   └── shard_db/
│       ├── __init__.py
│       ├── _binding.c
│       └── py.typed
test/                       (inside python/)
    └── test_basic.py
.github/workflows/
    python-wheels.yml       (new, alongside existing npm-prebuilds.yml)
```

---

## Task 1 — Build the C library

From the repo root:

```bash
SKIP_TESTS=1 ./build.sh
```

Verify `build/bin/libshard-db.a` exists and `build/bin/shard_db.h` exists (symlink or copy produced by the build script). If `build/bin/shard_db.h` does not exist, the `setup.py` will reference `src/db/shard_db.h` directly instead — see Task 3.

**Acceptance:** `ls -la build/bin/libshard-db.a` exits 0.

---

## Task 2 — Create `python/pyproject.toml`

Create the file at `python/pyproject.toml` with the following exact contents:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "shard-db"
version = "1.0.0"
description = "High-performance embedded database for Python (CPython C extension)"
readme = "README.md"
license = { text = "MIT" }
requires-python = ">=3.10"
keywords = ["database", "embedded", "key-value", "btree", "native"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Programming Language :: C",
    "Topic :: Database",
    "Topic :: Database :: Database Engines/Servers",
]

[project.urls]
Homepage = "https://github.com/sayyiditow/shard-db"
Repository = "https://github.com/sayyiditow/shard-db"
Documentation = "https://github.com/sayyiditow/shard-db/tree/main/docs"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools.package-data]
shard_db = ["py.typed", "*.pyi"]

# ---------------------------------------------------------------------------
# cibuildwheel configuration
# ---------------------------------------------------------------------------
[tool.cibuildwheel]
# Build CPython 3.10–3.13 only; skip PyPy and musllinux.
build = "cp310-* cp311-* cp312-* cp313-*"
skip = "pp* *-musllinux*"

# Run before each wheel build.
# On Linux: install OpenSSL dev headers (for the shard-db C build, even
# though the Python extension itself uses EMBED_NO_TLS and links without
# OpenSSL — the build.sh script still checks for headers on Linux).
# On macOS: brew install openssl@3.
# Then build the static library. SKIP_TESTS=1 skips the C test suite.
before-build = """
{
  if [ "$(uname)" = "Linux" ]; then
    if command -v apt-get >/dev/null 2>&1; then
      apt-get install -y libssl-dev || true
    elif command -v yum >/dev/null 2>&1; then
      yum install -y openssl-devel || true
    fi
  elif [ "$(uname)" = "Darwin" ]; then
    brew install openssl@3 || true
  fi
  SKIP_TESTS=1 ./build.sh
}
"""

# auditwheel / delocate repair commands (platform-specific defaults apply
# automatically when using cibuildwheel; these lines make them explicit).
[tool.cibuildwheel.linux]
repair-wheel-command = "auditwheel repair -w {dest_dir} {wheel}"

[tool.cibuildwheel.macos]
repair-wheel-command = "delocate-wheel --require-archs {delocate_archs} -w {dest_dir} {wheel}"
```

---

## Task 3 — Create `python/setup.py`

Create the file at `python/setup.py` with the following exact contents:

```python
"""
setup.py for shard-db CPython C extension.

Assumes it is executed with the REPO ROOT as cwd (pip install -e python/ from
repo root, or cibuildwheel which sets cwd to the project root which is python/).
The build.sh script must have already run (SKIP_TESTS=1 ./build.sh) so that
  build/bin/libshard-db.a   exists relative to repo root
  src/db/shard_db.h         exists relative to repo root
"""

import os
import sys
from pathlib import Path
from setuptools import setup, Extension

# ---------------------------------------------------------------------------
# Locate the repo root.  setup.py lives inside python/, so the repo root is
# one level up UNLESS cibuildwheel has set cwd to python/ itself.
# We detect the repo root by looking for build/bin/libshard-db.a.
# ---------------------------------------------------------------------------
HERE = Path(__file__).parent.resolve()

def find_repo_root():
    # Try: the parent of python/ (normal dev path)
    candidate = HERE.parent
    if (candidate / "build" / "bin" / "libshard-db.a").exists():
        return candidate
    # Try: HERE itself (cibuildwheel with project_dir=python/ copies repo root
    # to cwd — shouldn't happen with our setup, but guard anyway)
    if (HERE / "build" / "bin" / "libshard-db.a").exists():
        return HERE
    raise RuntimeError(
        "Cannot find build/bin/libshard-db.a. "
        "Run 'SKIP_TESTS=1 ./build.sh' from the repo root first."
    )

REPO_ROOT = find_repo_root()
LIB_DIR   = str(REPO_ROOT / "build" / "bin")
INC_DIR   = str(REPO_ROOT / "src" / "db")
LIB_A     = str(REPO_ROOT / "build" / "bin" / "libshard-db.a")

if not os.path.exists(LIB_A):
    sys.exit(f"ERROR: {LIB_A} not found. Run: SKIP_TESTS=1 ./build.sh")

# ---------------------------------------------------------------------------
# Platform-specific link libraries.
# shard-db links: -lpthread -lm  on all platforms.
# -latomic is required on Linux (GCC needs it for 128-bit atomics on some
# architectures; harmless to include on x86_64).
# macOS does not ship libatomic; omit it.
# ---------------------------------------------------------------------------
extra_link_args = [LIB_A]
libraries = ["pthread", "m"]
if sys.platform.startswith("linux"):
    libraries.append("atomic")

extra_compile_args = [
    "-O2",
    "-std=c11",
    "-fno-strict-aliasing",
    "-D_GNU_SOURCE",
    "-DEMBED_NO_TLS",  # Skip OpenSSL in the embedded build (same as npm binding)
]

binding_ext = Extension(
    name="shard_db._binding",
    sources=["src/shard_db/_binding.c"],
    include_dirs=[INC_DIR],
    libraries=libraries,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c",
)

setup(
    ext_modules=[binding_ext],
)
```

---

## Task 4 — Create `python/src/shard_db/_binding.c`

This is the CPython C extension. Create it at `python/src/shard_db/_binding.c` with the following exact contents:

```c
/*
 * python/src/shard_db/_binding.c
 * CPython C extension wrapping the shard-db embedded API.
 *
 * Public API exposed:
 *   Module function:  open(db_root: str) -> ShardDb
 *   ShardDb methods:  query(json: str) -> str
 *                     set_log_handler(fn: callable | None) -> None
 *                     close() -> None
 *                     __enter__ / __exit__ (context manager)
 *                     __del__ (calls close if not already closed)
 *
 * Thread safety:
 *   query() releases the GIL around shard_db_query so multiple threads can
 *   call concurrently on the same ShardDb instance. The C library is
 *   documented as thread-safe for concurrent queries on the same handle.
 *
 *   The log handler C callback (c_log_handler) is called synchronously on
 *   the thread that called query(). It acquires the GIL with
 *   PyGILState_Ensure before invoking the Python callable, then releases it.
 *   Because shard-db parallel workers can emit log events from threads other
 *   than the caller, the callback may be invoked from any thread — the
 *   PyGILState machinery handles all cases correctly.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "shard_db.h"

/* -------------------------------------------------------------------------
 * ShardDb Python type
 * ---------------------------------------------------------------------- */

typedef struct {
    PyObject_HEAD
    ShardDb    *db;
    int         closed;
    PyObject   *log_fn;      /* Python callable, or NULL */
    pthread_mutex_t log_lock; /* guards log_fn access from C callback */
} ShardDbObject;

/* Forward declaration */
static PyTypeObject ShardDbType;

/* -------------------------------------------------------------------------
 * C log callback — called synchronously on whichever thread emits the event
 * (may be a shard-db internal worker thread, not the Python thread).
 * Must acquire GIL before touching any Python objects.
 * ---------------------------------------------------------------------- */
static void c_log_handler(int type, const char *msg, void *userdata)
{
    ShardDbObject *self = (ShardDbObject *)userdata;

    /* Snapshot the callable under the lock to avoid racing with
     * set_log_handler() on another thread. */
    pthread_mutex_lock(&self->log_lock);
    PyObject *fn = self->log_fn;
    Py_XINCREF(fn);
    pthread_mutex_unlock(&self->log_lock);

    if (!fn) return;

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *result = PyObject_CallFunction(fn, "is", type, msg ? msg : "");
    if (!result) {
        /* Swallow exceptions from the log handler — don't let them
         * propagate into the C library's internal thread. */
        PyErr_Clear();
    } else {
        Py_DECREF(result);
    }

    Py_DECREF(fn);
    PyGILState_Release(gstate);
}

/* -------------------------------------------------------------------------
 * ShardDb.__new__ / tp_new
 * ---------------------------------------------------------------------- */
static PyObject *
ShardDb_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    ShardDbObject *self = (ShardDbObject *)type->tp_alloc(type, 0);
    if (!self) return NULL;
    self->db     = NULL;
    self->closed = 0;
    self->log_fn = NULL;
    pthread_mutex_init(&self->log_lock, NULL);
    return (PyObject *)self;
}

/* -------------------------------------------------------------------------
 * ShardDb.__init__(db_root: str)
 * ---------------------------------------------------------------------- */
static int
ShardDb_init(ShardDbObject *self, PyObject *args, PyObject *kwds)
{
    const char *db_root = NULL;
    static char *kwlist[] = {"db_root", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", kwlist, &db_root))
        return -1;

    self->db = shard_db_open(db_root);
    if (!self->db) {
        PyErr_SetString(PyExc_RuntimeError,
            "shard_db_open failed — check that db_root exists and is writable");
        return -1;
    }
    self->closed = 0;
    return 0;
}

/* -------------------------------------------------------------------------
 * ShardDb.__dealloc__ / tp_dealloc
 * ---------------------------------------------------------------------- */
static void
ShardDb_dealloc(ShardDbObject *self)
{
    if (!self->closed && self->db) {
        shard_db_close(self->db);
        self->db     = NULL;
        self->closed = 1;
    }
    pthread_mutex_lock(&self->log_lock);
    Py_CLEAR(self->log_fn);
    pthread_mutex_unlock(&self->log_lock);
    pthread_mutex_destroy(&self->log_lock);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

/* -------------------------------------------------------------------------
 * ShardDb.query(json: str) -> str
 *
 * Releases the GIL around shard_db_query so multiple Python threads can
 * issue concurrent queries on the same ShardDb instance.
 * ---------------------------------------------------------------------- */
static PyObject *
ShardDb_query(ShardDbObject *self, PyObject *args)
{
    const char *json = NULL;

    if (!PyArg_ParseTuple(args, "s", &json))
        return NULL;

    if (self->closed || !self->db) {
        PyErr_SetString(PyExc_RuntimeError, "ShardDb is closed");
        return NULL;
    }

    char   *out     = NULL;
    size_t  out_len = 0;
    int     rc;

    /* Release GIL during the blocking C call. */
    Py_BEGIN_ALLOW_THREADS
    rc = shard_db_query(self->db, json, &out, &out_len);
    Py_END_ALLOW_THREADS

    if (rc != 0) {
        PyErr_SetString(PyExc_MemoryError,
            "shard_db_query: allocation failure");
        return NULL;
    }

    PyObject *result = PyUnicode_FromStringAndSize(
        out ? out : "", (Py_ssize_t)out_len);
    shard_db_free_result(out);
    return result;
}

/* -------------------------------------------------------------------------
 * ShardDb.set_log_handler(fn: callable | None) -> None
 * ---------------------------------------------------------------------- */
static PyObject *
ShardDb_set_log_handler(ShardDbObject *self, PyObject *args)
{
    PyObject *fn = NULL;

    if (!PyArg_ParseTuple(args, "O", &fn))
        return NULL;

    if (self->closed || !self->db) {
        PyErr_SetString(PyExc_RuntimeError, "ShardDb is closed");
        return NULL;
    }

    pthread_mutex_lock(&self->log_lock);

    Py_CLEAR(self->log_fn);

    if (fn == Py_None) {
        self->log_fn = NULL;
        shard_db_set_log_handler(self->db, NULL, NULL);
    } else {
        if (!PyCallable_Check(fn)) {
            pthread_mutex_unlock(&self->log_lock);
            PyErr_SetString(PyExc_TypeError,
                "set_log_handler() argument must be callable or None");
            return NULL;
        }
        Py_INCREF(fn);
        self->log_fn = fn;
        shard_db_set_log_handler(self->db, c_log_handler, self);
    }

    pthread_mutex_unlock(&self->log_lock);
    Py_RETURN_NONE;
}

/* -------------------------------------------------------------------------
 * ShardDb.close() -> None
 * ---------------------------------------------------------------------- */
static PyObject *
ShardDb_close(ShardDbObject *self, PyObject *Py_UNUSED(ignored))
{
    if (!self->closed && self->db) {
        self->closed = 1;
        shard_db_set_log_handler(self->db, NULL, NULL);
        shard_db_close(self->db);
        self->db = NULL;
    }
    Py_RETURN_NONE;
}

/* -------------------------------------------------------------------------
 * Context manager: __enter__ returns self, __exit__ calls close()
 * ---------------------------------------------------------------------- */
static PyObject *
ShardDb_enter(ShardDbObject *self, PyObject *Py_UNUSED(ignored))
{
    if (self->closed || !self->db) {
        PyErr_SetString(PyExc_RuntimeError, "ShardDb is closed");
        return NULL;
    }
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *
ShardDb_exit(ShardDbObject *self, PyObject *args)
{
    /* args: (exc_type, exc_val, exc_tb) — we ignore them */
    return ShardDb_close(self, NULL);
}

/* -------------------------------------------------------------------------
 * Method table
 * ---------------------------------------------------------------------- */
static PyMethodDef ShardDb_methods[] = {
    {"query",
     (PyCFunction)ShardDb_query,
     METH_VARARGS,
     "query(json: str) -> str\n\n"
     "Execute a JSON query and return the JSON response string.\n"
     "Releases the GIL during the C call; safe to call concurrently\n"
     "from multiple threads on the same instance.\n"
     "Raises RuntimeError on OOM. The response may contain {\"error\":...};\n"
     "inspect the JSON to detect logical errors."},

    {"set_log_handler",
     (PyCFunction)ShardDb_set_log_handler,
     METH_VARARGS,
     "set_log_handler(fn: callable | None) -> None\n\n"
     "Register a callable to receive log events. Signature:\n"
     "  fn(type: int, msg: str) -> None\n"
     "type is 1=error 2=warn 3=info 4=debug 5=audit 6=slow.\n"
     "msg is pre-formatted: 'YYYY-MM-DD HH:MM:SS LEVEL [sub] text\\n'.\n"
     "The handler is called on the thread that called query() (after the\n"
     "GIL is re-acquired). Pass None to unregister."},

    {"close",
     (PyCFunction)ShardDb_close,
     METH_NOARGS,
     "close() -> None\n\nClose the database and release all resources. Idempotent."},

    {"__enter__",
     (PyCFunction)ShardDb_enter,
     METH_NOARGS,
     "Support use as a context manager."},

    {"__exit__",
     (PyCFunction)ShardDb_exit,
     METH_VARARGS,
     "Support use as a context manager; calls close()."},

    {NULL, NULL, 0, NULL}
};

/* -------------------------------------------------------------------------
 * ShardDb type object
 * ---------------------------------------------------------------------- */
static PyTypeObject ShardDbType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name      = "shard_db._binding.ShardDb",
    .tp_basicsize = sizeof(ShardDbObject),
    .tp_itemsize  = 0,
    .tp_dealloc   = (destructor)ShardDb_dealloc,
    .tp_flags     = Py_TPFLAGS_DEFAULT,
    .tp_doc       = "ShardDb(db_root: str)\n\n"
                    "Open a shard-db data directory for in-process use.\n"
                    "Use as a context manager for automatic cleanup:\n"
                    "  with shard_db.open('/path/to/db') as db:\n"
                    "      result = db.query('{\"mode\":\"stats\"}')\n",
    .tp_methods   = ShardDb_methods,
    .tp_new       = ShardDb_new,
    .tp_init      = (initproc)ShardDb_init,
};

/* -------------------------------------------------------------------------
 * Module-level open() function — convenience wrapper
 * ---------------------------------------------------------------------- */
static PyObject *
module_open(PyObject *Py_UNUSED(module), PyObject *args, PyObject *kwds)
{
    /* Delegate to ShardDb(db_root) */
    return PyObject_Call((PyObject *)&ShardDbType, args, kwds);
}

/* -------------------------------------------------------------------------
 * Module definition
 * ---------------------------------------------------------------------- */
static PyMethodDef module_methods[] = {
    {"open",
     (PyCFunction)(void(*)(void))module_open,
     METH_VARARGS | METH_KEYWORDS,
     "open(db_root: str) -> ShardDb\n\n"
     "Open a shard-db data directory for in-process use.\n"
     "Equivalent to ShardDb(db_root)."},

    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "_binding",
    "CPython C extension for shard-db embedded database.",
    -1,
    module_methods,
};

PyMODINIT_FUNC
PyInit__binding(void)
{
    if (PyType_Ready(&ShardDbType) < 0)
        return NULL;

    PyObject *m = PyModule_Create(&moduledef);
    if (!m) return NULL;

    /* Expose the type as shard_db._binding.ShardDb */
    Py_INCREF(&ShardDbType);
    if (PyModule_AddObject(m, "ShardDb", (PyObject *)&ShardDbType) < 0) {
        Py_DECREF(&ShardDbType);
        Py_DECREF(m);
        return NULL;
    }

    /* Log type constants matching shard_db.h defines */
    PyModule_AddIntConstant(m, "LOG_ERROR", 1);
    PyModule_AddIntConstant(m, "LOG_WARN",  2);
    PyModule_AddIntConstant(m, "LOG_INFO",  3);
    PyModule_AddIntConstant(m, "LOG_DEBUG", 4);
    PyModule_AddIntConstant(m, "LOG_AUDIT", 5);
    PyModule_AddIntConstant(m, "LOG_SLOW",  6);

    return m;
}
```

---

## Task 5 — Create `python/src/shard_db/__init__.py`

Create the file at `python/src/shard_db/__init__.py` with the following exact contents:

```python
"""
shard_db — Python bindings for the shard-db embedded database.

Usage::

    import shard_db

    # Open a database (context manager recommended):
    with shard_db.open("/path/to/db") as db:
        result = db.query('{"mode":"stats"}')
        import json
        print(json.loads(result))

    # Or manage lifetime manually:
    db = shard_db.open("/path/to/db")
    try:
        result = db.query('{"mode":"count","dir":"mydir","object":"myobj"}')
    finally:
        db.close()

    # Log handler (optional):
    def on_log(type: int, msg: str) -> None:
        print(f"[{shard_db.LOG_TYPES[type]}] {msg}", end="")

    db.set_log_handler(on_log)

Async usage::

    import asyncio
    import shard_db

    async def main():
        db = shard_db.open("/path/to/db")
        try:
            # Run blocking query in a thread pool so it doesn't block the
            # event loop. The GIL is released inside query() so other threads
            # can query concurrently.
            result = await asyncio.to_thread(
                db.query, '{"mode":"stats"}'
            )
            print(result)
        finally:
            db.close()

    asyncio.run(main())
"""

from ._binding import ShardDb, open  # noqa: F401  (re-export)

__all__ = ["ShardDb", "open", "LOG_TYPES"]

# Log type names indexed by the numeric type code from the C library.
# Index 0 is empty (no type code 0 is emitted by the library).
# Usage: shard_db.LOG_TYPES[type_int]  ->  'error' | 'warn' | ...
LOG_TYPES: tuple[str, ...] = ("", "error", "warn", "info", "debug", "audit", "slow")

__version__ = "1.0.0"
```

---

## Task 6 — Create `python/src/shard_db/py.typed`

This file is a PEP 561 marker that tells type checkers the package ships inline types. Create it at `python/src/shard_db/py.typed` with empty contents:

```
(empty file)
```

Command: `touch python/src/shard_db/py.typed`

---

## Task 7 — Create `python/test/test_basic.py`

Create the file at `python/test/test_basic.py` with the following exact contents:

```python
"""
Basic smoke tests for the shard_db Python package.

Run from the repo root:
    python python/test/test_basic.py

Requires the extension to be installed:
    pip install -e python/    (from repo root, after SKIP_TESTS=1 ./build.sh)
"""

import json
import os
import sys
import tempfile
import threading
import unittest

import shard_db


class TestOpen(unittest.TestCase):
    def test_open_nonexistent_raises(self):
        with self.assertRaises(RuntimeError):
            shard_db.open("/nonexistent/path/that/cannot/exist_xyz")

    def test_open_returns_sharddb(self):
        with tempfile.TemporaryDirectory() as d:
            db = shard_db.open(d)
            self.assertIsInstance(db, shard_db.ShardDb)
            db.close()

    def test_context_manager(self):
        with tempfile.TemporaryDirectory() as d:
            with shard_db.open(d) as db:
                self.assertIsInstance(db, shard_db.ShardDb)

    def test_double_close_is_idempotent(self):
        with tempfile.TemporaryDirectory() as d:
            db = shard_db.open(d)
            db.close()
            db.close()  # should not raise


class TestQuery(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = shard_db.open(self.tmpdir.name)

    def tearDown(self):
        self.db.close()
        self.tmpdir.cleanup()

    def test_stats_returns_json(self):
        result = self.db.query('{"mode":"stats"}')
        parsed = json.loads(result)
        self.assertIsInstance(parsed, dict)

    def test_create_and_insert(self):
        # create-object
        r = self.db.query(json.dumps({
            "mode": "create-object",
            "dir": "testdir",
            "object": "items",
            "splits": 8,
            "max_key": 64,
            "fields": ["name:varchar:64", "score:int"],
        }))
        self.assertNotIn("error", json.loads(r))

        # insert
        r = self.db.query(json.dumps({
            "mode": "insert",
            "dir": "testdir",
            "object": "items",
            "key": "k1",
            "value": {"name": "Alice", "score": 42},
        }))
        self.assertNotIn("error", json.loads(r))

        # get
        r = self.db.query(json.dumps({
            "mode": "get",
            "dir": "testdir",
            "object": "items",
            "key": "k1",
        }))
        parsed = json.loads(r)
        self.assertEqual(parsed["name"], "Alice")
        self.assertEqual(parsed["score"], 42)

    def test_count_returns_integer(self):
        self.db.query(json.dumps({
            "mode": "create-object",
            "dir": "testdir2",
            "object": "things",
            "splits": 8,
            "max_key": 32,
            "fields": ["x:int"],
        }))
        r = self.db.query(json.dumps({
            "mode": "count",
            "dir": "testdir2",
            "object": "things",
        }))
        self.assertEqual(json.loads(r), 0)

    def test_query_after_close_raises(self):
        self.db.close()
        with self.assertRaises(RuntimeError):
            self.db.query('{"mode":"stats"}')

    def test_concurrent_queries(self):
        """Multiple threads can query the same db concurrently."""
        results = []
        errors = []

        def worker():
            try:
                r = self.db.query('{"mode":"stats"}')
                results.append(json.loads(r))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 8)
        for r in results:
            self.assertIsInstance(r, dict)


class TestLogHandler(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db = shard_db.open(self.tmpdir.name)

    def tearDown(self):
        self.db.close()
        self.tmpdir.cleanup()

    def test_set_log_handler_callable(self):
        events = []
        def handler(type_int, msg):
            events.append((type_int, msg))

        self.db.set_log_handler(handler)
        # Run a query to potentially emit log events
        self.db.query('{"mode":"stats"}')
        self.db.set_log_handler(None)
        # We don't assert on events content — the library may or may not
        # emit at info level depending on config. Just check no error raised.

    def test_set_log_handler_none_unregisters(self):
        self.db.set_log_handler(lambda t, m: None)
        self.db.set_log_handler(None)  # should not raise

    def test_set_log_handler_non_callable_raises(self):
        with self.assertRaises(TypeError):
            self.db.set_log_handler("not callable")

    def test_set_log_handler_after_close_raises(self):
        self.db.close()
        with self.assertRaises(RuntimeError):
            self.db.set_log_handler(lambda t, m: None)


class TestConstants(unittest.TestCase):
    def test_log_types_tuple(self):
        self.assertIsInstance(shard_db.LOG_TYPES, tuple)
        self.assertEqual(len(shard_db.LOG_TYPES), 7)
        self.assertEqual(shard_db.LOG_TYPES[0], "")
        self.assertEqual(shard_db.LOG_TYPES[1], "error")
        self.assertEqual(shard_db.LOG_TYPES[2], "warn")
        self.assertEqual(shard_db.LOG_TYPES[3], "info")
        self.assertEqual(shard_db.LOG_TYPES[4], "debug")
        self.assertEqual(shard_db.LOG_TYPES[5], "audit")
        self.assertEqual(shard_db.LOG_TYPES[6], "slow")

    def test_version(self):
        self.assertEqual(shard_db.__version__, "1.0.0")


if __name__ == "__main__":
    unittest.main(verbosity=2)
```

---

## Task 8 — Create `python/README.md`

Create the file at `python/README.md` with the following exact contents:

```markdown
# shard-db Python bindings

High-performance embedded database for Python — CPython C extension wrapping
[shard-db](https://github.com/sayyiditow/shard-db).

## Installation

```bash
pip install shard-db
```

Requires Python 3.10+. Wheels are available for:

- Linux x86_64 (manylinux)
- Linux aarch64 (manylinux)
- macOS arm64 (Apple Silicon)

## Quick start

```python
import json
import shard_db

# Open a database directory (must exist and be writable)
with shard_db.open("/path/to/db") as db:
    # Create a schema
    db.query(json.dumps({
        "mode": "create-object",
        "dir": "mydir",
        "object": "users",
        "splits": 8,
        "max_key": 64,
        "fields": ["name:varchar:128", "score:int"],
    }))

    # Insert a record
    db.query(json.dumps({
        "mode": "insert",
        "dir": "mydir",
        "object": "users",
        "key": "user:1",
        "value": {"name": "Alice", "score": 100},
    }))

    # Query
    result = db.query(json.dumps({
        "mode": "find",
        "dir": "mydir",
        "object": "users",
        "criteria": {"score": {"gt": 50}},
        "limit": 10,
    }))
    print(json.loads(result))
```

## Async usage

`query()` is synchronous and releases the GIL during the C call. For async
applications, wrap with `asyncio.to_thread()`:

```python
import asyncio
import json
import shard_db

async def main():
    db = shard_db.open("/path/to/db")
    try:
        result = await asyncio.to_thread(db.query, '{"mode":"stats"}')
        print(json.loads(result))
    finally:
        db.close()

asyncio.run(main())
```

For concurrent queries from multiple threads, simply call `query()` from each
thread — the library is thread-safe and the GIL is released during each call.

## Log handler

```python
def on_log(type: int, msg: str) -> None:
    label = shard_db.LOG_TYPES[type]  # 'error'|'warn'|'info'|'debug'|'audit'|'slow'
    print(f"[shard-db/{label}] {msg}", end="")

db.set_log_handler(on_log)
# ...
db.set_log_handler(None)  # unregister
```

## Building from source

```bash
git clone https://github.com/sayyiditow/shard-db.git
cd shard-db
SKIP_TESTS=1 ./build.sh          # builds libshard-db.a
pip install -e python/            # builds and installs the Python extension
```

Requires: `gcc` or `clang`, `make`, Python 3.10+ with development headers.

## License

MIT
```

---

## Task 9 — Create `.github/workflows/python-wheels.yml`

Create the file at `.github/workflows/python-wheels.yml` with the following exact contents:

```yaml
name: Python wheels

# Triggered by tags matching python-v* (e.g. python-v1.0.0).
# Builds CPython wheels for Linux x86_64, Linux aarch64, macOS arm64
# using cibuildwheel, then publishes to PyPI.

on:
  push:
    tags:
      - 'python-v*'

permissions: read-all

jobs:
  # ---------------------------------------------------------------------------
  # Build wheels: one job per platform/arch combination
  # ---------------------------------------------------------------------------
  build-wheels:
    name: Build wheels (${{ matrix.os }} ${{ matrix.arch }})
    runs-on: ${{ matrix.runner }}
    permissions:
      contents: read

    strategy:
      fail-fast: false
      matrix:
        include:
          # Linux x86_64 — native runner
          - os: linux
            arch: x86_64
            runner: ubuntu-latest
            cibw_archs: x86_64

          # Linux aarch64 — emulated via QEMU (slow but correct)
          - os: linux
            arch: aarch64
            runner: ubuntu-latest
            cibw_archs: aarch64

          # macOS Apple Silicon
          - os: macos
            arch: arm64
            runner: macos-14
            cibw_archs: arm64

    steps:
      - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3

      # Set up QEMU for Linux aarch64 cross-compilation
      - name: Set up QEMU (Linux aarch64 only)
        if: matrix.arch == 'aarch64' && matrix.os == 'linux'
        uses: docker/setup-qemu-action@29109295f81e9208d7d86a4e6d60cfad7e9ebf09 # v3
        with:
          platforms: arm64

      - name: Set up Python
        uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5
        with:
          python-version: '3.12'

      - name: Install cibuildwheel
        run: pip install cibuildwheel==2.21.3

      - name: Build wheels
        working-directory: python
        env:
          CIBW_ARCHS: ${{ matrix.cibw_archs }}
          # Override the before-build to run from the repo root (one level up
          # from python/, which is cibuildwheel's project directory).
          # cibuildwheel sets {project} to the project directory (python/).
          # We need the repo root for ./build.sh and build/bin/libshard-db.a.
          CIBW_BEFORE_BUILD: |
            cd {project}/..
            if [ "$(uname)" = "Linux" ]; then
              if command -v apt-get >/dev/null 2>&1; then
                apt-get install -y libssl-dev || true
              elif command -v yum >/dev/null 2>&1; then
                yum install -y openssl-devel || true
              fi
            elif [ "$(uname)" = "Darwin" ]; then
              brew install openssl@3 || true
            fi
            SKIP_TESTS=1 ./build.sh
          CIBW_BUILD: "cp310-* cp311-* cp312-* cp313-*"
          CIBW_SKIP: "pp* *-musllinux*"
          CIBW_TEST_COMMAND: "python {project}/../python/test/test_basic.py"
        run: cibuildwheel --output-dir wheelhouse

      - name: Upload wheels artifact
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
        with:
          name: wheels-${{ matrix.os }}-${{ matrix.arch }}
          path: python/wheelhouse/*.whl
          retention-days: 1

  # ---------------------------------------------------------------------------
  # Publish to PyPI — runs after all wheel builds succeed
  # ---------------------------------------------------------------------------
  publish:
    name: Publish to PyPI
    needs: build-wheels
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write   # PyPI trusted publishing (OIDC)

    steps:
      - uses: actions/checkout@df4cb1c069e1874edd31b4311f1884172cec0e10 # v6.0.3

      - name: Set up Python
        uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065 # v5
        with:
          python-version: '3.12'

      - name: Install twine
        run: pip install twine

      - name: Download all wheel artifacts
        uses: actions/download-artifact@95815c38cf2ff2164869cbab79da8d1f422bc89e # v4
        with:
          pattern: wheels-*
          path: dist/
          merge-multiple: true

      - name: List wheels
        run: ls -la dist/

      - name: Build sdist
        working-directory: python
        run: |
          pip install build
          python -m build --sdist --outdir ../dist/

      - name: Publish to PyPI
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: twine upload dist/*
```

---

## Task 10 — Install and verify the extension

From the repo root:

```bash
# 1. Build the C library (if not already done in Task 1)
SKIP_TESTS=1 ./build.sh

# 2. Install the Python extension in editable mode
pip install -e python/

# 3. Run the test suite
python python/test/test_basic.py
```

**Expected output:** All tests pass with output like:

```
test_context_manager (test_basic.TestOpen) ... ok
test_double_close_is_idempotent (test_basic.TestOpen) ... ok
test_open_nonexistent_raises (test_basic.TestOpen) ... ok
test_open_returns_sharddb (test_basic.TestOpen) ... ok
...
----------------------------------------------------------------------
Ran N tests in X.XXXs

OK
```

No test may be FAIL or ERROR. Show the actual output before declaring Task 10 complete.

---

## Invariants and edge cases

1. **OOM in `shard_db_query`**: `query()` raises `MemoryError` (not `RuntimeError`) when `rc != 0` (allocation failure in the C library).

2. **`query()` on closed db**: Raises `RuntimeError("ShardDb is closed")`.

3. **`set_log_handler()` thread safety**: The C callback acquires the GIL before calling the Python callable. Calling `set_log_handler(None)` from one thread while another thread is inside `query()` is safe — `c_log_handler` snapshots the callable under `log_lock` before acquiring the GIL.

4. **`__del__` / `tp_dealloc`**: Calls `shard_db_close` if not already closed. Python's GC may call this from any thread; `shard_db_close` is documented safe to call from any thread.

5. **`EMBED_NO_TLS` define**: Both `setup.py` and `pyproject.toml` pass `-DEMBED_NO_TLS` to the C compiler. The binding links against `libshard-db.a` which was built with this flag by `build.sh`. Do NOT link `-lssl` or `-lcrypto`.

6. **Linux `-latomic`**: Required for GCC on some targets (aarch64, arm). Included unconditionally on Linux in `setup.py`. Harmless on x86_64.

7. **`before-build` in cibuildwheel**: The shell script in `CIBW_BEFORE_BUILD` cd's to the repo root (`{project}/..`) because cibuildwheel's `{project}` is the `python/` subdirectory. `build.sh` must be run from the repo root.

8. **cibuildwheel `CIBW_TEST_COMMAND`**: Path `{project}/../python/test/test_basic.py` reaches the test file from the repo root perspective. This runs inside the installed wheel environment inside the cibuildwheel container.

9. **PyPI trusted publishing**: The workflow uses `TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}`. The repo admin must add a `PYPI_API_TOKEN` secret in GitHub repository settings (Settings → Secrets → Actions). Alternatively, configure PyPI trusted publishing (OIDC) and replace the twine step with `pypa/gh-action-pypi-publish`.

10. **`setup.py` repo root detection**: The `find_repo_root()` function in `setup.py` checks for `build/bin/libshard-db.a` relative to both `python/`'s parent (normal dev) and `python/` itself (cibuildwheel edge case). If neither exists it raises a clear error message instead of a cryptic linker failure.

11. **`open` name conflict**: `python/src/shard_db/__init__.py` imports `open` from `_binding` and re-exports it. This shadows Python's built-in `open` within the module — that is intentional and documented. Users who need both can use `builtins.open` or alias: `import shard_db; shard_db_open = shard_db.open`.

12. **No `sdist` build script**: The `setup.py` calls `./build.sh` indirectly (cibuildwheel does it in `before-build`). An sdist built with `python -m build --sdist` will include the C source but NOT `libshard-db.a` — users installing from sdist must have the C toolchain and run `build.sh` themselves. The publish job builds the sdist after wheels are ready; the sdist is uploaded to PyPI for source installs.

---

## File checklist

After all tasks complete, these files must exist (relative to repo root):

- `python/pyproject.toml`
- `python/setup.py`
- `python/README.md`
- `python/src/shard_db/__init__.py`
- `python/src/shard_db/_binding.c`
- `python/src/shard_db/py.typed`
- `python/test/test_basic.py`
- `.github/workflows/python-wheels.yml`

And these must NOT be modified:

- `src/db/shard_db.h` — read-only reference
- `build.sh` — not modified
- Any existing `.github/workflows/*.yml` file
- Any file under `npm/`
