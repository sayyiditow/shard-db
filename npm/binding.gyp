{
  "targets": [
    {
      "target_name": "shard_db",
      "sources": [
        "src/binding.c",
        "../src/db/util.c",
        "../src/db/config.c",
        "../src/db/storage.c",
        "../src/db/index.c",
        "../src/db/query.c",
        "../src/db/server.c",
        "../src/db/btree.c",
        "../src/db/objlock.c",
        "../src/db/keyset.c",
        "../src/db/parallel.c",
        "../src/db/slotcask.c",
        "../src/db/simd.c",
        "../src/db/io_direct.c",
        "../src/db/bitmap.c",
        "../src/db/trigram.c",
        "../src/db/tls_stub.c",
        "../src/db/embedded.c"
      ],
      "include_dirs": [
        "../src/db"
      ],
      "cflags": [
        "-O2",
        "-std=c11",
        "-fno-strict-aliasing",
        "-D_GNU_SOURCE",
        "-DNAPI_VERSION=8",
        "-DEMBED_NO_TLS"
      ],
      "cflags!": [],
      "conditions": [
        ["OS=='linux'", {
          "libraries": [ "-lpthread", "-latomic" ]
        }],
        ["OS=='mac'", {
          "libraries": [],
          "xcode_settings": {
            "OTHER_CFLAGS": [ "-O2", "-std=c11", "-fno-strict-aliasing", "-D_GNU_SOURCE", "-DNAPI_VERSION=8", "-DEMBED_NO_TLS" ]
          }
        }]
      ]
    }
  ]
}
