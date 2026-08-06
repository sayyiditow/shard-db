{
  "targets": [
    {
      "target_name": "shard_db",
      "sources": [
        "src/binding.c",
        "../src/db/util.c",
        "../src/db/durability.c",
        "../src/db/config.c",
        "../src/db/type_desc.c",
        "../src/db/storage.c",
        "../src/db/index.c",
        "../src/db/query.c",
        "../src/db/query_aggregate.c",
        "../src/db/query_join.c",
        "../src/db/query_plan.c",
        "../src/db/query_maint.c",
        "../src/db/query_schema.c",
        "../src/db/query_bulk.c",
        "../src/db/query_find.c",
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
        "../src/db/nql.c",
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
