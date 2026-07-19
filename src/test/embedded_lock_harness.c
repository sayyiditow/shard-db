#include "shard_db.h"

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    ShardDb *db = shard_db_open(argv[1]);
    if (!db) return 0;
    shard_db_close(db);
    return 1;
}
