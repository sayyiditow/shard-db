/* src/db/test_control.h
 *
 * TEST_BUILD-only control channel for the deterministic single-partial-update
 * regression seam. Compiled only into the shard-db-test-server daemon
 * (never the production shard-db, embedded library, CLI, benchmark, or test
 * runner). The control thread talks to the test runner over an inherited
 * anonymous Unix socketpair and installs/clears the slotcask pause hook
 * inside the daemon process.
 */
#ifndef SHARD_DB_TEST_CONTROL_H
#define SHARD_DB_TEST_CONTROL_H

#ifdef TEST_BUILD
int shard_db_test_control_start(int fd);
void shard_db_test_control_stop(void);
#endif

#endif
