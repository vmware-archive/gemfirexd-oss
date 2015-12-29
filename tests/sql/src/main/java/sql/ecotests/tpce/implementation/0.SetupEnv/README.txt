1. Start the GemFireXD DS with 1 locator + 4 data servers
2. Join a data server to off-load some data from other servers (see the ddl replay error in joined server)
3. Change the GemFireXD DS to various topologies (WAN, DBSynchronizer etc) and configurations (heap size, persistence etc)