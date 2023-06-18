# A toy memory-only key-value store implementation

Communication protocol: Redis RESP protocol

Only a few Redis commands are implemented:

1. ping
2. get
3. set key value
4. set key value ex expiry
5. dbsize
6. select db_number (db_number parameter is ignored) - application supports only one db.
7. config get save -> always returns ""
8. config get appendonly -> always returns "no"
