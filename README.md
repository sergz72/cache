# A toy memory-only key-value store implementation

Communication protocol: Redis RESP protocol

Usage: cache
  -p port (default is 6379)
  -c (client mode)
  -v (verbose)
  -k number of keys for benchmark (default us 50000)
  -h host for client to connect (default is 127.0.0.1)
  -b (benchmark mode)
  -r number of requests per thread for benchmark (default is 50000)
  -m maximum memory for server (default is 1GB, check is not implemented yet)
  -t request types for benchmark (possible values - get,set, default: get,set)
  --nx key expiration in ms for benchmark (default is 100 ms)
  --th number of threads for benchmark (default is 10)

Only a few Redis commands are implemented:

1. ping
2. get
3. set key value
4. set key value ex expiry
5. set key value nx expiry
6. flushdb
7. flushall
8. del
9. dbsize
10. select db_number (db_number parameter is ignored) - application supports only one db.
11. config get save -> always returns ""
12. config get appendonly -> always returns "no"

Application can be started in the following modes:
1. Server mode 
2. Client mode (with -c switch)
3. Benchmark mode (with -b switch)

