# A simple memory-only key-value store implementation

**Communication protocol: Redis RESP protocol**

**Usage:** cache<br>
  -p port (default is 6379)<br>
  -c (client mode)<br>
  -v (verbose)<br>
  -k number of keys for benchmark (default us 50000)<br>
  -h host for client to connect (default is 127.0.0.1)<br>
  -b (benchmark mode)<br>
  -r number of requests per thread for benchmark (default is 50000)<br>
  -m maximum memory for server (default is 1GB)<br>
  -t request types for benchmark (possible values - get,set,setpx,ping, default: get,set,get,setpx)<br>
  --nx key expiration in ms for benchmark (default is 100 ms)<br>
  --th number of threads for benchmark (default is 10)<br>
  --km numer of key maps (default 256)<br>
  --hb hash builder type (default sum)<br>

**Only a few Redis commands are implemented:**

1. ping
2. get
3. set key value
4. set key value ex expiry
5. set key value px expiry
6. flushdb
7. flushall
8. del
9. dbsize
10. select db_number (db_number parameter is ignored) - application supports only one db.
11. config get save -> always returns ""
12. config get appendonly -> always returns "no"

**Application can be started in the following modes:**
1. Server mode 
2. Client mode (with -c switch)
3. Benchmark mode (with -b switch)

**In benchmark mode the following server commands can be used:** (key is a random number between 0 and number of keys converted to string)
1. get key
2. set key value nx expiration_in_ms (value always = key)

**Current benchmark results on my laptop:**

**1. Redis server 7.0.11 64 bit**<br>
Command: redis-benchmark -t get,set -q -n 1000000<br>
SET: 178284.89 requests per second, p50=0.143 msec                    
GET: 179179.36 requests per second, p50=0.143 msec

**2. This application**<br>
Command: redis-benchmark -t get,set -q -n 1000000<br>
SET: 109914.27 requests per second, p50=0.255 msec                    
GET: 109337.41 requests per second, p50=0.255 msec

**3. Redis server 7.0.11 64 bit**<br>
Command: cache -b -v<br>                  
Port = 6379<br>
Host = 127.0.0.1<br>
Keys= 50000<br>
Requests per thread = 50000<br>
Threads = 10<br>
Expiration = 100 ms<br>
Request types = get,set,get,setpx<br>
Preparing data for tests...<br>
36 ms Starting tests...<br>
Elapsed: 4095 ms, 122100 requests per second 0 errors

**4. This application**<br>
Command: cache -b -v<br>                  
Port = 6379<br>
Host = 127.0.0.1<br>
Keys= 50000<br>
Requests per thread = 50000<br>
Threads = 10<br>
Expiration = 100 ms<br>
Request types = get,set,get,setpx<br>
Preparing data for tests...
30 ms Starting tests...
Elapsed: 3520 ms, 142045 requests per second 0 errors
