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
  -m maximum memory per database (default is 1GB)<br>
  -t request types for benchmark (possible values - get,set,setpx,ping, default: get,set,get,setpx)<br>
  --nx key expiration in ms for benchmark (default is 100 ms)<br>
  --th number of threads for benchmark (default is 10)<br>
  --km numer of key maps (default 256)<br>
  --hb hash builder type (default sum)<br>
  --lru (cleanup using lru)<br>
  --maxdb maximum number of open databases (default 10)<br>

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
10. hset key key1 value1 [key2 value2]...
11. hget key map_key
12. hgetall key
13. hdel key map_key1 [map_key2]...
14. select db_name - **db_name can be string**.
15. config get save -> always returns ""
16. config get appendonly -> always returns "no"
17. save - saves ONLY CURRENT db to xzipped file with name = database name 

**Non standard commands**
1. createdb db_name -> creates db if it does not exist, returns an error otherwise
2. loaddb db_name -> tries to load db from file if required, returns an error if file does not exist

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
Elapsed: 4431 ms, 112841 requests per second 0 errors

**4. This application**<br>
Command: cache -b -v<br>                  
Port = 6379<br>
Host = 127.0.0.1<br>
Keys= 50000<br>
Requests per thread = 50000<br>
Threads = 10<br>
Expiration = 100 ms<br>
Request types = get,set,get,setpx<br>
Elapsed: 3664 ms, 136462 requests per second 0 errors
