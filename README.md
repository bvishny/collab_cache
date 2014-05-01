Collab Cache
============

A cache designed for values expensive to generate. Any number of Goroutines can simultaneously request a value, passing in a function that will be used to generate said value if it does not exist. Only the first Goroutine will have its function called, preventing duplicate work. The other Goroutines will block until the call completes. This cache also supports TTLs, and Stale Times. Values will be refreshed in the background when less than half of the original TTL is remaining - assuming that the value is still being requested. 

You can create a new CollabCache as follows:

    NewCollabCache(quitChannel chan bool) *CollabCache

__quitChannel__ is a _chan bool_ that should be closed when your program quits. This ensures that the separate Goroutine used to delete expired cache items shuts down along with your program. 

CollabCache consists of a single call:

    GetOrPublish(key string, ttl int64, generatorFunc func(string) interface{}, timeout int64) interface{}

__key__: The key requested

__ttl__: A TTL in Milliseconds

__generatorFunc__: A function that given a key name returns an interface value

__timeout__: The max amount of time generatorFunc can take before it is assumed to have failed. Another Goroutine will then attempt to generate the value.

GetOrPublish is a blocking call that always returns some interface value. When GetOrPublish is called on a value that is stale but not expired, the value will be refreshed in the background and the stale value will be returned immediately. 


