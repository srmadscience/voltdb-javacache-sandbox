<img title="Volt Active Data" alt="Volt Active Data Logo" src="http://52.210.27.140:8090/voltdb-awswrangler-servlet/VoltActiveData.png?repo=voltdb-javacache-sandbox">


# voltdb-javacache-sandbox

voltdb-javacache-sandbox is a Proof-Of-Concept implementation of a [JSR107](https://github.com/jsr107/jsr107spec) compliant Java Cache.

it also includes a sandbox application so you can see it working.


## Description

This project has a class called [VoltDBCache](src/org/voltdb/jsr107/VoltDBCache.java) which implements [javax.cache.Cache](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java). 

This API is used as a basis for multiple implementations:

* Hazelcast / GridGain
* Oracle Coherence
* Terracotta Ehcache
* Infinispan

VoltDBCache doesn't actually cache anything. Instead it's a cunningly disguised API for VoltDB.

Internally data is stored in a VoltDB Table:

    CREATE TABLE kv 
    (c varchar(30) not null 
    ,k varchar(128) not null 
    ,v varbinary(1048576)
    ,primary key (c, k));

    PARTITION TABLE kv ON COLUMN k;


* 'c' is the name of the cache, as we assume you might want more than one cache and don't want to share a namespace.
* 'k' is the key. We assume it's a string
* 'v' is a long var binary (byte[])

We also have an output stream that is connected to a Kafka Topic:

    CREATE STREAM kv_deltas 
    PARTITION ON COLUMN k 
    (c varchar(30) not null 
    ,k varchar(128)  not null
    ,v varbinary(1048576)
    ,event_type varchar(1));
    
    CREATE TOPIC USING STREAM kv_deltas
     PROPERTIES (consumer.keys='k');

This is used by our implementation of [CacheEventConsumer](src/org/voltdb/jsr107/CacheEventConsumer.java)

## Sandbox Usage



## General Usage

    org.voltdb.jsr107.VoltDBCache.VoltDBCache(String hostnames, int retryAttempts, String cacheName, String entryProcessorDirName, String entryProcessorPackageName, int kafkaPort)
    
    Parameters:
    hostnames comma delimited list of hostnames that make up the VoltDB cluster
    retryAttempts How many times we try to speak to VoltDB before giving up. Exponential backoff is in use.
    cacheName name of our cache.
    entryProcessorDirName If we are using Invoke this is the physical directory the .class files live in.
    entryProcessorPackageName If we are using Invoke this is the package name our Invokeable classes use
    kafkaPort - kafka port number on VoltDB, usually 9092

For example:

    VoltDBCache mycache = new VoltDBCache("localhost", 10, "MYCACHE",
                "/Users/drolfe/Desktop/EclipseWorkspace/voltdb-javacache/bin", "jsr107.test", 9092);

Once you have a cache you can use all the methods defined in [javax.Cache](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java)

## Interesting bits

### 'all' methods

For 'all' methods we launch each request individually and the use a callback to count and manage the responses

### 'invoke' methods

For 'invoke' methods we load the class files containing the EntryProcessor code into VoltDB prior to execution.

### Event Listener

We use VoltDB's topics to implement the Event Listeners.

## Known Limitations

### Sync operations

The API is fundamentally sync in nature, whereas VoltDB is fundamentally async. We may extend the base Cache classes in future to resolve this. It is interesting to note that several implementers of Cache added their own async extensions. 

### HA and group methods

By default the Cache will make as many attempts as it needs to to communicate with VoltDB. In the current implementation this is not guaranteed for:

* [getAll](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java#L121) 
* [putAll](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java#L278)
* [invoke](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java#L603)
* [invokeAll](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/Cache.java#L640)

### invoke and invokeAll

These methods assume code that implements [EntryProcessor](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/processor/EntryProcessor.java) has been loaded onto the server. This is not done automatically - you need to call the non-API method 'loadEntryProcessors()' first. 

### iterator will fail if asked to return > 50MB of data

the iterator() method attempts to unload the entire contents of the Cache into a data structure, which is not a great idea if you are using VoltDB. It will work on small caches but throw a 'Too Much Data Requesrted' CacheException otherwise.

### EntryProcessor

Any class that implements EntryProcessor can be used by Invoke,  but implementors need to remember that it runs inside VoltDB, possibly more than once at the same time, so:

* It must be deterministic in nature. No random behavior, network activity etc.
* It needs to be reasonably fast.
* It needs to exist as a stand alone class file in the directory whose name is passed in to VoltDBCache at creation time
* All EntryProcessors need to be in the package whose name is passed in to VoltDBCache at creation time
* It must use the org.voltdb.autojar.IsNeededByAVoltDBProcedure annotation.

Making updates and returning data:

* Changes are made by manipulating the [MutableEntry](https://github.com/jsr107/jsr107spec/blob/master/src/main/java/javax/cache/processor/MutableEntry.java) passed in as a parameter.
* An implementation may return either null or VoltTable[], which will be sent back to the client.

Error Handling:

* Implementors of EntryProcessor should always check the status code to see what happened. An example of this is in the [sandbox code](https://github.com/srmadscience/voltdb-javacache-sandbox/blob/main/demoSrc/org/voltdb/jsr107/sandbox/CacheSandboxThread.java#L250).
* The two normal results are [AbstractEventTrackingProcedure.OK](https://github.com/srmadscience/voltdb-javacache-sandbox/blob/main/serverSrc/jsr107/AbstractEventTrackingProcedure.java#L73) and [AbstractEventTrackingProcedure.OK_BUT_NOT_FOUND](https://github.com/srmadscience/voltdb-javacache-sandbox/blob/main/serverSrc/jsr107/AbstractEventTrackingProcedure.java#L74), which occurs when you tried to call invoke on a non-existent record.
* Other [responses](https://github.com/srmadscience/voltdb-javacache-sandbox/blob/main/serverSrc/jsr107/AbstractEventTrackingProcedure.java#L75) are errors, and in each case the Java stack trace will be [returned](https://github.com/srmadscience/voltdb-javacache-sandbox/blob/main/serverSrc/jsr107/Invoke.java#L102) in a VoltTable.


## Next Steps

Future work might involve going beyond the canonical JSR107 implementation:

* We're looking at an async extension
* We're looking at pessimistic locking. We have lots of customers who already do thjis

