# Cradle API (2.21.0)
## Overview

Cradle API is used to work with Cradle - the datalake where th2 stores its data.

Cradle API has implementation for Apache Cassandra, wrapping interaction with this database and adding Cradle-specific data processing.

## Getting started

To build Cradle API binaries you will need to have JDK 8 or higher.

Clone the project to some directory, navigate to that directory and execute the following command:
```
$ ./gradlew clean build publishToMavenLocal
```

The binaries will be published to your local Maven repository which can be added to your Gradle project:
```
repositories {
	mavenLocal()
	...
}
```

Alternatively, you can use pre-built artifacts by adding the following repository to your Gradle project:
```
repositories {
	maven {
		name 'Sonatype_releases'
		url 'https://s01.oss.sonatype.org/content/repositories/releases/'
	}
	...
}
```

To use Cradle API, add the following dependency to your project:
```
dependencies {
	implementation 'com.exactpro.th2:cradle-cassandra:2.12.0'
	...
}
```

Or other Cradle API version you need to use.

Once the dependency is resolved, Cradle API classes will become available.

The main classes are `CradleManager` and `CradleStorage`.

`CradleManager` initializes Cradle API to work with particular database and provides access to `CradleStorage` object bound to that database.

Cradle API uses DataStax Java driver to connect to Cassandra. To manage the additional driver settings you can put application.conf file
into the root directory of your project. The structure of this file is described in https://github.com/datastax/java-driver/blob/4.0.1/core/src/main/resources/reference.conf

Example of Cradle API initialization to work with Cassandra:
```java
CassandraConnectionSettings settings = new CassandraConnectionSettings("datacenter1", "cassandra-host", 9042, "cassandra-keyspace");
settings.setUsername("cassandra-username");
settings.setPassword("cassandra-password");

CassandraConnection connection = new CassandraConnection(settings);

CradleManager manager = new CassandraCradleManager(connection);
manager.init("instance1");

CradleStorage storage = manager.getStorage();
```

`CassandraConnectionSettings` object is used to define Cassandra host to connect to, username and password to use and other connection settings.

`CassandraCradleManager` will establish the connection when `init()` method is called. Parameter of `init()` method ("instance1") is a name of Cradle instance to use when writing/reading data. It is used to divide data within one database (in case of Cassandra, within one Cassandra keyspace). So, if multiple applications/services need to work with the same data in Cradle, they should use the same instance name.

Once initialized, `CradleStorage` can be used to write/read data:
```java
String streamName = "stream1";
Direction direction = Direction.FIRST;
Instant now = Instant.now();
long index = now.toEpochMilli();

//Writing a message
StoredMessageBatch batch = new StoredMessageBatch();
batch.addMessage(new MessageToStoreBuilder().streamName(streamName).direction(direction).index(index).timestamp(now)
		.content("Message1".getBytes()).build());
storage.storeMessageBatch(batch);

//Reading messages by filter
StoredMessageFilter filter = new StoredMessageFilterBuilder()
		.streamName().isEqualTo(streamName)
		.direction().isEqualTo(direction)
		.limit(100)
		.build();
for (StoredMessage msg : storage.getMessages(filter)) {
	System.out.println(msg.getId()+" - "+msg.getTimestamp());
}

//Writing a test event
TestEventToStore event = new TestEventToStoreBuilder().id(new StoredTestEventId(UUID.randomUUID().toString()))
		.name("Test event 1").startTimestamp(now).content("Test content".getBytes()).build();
storage.storeTestEvent(StoredTestEvent.newStoredTestEventSingle(event));

//Reading a test event
StoredTestEventWrapper storedEvent = storage.getTestEvent(event.getId());
System.out.println(storedEvent.getName()+" - "+storedEvent.getStartTimestamp());
```

## Data in Cradle

th2 stores data about messages it sends and receives and test events generated during the work.

Test events form a hierarchy that builds a test execution report.

Test events and messages can be linked if, for example, a message was verified during a test event. The links are stored in Cradle as well.

IDs for stored data are generated outside of Cradle and are supplied with the objects being stored.

### Messages

Messages are stored in batches, i.e. if multiple messages arrive in a short period of time, they can be put in a batch and saved as one record in Cradle. Or you can have one message per batch.

Each message has an ID that consists of `stream_name`:`direction`:`message_index`.

Stream name is similar to session alias, i.e. is a name for a pair of connected endpoints that exchange messages.

Direction is "first" or "second" depending on endpoint that generated the message.

Message index is a number, incremented for each new message within the same stream and direction.

I.e. if for the stream name="stream1" and direction="first" the last message index was 10, the next message index for this stream name and direction is expected to be 11. It can be different, but greater than 10.

Messages can have metadata as a set of key-value string pairs, providing additional details about the message. Metadata cannot be used in any search requests or filtering.

### Test events

Test events in Cradle can be stored separately or in batches, if an event has complex hierarchical structure.

A test event can have a reference to its parent, thus forming a hierarchical structure. Events that started the test execution have no parent and are called "root test events".

Events in a batch can have a reference only to the parent of the batch or other test events from the same batch. Events outside of the batch should not reference events within the batch.

Test events have mandatory parameters that are verified when storing an event. These are: id, name (for non-batch events), start timestamp.

## Release notes

### 2.21.0

+ Replaced recursive calls in `MessagesIterator` with loop
+ Added table `messages_timestamps` instead `time_messages`
+ Lower the logging level for gaps in message sequences in `StoredMessageBatch`
+ Apply max event batch size from settings to the batch which read from cradle
+ Added check for timestamp of message when added to batch

### 2.20.2

+ Added retries for methods `getMessage()`, `getNearestMessageId()`, `getTestEventsDates()` and `getRootEventsDates()`
+ Applied timeout value from storage settings to the initial query

### 2.20.1

+ Implemented delay between retrying queris
+ Implemented retries with `FixedNumberRetriesPolicy` for single row result queries
+ More efficient message filtering by timestamp
+ Upgraded datastax/java-driver to ver.`4.6.1`

### 2.19

+ Added methods `getFirstMessageIndex(String streamName, Direction direction) and getFirstProcessedMessageIndex(String streamName, Direction direction)`
+ Applied read attributes to query that receive events with body by set of id
+ Fixed bug with "Invalid unset value for column message_index" exception when there is no session alias in database
+ Applying a timeout from the configuration to the keyspace create query avoids these issues

### 2.18

+ Fixed unlikely, but possible situation with dead-locking Cassandra Driver
+ Implemented `AdjustSizeRetryPolicy` for query is retried with a smaller requested result size

### 2.17

+ Fixed a bug which caused an unexpected message batch to be obtained when the expected one is absent in Cradle
+ Replaced RecoveryState object that Crawlers operate with a String so that Crawlers can implement their own format. Usually, it is JSON

### 2.16

_Note: Cradle API 2.16 is preferable against this one because some methods are confusing and excessive in 2.15 and are deprecated in 2.16_
+ Added the ability to obtain messages and test events in reverse order: specify `Order.DIRECT` or `Order.REVERSE` in corresponding methods to get data in needed order. StoredMessageFilter now has an order to define order to the returned messages.
+ Deprecated confusing and excessive methods from 2.15

### 2.15

+ Added the ability to obtain messages and test events in reverse order: specify Order.DIRECT or Order.REVERSE in corresponding methods to get data in needed order.
+ StoredMessageFilter now has an order to define order to the returned messages.

### 2.14

+ New methods in CradleStorage:
  + `getIntervalsWorker()` - returns object to work with intervals, i.e. to obtain data about processed intervals, store data about new/updated intervals, etc. 
  + `updateEventStatus()` and updateEventStatusAsync() to change status (success=true/false) of an event.
+ **In this version the link between message and test event was switched off, only the link between test event and message is stored. i.e. you can only get it from Cradle if a test event has linked messages, but it’s not possible to know if a message is linked to any test event.**

### 2.13

+ New methods in CradleStorage:
  + `getMessagesBatches(StoredMessageFilter):Iterable<StoredMessageBatch>` and `getMessagesBatchesAsync(StoredMessageFilter):CompletableFuture<Iterable<StoredMessageBatch>>
 that allow to iterate over message batches directly.
    
### 2.12

+ Addded support for  Cassandra Java Driver configuration (https://github.com/datastax/java-driver/blob/4.0.1/core/src/main/resources/reference.conf)

### 2.11

+ Implemented the `getCompleteTestEvents()` and `getCompleteTestEventsAsync()` methods that accepts a list of ids

### 2.10

+ Implemented method for asynchronously retrieve ID of first message appeared in given timestamp or before/after it `getNearestMessageIdAsync()`

### 2.9

+ Implemented compression of batch events metadata after serialization.

### 2.8

+ Implemented `resultPageSize` setting in `CassandraConnectionSettings` to define the size of the result set to fetch at a time.

### 2.7

+ Implemented methods in CradleStorage to asynchronously store test events and their links with messages: `storeTestEventAsync`, `storeTestEventMessagesLinkAsync`;
+ Added methods to `TestEventsMessagesLinker` to asynchronously get IDs of messages linked to a given test event and vice versa;
+ Fixes previously implemented asynchronous methods that might cause timeout exceptions while working with large number of records in Cassandra.

### 2.6.1

+ Removed batch limits by number of messages or test events, i.e. now `StoredMessageBatchand` and `StoredTestEventBatch` can contain any number of records
+ Added parameters to `CradleManager.init()` and `CradleManager.initStart()` to define maximum batch sizes in bytes

### 2.5

+ Implemented flag for `CradleManaget.init()` and `CradleStorage.init()` to indicate if database should be created/updated, if needed. Set to "false" by default.

### 2.4

+ Implemented method to get last processed message index - `getLastProcessedMessageIndex()`

### 2.3

+ Implemented `toString()` for `StoredMessageFilter` and its parts
+ Fixed the calculation of message number on the left of a given one in current batch
+ Postponed event batch metadata deserialization

### 2.2

+ Fix for the case when "less or equals" comparison is used in filter. Previously, `getMessages()` method could not return enough messages than actually were available in Cradle
+ CradleStorage now has methods to store and obtain messages asynchronously:
  + `storeMessageBatchAsync()`
  + `storeProcessedMessageBatchAsync()`
  + `getMessageAsync()`
  + `getMessageBatchAsync()`
  + `getProcessedMessageAsync()`
  + `getMessagesAsync()`
    
### 2.1

+ Changed group ID _from com.exactpro.cradle_ to _com.exactpro.th2_

### 1.7

+ Implemented asynchronous API for part of the queries. API supports up to 1024 simultaneous queries to Cassandra. This is managed by MaxParallelQueries setting of CassandraConnectionSettings. The default value is 500
+ New methods:
  + getTestEventAsync
  + getRootTestEventsAsync 
  + getTestEventsAsync by parent and time
  + getTestEventsAsync by time