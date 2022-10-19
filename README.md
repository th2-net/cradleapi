# Overview

Cradle API is used to work with Cradle - the datalake where th2 stores its data.

Cradle API has implementation for Apache Cassandra, wrapping interaction with this database and adding Cradle-specific data processing.

# Getting started

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
into the root directory of your project. The structure of this file is described in https://github.com/datastax/java-driver/blob/4.x/core/src/main/resources/reference.conf

Example of Cradle API initialization to work with Cassandra:
```java
CassandraConnectionSettings connectionSettings = new CassandraConnectionSettings(CASSANDRA_HOST, CASSANDRA_PORT, DATACENTER);
connectionSettings.setUsername(CASSANDRA_USERNAME);
connectionSettings.setPassword(CASSANDRA_PASSWORD);

CassandraStorageSettings storageSettings = new CassandraStorageSettings();
storageSettings.setKeyspace(CASSANDRA_KEYSPACE);
storageSettings.setResultPageSize(FETCH_PAGE_SIZE);

CradleManager manager = new CassandraCradleManager(connectionSettings, storageSettings, true);
CradleStorage storage = manager.getStorage();

// Operations on cradle can be performed using storage object.
Collection<BookInfo> books = storage.getBooks();

// After completing work with cradle you should close manager
manager.close();
```

`CassandraConnectionSettings` object is used to define Cassandra host to connect to, username and password to use and other connection settings.

`CassandraCradleManager` will establish the connection when constructed.

Once initialized, `CradleStorage` can be used to write/read data:
```java
String bookName = "new_test_book";
String groupName = "group1";
Direction direction = Direction.FIRST;
long seq = 4588290;

// Writing a message batch
// Messages in the batch must be from the same book, session alias and direction can be mixed
MessageToStore m1 = new MessageToStoreBuilder()
                        .bookId(new BookId(bookName))
                        .sessionAlias("session1")
                        .direction(Direction.FIRST)
                        .timestamp(Instant.now())
                        .sequence(4588290)
                        .content("Message from code example".getBytes(StandardCharsets.UTF_8))
                        .build();
MessageToStore m2 = new MessageToStoreBuilder()
                        .bookId(new BookId(bookName))
                        .sessionAlias("session2")
                        .direction(Direction.SECOND)
                        .timestamp(Instant.now())
                        .sequence(29098023)
                        .content("Yet another message from code example".getBytes(StandardCharsets.UTF_8))
                        .build();

GroupedMessageBatchToStore batch = storage.getEntitiesFactory().groupedMessageBatch(groupName);
batch.addMessage(m1);
batch.addMessage(m2);
storage.storeGroupedMessageBatch(batch);

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

# Data in Cradle

th2 stores data about messages it sends and receives and test events generated during the work.

Test events form a hierarchy that builds a test execution report.

Test events and messages can be linked if, for example, a message was verified during a test event. The links are stored in Cradle as well.

IDs for stored data are generated outside of Cradle and are supplied with the objects being stored.

## Messages

Messages are stored in batches, i.e. if multiple messages arrive in a short period of time, they can be put in a batch and saved as one record in Cradle. Or you can have one message per batch.

Each message has an ID that consists of `stream_name`:`direction`:`message_index`.

Stream name is similar to session alias, i.e. is a name for a pair of connected endpoints that exchange messages.

Direction is "first" or "second" depending on endpoint that generated the message.

Message index is a number, incremented for each new message within the same stream and direction.

I.e. if for the stream name="stream1" and direction="first" the last message index was 10, the next message index for this stream name and direction is expected to be 11. It can be different, but greater than 10.

Messages can have metadata as a set of key-value string pairs, providing additional details about the message. Metadata cannot be used in any search requests or filtering.

## Test events

Test events in Cradle can be stored separately or in batches, if an event has complex hierarchical structure.

A test event can have a reference to its parent, thus forming a hierarchical structure. Events that started the test execution have no parent and are called "root test events".

Events in a batch can have a reference only to the parent of the batch or other test events from the same batch. Events outside of the batch should not reference events within the batch.

Test events have mandatory parameters that are verified when storing an event. These are: id, name (for non-batch events), start timestamp.
