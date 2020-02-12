# reactive-kinesis [![Build Status](https://travis-ci.org/WW-Digital/reactive-kinesis.svg?branch=master)](https://travis-ci.org/WW-Digital/reactive-kinesis) [![Coverage Status](https://coveralls.io/repos/github/WW-Digital/reactive-kinesis/badge.svg?branch=master)](https://coveralls.io/github/WW-Digital/reactive-kinesis?branch=master)

Kinesis client built upon Amazon's KCL ([Kinesis Client Library](http://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)) & KPL ([Kinesis Producer Library](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html)).

It's worth familiarising yourself with [Sequence numbers and Sub sequence numbers](http://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-consumer-deaggregation.html).

# Contents
* [Dependency Resolution](#dependency-resolution)
* [Considerations When Using Kinesis in a Distributed Environment](#considerations-when-using-kinesis-in-a-distributed-environment)
    * [Required Minimum Sharding for Read-Based Applications](#considerations-when-using-kinesis-in-a-distributed-environment-required-minimum-sharding-for-read-based-applications)
    * [DynamoDB Checkpoint Storage](#considerations-when-using-kinesis-in-a-distributed-environment-dynamodb-checkpoint-storage)
* [Usage](#usage)
    * [Kinesis streams and auth](#usage-kinesis-streams-and-auth)
    * [Defining a config file in the client application](#usage-defining-a-config-file-in-the-client-application)
        * [Notable Consumer Configuration Values](#usage-defining-a-config-file-in-the-client-application-notable-consumer-configuration-values)
        * [Notable Producer Configuration Values](#usage-defining-a-config-file-in-the-client-application-notable-producer-configuration-values)
        * [Typed Configuration - Producer](#typed-configuration---producer)
    * [Usage: Consumer](#usage-usage-consumer)
        * [Actor Based Consumer](#actor-based-consumer)
            * [Important considerations when implementing the Event Processor](#usage-usage-consumer-important-considerations-when-implementing-the-event-processor)
            * [Checkpointing](#usage-usage-consumer-checkpointing)
        * [Akka Stream Source](#akka-stream-source)
        * [Graceful Shutdown](#usage-usage-consumer-graceful-shutdown)
    * [Usage: Producer](#usage-usage-producer)
        * [Actor Based Implementation](#usage-usage-producer-actor-based-implementation)
        * [Pure Scala based implementation (simple wrapper around KPL)](#usage-usage-producer-pure-scala-based-implementation-simple-wrapper-around-kpl)
        * [Akka Stream Sink](#akka-stream-sink)
* [Running the reliability test](#running-the-reliability-test)
    * [Delete & recreate kinesisstreams and dynamo table](#running-the-reliability-test-delete-recreate-kinesisstreams-and-dynamo-table)
    * [Running the producer-consumer test](#running-the-reliability-test-running-the-producer-consumer-test)
* [FAQ](#faq)
* [Contributor Guide](#contributor-guide)
    * [Code Formatting](#contributor-guide-code-formatting)
    * [Integration Tests](#contributor-guide-integration-tests)
    * [Tag Requirements](#contributor-guide-tag-requirements)
        * [Version information](#contributor-guide-tag-requirements-version-information)
        * [Valid Release Tag Examples:](#contributor-guide-tag-requirements-valid-release-tag-examples)
        * [Invalid Release Tag Examples:](#contributor-guide-tag-requirements-invalid-release-tag-examples)
* [Contribution policy](#contribution-policy)
* [Changelog](#changelog)
* [License](#license)


<a name="dependency-resolution"></a>
# Dependency Resolution
SBT
```
"com.weightwatchers" %% "reactive-kinesis" % "0.5.5"
```

Maven 2.11
```
<dependency>
  <groupId>com.weightwatchers</groupId>
  <artifactId>reactive-kinesis_2.11</artifactId>
  <version>0.5.5</version>
  <type>pom</type>
</dependency>
```

Maven 2.12
```
<dependency>
  <groupId>com.weightwatchers</groupId>
  <artifactId>reactive-kinesis_2.12</artifactId>
  <version>0.5.5</version>
  <type>pom</type>
</dependency>
```

Snapshots will be published [here](https://oss.jfrog.org/webapp/#/artifacts/browse/simple/General/oss-snapshot-local/com/weightwatchers)

You will need the following resolver for snapshots:
`https://oss.jfrog.org/artifactory/oss-snapshot-local`

<a name="considerations-when-using-kinesis-in-a-distributed-environment"></a>
# Considerations When Using Kinesis in a Distributed Environment

<a name="considerations-when-using-kinesis-in-a-distributed-environment-required-minimum-sharding-for-read-based-applications"></a>
## Required Minimum Sharding for Read-Based Applications

From http://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-scaling.html:

> Typically, when you use the KCL, you should ensure that the number of instances
> does not exceed the number of shards (except for failure standby purposes).
> Each shard is processed by exactly one KCL worker and has exactly one corresponding
> record processor, so you never need multiple instances to process one shard. However,
> one worker can process any number of shards, so it's fine if the number of shards
> exceeds the number of instances.

For our purposes, this means *any service reading from Kinesis should expect to
have one shard per instance, as a minimum*. Note that this is specifically for consuming events.
Producers don't have the same shard restrictions.

<a name="considerations-when-using-kinesis-in-a-distributed-environment-dynamodb-checkpoint-storage"></a>
## DynamoDB Checkpoint Storage

Amazon's KCL uses DynamoDB to checkpoint progress through reading the stream.  When DynamoDB tables are provisioned automatically, for this purpose, they may have a relatively high write-throughput, which can incur additional cost.

You should make sure that the DynamoDB table used for checkpointing your stream

1. Has a reasonable write throughput defined

2. Is cleaned up when you're done with it -- KCL will not automatically delete it for you

The checkpointer will automatically throttle if the write throughput is not sufficient, look out for the following info log:

`Throttled by DynamoDB on checkpointing -- backing off...`

<a name="usage"></a>
# Usage

<a name="usage-kinesis-streams-and-auth"></a>
## Kinesis streams and auth

The stream you have configured must already exist in AWS, e.g.

```
aws kinesis create-stream --stream-name core-test-foo --shard-count 1
```

For local development, it's expected that you already have a file called `~/.aws/credentials`,
which contains an AWS access key and secret e.g.
```
[default]
aws_access_key_id=AKIAXXXXXXXXX999999X
aws_secret_access_key=AAAAAAAAAAAA000000000+AAAAAAAAAAAAAAAAAA
```

Both the producer and consumer use the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/java-sdk/latest/developer-guide/credentials.html#id6).


<a name="usage-defining-a-config-file-in-the-client-application"></a>
## Defining a config file in the client application


You'll need some configuration values provided in the application which leverages this library,
As a minimum you'll need:

```
kinesis {

   application-name = "SampleService"

   # The name of the this producer, we can have many producers per application.
   # MUST contain the stream-name as a minimum. Any additional settings defined will override
   # defaults in the kinesis.producer reference.conf for this producer only.
   some-producer {
      # The name of the producer stream
      stream-name = "sample-producer"

      kpl {
         Region = us-east-1
      }
   }

   # The name of the consumer, we can have many consumers per application
   some-consumer {
      # The name of the consumer stream, MUST be specified per consumer and MUST exist
      stream-name = "sample-consumer"
   }

   some-other-consumer {
      stream-name = "another-sample-consumer"
   }
}
```

These values will override the default reference.conf.
See `src/main/resources/reference.conf` for a complete reference configuration.
and `src/it/resources/application.conf` for a more detailed override example.

The name of the producer/consumer configuration value MUST match what is specified when instantiating the library.
This will be merged with the `default-consumer`/`default-producer` from `reference.conf`configuration at runtime.

Once these are defined, you can pass them into the Kinesis producer and consumer using a config object (see code examples below).

Note that the `application-name` is combined with the `stream-name` for each consumer to define the DynamoDB table for checkpointing.
For example: `SampleService-sample-consumer`.

<a name="usage-defining-a-config-file-in-the-client-application-notable-consumer-configuration-values"></a>
### Notable Consumer Configuration Values
* `kinesis.<consumer-name>.akka.dispatcher` - Sets the dispatcher for the consumer, defaults to `kinesis.akka.default-dispatcher`
* `kinesis.<consumer-name>.worker.batchTimeoutSeconds` - The timeout for processing a batch.
Note that any messages not processed within this time will be retried (according to the configuration). After retrying any
unconfirmed messages will be considered failed.
* `kinesis.<consumer-name>.worker.failedMessageRetries` - The number of times to retry failed messages within a batch, after the batch timeout.
* `kinesis.<consumer-name>.worker.failureTolerancePercentage` - If, after retrying, messages are still unconfirmed.
We will either continue processing the next batch, or shutdown processing completely depending on this tolerance percentage.
* `kinesis.<consumer-name>.checkpointer.backoffMillis` - When DynamoDB throttles us (due to hitting the write threshold)
we wait for this amount of time.
* `kinesis.<consumer-name>.checkpointer.intervalMillis` - The interval between checkpoints. Setting this too high will cause lots of
 messages to be duplicated in event of a failed node. Setting it too low will result in throttling from DynamoDB.
* `kinesis.<consumer-name>.kcl.initialPositionInStream` - Controls our strategy for pulling from Kinesis (LATEST, TRIM_HORIZON, ..)
* `kinesis.<consumer-name>.kcl.maxRecords` - The maximum batch size.

<a name="usage-defining-a-config-file-in-the-client-application-notable-producer-configuration-values"></a>
### Notable Producer Configuration Values
* `kinesis.<producer-name>.akka.dispatcher` - Sets the dispatcher for the producer, defaults to `kinesis.akka.default-dispatcher`
* `kinesis.<producer-name>.akka.max-outstanding-requests` - Enables artificial throttling within the Producer.
This limits the number of futures in play at any one time. Each message creates a new future (internally in the KPL),
which allows us to track the progress of sent messages when they go with the next batch.
* `kinesis.<producer-name>.akka.throttling-retry-millis` - How soon to retry after hitting the above throttling cap.
* `kinesis.<producer-name>.kpl.AggregationEnabled` - Enables [aggregation of messages](http://docs.aws.amazon.com/streams/latest/dev/kinesis-producer-adv-aggregation.html).
* `kinesis.<producer-name>.kpl.Region` - The AWS Region to use.
* `kinesis.<producer-name>.kpl.RateLimit` - Limits the maximum allowed put rate for a shard, as a percentage of the backend limits.

<a name="usage-usage-consumer"></a>

### Typed Configuration - Producer
If you don't want to depend on config files, there's a typed configuration class available: [KinesisProducerConfig](src/main/scala/com/weightwatchers/reactive/kinesis/producer/KinesisProducerConfig.scala)

You can construct it in a few ways:

```scala
// With default values
val defaultProducerConfig = KinesisProducerConfig()

// With a provided KinesisProducerConfiguration from the Java KPL library
val awsKinesisConfig: KinesisProducerConfiguration = ...
val producerConfig = KinesisProducerConfig(awsKinesisConfig)

// With a typesafe-config object
val typesafeConfig: Config = ...
val producerConfig = KinesisProducerConfig(typesafeConfig)

// With a typesafe-config object and an AWSCredentialsProvider
val typesafeConfig: Config = ...
val credentialsProvider: AWSCredentialsProvider = ...
val producerConfig = KinesisProducerConfig(typesafeConfig, credentialsProvider)
``` 

These can be used to create a ProducerConf and ultimately a KinesisProducer, like so:

```scala
val producerConfig: KinesisProducerConfig = ...
val producerConf: ProducerConf = ProducerConf(producerConfig, "my-stream-name", None, None)
```

## Usage: Consumer
`reactive-kinesis` provides two different ways to consume messages from Kinesis: [Actor Based Consumer](#actor-based-consumer) and [Akka Stream Source](#akka-stream-source).

![Consumer Architecture](https://www.lucidchart.com/publicSegments/view/69b7b7d1-bc09-4dcc-ab1b-a0f7c6e1ffc6/image.png)

<a name="actor-based-consumer"></a>
### Actor Based Consumer

Implementing the consumer requires a simple actor which is responsible for processing messages sent to it by the library.
We call this the `Event Processor`.
Upon creating an instance of the `KinesisConsumer`, internally one `ConsumerWorker` (this is different from the KCL Worker) is
created per shard (shards are distributed amongst consumers automatically). This consumer worker is what sends messages to the
`Event Processor`. Note that the `Event Processor` is shared amongst ALL shards, so it is important not to cache the sender of previous messages.
It is perfectly valid to use a router to spread the work amongst many `Event Processor` actors.

```scala
import akka.actor.{Actor, Props}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{ConsumerShutdown, ConsumerWorkerFailure, EventProcessed, ProcessEvent}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf

class TestEventProcessor() extends Actor with LazyLogging {

  import scala.concurrent.duration._

  implicit val timeout = akka.util.Timeout(5.minutes)


  // scalastyle:off method.length
  override def receive: Receive = {

    case ProcessEvent(event) => {
      //Do some processing here...
      sender ! EventProcessed(event.sequenceNumber)
    }
    case ConsumerWorkerFailure(failedEvents, shardId) =>
    // Consumer failure, no more messages will be consumed!! Depending on the purpose of this service this may be critical.

    case ConsumerShutdown(shardId) =>
    // The Consumer has shutdown all shards on this instance (gracefully, or as a result of a failure).
  }
}

object Consumer extends App {
  val system = akka.actor.ActorSystem.create("test-system")
  val config = ConfigFactory.load()
  val eventProcessor = system.actorOf(Props[TestEventProcessor], "test-processor")
  val consumer = KinesisConsumer(ConsumerConf(config.getConfig("kinesis"), "some-consumer"),
                                 eventProcessor, system)
  consumer.start()
}
```

The following types must be handled:

```scala
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{ConsumerShutdown, ConsumerWorkerFailure, EventProcessed, ProcessEvent}

/**
  * Sent to the eventProcessor for each message in the batch.
  */
  case class ProcessEvent(consumerEvent: ConsumerEvent)

/**
  * Expected in response to a [[ProcessEvent]] message after processing is complete
  *
  * @param compoundSeqNo This is a combination of the sequence and subsequence numbers
  * @param successful    Set this to false to skip this message.
  */
case class EventProcessed(compoundSeqNo: CompoundSequenceNumber, successful: Boolean = true)

/**
  * Sent to the eventProcessor if batch processing fails (above the tolerance after retrying)
  * before shutting down processing on this shard.
  * @param failedEvents The events that failed processing within the time.
  * @param shardId      The shardId of the worker causing the failure.
  */
case class ConsumerWorkerFailure(failedEvents: Seq[ConsumerEvent], shardId: String)

/**
  * Sent to the eventProcessor upon shutdown of this worker.
  */
case class ConsumerShutdown(shardId: String)
```

<a name="usage-usage-consumer-important-considerations-when-implementing-the-event-processor"></a>
#### Important considerations when implementing the Event Processor
* The Event Processor MUST handle [[ProcessEvent]] messages (for each message)
* The Event Processor MUST respond with [[EventProcessed]] after processing of the [[ProcessEvent]]
* The Event Processor may set `successful` to false to indicate the message can be skipped
* The Event Processor SHOULD handle [[ConsumerWorkerFailure]] messages which signal a critical failure in the Consumer.
* The Event Processor SHOULD handle [[ConsumerShutdown]] messages which siganl a graceful shutdown of the Consumer.

<a name="usage-usage-consumer-checkpointing"></a>
#### Checkpointing
The client will handle checkpointing asynchronously PER SHARD according to the configuration using a separate actor.

<a name="akka-stream-source"></a>
### Akka Stream Source

An Akka `Source` is provided that can be used with streams. It is possible to create a source from a `ConsumerConf` or 
directly from the consumer name that is defined in the configuration.
Every message that is emitted to the stream is of type `CommitableEvent[ConsumerEvent]` and has to be committed 
explicitly downstream with a call to `event.commit()`. It is possible to map to a different type of `CommittableEvent` 
via the `map` and `mapAsync` functionality. A `KinesisConsumer` is created internally for the `Kinesis.source`, when the factory method isn't defined.

```scala
import com.weightwatchers.reactive.kinesis.stream._

Kinesis
  .source("consumer-name")
  .take(100)
  .map(event => event.map(_.payloadAsString())) // read and map payload as string
  .mapAsync(10)(event => event.mapAsync(Downloader.download(event.payload))) // handle an async message
  .map(event => event.commit()) // mark the event as handled by calling commit
  .runWith(Sink.seq) 
```

Or you can explicitly pass a lambda, to create the `KinesisConsumer`. 
You can save a reference to this `KinesisConsumer` and use it to manually
shutdown your consumer when needed.


```scala
  import akka.actor.ActorSystem
  import akka.stream.scaladsl.Sink
  import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer
  import com.weightwatchers.reactive.kinesis.stream._


  implicit val sys = ActorSystem("kinesis-consumer-system")
  implicit val materializer: Materializer = ActorMaterializer()
  import sys.dispatcher

  var consumer = Option.empty[KinesisConsumer]

  Kinesis.source(
    consumerName = "some-consumer",
    createConsumer = (conf, ref) => {
      val c = KinesisConsumer(conf, ref, sys)
      consumer = Option(c)
      c
    })
    .take(100)
    .map(event => event.map(_.payloadAsString()))
    .mapAsync(10)(event => event.mapAsync(Downloader.download(event.payload)))
    .map(event => event.commit())
    .runWith(Sink.seq)

  consumer.foreach { c =>
    c.stop()
  }
```

All rules described here for the `KinesisConsumer` also apply for the stream source.

<a name="usage-usage-consumer-graceful-shutdown"></a>
### Graceful Shutdown

Currently the KinesisConsumer Shutdown works as follows:
* `stop()` is called on the `KinesisConsumer` (either explicitly or via the jvm shutdown hook)
* This then calls `requestShutdown` on the KCL Worker, blocking until completion.
* The KCL Worker propagates this down to the `ConsumerProcessingManager` (Which is the `IRecordProcessor`) - calling `shutdownRequested` on each instance (one per shard).
* When `shutdownRequested` is called, this sends a `GracefulShutdown` message to the `ConsumerWorker` Actor, blocking until a response is received (Ask + Await).
* On receipt of this message, the `ConsumerWorker` switches context to ignore all future messages. If a batch is currently being processed, it responds to the sender of that batch (the manager), which will currently be blocking awaiting confirmation of the batch (this is by design, the KCL requires that we don't complete the `processRecords` function until we have finished the batch, otherwise the next batch is immediately sent)
* The `ConsumerWorker` then forces a final checkpoint, responding to the manager once completed (or failed), which allows shutdown to continue and the `KinesisConsumer` to shutdown.
* The shutdown timeout is configured by: `kinesis.<consumer-name>.worker.shutdownTimeoutSeconds`
* The shutdown hooks can be disabled using: `kinesis.<consumer-name>.worker.gracefulShutdownHook`

<a name="usage-usage-producer"></a>
## Usage: Producer

The KPL Client sends messages in batches, each message creates a Future which completes upon successful send or failure.

See Amazon's documentation for more information:
https://github.com/awslabs/amazon-kinesis-producer

<a name="usage-usage-producer-actor-based-implementation"></a>
### Actor Based Implementation

This implementation will optionally throttle of the number of futures currently in play,
according to the `max-outstanding-requests` property.

Each future is handled within the actor and a message will be returned to the sender upon completion.

The following messages are supported:

```scala
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.{SendFailed, SendSuccessful, SendWithCallback}

/**
  * Send a message to Kinesis, registering a callback response of [[SendSuccessful]] or [[SendFailed]] accordingly.
  */
case class SendWithCallback(producerEvent: ProducerEvent, messageId: String = UUID_GENERATOR.generate().toString)

/**
  * Send a message to Kinesis witout any callbacks. Fire and forget.
  */
case class Send(producerEvent: ProducerEvent)

/**
  * Sent to the sender in event of a successful completion.
  *
  * @param messageId        The id of the event that was sent.
  * @param userRecordResult The Kinesis data regarding the send.
  */
case class SendSuccessful(messageId: String, userRecordResult: UserRecordResult)

/**
    * Sent to the sender in event of a failed completion.
    *
    * @param event     The current event that failed.
    * @param messageId The id of the event that failed.
    * @param reason    The exception causing the failure.
    */
case class SendFailed(event: ProducerEvent, messageId: String, reason: Throwable)
```



<a name="usage-usage-producer-actor-based-implementation-within-an-actor-strongly-recommended"></a>
#### Within an Actor (Strongly recommended)

```scala
import java.util.UUID

import akka.actor.Actor
import com.typesafe.config.Config
import com.weightwatchers.reactive.kinesis.models.ProducerEvent
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.{SendFailed, SendSuccessful, SendWithCallback}
import samples.SomeActor.DoSomething

object SomeActor {
  case object DoSomething
}

class SomeActor(kinesisConfig: Config) extends Actor {

  val kpa = context.actorOf(
    KinesisProducerActor.props(kinesisConfig, "some-producer"))

  override def receive: Receive = {
    case DoSomething =>
      //Do something exciting!
      val producerEvent = ProducerEvent(UUID.randomUUID.toString, "{Some Payload}")
      kpa ! SendWithCallback(producerEvent)

    //Callbacks from the KinesisProducerActor
    case SendSuccessful(messageId, _) =>
      println(s"Successfully sent $messageId")

    case SendFailed(event, messageId, reason) =>
      println(s"Failed to send event ${event.partitionKey} with $messageId, cause: ${reason.getMessage}")
  }
}
```

#### Scheme for kinesis retries in services

Kinesis Retry model with service DB:
<ul>
<li>The service itself manage their kinesis failures.</li>
<li>Practical, develop only in service.</li>
<li>Save time for learning and integration with other technology.</li>
</ul>
 Algorithm steps to publish event to stream:

```
if sendSuccessful
    continue
if sendFailed
    save event in db
if backgroundJob.connect(stream).isSuccessful
    go to step1
    delete event from DB 
else
    do nothing.
```

![Kinesis retries diagram scheme](https://www.lucidchart.com/publicSegments/view/e9674455-b544-4634-a29c-7066c32ddc45/image.png)

<a name="usage-usage-producer-actor-based-implementation-from-outside-of-an-actor"></a>
#### From outside of an Actor

```scala
import java.util.UUID
import com.typesafe.config._
import com.weightwatchers.reactive.kinesis.models._
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor
import com.weightwatchers.reactive.kinesis.producer.KinesisProducerActor.Send

implicit val system = akka.actor.ActorSystem.create()

val kinesisConfig: Config = ConfigFactory.load().getConfig("kinesis")

// where testProducer is the name in the configuration
val kpa = system.actorOf(KinesisProducerActor.props(kinesisConfig, "some-producer"))

val producerEvent = ProducerEvent(UUID.randomUUID.toString, "{Some Payload}")
kpa ! Send(producerEvent) //Send without a callback confirmation
```


<a name="usage-usage-producer-pure-scala-based-implementation-simple-wrapper-around-kpl"></a>
### Pure Scala based implementation (simple wrapper around KPL)
*Note that future throttling will be unavailable using this method.*

```scala
import java.util.UUID
import com.amazonaws.services.kinesis.producer.{UserRecordFailedException, UserRecordResult}
import com.weightwatchers.reactive.kinesis.producer.KinesisProducer
import com.weightwatchers.reactive.kinesis.producer.ProducerConf
import com.typesafe.config._
import com.weightwatchers.reactive.kinesis.models._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global //Not for production

val kinesisConfig: Config = ConfigFactory.load().getConfig("kinesis")

val kpl = KinesisProducer(ProducerConf(kinesisConfig, "some-producer"))

val producerEvent = ProducerEvent(UUID.randomUUID.toString, "{Some Payload}")

val callback: Future[UserRecordResult] = kpl.addUserRecord(producerEvent)

callback onSuccess {
  case result =>
    println("Success!!")
}

callback onFailure {
  case ex: UserRecordFailedException =>
    println(s"Failure! ${ex.getMessage}")
  case ex =>
    println(s"Critical Failure! ${ex.getMessage}")
}
```

<a name="akka-stream-sink"></a>
### Akka Stream Sink

An Akka `Sink` is provided which can be used to publish messages via streams. 
Every message is sent as `ProduserEvent` to the `Sink`, which defines the PartitionKey as well as the payload.
The `Sink` is created from a `ProducerConf` or directly with a `KinesisProducerActor`. See [Kinesis](https://github.com/WW-Digital/reactive-kinesis/blob/master/src/main/scala/com/weightwatchers/reactive/kinesis/stream/Kinesis.scala)  for the various options.

The `Sink` expects an acknowledgement for every message sent to Kinesis. 
An amount of unacknowledged messages can be configured, before back pressure is applied.
This throttling is controlled by the kinesis.{producer}.akka.max-outstanding-requests configuration value.
Please note: a default value (1000 messages) is applied, if throttling is not configured.

The provided `Sink` produces a `Future[Done]` as materialized value.
This future succeeds, if all messages from upstream are sent to Kinesis and acknowledged.
It fails if a message could not be send to Kinesis or upstream fails.

```scala
import akka.stream.scaladsl.Source
import com.weightwatchers.reactive.kinesis.models._
import com.weightwatchers.reactive.kinesis.stream._

Source(1.to(100).map(_.toString))
  .map(num => ProducerEvent(num, num))
  .runWith(Kinesis.sink("producer-name"))
  .onComplete {
    case Success(_) => println("All messages are published successfully.")
    case Failure(ex) => println(s"Failed to publish messages: ${ex.getMessage}")
  }
```

A long running flow can be easily achieved using a `SourceQueue`.
In this case the flow stays open as long as needed.
New elements can be published via the materialized queue:

```scala
import akka.stream.scaladsl.Source
import com.weightwatchers.reactive.kinesis.models._
import com.weightwatchers.reactive.kinesis.stream._

val sourceQueue = Source.queue[ProducerEvent](1000, OverflowStrategy.fail)
  .toMat(Kinesis.sink("producer-name"))(Keep.left)
  .run()

sourceQueue.offer(ProducerEvent("foo", "bar"))
sourceQueue.offer(ProducerEvent("foo", "baz"))
```

The `Sink` uses a `KinesisProducerActor` under the cover. All rules regarding this actor also apply for the `Sink`.  


<a name="running-the-reliability-test"></a>
# Running the reliability test

<a name="running-the-reliability-test-delete-recreate-kinesisstreams-and-dynamo-table"></a>
## Delete &amp; recreate kinesisstreams and dynamo table
Execute this command in a shell.  If you don't have access to WW AWS resources, you'll need it:
```
aws kinesis delete-stream --stream-name test-kinesis-reliability && aws dynamodb delete-table --table-name KinesisReliabilitySpec && sleep 90 && aws kinesis create-stream --stream-name test-kinesis-reliability --shard-count 2
```

<a name="running-the-reliability-test-running-the-producer-consumer-test"></a>
## Running the producer-consumer test

Run the `SimpleKinesisProducer` using the App object.

Wait for hte producer to publish all messages.

At the end the producer will print the number of unconfirmed and failed messages (both should be 0!).

Run the `SimpleKinesisConsumer` using the App object.

Now, wait for two messages that look like this to appear in the consumer window:
```
2016-06-14 20:09:24,938 c.w.c.e.KinesisRecordProcessingManager - Initializing record processor for shard: shardId-000000000001
2016-06-14 20:09:24,963 c.w.c.e.KinesisRecordProcessingManager - Initializing record processor for shard: shardId-000000000000
```

As the test progresses, watch the consumer window for a message of this format:
```
**** PIT STOP OK
```

You'll see some stats logged regarding messages/sec processed, near that line.

<a name="faq"></a>
# FAQ
* How is DynamoDB used in relation to out checkpointing?
  * DynamoDB tables will be automatically created, however the write throughput must be configured appropriately using the AWS console or CLI. There is a cost associated with this, but note that setting it too low will cause checkpoint throttling. Configure `kinesis.<consumer-name>.checkpointer.intervalMillis` accordingly.
* How is data sharded?
  * Sharding relates to the distribution of messages across the shards for a given stream. Ideally you want an even distribution amongst our shards. However ordering is only guaranteed within a given shard, it is therefore important to group related messages by shard. For example if a specific user performs several operations, in which the order of execution matters, then ensuring they land on the same shard will guarantee the order is maintained.
  * For this, the `partition key` is used. Messages with the same partition key will land on the same shard. In the example above a userId may be a good `partition key`.
  * Note that if the number of partition keys exceeds the number of shards, some shards necessarily contain records with different partition keys. From a design standpoint, to ensure that all your shards are well utilized, the number of shards should be substantially less than the number of unique partition keys.
* How long do we keep data?
  * By default Kinesis stores data for 24 hours, however this can be extended to 7 days for a fee.
* What data is already in the Stream and from where is it consumed?
  * Kinesis persists data up until the retention period, consuming data does not remove it from the stream. Rather, it moves your `checkpoint` to the appropriate position.
  * There are a number of options for configuring at which position in the stream a consumer will start, configured by the `initialPositionInStream` setting.
  * This library default to `TRIM_HORIZON`, note that if a checkpoint exists in DynamoDB, the application will **always** continue from the last checkpoint.
    * `AT_SEQUENCE_NUMBER` - Start reading from the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
    * `AFTER_SEQUENCE_NUMBER` - Start reading right after the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
    * `AT_TIMESTAMP` - Start reading from the position denoted by a specific timestamp, provided in the value Timestamp.
    * `TRIM_HORIZON` - Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard.
    * `LATEST` - Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.
* How do sequence numbers work?
  * Sequence numbers for the same partition key generally increase over time, but **NOT** necessarily in a continuous sequence. The longer the time period between records, the bigger the gap between the sequence numbers.
  * To uniquely identify a record on a shard, you need to use **BOTH** the `sequence number` and the `sub-sequence number`. This is because messages that are aggregated together have the same sequence number (they are treated as one messages by Kinesis). Therefore it is important to also use the sub-sequence number to distinguish between them.


<a name="contributor-guide"></a>
# Contributor Guide

<a name="contributor-guide-code-formatting"></a>
## Code Formatting
This project uses [scalafmt](http://scalameta.org/scalafmt/) and will automatically fail the build if any files do not match the expected formatting.

Please run `sbt scalafmt` before committing and pushing changes.

<a name="contributor-guide-integration-tests"></a>
## Integration tests
As part of the travis build, integration tests will run against a Kinesis localstack instance. You can run these locally as follows:
* `docker-compose -f localstack/docker-compose.yml up`
* `sbt it:test`

<a name="contributor-guide-tag-requirements"></a>
## Tag Requirements
Uses tags and [sbt-git](https://github.com/sbt/sbt-git) to determine the current version.
* Each merge into master will automatically build a snapshot and publish to [bintray OSS artifactory](https://www.jfrog.com/confluence/display/RTF/Deploying+Snapshots+to+oss.jfrog.org).
* Tagging the master branch will automatically build and publish both Scala 2.11 & Scala 2.12 artifacts (to bintray and maven central).
* Tags are in the format vX.X.X

<a name="contributor-guide-tag-requirements-version-information"></a>
### Version information
* IF the current commit is tagged with "vX.Y.Z" (ie semantic-versioning), the version is "X.Y.Z"
* ELSE IF the current commit is tagged with "vX.Y.Z-Mx", the version is "X.Y.Z-Mx"
* ELSE IF the latest found tag is "vX.Y.Z", the version is "X.Y.Z-commitsSinceVersion-gCommitHash-SNAPSHOT"
* ELSE the version is "0.0.0-commitHash-SNAPSHOT"

<a name="contributor-guide-tag-requirements-valid-release-tag-examples"></a>
### Valid Release Tag Examples:
v1.2.3 (version=1.2.3)
v1.2.3-M1 (version=1.2.3-M1)

<a name="contributor-guide-tag-requirements-invalid-release-tag-examples"></a>
### Invalid Release Tag Examples:
v1.2.3-SNAPSHOT
v1.2.3-M1-SNAPSHOT
v1.2.3-X1
1.2.3

If the current version on master is a snapshot (release tag + x commits),
then the artifact will be deployed to the [JFrog OSS repository](https://oss.jfrog.org/webapp/#/artifacts/browse/simple/General/oss-snapshot-local/com/weightwatchers):

<a name="contribution-policy"></a>
# Contribution policy

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license
the work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you
agree to license the material under the project's open source license and warrant that you have the
legal authority to do so.

<a name="changelog"></a>
# Changelog
See the releases tab: https://github.com/WW-Digital/reactive-kinesis/releases

<a name="license"></a>
# License

This code is open source software licensed under the
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0) license.
