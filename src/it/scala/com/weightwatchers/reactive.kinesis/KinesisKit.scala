package com.weightwatchers.reactive.kinesis

import java.nio.ByteBuffer

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import com.weightwatchers.reactive.kinesis.common.TestCredentials
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._

/**
  * Base trait to create a KinesisConfiguration from application config + override options.
  * Handles cleanup of and creation of streams before tests.
  */
trait KinesisConfiguration {

  // The global application config
  def config: Config

  def consumerConfig(appName: String, workerId: String, batchSize: Int): ConsumerConf = {
    val conf = ConsumerConf(config.getConfig("kinesis"), "testConsumer")
    val kcl  = conf.kclConfiguration
    conf.copy(
      kclConfiguration = new KinesisClientLibConfiguration(appName,
                                                           kcl.getStreamName,
                                                           kcl.getKinesisCredentialsProvider,
                                                           workerId)
        .withKinesisEndpoint(kcl.getKinesisEndpoint)
        .withDynamoDBEndpoint(kcl.getDynamoDBEndpoint)
        .withMetricsLevel(kcl.getMetricsLevel)
        .withMaxRecords(batchSize)
        .withCallProcessRecordsEvenForEmptyRecordList(
          kcl.shouldCallProcessRecordsEvenForEmptyRecordList()
        )
        .withCleanupLeasesUponShardCompletion(kcl.shouldCleanupLeasesUponShardCompletion())
        .withFailoverTimeMillis(kcl.getFailoverTimeMillis)
        .withIdleTimeBetweenReadsInMillis(kcl.getIdleTimeBetweenReadsInMillis)
        .withInitialLeaseTableReadCapacity(kcl.getInitialLeaseTableReadCapacity)
        .withInitialLeaseTableWriteCapacity(kcl.getInitialLeaseTableWriteCapacity)
        .withInitialPositionInStream(kcl.getInitialPositionInStream)
        .withMaxLeaseRenewalThreads(kcl.getMaxLeaseRenewalThreads)
        .withMaxLeasesForWorker(kcl.getMaxLeasesForWorker)
        .withMaxLeasesToStealAtOneTime(kcl.getMaxLeasesToStealAtOneTime)
        .withMetricsBufferTimeMillis(kcl.getMetricsBufferTimeMillis)
        .withParentShardPollIntervalMillis(kcl.getParentShardPollIntervalMillis)
        .withShardSyncIntervalMillis(kcl.getShardSyncIntervalMillis)
        .withShardPrioritizationStrategy(kcl.getShardPrioritizationStrategy)
        .withSkipShardSyncAtStartupIfLeasesExist(
          kcl.getSkipShardSyncAtWorkerInitializationIfLeasesExist
        )
        .withTaskBackoffTimeMillis(kcl.getTaskBackoffTimeMillis)
        .withValidateSequenceNumberBeforeCheckpointing(
          kcl.shouldValidateSequenceNumberBeforeCheckpointing()
        )
    )
  }
}

/**
  * Use this trait to interact with Kinesis.
  * Every suite will have a clean Kinesis and Dynamo as well as one Stream with 2 Shards with 100 Messages each.
  */
trait KinesisKit
    extends BeforeAndAfter
    with BeforeAndAfterAll
    with StrictLogging
    with KinesisConfiguration { self: Suite =>

  val TestStreamNrOfMessagesPerShard: Long = 100
  val TestStreamNumberOfShards: Long       = 2
  val TestStreamName: String               = "test-kinesis-reliability"

  /**
    * Cleanup dynamo before each test
    */
  before {
    cleanDynamo()
  }

  override protected def beforeAll(): Unit = {
    val kinesis = kinesisClient()

    // clean up from eventually last run
    cleanKinesis(kinesis)

    // create new stream
    createKinesisStream(kinesis)

    // Pumping test data inside.
    createTestData(TestStreamNrOfMessagesPerShard.toInt, kinesis)

    kinesis.shutdown()
  }

  protected def createKinesisStream(kinesisClient: AmazonKinesisAsync): Unit = {

    kinesisClient.createStream(TestStreamName, TestStreamNumberOfShards.toInt)

    // Block until the stream is ready to rumble.
    while (kinesisClient
             .describeStream(TestStreamName)
             .getStreamDescription
             .getStreamStatus != "ACTIVE") {
      Thread.sleep(100)
    }
    logger.info(s"Stream: $TestStreamName is created.")
  }

  protected def cleanKinesis(kinesisClient: AmazonKinesisAsync): Unit = {
    // We delete our stream if it exist.
    kinesisClient.listStreams().getStreamNames.asScala.toList.find(_ == TestStreamName).foreach {
      kinesisClient.deleteStream
    }

    // Blocking until it is really deleted.
    while (kinesisClient.listStreams().getStreamNames.contains(TestStreamName)) {
      Thread.sleep(100)
    }
  }

  protected def cleanDynamo(): Unit = {
    val dynamo = dynamoClient()
    cleanDynamo(dynamo)
    dynamo.shutdown()
  }

  protected def cleanDynamo(dynamoClient: AmazonDynamoDB): Unit = {
    val result = dynamoClient.listTables()
    result.getTableNames.asScala.foreach { tableName =>
      logger.info(s"Delete dynamo table $tableName")
      dynamoClient.deleteTable(tableName)
    }
  }

  protected def kinesisClient(): AmazonKinesisAsync = {
    val kcl = consumerConfig(suiteName, "setup", batchSize = 1000).kclConfiguration
    AmazonKinesisAsyncClientBuilder
      .standard()
      .withClientConfiguration(kcl.getKinesisClientConfiguration)
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getKinesisEndpoint, kcl.getRegionName)
      )
      .withCredentials(TestCredentials.Credentials)
      .build()
  }

  protected def dynamoClient(): AmazonDynamoDB = {
    val kcl = consumerConfig(suiteName, "setup", batchSize = 1000).kclConfiguration
    AmazonDynamoDBClientBuilder
      .standard()
      .withClientConfiguration(kcl.getDynamoDBClientConfiguration)
      .withEndpointConfiguration(
        new EndpointConfiguration(kcl.getDynamoDBEndpoint, kcl.getRegionName)
      )
      .build()
  }

  protected def createTestData(testDataCount: Int, client: AmazonKinesisAsync): Unit = {
    import scala.collection.JavaConverters._

    client
      .describeStream(TestStreamName)
      .getStreamDescription
      .getShards
      .asScala
      .toList
      .map(_.getShardId)
      .foreach { shardId =>
        (1 to testDataCount).foreach { nr =>
          val msg = new PutRecordRequest()
            .withData(ByteBuffer.wrap(nr.toString.getBytes))
            .withStreamName(TestStreamName)
            .withPartitionKey(shardId)
          client.putRecord(msg)
        }
      }
  }
}
