package com.weightwatchers.reactive.kinesis.stream

import akka.stream.scaladsl.Sink
import com.weightwatchers.reactive.kinesis.common.{AkkaUnitTestLike, KinesisConfiguration, KinesisSuite}
import org.scalatest._

import scala.concurrent.duration._

class KinesisSourceGraphStageIntegrationSpec
    extends FreeSpec
    with KinesisSuite
    with KinesisConfiguration
    with AkkaUnitTestLike
    with Matchers {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(60.seconds, 1.second)

  val TestStreamNrOfMessagesPerShard: Long = 100

  "A Kinesis Source" - {

    "process all messages of a stream with one worker" in new withKinesisConfForApp("1worker") {
      val result = ConsumerStreamFactory
        .source(consumerConf = consumerConf())
        .takeWhile(_.payload.payloadAsString().toLong < TestStreamNrOfMessagesPerShard,
                   inclusive = true)
        .map { event =>
          event.commit()
          event.payload.payloadAsString()
        }
        .runWith(Sink.seq)

      val grouped = result.futureValue.groupBy(identity)
      result.futureValue.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_.size.toLong shouldBe >=(TestStreamNumberOfShards))
    }

    "process all messages of a stream with 2 workers" in new withKinesisConfForApp("2worker") {
      // Please note: since source1 and source2 are started simultaneously, both will assume there is no other worker.
      // During register one will fail and not read any message until retry
      // Depending on timing one or both sources will read all events
      val batchSize = TestStreamNrOfMessagesPerShard
      val source1   = ConsumerStreamFactory.source(consumerConf = consumerConf())
      val source2   = ConsumerStreamFactory.source(consumerConf = consumerConf())
      val result = source1
        .merge(source2)
        .takeWhile(_.payload.payloadAsString().toLong < TestStreamNrOfMessagesPerShard,
                   inclusive = true)
        .map { event =>
          event.commit()
          event.payload.payloadAsString()
        }
        .runWith(Sink.seq)

      val grouped = result.futureValue.groupBy(identity)
      result.futureValue.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_.size.toLong shouldBe >=(TestStreamNumberOfShards))
    }

    "maintain the read position in the stream correctly" in new withKinesisConfForApp(
      "read_position"
    ) {
      val batchSize = TestStreamNrOfMessagesPerShard / 2 // 2 * NrOfShards batches needed

      // We create multiple Sources (one after the other!). Each source:
      // - takes batchSize of messages and commits all of them
      // - dies after one batch
      // We expect to get all messages by n reads (which means, that the read position was stored correctly)
      val result =
        for (iteration <- 1
               .to((TestStreamNumberOfShards * TestStreamNrOfMessagesPerShard / batchSize).toInt))
          yield {
            ConsumerStreamFactory
              .source(consumerConf = consumerConf(batchSize = batchSize))
              .takeWhile(_.payload.payloadAsString().toLong < batchSize * iteration,
                         inclusive = true)
              .map { event =>
                event.commit()
                event.payload
              }
              .runWith(Sink.seq)
              .futureValue
          }

      val allMessages = result.flatten

      val grouped = allMessages.groupBy(identity)
      allMessages.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_.size.toLong shouldBe >=(TestStreamNumberOfShards))
    }

    "not commit the position, if the event is not committed" in new withKinesisConfForApp(
      "not_committed"
    ) {
      // This worker will read all events and will not commit
      // We expect that the read position will not change
      val uncommitted = ConsumerStreamFactory
        .source(consumerConf())
        .takeWhile(_.payload.payloadAsString().toLong < TestStreamNrOfMessagesPerShard,
                   inclusive = true)
        .runWith(Sink.seq)
        .futureValue

      // This worker will read all available events.
      // This works only, if the first worker has not committed anything
      val committed = ConsumerStreamFactory
        .source(consumerConf = consumerConf())
        .takeWhile(_.payload.payloadAsString().toLong < TestStreamNrOfMessagesPerShard,
                   inclusive = true)
        .map { event =>
          event.commit()
          event.payload.payloadAsString()
        }
        .runWith(Sink.seq)
        .futureValue

      uncommitted should have size TestStreamNrOfMessagesPerShard
      val grouped = committed.groupBy(identity)
      committed.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_.size.toLong shouldBe >=(TestStreamNumberOfShards))
    }
  }
}
