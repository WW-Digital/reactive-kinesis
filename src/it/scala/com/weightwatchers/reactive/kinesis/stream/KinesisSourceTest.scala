package com.weightwatchers.reactive.kinesis.stream

import akka.stream.scaladsl.Sink
import com.weightwatchers.reactive.kinesis.{AkkaUnitTestLike, KinesisKit}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import org.scalatest._

import scala.concurrent.duration._

class KinesisSourceTest extends WordSpec with KinesisKit with AkkaUnitTestLike with Matchers {

  "A Kinesis Source" should {

    "process all messages of a stream with one worker" in new WithKinesis {
      val appName = "1worker"
      val result = Kinesis
        .source(consumerConf = consumerConf(appName, TestStreamNrOfMessagesPerShard))
        .take(TestStreamNumberOfShards * TestStreamNrOfMessagesPerShard)
        .map { event =>
          event.commit()
          event.event.payload
        }
        .runWith(Sink.seq)

      val grouped = result.futureValue.groupBy(identity)
      result.futureValue.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_ should have size TestStreamNumberOfShards)
    }

    "process all messages of a stream with 2 workers" in new WithKinesis {
      // Please note: since source1 and source2 are started simultaneously, both will assume there is no other worker.
      // During register one will fail and not read any message until retry
      // Depending on timing one or both sources will read all events
      val appName   = "2worker"
      val batchSize = TestStreamNrOfMessagesPerShard
      val source1   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val source2   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val result = source1
        .merge(source2)
        .take(TestStreamNrOfMessagesPerShard * TestStreamNumberOfShards)
        .map { event =>
          event.commit()
          event.event.payload
        }
        .runWith(Sink.seq)

      val grouped = result.futureValue.groupBy(identity)
      result.futureValue.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_ should have size TestStreamNumberOfShards)
    }

    "process all messages of a stream with 4 workers" in new WithKinesis {
      // Please note: since all sources are started simultaneously, all will assume there is no other worker.
      // During register all except one will fail and not read any message until retry
      // Depending on timing one or multiple sources will read all events
      val batchSize = TestStreamNrOfMessagesPerShard
      val appName   = "4worker"
      val source1   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val source2   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val source3   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val source4   = Kinesis.source(consumerConf = consumerConf(appName, batchSize))
      val result = source1
        .merge(source2)
        .merge(source3)
        .merge(source4)
        // Since only 2 clients can take batchSize messages, an overall take is needed here to end the stream
        .take(TestStreamNrOfMessagesPerShard * TestStreamNumberOfShards)
        .map { event =>
          event.commit()
          event.event.payload
        }
        .runWith(Sink.seq)

      val grouped = result.futureValue.groupBy(identity)
      result.futureValue.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_ should have size TestStreamNumberOfShards)
    }

    "maintain the read position in the stream correctly" in new WithKinesis {
      val batchSize = TestStreamNrOfMessagesPerShard / 2 // 2 * NrOfShards batches needed
      val appName   = "stream_read_position"

      // We create multiple Sources (one after the other!). Each source:
      // - takes batchSize of messages and commits all of them
      // - dies after one batch
      // We expect to get all messages by n reads (which means, that the read position was stored correctly)
      val result =
        for (_ <- 1
               .to((TestStreamNumberOfShards * TestStreamNrOfMessagesPerShard / batchSize).toInt))
          yield {
            Kinesis
              .source(consumerConf = consumerConf(appName, batchSize))
              .take(batchSize)
              .map { event =>
                event.commit()
                event.event.payload
              }
              .runWith(Sink.seq)
              .futureValue
          }

      val allMessages = result.flatten

      val grouped = allMessages.groupBy(identity)
      allMessages.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
    }

    "not commit the position, if the event is not committed" in new WithKinesis {
      val batchSize = TestStreamNrOfMessagesPerShard / 2 // 2 * NrOfShards batches needed
      val appName   = "stream_not_commited"

      // This worker will read batchSize events and will not commit
      // We expect that the read position will not change
      val uncomitted = Kinesis
        .source(consumerConf(appName, batchSize = batchSize))
        .take(batchSize)
        .runWith(Sink.seq)
        .futureValue

      // This worker will read all available events.
      // This works only, if the first worker has not committed anything
      val commited = Kinesis
        .source(consumerConf = consumerConf(appName, batchSize = batchSize))
        .take(TestStreamNumberOfShards * TestStreamNrOfMessagesPerShard)
        .map { event =>
          event.commit()
          event.event.payload
        }
        .runWith(Sink.seq)
        .futureValue

      uncomitted should have size batchSize
      val grouped = commited.groupBy(identity)
      commited.distinct should have size TestStreamNrOfMessagesPerShard
      grouped should have size TestStreamNrOfMessagesPerShard
      grouped.values.foreach(_ should have size TestStreamNumberOfShards)
    }
  }

  class WithKinesis {
    val workerIdGen: Iterator[String] = 1.to(Int.MaxValue).iterator.map(id => s"wrk-$id")
    def consumerConf(appName: String, batchSize: Long): ConsumerConf = {
      consumerConfig(appName, appName + "-" + workerIdGen.next(), batchSize.toInt)
    }
  }

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(60.seconds)
}
