package com.weightwatchers.reactive.kinesis

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config._
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.reactive.kinesis.SimpleKinesisConsumer._
import com.weightwatchers.reactive.kinesis.consumer.ConsumerWorker.{
  ConsumerShutdown,
  ConsumerWorkerFailure,
  EventProcessed,
  ProcessEvent
}
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer
import com.weightwatchers.reactive.kinesis.consumer.KinesisConsumer.ConsumerConf
import com.weightwatchers.reactive.kinesis.models.CompoundSequenceNumber
import org.joda.time.{DateTime, DateTimeZone, Period}

import scala.collection.mutable.ListBuffer
import com.weightwatchers.eventing.system

object RunSimpleConsumer extends App {
  val consumer = system.actorOf(SimpleKinesisConsumer.props, "simple-consumer")
}

object SimpleKinesisConsumer {

  sealed trait State

  case object TestInProgress extends State

  case object TestFailed extends State

  case object TestSucceeded extends State

  case object ResetState

  case object GetState

  case class PitStop(client: ActorRef, seqNo: CompoundSequenceNumber)

  case class Append(int: Int)

  def props: Props = {
    val config = ConfigFactory.load("sample.conf").getConfig("kinesis")
    Props(classOf[SimpleKinesisConsumer], config)
  }
}

/**
  * Helpful for resetting this test:
  *
  * aws kinesis delete-stream --stream-name test-kinesis-reliability && \
  * aws dynamodb delete-table --table-name KinesisReliabilitySpec && \
  * sleep 120 && \
  * aws kinesis create-stream --stream-name test-kinesis-reliability --shard-count 2
  */
class SimpleKinesisConsumer(kinesisConfig: Config) extends Actor with LazyLogging {

  import scala.concurrent.duration._
  import context.dispatcher

  implicit val timeout = akka.util.Timeout(5.minutes)

  var messagesConsumed = ListBuffer.empty[Int]

  private var state: State = TestInProgress

  val consumer = KinesisConsumer(ConsumerConf(kinesisConfig, "testConsumer"), self, context)

  //We don't want to keep running without a consumer in play
  consumer.start().onComplete { _ =>
    logger.error("KCL Worker completed (this is unexpected!), shutting down")
    System.exit(3)
  }

  private var expectedNumberOfMessages = kinesisConfig.getInt("test.expectedNumberOfMessages")
  private val PITSTOP_COUNT            = kinesisConfig.getInt("test.consumer.pitstopCount")
  private val PITSTOP_COEFF            = 0.5

  private var totalReceived = 0
  private var totalVerified = 0

  private var timeStarted: Option[DateTime] = None

  // scalastyle:off method.length
  override def receive: Receive = {

    case GetState =>
      sender() ! state

    case ResetState =>
      state = TestInProgress
      messagesConsumed.clear()
      totalReceived = 0
      totalVerified = 0
      logger.info(s">>> Consumer state reset to $TestInProgress, messages buffer cleared")

    case PitStop(client, seqNo) => {
      // buffer will have PITSTOP_COUNT messages in it
      // ensure that the first 90% of those messages are contiguous
      val took = (PITSTOP_COUNT * PITSTOP_COEFF).toInt
      messagesConsumed = messagesConsumed.sorted // IMPORTANT: SORT BEFORE CHECKING HEALTH
      val firstNpct = messagesConsumed.take(took)
      // finally, clear that first 90% of messages if we're healthy
      if (!isHealthy(took, firstNpct)) {
        logger.error(s"\n\nFailed pit stop check @ $totalReceived!\n\n")
        logger.error(s"\n\nFailed pit stop check @ $totalReceived!\n\n")
        logger.error(s"\n\nFailed pit stop check @ $totalReceived!\n\n")
        context.stop(self)
        System.exit(3)
      } else {
        logger.warn(s"\n\n**** PIT STOP OK: $totalVerified records verified\n\n")
        logTiming()
        logger.warn(s"\n**** PIT STOP OK\n")
        expectedNumberOfMessages -= took
        messagesConsumed.remove(0, took)
        totalVerified += took
        client ! EventProcessed(seqNo)
      }
    }

    case ProcessEvent(event) => {
      //logger.trace(s"[!] Incoming message: ${event.payload}, with seqNo: ${event.sequenceNumber}")

      val payload = event.payload.toInt

      val client = sender()
      //payload is literally an int
      payload match {
        case SimpleKinesisProducer.END_TEST_SIG_MSG =>
          // producer will send this message when it's finished
          logger.info(">>> SimpleConsumer received finished sig, ending processing")
          determineFinalState()
          client ! EventProcessed(event.sequenceNumber)

        case SimpleKinesisProducer.IGNORE_MSG =>
          // cheater / dummy case so that we can spam messages which
          // don't affect test output, but force checkpointing (so that subsequent test runs are clean)
          logger.info(">>> Force checkpoint")
          client ! EventProcessed(event.sequenceNumber)

        case _ =>
          totalReceived += 1
          messagesConsumed.append(payload)
          if (timeStarted.isEmpty) timeStarted = Some(new DateTime(DateTimeZone.UTC))
          if (messagesConsumed.length % 1000 == 0)
            logger.info(s">>> SimpleConsumer is storing its ${totalReceived}th message")
          // make sure we're healthy every once in a while
          if (messagesConsumed.length % PITSTOP_COUNT == 0 && messagesConsumed.nonEmpty)
            self ! PitStop(client, event.sequenceNumber)
          else
            client ! EventProcessed(event.sequenceNumber)
      }
    }

    case ConsumerWorkerFailure(failedEvents, shardId) =>
      logger.info(">>> ConsumerWorker failed shutting down SimpleConsumer (processor)")
      System.exit(3)

    case ConsumerShutdown(shardId) =>
      logger.info(">>> Consumer Shutdown received...")
      System.exit(3)
  }

  //scalastyle:on

  private def isHealthy(expectedNum: Int, section: Seq[Int]): Boolean = {
    val expectedRange: Iterator[Int]      = Range(totalVerified, totalVerified + expectedNum).iterator
    val actualSortedResult: Iterator[Int] = section.iterator
    def compare(actual: Iterator[Int], expected: Iterator[Int]): Boolean = {
      logger.info(s"\nMATCHING    : $totalVerified to ${totalVerified + expectedNum}")
      val allMatch = actual
        .zip(expected)
        .forall(x => {
          val matchSucceeded = x._1 == x._2

          if (!matchSucceeded)
            logger.info(s"MATCH FAILED: actual ${x._1} == ${x._2} expected")

          matchSucceeded
        })
      val lengthsMatch = actual.length == expected.length
      logger.warn(s"isHealthy allMatch $allMatch, lengthsMatch $lengthsMatch\n")
      allMatch && lengthsMatch
    }
    compare(actualSortedResult, expectedRange)
  }

  private def determineFinalState(expectedNum: Int = expectedNumberOfMessages): Unit = {
    // IMPORTANT: SORT BEFORE CHECKING HEALTH
    val healthy = isHealthy(expectedNum, messagesConsumed.sorted)

    logTiming()

    if (healthy) {
      state = TestSucceeded
      logger.info(
        s">>> SimpleConsumer: ♡♡♡ Test Succeeded: Found ${messagesConsumed.length} entries, no duplicates ♡♡♡"
      )
    } else {

      state = TestFailed
      logger.info(s">>> SimpleConsumer: ✖︎✖︎✖︎ Test FAILED ✖︎✖︎✖︎")
    }
  }

  private def logTiming() = {
    val now              = new DateTime(DateTimeZone.UTC)
    val processingPeriod = new Period(timeStarted.get, now)
    logger.warn(s"\n- $totalReceived records received in ${processingPeriod.toStandardMinutes}")
    val secondsElapsed = processingPeriod.toStandardSeconds.getSeconds
    logger.warn(s"- Records/sec:     ${totalReceived / secondsElapsed}")
    logger.warn(s"- Test started at  ${timeStarted.get}")
    logger.warn(s"- Current time is  $now")
    logger.warn(s"- Seconds elapsed: $secondsElapsed\n\n")
  }

}
