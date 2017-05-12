package com.weightwatchers.eventing

import akka.actor.{Actor, PoisonPill, Props}
import com.fasterxml.uuid.Generators
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import com.weightwatchers.eventing.SimpleKinesisProducer._
import com.weightwatchers.eventing.models.ProducerEvent
import com.weightwatchers.eventing.producer.KinesisProducerActor.{Send, SendFailed, SendSuccessful, SendWithCallback}
import com.weightwatchers.eventing.producer.{KinesisProducerActor, KinesisProducerKPL}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object RunSampleProducer extends App {
  val producer = system.actorOf(SimpleKinesisProducer.props)
  producer ! Start
}

object SimpleKinesisProducer {

  //Generator for the partition keys (overkill...)
  val UUID_GENERATOR = Generators.timeBasedGenerator()

  val IGNORE_MSG: Int = -2

  val END_TEST_SIG_MSG: Int = -1

  case object DrainBatch

  case object Start

  case object StopBatch

  case object LogResults

  case class ProduceWithBatch(upTo: Int)

  def props: Props = {
    val config = ConfigFactory.load("sample.conf").getConfig("kinesis")
    Props(classOf[SimpleKinesisProducer], config)
  }
}

class SimpleKinesisProducer(kConfig: Config) extends Actor with LazyLogging {

  protected def kinesisConfig = kConfig

  var current: Int = 0
  val totalMessages = kinesisConfig.getInt("test.expectedNumberOfMessages")
  val batchSize = kinesisConfig.getInt("test.producer.batchSize")
  //Delay between batches of messages
  val batchDelay = kinesisConfig.getInt("test.producer.batchDelayMillis")

  //We're creating the producer the hard way to get access to the underlying KPL
  val kpaProps = KinesisProducerActor.props(kinesisConfig, "testProducer")
  val kpa = context.actorOf(kpaProps)
  val kinesisProducerKPL = kpaProps.args.head.asInstanceOf[KinesisProducerKPL]

  /* producer without actor:
  val producerConfig = kinesisConfig.getConfig("testProducer")
  val streamName = producerConfig.getString("stream-name")
  val kinesisProducer = KinesisProducerKPL(
  producerConfig.getConfig("kpl"), kinesisConfig.getString("aws.profile"), streamName)
  */

  val outstandingMessages = mutable.AnyRefMap[String, Int]()
  val failedMessages = mutable.AnyRefMap[String, Int]()

  val SECONDS_IN_NANO = 1000000000
  var startTime: Long = 0

  //scalastyle:off method.length
  override def receive: Receive = {
    case Start =>
      startTime = System.nanoTime() // Not perfectly accurate but we're working in seconds so good enough
      self ! ProduceWithBatch(Math.min(totalMessages, current + batchSize))

    case DrainBatch =>

      kpa ! Send(ProducerEvent(UUID_GENERATOR.generate().toString, IGNORE_MSG.toString))
      //kinesisProducer.addUserRecord(ProducerEvent(UUID.randomUUID.toString, IGNORE_MSG.toString))
      // give some time for messages to drain, then signal to finish the test
      context.system.scheduler.scheduleOnce(30.seconds, self, StopBatch)

    case StopBatch =>
      kpa ! Send(ProducerEvent(UUID_GENERATOR.generate().toString, END_TEST_SIG_MSG.toString))
      logger.info(s"************************************")
      logger.info(s"***Finished sending batch to Actor****")
      val endTime = System.nanoTime()
      val durationSecs = (endTime - startTime) / SECONDS_IN_NANO
      logger.info(s"***Time sending all messages to ProducerActor: $durationSecs Seconds****")
      logger.info(s"************************************")

      //kinesisProducer.addUserRecord(ProducerEvent(UUID.randomUUID.toString, END_TEST_SIG_MSG.toString))
      self ! LogResults

    case LogResults =>
      logger.info(s"***ProducerActor with ${outstandingMessages.size} unconfirmed sends***")
      logger.info(s"***ProducerActor with ${failedMessages.size} failed sends***")
      logger.info(s"***KPL Has ${kinesisProducerKPL.outstandingRecordsCount()} Outstanding Concurrent Requests***")
      logger.info(s"************************************")
      //Keep logging incase we're not yet done processing....

      if (outstandingMessages.nonEmpty) {
        context.system.scheduler.scheduleOnce(10.seconds, self, LogResults)
      } else {
        logger.info(s"************************************")
        logger.info(s"****Finished Producer****")
        val endTime = System.nanoTime()
        val durationSecs = (endTime - startTime) / SECONDS_IN_NANO
        logger.info(s"****Total time to completion: $durationSecs Seconds****")
        logger.info(s"************************************")
        self ! PoisonPill
      }

    case ProduceWithBatch(upTo) =>
      Range(current, upTo).foreach {
        case i =>
          val event = ProducerEvent(UUID_GENERATOR.generate().toString, s"$i")

          val msg = SendWithCallback(event)
          kpa ! msg
          //kinesisProducer.addUserRecord(event)
          outstandingMessages += (msg.messageId, i)
      }

      current = upTo
      if (current < totalMessages) {
        val nextUp = Math.min(totalMessages, current + batchSize)
        logger.info(s"* Queueing a batch of messages, up to $nextUp")
        context.system.scheduler.scheduleOnce(batchDelay.millis, self, ProduceWithBatch(nextUp))
      } else
        context.system.scheduler.scheduleOnce(3.seconds, self, DrainBatch)

    case SendSuccessful(messageId, _) =>
      logger.trace(
        s"Successfully sent $messageId")
      outstandingMessages -= messageId

    case SendFailed(messageId, reason) =>
      val failedPayload = outstandingMessages(messageId)
      logger.info(
        s"""Failed to send ProducerEvent($messageId, $failedPayload""".stripMargin)
      outstandingMessages -= messageId
      failedMessages += (messageId, failedPayload)
  }

  //scalastyle:on
}
