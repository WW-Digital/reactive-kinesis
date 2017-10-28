package com.weightwatchers.reactive.kinesis

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKitBase
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.ExecutionContextExecutor

/**
  * Use this test trait for akka based tests.
  * An akka system is started and cleaned up automatically.
  */
trait AkkaUnitTestLike extends TestKitBase with ScalaFutures with BeforeAndAfterAll {
  self: Suite =>

  implicit lazy val config: Config                = ConfigFactory.load("sample.conf")
  implicit lazy val system: ActorSystem           = ActorSystem(suiteName, config)
  implicit lazy val scheduler: Scheduler          = system.scheduler
  implicit lazy val mat: Materializer             = ActorMaterializer()
  implicit lazy val ctx: ExecutionContextExecutor = system.dispatcher

  abstract override def afterAll(): Unit = {
    super.afterAll()
    // intentionally shutdown the actor system last.
    system.terminate().futureValue
  }
}
