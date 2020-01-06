package com.weightwatchers.reactive.kinesis.common

import akka.actor.{ActorSystem, Scheduler}
import akka.testkit.TestKitBase
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, Suite}
import org.scalatestplus.mockito.MockitoSugar
import scala.concurrent.ExecutionContextExecutor


/**
  * Base trait for all integration tests.
  */
trait IntegrationTest
  extends AnyFreeSpecLike
    with Matchers
    with MockitoSugar
    with GivenWhenThen
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll

/**
  * Use this test trait for akka based tests.
  * An akka system is started and cleaned up automatically.
  */
trait AkkaUnitTestLike extends TestKitBase with ScalaFutures with BeforeAndAfterAll {
  self: Suite =>

  implicit lazy val config: Config                = ConfigFactory.load("sample.conf")
  implicit lazy val system: ActorSystem           = ActorSystem(suiteName, config)
  implicit lazy val scheduler: Scheduler          = system.scheduler
  implicit lazy val ctx: ExecutionContextExecutor = system.dispatcher

  abstract override def afterAll(): Unit = {
    super.afterAll()
    // intentionally shutdown the actor system last.
    system.terminate().futureValue
  }
}
