/*
 * Copyright 2017 WeightWatchers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.weightwatchers.reactive.kinesis

import akka.actor.{ActorSystem, Scheduler}
import akka.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, Suite}
import org.scalatest.freespec.AnyFreeSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.mockito.MockitoSugar

/**
  * Base trait for all unit tests.
  */
trait UnitTest
    extends AnyFreeSpecLike
    with Matchers
    with MockitoSugar
    with GivenWhenThen
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll

trait AkkaTest extends BeforeAndAfterAll with PatienceConfiguration { this: Suite =>

  protected implicit lazy val system: ActorSystem = ActorSystem(suiteName, akkaConfig)

  protected implicit lazy val akkaScheduler: Scheduler = system.scheduler

  protected def akkaConfig: Config = ConfigFactory.load()

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  // Adjust the default patience configuration for all akka based tests
  override implicit def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)))
}
