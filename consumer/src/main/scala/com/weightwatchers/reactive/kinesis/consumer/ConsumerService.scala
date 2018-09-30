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

package com.weightwatchers.reactive.kinesis.consumer

import scala.concurrent.Future

/**
  * Abstraction for consumer service.
  */
trait ConsumerService {

  /**
    * Start service.
    * @return a future that refers to the lifecycle of this service.
    */
  def start(): Future[Unit]

  /**
    * Stop this service.
    */
  def stop(): Unit
}
