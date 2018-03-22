/*
 Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
