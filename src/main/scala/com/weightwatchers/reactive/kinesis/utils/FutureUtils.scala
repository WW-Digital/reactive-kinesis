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

package com.weightwatchers.reactive.kinesis.utils

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

/**
  * Utilities to help interop between Java/Guava and Scala Futures
  */
object FutureUtils {

  /**
    * Provides implicit extensions to improve usability of Java Futures in Scala.
    */
  implicit class ScalaListenableFuture[T](lf: ListenableFuture[T]) {

    /**
      * Converts a Guava ListenableFuture to a Scala Future.
      */
    def asScalaFuture(implicit ec: ExecutionContextExecutor): Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onSuccess(result: T): Unit = p success result

        def onFailure(t: Throwable): Unit = p failure t
      }, ec)
      p.future
    }
  }

}
