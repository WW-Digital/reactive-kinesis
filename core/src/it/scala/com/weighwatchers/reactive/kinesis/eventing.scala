package com.weighwatchers.reactive

package object eventing {

  /** Used by SimpleKinesisConsumer and SimpleKinesisProducer */
  implicit val system = akka.actor.ActorSystem.create("test-system")
}
