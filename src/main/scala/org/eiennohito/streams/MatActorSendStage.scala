package org.eiennohito.streams

import akka.actor.ActorRef
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet}

/**
  * @author eiennohito
  * @since 2016/10/01
  */
class MatActorSendStage extends GraphStage[BiSink[ActorRef, Any]] {
  val actor = Inlet[ActorRef]("actor")
  val messages = Inlet[Any]("messages")

  val shape = BiSink(actor, messages)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler {

    private var actorRef: ActorRef = null

    setHandler(actor, new InHandler {
      override def onPush(): Unit = {
        actorRef = grab(actor)
        //cancel(actor)
        pull(messages)
      }

      override def onUpstreamFinish(): Unit = {}
    })

    override def onPush(): Unit = {
      actorRef ! grab(messages)
      pull(messages)
    }

    override def preStart(): Unit = {
      pull(actor)
    }

    setHandler(messages, this)
  }
}
