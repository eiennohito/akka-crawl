package com.example

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.Host

object Hello {
  def main(args: Array[String]): Unit = {
    val asys = ActorSystem("crawl")
    Http().outgoingConnection()

    println("Hello, world!")
  }
}
