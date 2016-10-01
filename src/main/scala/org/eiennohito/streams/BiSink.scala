package org.eiennohito.streams

import akka.stream.{Inlet, Outlet, Shape}

import scala.collection.immutable.Seq

/**
  * @author eiennohito
  * @since 2016/10/01
  */
final case class BiSink[T1, T2](in0: Inlet[T1], in1: Inlet[T2]) extends Shape {
  override val inlets: Seq[Inlet[_]] = Vector(in0, in1)
  override val outlets: Seq[Outlet[_]] = Nil
  override def deepCopy(): Shape = copy()
  override def copyFromPorts(inlets: Seq[Inlet[_]], outlets: Seq[Outlet[_]]): Shape = {
    if (inlets.size != 2) {
      throw new IllegalArgumentException("inlets should have size 2")
    }

    if (outlets.nonEmpty) {
      throw new IllegalArgumentException("outlets should be empty")
    }

    BiSink(inlets(0).asInstanceOf[Inlet[T1]], inlets(1).asInstanceOf[Inlet[T2]])
  }
}
