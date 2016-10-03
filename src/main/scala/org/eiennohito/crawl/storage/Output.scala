package org.eiennohito.crawl.storage

import java.io.{ByteArrayOutputStream, OutputStreamWriter}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.http.scaladsl.coding.Gzip
import akka.stream.SubstreamCancelStrategy
import akka.stream.scaladsl.{FileIO, Flow, Sink}
import akka.util.ByteString
import io.mola.galimatias.URL
import org.eiennohito.crawl.http.{ProcessedDocument, UriHandled}

import scala.concurrent.Future
import scala.util.Success

/**
  * @author eiennohito
  * @since 2016/10/03
  */
object Output {

  object CountTo {
    def apply[T](upto: Int): T => Boolean = new Function[T, Boolean] {
      private[this] var i = 0
      override def apply(v1: T): Boolean = {
        val v = i
        i += 1
        if (v >= upto) {
          i -= upto
          true
        } else {
          false
        }
      }
    }
  }

  object CountWeighted {
    def apply[T](upto: Long, f: T => Long): T => Boolean = new Function[T, Boolean] {
      private[this] var i = 0L
      override def apply(v1: T): Boolean = {
        val v = i
        i += f(v1)
        if (v >= upto) {
          i = 0
          true
        } else {
          false
        }
      }
    }
  }

  def saveUris(root: Path, perFile: Int): Sink[UriHandled, NotUsed] = {
    Files.createDirectories(root)
    val flow = Flow[UriHandled].splitWhen(SubstreamCancelStrategy.propagate)(CountTo(perFile))
    val cnt = new AtomicInteger()

    val sink = Sink.lazyInit[ByteString, NotUsed](
      _ => Future.successful(Gzip.encoderFlow.to(FileIO.toPath(root.resolve(f"${cnt.getAndIncrement()}%04d.uris.gz")))),
      () => NotUsed
    )

    flow.map { u =>
      val url = URL.parse(u.uri.toString())
      ByteString.fromString(s"${url.toHumanString}\t${u.status.intValue()}\n")
    }.to(sink)
  }

  def saveDocs(root: Path, perFile: Long): Sink[ProcessedDocument, NotUsed] = {
    Files.createDirectories(root)
    val cnt = new AtomicInteger()
    val flow = Flow[ProcessedDocument].splitWhen(SubstreamCancelStrategy.propagate)(CountWeighted(perFile, _.doc.read))

    val sink = Sink.lazyInit[ByteString, NotUsed](
      _ => Future.successful(Gzip.encoderFlow.to(FileIO.toPath(root.resolve(f"${cnt.getAndIncrement()}%06d.html.gz")))),
      () => NotUsed
    )

    flow.map { doc =>
      val o = doc.doc
      o.document match {
        case Success(d) =>
          val baos = new ByteArrayOutputStream(o.read + 2048)
          val sbld = new OutputStreamWriter(baos, "utf-8")
          val url = URL.parse(doc.uri.toString())
          sbld.append(url.toHumanString)
          sbld.append('\n')
          d.html(sbld)
          sbld.append("\n-----!!!----html-document----!!!-----\n")
          sbld.flush()
          ByteString.fromArray(baos.toByteArray)
        case _ => ByteString.empty
      }
    }.to(sink)
  }
}
