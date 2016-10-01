package org.eiennohito.crawl.http

import java.nio.ByteBuffer

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import org.jsoup.helper.JsoupUtil
import org.jsoup.nodes.Document
import org.jsoup.parser.Parser

import scala.util.Try

/**
  * @author eiennohito
  * @since 2016/10/01
  */
class HttpParserStage(charset: Option[String], uri: String, parser: Parser) extends GraphStage[FlowShape[ByteString, ParsedDocument]] {
  val in = Inlet[ByteString]("in")
  val out = Outlet[ParsedDocument]("out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private var seq = ByteString.empty

    override def onPush(): Unit = {
      val fresh = grab(in)
      seq = seq.concat(fresh)
      pull(in)
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      val len = seq.length
      val buff = ByteBuffer.allocate(len)
      seq.copyToBuffer(buff)
      buff.clear()
      val doc = ParsedDocument(len, buff, Try(JsoupUtil.fromBuffer(buff, charset.getOrElse("utf-8"), uri, parser)))
      push(out, doc)
      completeStage()
    }

    setHandlers(in, out, this)
  }
}

case class ParsedDocument(read: Int, bytes: ByteBuffer, document: Try[Document])
