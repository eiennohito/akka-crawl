package org.eiennohito.crawl.http

import java.nio.ByteBuffer

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractor
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import org.jsoup.helper.JsoupUtil
import org.jsoup.nodes.Document
import org.jsoup.parser.Parser

import scala.util.{Failure, Success, Try}

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
      val doc = HttpParserStage.parseDocument(len, parser, buff, charset, uri)
      push(out, doc)
      completeStage()
    }

    setHandlers(in, out, this)
  }
}

object HttpParserStage {
  private[this] val detector = LanguageDetectorBuilder
    .create(NgramExtractor.gramLengths(1, 2, 3))
    .withProfiles(new LanguageProfileReader().readAllBuiltIn())
    .seed(0xdeadbeefL)
    .build()

  private[this] val tof = CommonTextObjectFactories.forDetectingOnLargeText()

  def parseDocument(len: Int, parser: Parser, data: ByteBuffer, charset: Option[String], uri: String) = {
    try {
      data.clear()
      val pdoc = JsoupUtil.fromBuffer(data, charset.orNull, uri, parser)
      val txt = tof.forText(pdoc.text())
      val lang = detector.detect(txt)
      val theLang = if (lang.isPresent) Some(lang.get().getLanguage) else None
      ParsedDocument(len, data, Success(pdoc), theLang)
    } catch {
      case e: Exception =>
        val pdoc = Failure(e)
        ParsedDocument(len, data, pdoc, None)
    }
  }
}

case class ParsedDocument(read: Int, bytes: ByteBuffer, document: Try[Document], lang: Option[String])
