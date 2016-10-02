package org.eiennohito.crawl.http

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.stream._
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Partition, Sink, Source, Zip}
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.BaseRobotRules
import org.eiennohito.streams.MatActorSendStage
import org.jsoup.parser.Parser

import scala.collection.mutable

/**
  * @author eiennohito
  * @since 2016-08-14
  */
class AkkaSpider(asys: ActorSystem, val filter: StringBasedFilter) extends StrictLogging {
  private val http = Http(asys)
  val botName = "KU-akka"
  val rootMat = ActorMaterializer.create(asys)
  val factory = new RequestFactory

  def pipeline(scheme: String, host: String, port: Int, notify: Sink[UriHandled, _]) = {
    val conn = scheme.toLowerCase match {
      case "http" => http.outgoingConnection(host = host, port = port)
      case "https" => http.outgoingConnectionHttps(host = host, port = port)
      case _ =>
        logger.warn(s"invalid scheme: $scheme")
        Flow.fromSinkAndSource(Sink.cancelled, Source.empty)
    }
    AkkaSpider.makePipeline(conn, rootMat, notify)
  }
}

case class HandleRedirect(req: HttpRequest, resp: HttpResponse)
case class UriHandled(uri: Uri, status: StatusCode)
case class ProcessedDocument(uri: Uri, doc: ParsedDocument, headers: Seq[HttpHeader])

object AkkaSpider extends StrictLogging {
  def parsePage(req: HttpRequest, resp: HttpResponse): Source[ParsedDocument, _] = {
    val ctype = resp.header[`Content-Type`]
    val charset = ctype.flatMap(h => h.contentType.charsetOption.map(c  => c.value))
    val parser = ctype.map { ct => ct.contentType.mediaType match {
      case MediaTypes.`text/html` | MediaTypes.`text/plain` => Parser.htmlParser()
      case MediaTypes.`text/xml` | MediaTypes.`application/xhtml+xml` | MediaTypes.`application/xml` => Parser.xmlParser()
      case _ => Parser.htmlParser()
    }}.getOrElse(Parser.htmlParser())

    val path = req.uri.path.toString()
    val processor = Flow.fromGraph(new HttpParserStage(charset, path, parser))
    resp.entity.withSizeLimit(2 * 1024 * 1024).dataBytes.via(processor)
  }

  def makePipeline[Mat](
    conn: Flow[HttpRequest, HttpResponse, Mat],
    rootMat: ActorMaterializer,
    notifyHandled: Sink[UriHandled, _] = Sink.ignore
  ): Source[ProcessedDocument, (ActorRef, Mat)] = {
    import scala.concurrent.duration._
    val src = Source.actorPublisher[HttpRequest](Props[HostPageSource]())

    type ReqResp = (HttpRequest, HttpResponse)

    val ourConn = if (logger.underlying.isTraceEnabled) {
      Flow[HttpRequest].map(f => {
        logger.trace(s"req: ${f.uri}")
        f
      }).viaMat(conn)(Keep.right)
    } else conn

    import GraphDSL.Implicits._
    val graph = GraphDSL.create(src, ourConn) (Keep.both) { implicit b => (is, conn) =>

      val input = is.out.throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      val bc = b.add(Broadcast[HttpRequest](2))
      input ~> bc

      val zip = b.add(Zip[HttpRequest, HttpResponse]())

      bc ~> zip.in0
      bc ~> conn ~> zip.in1

      val actorSender = b.add(new MatActorSendStage)

      b.materializedValue.map { case (a, _) => a } ~> actorSender.in0

      val handleStatus = b.add(Partition[ReqResp](3, { case (_, resp) =>
        val status = resp.status
        if (status.intValue() == 200) 0
        else if (status.isRedirection()) 1
        else 2
      }))

      val zipbcast = b.add(Broadcast[ReqResp](2))

      zip.out ~> zipbcast.in

      zipbcast.out(0).map { case (req, resp) => UriHandled(req.uri, resp.status) } ~> notifyHandled
      zipbcast.out(1) ~> handleStatus

      val res = handleStatus.out(0).flatMapConcat { case (req, resp) =>
        AkkaSpider.parsePage(req, resp).map(d => ProcessedDocument(req.uri, d, resp.headers))
      }
      handleStatus.out(1).map { case (req, resp) =>
        resp.entity.discardBytes(rootMat)
        HandleRedirect(req, resp)
      } ~> actorSender.in1

      handleStatus.out(2).to(Sink.foreach { case (req, resp) => resp.entity.discardBytes(rootMat) })

      SourceShape(res.outlet)
    }

    Source.fromGraph(graph)
  }
}


class RequestFactory {

  val headers = List(
    `Accept-Language`(LanguageRange(Language("jp"), 1.0f), LanguageRange(Language("en"), 0.5f)),
    `Accept-Charset`(HttpCharsetRange(HttpCharsets.`UTF-8`)),
    Connection("keep-alive"),
    `User-Agent`(
      ProductVersion("Kyoto-University-Kurohashi-Lab-Crawler-Ng", "0.1-beta"),
      ProductVersion("akka-http", "2.4.11")
    )
  )

  def make(uri: Uri): HttpRequest = {
    val actual = Host(uri.authority) :: headers

    HttpRequest(
      method = HttpMethods.GET,
      uri = uri,
      headers = actual
    )
  }
}

trait BaseUriFilter {
  def shouldAllow(uri: Uri): Boolean
  def isCompletelyForbidden(): Boolean
}

class UrlFilter(robots: BaseRobotRules) extends BaseUriFilter {
  def shouldAllow(uri: Uri): Boolean = {
    val strval = uri.path.toString()
    robots.isAllowed(strval)
  }

  def isCompletelyForbidden(): Boolean = robots.isAllowNone
}

class HostPageSource extends ActorPublisher[HttpRequest] with StrictLogging {

  private val requests = new mutable.Queue[HttpRequest]()

  def send(req: HttpRequest): Unit = {
    onNext(req)
  }

  override def onError(cause: Throwable): Unit = {
    super.onError(cause)
  }


  override def onComplete(): Unit = {
    logger.trace("host completed")
    super.onComplete()
  }

  override def receive: Receive = {
    case ActorPublisherMessage.Request(_) =>
      while (requests.nonEmpty && totalDemand > 0) {
        send(requests.dequeue())
      }
    case r: HttpRequest =>
      if (totalDemand > 0) {
        send(r)
      } else requests += r
  }
}
