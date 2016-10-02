package org.eiennohito.crawl.http

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.settings.ClientConnectionSettings
import akka.stream._
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source, Zip}
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.BaseRobotRules
import org.jsoup.parser.Parser

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * @author eiennohito
  * @since 2016-08-14
  */
class AkkaSpider(asys: ActorSystem, val filter: StringBasedFilter, val timeout: FiniteDuration = 5.seconds) extends StrictLogging {
  private val http = Http(asys)
  val botName = "KU-akka-bot"
  val rootMat = ActorMaterializer.create(asys)
  val factory = new RequestFactory

  private val ua = `User-Agent`(
    ProductVersion(botName, "0.1-beta"),
    ProductVersion("akka-http", "2.4.11")
  )

  val settings = ClientConnectionSettings(asys)
    .withUserAgentHeader(Some(ua))

  def invalidUri(refer: Uri, lnk: String): Unit = {

  }

  def pipeline(scheme: String, host: String, port: Int, notify: Sink[UriHandled, _]) = {
    val loggingAdapter = new SpiderHtmlLogAdapter(host)

    val conn = scheme.toLowerCase match {
      case "http" => http.outgoingConnection(host = host, port = port, log = loggingAdapter)
      case "https" => http.outgoingConnectionHttps(host = host, port = port, log = loggingAdapter)
      case _ =>
        logger.warn(s"invalid scheme: $scheme")
        Flow.fromSinkAndSource(Sink.cancelled, Source.empty)
    }
    AkkaSpider.makePipeline(conn, rootMat, notify)
  }
}

case class UriHandled(uri: Uri, status: StatusCode, headers: Seq[HttpHeader])
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

    import GraphDSL.Implicits._
    val graph = GraphDSL.create(src, conn) (Keep.both) { implicit b => (is, conn) =>

      val input = is.out.throttle(1, 5.seconds, 1, ThrottleMode.Shaping)
      val bc = b.add(Broadcast[HttpRequest](2))
      input ~> bc

      val zip = b.add(Zip[HttpRequest, HttpResponse]())

      bc ~> zip.in0
      bc ~> conn ~> zip.in1

      val zipbcast = b.add(Broadcast[ReqResp](2))

      zip.out ~> zipbcast.in

      zipbcast.out(0).map { case (req, resp) =>
        if (logger.underlying.isTraceEnabled()) {
          logger.trace(s"${req.uri} -> ${resp.status.intValue()}")
        }
        UriHandled(req.uri, resp.status, resp.headers)
      } ~> notifyHandled

      val res = zipbcast.out(1).filter { case (req, resp) =>
          if (resp.status.intValue() != 200) {
            resp.entity.discardBytes(rootMat)
            false
          } else true
      }.flatMapConcat { case (req, resp) =>
        AkkaSpider.parsePage(req, resp).map(d => ProcessedDocument(req.uri, d, resp.headers))
      }

      SourceShape(res.outlet)
    }

    Source.fromGraph(graph)
  }
}


class RequestFactory {

  val headers = List(
    `Accept-Language`(
      LanguageRange(Language("ja"), 1.0f),
      LanguageRange(Language("ja_JP"), 1.0f),
      LanguageRange(Language("en"), 0.5f))
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

class UrlFilter(val robots: BaseRobotRules) extends BaseUriFilter {
  def shouldAllow(uri: Uri): Boolean = {
    val strval = uri.path.toString()
    robots.isAllowed(strval)
  }

  def isCompletelyForbidden(): Boolean = robots.isAllowNone
}

class HostPageSource extends ActorPublisher[HttpRequest] with StrictLogging {

  private val requests = new mutable.Queue[HttpRequest]()
  private var canStop = false

  def send(req: HttpRequest): Unit = {
    onNext(req)
  }

  override def receive: Receive = {
    case ActorPublisherMessage.Request(_) =>
      while (requests.nonEmpty && totalDemand > 0) {
        send(requests.dequeue())
      }
      if (requests.isEmpty && canStop) {
        onCompleteThenStop()
      }
    case r: HttpRequest =>
      if (totalDemand > 0) {
        send(r)
      } else requests += r
    case HostPageSource.Finish =>
      if (requests.isEmpty) {
        onCompleteThenStop()
      } else {
        canStop = true
      }
    case ActorPublisherMessage.Cancel =>
      context.stop(self)
  }

  override def preStart(): Unit = {
    logger.trace("started")
    super.preStart()
  }

  override def postStop(): Unit = {
    logger.trace(s"stopped: own=$canStop")
    super.postStop()
  }
}

object HostPageSource {
  case object Finish
}
