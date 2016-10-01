package org.eiennohito.crawl.http

import akka.NotUsed
import akka.actor.Status.Failure
import akka.actor.{Actor, ActorRef}
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Keep, Sink}
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.SimpleRobotRules.RobotRulesMode
import crawlercommons.robots.{SimpleRobotRules, SimpleRobotRulesParser}

import scala.collection.mutable

/**
  * @author eiennohito
  * @since 2016/10/01
  */
class HostHandler(base: Uri, spider: AkkaSpider) extends Actor with StrictLogging {
  import HostHandler._
  private var source: ActorRef = null
  private var filter: UrlFilter = null

  private val unhandled = new mutable.HashSet[Uri]()
  private val handled = new mutable.HashSet[Uri]()
  private val robotsUri = base.withPath(base.path / "robots.txt")

  private def makePipeline: ActorRef = {
    val meAsSink1 = Sink.actorRef(self, NotUsed)
    val meAsSink2 = Sink.actorRef(self, Completed(base))
    val pipeline = spider.pipeline(base.scheme, base.authority.host.address(), base.effectivePort, meAsSink1)
    val (a1, a2) = pipeline.toMat(meAsSink2)(Keep.left).run()(spider.rootMat)
    source = a1
    source
  }

  private def srcActor: ActorRef = {
    if (source != null) source
    else makePipeline
  }

  private def makeReq(u: Uri) = {
    spider.factory.make(u)
  }

  private var failed = false
  private var successiveFailures = 0

  private def handleFailure(e: Throwable): Unit = {
    e match {
      case x: EntityStreamSizeException =>
        if (unhandled.nonEmpty) {
          unhandled.remove(unhandled.head)
        }
      case _ =>
        logger.error("failure", e)
    }
    import scala.concurrent.duration._
    if (!failed) {
      failed = true
      successiveFailures += 1
      val mpler = successiveFailures * successiveFailures
      context.system.scheduler.scheduleOnce(5.seconds * mpler, self, Retry)(context.dispatcher)
    }
  }

  private def handleRetry(): Unit = {
    source = null
    resendUnhandled()
  }

  private def resendUnhandled(): Unit = {
    val copy = unhandled.clone()
    unhandled.clear()
    for (u <- copy) {
      handleUri(u)
    }
  }

  private def handleUri(u: Uri): Unit = {
    if (filter == null) {
      unhandled += u
    } else {
      if (filter.shouldAllow(u) && !handled.contains(u)) {
        unhandled += u
        srcActor ! makeReq(u)
      }
    }
  }

  override def preStart(): Unit = {
    srcActor ! makeReq(robotsUri)
  }

  def handleRedirect(req: HttpRequest, resp: HttpResponse): Unit = {
    logger.trace("{} was a redirect", req.uri)
  }

  import scala.concurrent.duration._

  override def receive: Receive = {
    case u: Uri => handleUri(u)
    case UriHandled(u, status) =>
      logger.trace(s"uri $u handled: $status")
      if (u eq robotsUri) {
        handleRobotUri(status)
      } else {
        unhandled.remove(u)
        handled.add(u)
      }
    case doc: ProcessedDocument =>
      successiveFailures = 0
      if (doc.uri eq robotsUri) {
        handleRobots(doc)
      } else context.parent ! doc
    case HandleRedirect(req, resp) => handleRedirect(req, resp)
    case Failure(e) =>
      if (successiveFailures < 3) {
        handleFailure(e)
      } else context.parent ! Completed(base)
    case Retry => handleRetry()
    case c: Completed =>
      context.system.scheduler.scheduleOnce(100.milli, self, MaybeComplete)(context.dispatcher)
    case MaybeComplete =>
      if (unhandled.nonEmpty) {
        context.system.scheduler.scheduleOnce(5.seconds, self, Retry)(context.dispatcher)
      } else context.parent ! Completed(base)
    case NotUsed =>
  }

  private def handleRobotUri(status: StatusCode): Unit = {
    if (status.isFailure()) {
      filter = allowAll
      resendUnhandled()
    } else if (status.isRedirection()) {
      filter = allowNone
      context.parent ! Completed(base)
    }
  }

  private def handleRobots(doc: ProcessedDocument): Unit = {
    filter = makeFilter(
      robotsUri.toString(),
      doc.doc.bytes.array(),
      doc.headers.find(_.is("content-type")).map(_.value()).orNull,
      spider.botName
    )
    if (filter.isCompletelyForbidden()) {
      context.parent ! Completed(base)
    } else resendUnhandled()
  }
}

object HostHandler extends StrictLogging {
  case class Completed(base: Uri)
  case object Retry
  case object HandleRobots
  case object RobotsUnavailable
  case object MaybeComplete

  def makeFilter(uri: String, content: Array[Byte], ctype: String, name: String) = {
    val parser = new SimpleRobotRulesParser
    val rules = parser.parseContent(uri, content, ctype, name)
    try {
      new UrlFilter(rules)
    } catch {
      case e: Exception =>
        logger.debug(s"could not parse robots.txt for $uri, forbidding crawl")
        allowNone
    }
  }

  private val allowNone = new UrlFilter(new SimpleRobotRules(RobotRulesMode.ALLOW_NONE))
  private val allowAll = new UrlFilter(new SimpleRobotRules(RobotRulesMode.ALLOW_ALL))
}
