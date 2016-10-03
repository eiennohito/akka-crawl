package org.eiennohito.crawl.http

import akka.NotUsed
import akka.actor.{Actor, ActorRef, Terminated}
import akka.actor.Status.Failure
import akka.http.scaladsl.model.{EntityStreamSizeException, HttpRequest, Uri}
import akka.http.scaladsl.model.headers.Location
import akka.stream.Fusing
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink}
import com.typesafe.scalalogging.StrictLogging
import crawlercommons.robots.BaseRobotRules

import scala.collection.mutable
import scala.concurrent.TimeoutException

/**
  * @author eiennohito
  * @since 2016/10/02
  */
class HostHandler2(base: Uri, spider: AkkaSpider) extends Actor with StrictLogging {
  import HostHandler._

  import scala.concurrent.duration._

  private val robotsUri = base.withPath(Uri.Path("/robots.txt"))
  private val rf = new RequestFactory

  private val httpPipeline = {
    val meAsSink1 = Sink.actorRef(self, NotUsed)
    val meAsSink2 = Sink.actorRef(self, NotUsed)
    val pipe1 = spider.pipeline(base.scheme, base.authority.host.address(), base.effectivePort, meAsSink1)
    val full = pipe1.toMat(meAsSink2)(Keep.left)
    RunnableGraph.fromGraph(Fusing.aggressive(full))
  }

  private var requester: ActorRef = null

  private def requestOne(uri: Uri): Unit = {
    val req = rf.make(uri)
    requestOne(req)
  }

  private var terminateConnections = false

  def requestOne(req: HttpRequest): Unit = {
    if (requester == null) {
      val (ac, _) = httpPipeline.run()(spider.rootMat)
      requester = ac
      if (!terminateConnections) {
        context.watch(requester)
      }
    }
    requester ! req
    if (terminateConnections) {
      requester ! HostPageSource.Finish
    }
  }

  private def finishCrawl(success: Boolean = false) = {
    if (requester != null) {
      context.unwatch(requester)
      requester ! HostPageSource.Finish
    }
    if (success) {
      context.parent ! Completed(base, viewed.toSet)
    } else {
      context.parent ! Skipped(base)
    }
  }

  private def startCrawl(wait: FiniteDuration) = {
    context.become(crawling(wait))
    pending.foreach(self ! _)
    pending = Set.empty
    self ! Retry
  }

  def robotsBase: Receive = {
    case u: Uri => pending += u
    case uh: UriHandled =>
      context.system.eventStream.publish(uh)
      uh.status.intValue() match {
        case 200 => //wait for actual content
        case _ if uh.status.isRedirection() =>
          uh.headers.find(_.is("location")) match {
            case Some(l: Location) =>
              val u = l.uri.withoutFragment

              if (u.isAbsolute && u.authority != base.authority) {
                getBots(u) match {
                  case Some(d) => self ! d
                  case None =>
                    logger.trace(s"could not get robots.txt from a redirect to another domain: $base -> $u")
                    finishCrawl()
                }
              } else {
                val resolved = if (u.isRelative) u.resolvedAgainst(uh.uri) else u
                redirects.add(uh.uri)
                if (redirects.contains(resolved)) {
                  logger.trace(s"redirect loop on $base robots.txt, treat as missing")
                  startCrawl(spider.timeout)
                } else if (redirects.size > 10) {
                  logger.trace(s"too many redirects for $base robots.txt, last is ${uh.uri}")
                } else {
                  logger.trace(s"robots txt redirect: ${uh.uri} -> $resolved")
                  doProcess(spider.timeout, resolved)
                }
              }
            case _ => finishCrawl()
          }
        case 403 | 404 => startCrawl(spider.timeout) //no robots.txt, everything is allowed
        case _ => finishCrawl()
      }
    case doc: ProcessedDocument =>
      parseRobots(doc, spider.botName) match {
        case None => finishCrawl()
        case Some(r) =>
          val waitTime = {
            val delay = r.getCrawlDelay
            if (delay == BaseRobotRules.UNSET_CRAWL_DELAY) {
              spider.timeout
            } else FiniteDuration(delay, SECONDS)
          }
          if (r.isAllowNone || waitTime > 2.minutes) {
            logger.trace(s"$base had too strict settings in for robots: forbid=${r.isAllowNone}, wait=$waitTime")
            finishCrawl()
          } else {
            if (waitTime > 20.seconds) {
              terminateConnections = true
            }

            filter = new UrlFilter(r)
            startCrawl(waitTime)
          }
      }
    case akka.actor.Status.Failure(e) =>
      logger.trace(s"could not acquire robots.txt from $base", e)
      finishCrawl()
  }

  var pending: Set[Uri] = Set.empty
  val viewed = new mutable.HashSet[Uri.Path]()
  var filter = allowAll
  val redirects = new mutable.HashSet[Uri]()

  private var trialNumber = 0
  var current: Uri = null
  private def doNext(wait: FiniteDuration) = {
    if (pending.isEmpty) {
      current = null
      context.system.scheduler.scheduleOnce(1.second, self, MaybeComplete)(context.dispatcher)
    } else {
      redirects.clear()
      val u = pending.head
      pending -= u
      trialNumber = 0
      current = u
      doProcess(wait, u)
    }
  }

  private def doProcess(after: FiniteDuration, u: Uri) = {
    val req = rf.make(u)
    context.system.scheduler.scheduleOnce(after, self, req)(context.dispatcher)
  }

  private def maybeRetry(waitTime: FiniteDuration, e: Throwable): Unit = {
    trialNumber += 1

    val maxTrials = 2

    e match {
      case _: EntityStreamSizeException => doNext(waitTime)
      case _: TimeoutException if trialNumber > maxTrials => doNext(waitTime) //don't log timeouts
      case _ if trialNumber > maxTrials =>
        logger.debug(s"error when processing $current", e)
        doNext(waitTime)
      case _ => doProcess(waitTime, current)
    }
  }

  def crawlingBase(waitTime: FiniteDuration): Receive = {
    case u: Uri =>
      if (filter.shouldAllow(u) && !viewed.contains(u.path) && spider.filter.accept(u.path)) {
        pending += u
      }
    case uh: UriHandled =>
      context.system.eventStream.publish(uh)
      viewed.add(uh.uri.path)
      val s = uh.status
      s.intValue() match {
        case 200 => //wait for data
        case _ if s.isRedirection() => // wait for redirect msg
          uh.headers.find(_.is("location")).foreach {
            case l: Location =>
              val u = l.uri.withoutFragment
              if (u.isAbsolute && u.authority != base.authority) {
                context.parent ! u
              } else {
                val resolved = if (u.isRelative) u.resolvedAgainst(uh.uri) else u
                self ! resolved
              }
              doNext(waitTime)
            case _ => //do nothing
          }
          doNext(waitTime)
        case _ => doNext(waitTime)
      }
    case doc: ProcessedDocument =>
      context.parent ! doc
      doNext(waitTime)
    case MaybeComplete =>
      (current == null, pending.isEmpty) match {
        case (true, true) => finishCrawl(success = true)
        case (true, false) => doNext(waitTime)
        case (false, true) => //do nothing
        case (false, false) => //do nothing
      }
    case Retry => doNext(waitTime)
    case Failure(e) => maybeRetry(waitTime, e)
  }

  override def preStart(): Unit = {
    super.preStart()
    requestOne(robotsUri)
  }

  def crawling(waitTime: FiniteDuration) = crawlingBase(waitTime) orElse default
  def robots = robotsBase orElse default

  override def receive: Receive = robots

  private def default: Receive = {
    case req: HttpRequest =>
      requestOne(req)
    case NotUsed =>
    case Terminated(a) =>
      requester = null
    case msg => logger.warn(s"unhandled message $msg")
  }
}
