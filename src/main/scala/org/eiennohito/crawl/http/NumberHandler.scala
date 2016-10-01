package org.eiennohito.crawl.http

import java.util.Comparator

import akka.actor.{Actor, ActorRef, ActorRefFactory, PoisonPill, Props}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import com.google.common.collect.MinMaxPriorityQueue
import com.optimaize.langdetect.LanguageDetectorBuilder
import com.optimaize.langdetect.ngram.NgramExtractor
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.text.CommonTextObjectFactories
import com.typesafe.scalalogging.StrictLogging
import io.mola.galimatias.URL
import org.jsoup.nodes.{Document, Node}
import org.jsoup.select.{NodeTraversor, NodeVisitor}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * @author eiennohito
  * @since 2016/10/01
  */
class NumberHandler(spider: AkkaSpider, maxConcurrent: Int) extends Actor with StrictLogging {
  import NumberHandler._

  import scala.concurrent.duration._

  private val ctx = new CrawlContext(spider)

  private val cancel = context.system.scheduler.schedule(3.seconds, 1.minute, self, StartCrawl)(context.dispatcher)
  private val reporting = context.system.scheduler.schedule(5.seconds, 5.seconds, self, Report)(context.dispatcher)

  private var numBytes = 0L
  private var numDocs = 0L

  override def receive: Receive = {
    case StartCrawl => ctx.startCrawl(context, maxConcurrent)
    case HostHandler.Completed(uri) => ctx.complete(uri)
    case doc: ProcessedDocument =>
      numDocs += 1
      numBytes += doc.doc.read
      ctx.extract(doc)
    case Report => report()
    case uri: Uri => ctx.addUri(uri)
  }

  def report() = {
    logger.info(s"active=${ctx.active.size}, pending=${ctx.links}/${ctx.pending.size}, processed=${numBytes}|${numDocs}")
    numBytes = 0
    numDocs = 0
  }

  override def postStop(): Unit = {
    cancel.cancel()
    reporting.cancel()
  }
}

object NumberHandler {
  case object StartCrawl
  case object Report
}


class CrawlContext(spider: AkkaSpider)(implicit sender: ActorRef) extends StrictLogging {

  private val detector = LanguageDetectorBuilder
    .create(NgramExtractor.gramLengths(1, 2, 3))
    .withProfiles(new LanguageProfileReader().readAllBuiltIn())
    .seed(0xdeadbeefL)
    .build()

  val tof = CommonTextObjectFactories.forDetectingOnLargeText()

  def extractImpl(refer: Uri, d: Document): Unit = {
    val txt = d.text()


    val data = tof.forText(txt)

    val lng = detector.detect(data)

    if (!lng.isPresent || lng.get().getLanguage != "ja") {
      logger.trace(s"$refer was not Jp enough, it was $lng")
      return
    }

    val trav = new NodeTraversor(new NodeVisitor {
      private val baseUrl = URL.parse(refer.toString())
      override def head(node: Node, depth: Int): Unit = {
        if (node.nodeName() == "a") {
          if (node.hasAttr("href")) {
            val lnk = node.attr("href")

            if (lnk.startsWith("javascript:")) return

            try {
              val java = URL.parse(baseUrl, lnk)

              val path = java.path()
              if (!spider.filter.filter(path)) return

              val uri = Uri(
                scheme = java.scheme(),
                authority = Uri.Authority(
                  host = Uri.Host(java.host().toString),
                  port = if (java.port() == java.defaultPort()) 0 else java.port(),
                  userinfo = java.userInfo()
                ),
                path = Uri.Path(path),
                queryString = Option(java.query()),
                fragment = None
              )

              handleUri(refer, uri)
            } catch {
              case e: Exception =>
                logger.warn(s"could not parse uri: $lnk", e)
            }
          }
        }
      }
      override def tail(node: Node, depth: Int): Unit = {}
    })
    trav.traverse(d)
  }

  def extract(doc: ProcessedDocument): Unit = {
    doc.doc.document match {
      case Success(d) => extractImpl(doc.uri, d)
      case Failure(e) => logger.trace(s"failed to process ${doc.uri}", e)
    }
  }

  def complete(uri: Uri): Unit = {
    logger.trace(s"host $uri is completed")
    active.remove(uri).foreach(_ ! PoisonPill)
  }

  def addUri(uri: Uri) = {
    val base = CrawlContext.baseUri(uri)
    active.get(base) match {
      case Some(actor) => actor ! uri
      case _ => pending.get(base) match {
        case Some(set) =>
          if (set.add(uri)) {
            links += 1
          }
        case None =>
          val set = new mutable.HashSet[Uri]()
          pending.put(base, set)
          set.add(uri)
          links += 1
      }
    }
  }

  def handleUri(base: Uri, uri: Uri) = {
    if (uri.isAbsolute) {
      addUri(uri)
    } else {
      val resolved = uri.resolvedAgainst(base)
      addUri(resolved)
    }
  }

  val pending = new mutable.HashMap[Uri, mutable.HashSet[Uri]]()
  val active = new mutable.HashMap[Uri, ActorRef]()
  var links = 0L

  private def doStart(toLaunch: Int, fact: ActorRefFactory) = {
    val uris = selectUris(toLaunch)
    for (u <- uris) {
      logger.trace(s"starting crawling host $u")
      val props = Props(new HostHandler(u, spider))
      val actor = fact.actorOf(props)
      active.put(u, actor)
      pending.remove(u).foreach { uris =>
        uris.foreach(cu => actor ! cu)
        links -= uris.size
      }
    }
  }

  private[http] def selectUris(toLaunch: Int): Iterable[Uri] = {
    val heap = MinMaxPriorityQueue.orderedBy(new Comparator[(Uri, Int)] {
      override def compare(o1: (Uri, Int), o2: (Uri, Int)): Int = -o1._2.compareTo(o2._2)
    }).maximumSize(toLaunch).create[(Uri, Int)]()

    for ((u, m) <- pending) {
      heap.add((u, m.size))
    }

    logger.trace(s"selected ${heap.size()}/$toLaunch hosts")
    heap.asScala.map(_._1)
  }

  def startCrawl(fact: ActorRefFactory, maxConcurrent: Int): Unit = {
    val launchBorder = (maxConcurrent * 5 / 100) max 1 //5% of maxValue, but not less than one
    val running = active.size
    val toLaunch = maxConcurrent - running
    if (toLaunch >= launchBorder) {
      doStart(toLaunch, fact)
    }
  }
}

object CrawlContext {
  def baseUri(u: Uri) = {
    u.copy(
      authority = u.authority.copy(userinfo = ""),
      path = Path.SingleSlash,
      rawQueryString = None,
      fragment = None
    )
  }
}
