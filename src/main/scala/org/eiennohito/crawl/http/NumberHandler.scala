package org.eiennohito.crawl.http

import java.util.Comparator

import akka.actor.{Actor, ActorRef, ActorRefFactory, PoisonPill, Props}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import com.google.common.cache.CacheBuilder
import com.google.common.collect.MinMaxPriorityQueue
import com.typesafe.scalalogging.StrictLogging
import io.mola.galimatias.URL
import org.jsoup.nodes.{Document, Node}
import org.jsoup.select.{NodeTraversor, NodeVisitor}

import scala.collection.JavaConverters._
import scala.collection.generic.Growable
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

  private val cancel = context.system.scheduler.schedule(3.seconds, 5.seconds, self, StartCrawl)(context.dispatcher)
  private val reporting = context.system.scheduler.schedule(5.seconds, 5.seconds, self, Report)(context.dispatcher)

  private var numBytes = 0L
  private var numDocs = 0L

  override def receive: Receive = {
    case StartCrawl => ctx.startCrawl(context, maxConcurrent)
    case HostHandler.Completed(uri, paths) => ctx.complete(uri)
    case HostHandler.Skipped(uri) => ctx.skip(uri)
    case doc: ProcessedDocument =>
      numDocs += 1
      numBytes += doc.doc.read
      docProcessor.offer(doc)
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

  private val docProcessor = {
    val input = Source.queue[ProcessedDocument](100, OverflowStrategy.backpressure)
    val sink = Sink.actorRef(self, Report)
    val graph = input.flatMapMerge(6, d => {
      val bldr = Set.newBuilder[Uri]
      val proc = new DocProcessor(spider, bldr)
      proc.extract(d)
      Source.apply(bldr.result())
    }).to(sink)
    graph.run()(spider.rootMat)
  }
}

object NumberHandler {

  case object StartCrawl

  case object Report

}


class DocProcessor(spider: AkkaSpider, bldr: Growable[Uri]) extends StrictLogging {

  def extract(doc: ProcessedDocument): Unit = {
    val lang = doc.doc.lang
    if (!lang.contains("ja") && doc.uri.path != Path.SingleSlash) {
      logger.trace(s"${doc.uri} was not Jp enough, it was $lang: ${doc.doc.charset}")
      return
    }

    doc.doc.document match {
      case Success(d) => extractImpl(doc.uri, d)
      case Failure(e) => logger.trace(s"failed to process ${doc.uri}", e)
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

  def addUri(uri: Uri) = bldr += uri.withoutFragment

  def extractImpl(refer: Uri, d: Document): Unit = {

    val trav = new NodeTraversor(new NodeVisitor {
      private val baseUrl = URL.parse(refer.toString())

      override def head(node: Node, depth: Int): Unit = {
        if (node.nodeName() == "a") {
          if (node.hasAttr("href")) {
            val lnk = node.attr("href")

            val lower = lnk.toLowerCase

            if (lower.startsWith("javascript:") || lower.startsWith("mailto:")) return

            try {
              val java = URL.parse(baseUrl, lnk)

              val path = java.path()
              if (path != null && !spider.filter.accept(path)) return

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
              case e: Exception => spider.invalidUri(refer, lnk)
            }
          }
        }
      }

      override def tail(node: Node, depth: Int): Unit = {}
    })
    trav.traverse(d)
  }

}


class CrawlContext(spider: AkkaSpider)(implicit sender: ActorRef) extends StrictLogging {

  def skip(base: Uri): Unit = {
    logger.trace(s"host $base was skipped")
    ignore.put(base, Integer.valueOf(100))
    active.remove(base).foreach(_ ! PoisonPill)
  }

  def complete(uri: Uri): Unit = {
    logger.trace(s"host $uri is completed")
    ignore.put(uri, cachedWeight(uri) + 20)
    active.remove(uri).foreach(_ ! PoisonPill)
  }

  def addUri(uri: Uri): Unit = {
    val base = CrawlContext.baseUri(uri)

    if (cachedWeight(uri) >= 100) return

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

  def cachedWeight(uri: Uri) = {
    val present = ignore.getIfPresent(uri)
    if (present == null) { 0 } else present.intValue()
  }

  val pending = new mutable.HashMap[Uri, mutable.HashSet[Uri]]()
  val active = new mutable.HashMap[Uri, ActorRef]()

  val ignore =
    CacheBuilder.newBuilder()
      .maximumSize(300000)
      .build[Uri, Integer]()

  var links = 0L

  private def doStart(toLaunch: Int, fact: ActorRefFactory) = {
    val uris = selectUris(toLaunch)
    for (u <- uris) {
      logger.trace(s"starting crawling host $u")
      val props = Props(new HostHandler2(u, spider))
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
      val weight = m.size - cachedWeight(u)
      heap.add((u, weight))
    }

    logger.trace(s"selected ${heap.size()}/$toLaunch hosts")
    heap.asScala.map(_._1)
  }

  def startCrawl(fact: ActorRefFactory, maxConcurrent: Int): Unit = {
    val launchBorder = 100 max (pending.size * 2 / 100 + 1)
    val running = active.size
    val toLaunch = maxConcurrent - running
    doStart(toLaunch min launchBorder, fact)
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
