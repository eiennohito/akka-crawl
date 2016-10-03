package org.eiennohito.crawl.http

import java.nio.file.{Files, Paths}

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.eiennohito.crawl.storage.Output

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

object CrawlerMain extends StrictLogging {

  def handleRoots(globs: mutable.Buffer[String]): Seq[String] = {
    val x = for (g <- globs) yield try {
      val p = Paths.get(g)
      val nameGlob = p.getFileName.toString
      val dir = p.getParent
      if (dir == null) { List(p) } else {
        val str = Files.newDirectoryStream(dir, nameGlob)
        val bldr = List.newBuilder[java.nio.file.Path]
        try {
          bldr ++= str.iterator().asScala
        } finally {
          str.close()
        }
        bldr.result()
      }
    } catch {
      case e: Exception =>
        logger.warn(s"could not process globs: $globs")
        Nil
    }
    x.flatMap { l => l.flatMap { p => Files.readAllLines(p).asScala }}
  }

  def main(args: Array[String]): Unit = {
    val defaultConfig = ConfigFactory.defaultApplication().withFallback(ConfigFactory.defaultOverrides())

    val cfg = if (args.length > 0) {
      val path = Paths.get(args(0))
      logger.info(s"reading configuration from $path")
      val init = ConfigFactory.parseFile(path.toFile)
      init.withFallback(defaultConfig)
    } else defaultConfig

    val asys = ActorSystem("crawl", cfg)

    val ignored = cfg.getStringList("crawler.ignore").asScala.toArray

    val spider = new AkkaSpider(asys, new StringBasedFilter(ignored))

    val roots = IOUtils.readLines(getClass.getClassLoader.getResourceAsStream("roots.txt"))
    val otherRootFiles = cfg.getStringList("crawler.roots")
    val otherRoots = handleRoots(otherRootFiles.asScala)

    val allRoots = roots.asScala ++ otherRoots

    logger.info(s"using ${allRoots.size} roots")

    val conc = cfg.getInt("crawler.concurrency")

    logger.info(s"starting a crawler with concurrency of $conc")

    val rootProps = Props(new NumberHandler(spider, conc))
    val actor = asys.actorOf(rootProps, "base")

    if (cfg.hasPath("crawler.output.uris")) {
      val path = Paths.get(cfg.getString("crawler.output.uris"))
      val size = if(cfg.hasPath("crawler.output.uris-batch")) {
        cfg.getInt("crawler.output.uris-batch")
      } else 100000

      logger.info(s"saving uris to: $path, split after $size lines")

      val sink = Output.saveUris(path, size)
      val src = Source.actorRef[UriHandled](5000, OverflowStrategy.dropNew)
      val ref = sink.runWith(src)(spider.rootMat)
      asys.eventStream.subscribe(ref, classOf[UriHandled])
    }

    if (cfg.hasPath("crawler.output.html")) {
      val path = Paths.get(cfg.getString("crawler.output.html"))
      val size = if(cfg.hasPath("crawler.output.html-batch")) {
        cfg.getBytes("crawler.output.html-batch").longValue()
      } else 200 * 1024 * 1024L

      logger.info(s"saving html to: $path, split after $size uncompressed bytes")

      val sink = Output.saveDocs(path, size)
      val src = Source.actorRef[ProcessedDocument](500, OverflowStrategy.dropNew)
      val ref = sink.runWith(src)(spider.rootMat)
      asys.eventStream.subscribe(ref, classOf[ProcessedDocument])
    }

    allRoots.foreach { l =>
      actor ! Uri(l)
    }

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = { asys.terminate() }
    }))
  }
}

class StringBasedFilter(lines: Array[String]) {
  private[this] val minlen = lines.map(_.length).min

  @tailrec
  private def lastSegment(path: Path): String = {
    path match {
      case Path.Segment(s, Path.Empty) => s
      case Path.Segment(_, x) => lastSegment(x)
      case Path.Empty => ""
      case Path.Slash(p) => lastSegment(p)
    }
  }

  def accept(path: Path): Boolean = {
    val seg = lastSegment(path)
    accept(seg)
  }

  def accept(s: String): Boolean = {
    if (s == null || s.length < minlen) return true

    var i = 0
    while (i < lines.length) {
      val l = lines(i)
      if (s.endsWith(l)) return false
      i += 1
    }

    true
  }
}
