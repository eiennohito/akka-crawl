package org.eiennohito.crawl.http

import java.nio.file.Paths

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Path
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.IOUtils
import org.apache.tika.language.LanguageIdentifier

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object CrawlerMain extends StrictLogging {
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

    val conc = cfg.getInt("crawler.concurrency")

    logger.info(s"starting a crawler with concurrency of $conc")

    val rootProps = Props(new NumberHandler(spider, conc))
    val actor = asys.actorOf(rootProps, "base")



    LanguageIdentifier.initProfiles()

    roots.asScala.foreach { l =>
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
