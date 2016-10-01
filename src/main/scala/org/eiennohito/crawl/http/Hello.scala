package org.eiennohito.crawl.http

import java.nio.file.Paths

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.apache.tika.language.LanguageIdentifier

import scala.collection.JavaConverters._

object Hello {
  def main(args: Array[String]): Unit = {

    val defaultConfig = ConfigFactory.defaultApplication().withFallback(ConfigFactory.defaultOverrides())

    val cfg = if (args.length > 1) {
      val init = ConfigFactory.parseFile(Paths.get(args(0)).toFile)
      init.withFallback(defaultConfig)
    } else defaultConfig

    val asys = ActorSystem("crawl", cfg)

    val ignored = cfg.getStringList("crawler.ignore").asScala.toArray

    val spider = new AkkaSpider(asys, new StringBasedFilter(ignored))

    val roots = IOUtils.readLines(getClass.getClassLoader.getResourceAsStream("roots.txt"))

    val rootProps = Props(new NumberHandler(spider, cfg.getInt("crawler.concurrency")))
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
  def filter(s: String): Boolean = {
    var i = 0
    while (i < lines.length) {
      val l = lines(i)
      if (s.endsWith(l)) return false
      i += 1
    }
    true
  }
}
