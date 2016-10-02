package org.eiennohito.crawl.http

import akka.event.LoggingAdapter
import org.slf4j.{LoggerFactory, MarkerFactory}

/**
  * @author eiennohito
  * @since 2016/10/02
  */
class SpiderHtmlLogAdapter(host: String) extends LoggingAdapter {
  private val logger = LoggerFactory.getLogger("org.eiennohito.crawl.http.AkkaSpider.Http")
  private val marker = MarkerFactory.getMarker(host)

  override def isErrorEnabled: Boolean = logger.isErrorEnabled(marker)
  override def isWarningEnabled: Boolean = logger.isWarnEnabled(marker)
  override def isInfoEnabled: Boolean = logger.isInfoEnabled(marker)
  override def isDebugEnabled: Boolean = logger.isDebugEnabled(marker)

  override protected def notifyError(message: String): Unit = logger.error(marker, message)

  override protected def notifyError(cause: Throwable, message: String): Unit = {
    logger.error(marker, message, cause)
  }

  override protected def notifyWarning(message: String): Unit = logger.warn(marker, message)

  override protected def notifyInfo(message: String): Unit = {
    logger.info(marker, message)
  }

  override protected def notifyDebug(message: String): Unit = logger.debug(message)
}
