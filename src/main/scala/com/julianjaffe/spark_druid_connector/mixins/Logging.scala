/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.julianjaffe.spark_druid_connector.mixins

import org.apache.druid.java.util.common.logger.Logger

/**
  * Simplified version of org.apache.spark.internal.Logging. Uses
  * org.apache.druid.java.util.common.logger.Logger instead of slf4j's Logger directly.
  */
trait Logging {

  @transient private var logger: Logger = _

  private lazy val logName = this.getClass.getName.stripSuffix("$")

  /**
    * Return the configured underlying logger
    *
    * @return the configured underlying logger
    */
  protected def log: Logger = {
    if (logger == null) {
      logger= new Logger(logName)
    }
    logger
  }

  /**
    * Log a message at the TRACE level.
    *
    * @param msg the message string to be logged
    */
  protected def logTrace(msg: => String): Unit = {
    if (log.isTraceEnabled) {
      log.trace(msg)
    }
  }

  /**
    * Log a message at the DEBUG level.
    *
    * @param msg the message string to be logged
    */
  protected def logDebug(msg: => String): Unit = {
    if (log.isDebugEnabled) {
      log.debug(msg)
    }
  }

  /**
    * Log a message at the INFO level.
    *
    * @param msg the message string to be logged
    */
  protected def logInfo(msg: => String): Unit = {
    if (log.isInfoEnabled) {
      log.info(msg)
    }
  }

  /**
    * Log a message at the WARN level.
    *
    * @param msg the message string to be logged
    */
  protected def logWarn(msg: => String): Unit = {
    log.warn(msg)
  }

  /**
    * Log a message with an exception at the WARN level.
    *
    * @param msg       the message string to be logged
    * @param exception the exception to log in addition to the message
    */
  protected def logWarn(msg: => String, exception: Throwable): Unit = {
    log.warn(exception, msg)
  }

  /**
    * Log a message at the ERROR level.
    *
    * @param msg the message string to be logged
    */
  protected def logError(msg: => String): Unit = {
    log.error(msg)
  }

  /**
    * Log a message with an exception at the ERROR level.
    *
    * @param msg       the message string to be logged
    * @param exception the exception to log in addition to the message
    */
  protected def logError(msg: => String, exception: Throwable): Unit = {
      log.error(exception, msg)
  }
}
