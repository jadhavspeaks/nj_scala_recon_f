package com.reconciliation.framework.sinks

import org.slf4j.LoggerFactory

trait Logger {
  val logger = LoggerFactory.getLogger(this.getClass)
}
