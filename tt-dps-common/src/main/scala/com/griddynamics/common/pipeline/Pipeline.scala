package com.griddynamics.common.pipeline

import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps

class Pipeline(dag: DAG) {
  private var continuous: Boolean = false
  private var interval: Duration = 1 second
  def asContinuous(interval: Duration): this.type = {
    this.continuous = true
    this.interval = interval
    this
  }
  def evaluate(): Unit = {
    do {
      dag.evaluate()
      Thread.sleep(interval.toSeconds)
    } while (continuous)
  }
}

object Pipeline {
  def apply(dag: DAG): Pipeline = new Pipeline(dag)
}
