package com.griddynamics.common.pipeline

import com.griddynamics.common.SessionManager
import com.griddynamics.common.SnowflakeUtils.{TransactionalOperation, TransactionalOperationWithParams}

class Operation(
    override val name: String,
    operation: TransactionalOperationWithParams,
    parameters: Seq[(String, Any)]
)(implicit sessionManager: SessionManager)
    extends Node {
  override def children: Seq[Node] = Seq.empty
  override def execute: () => Any = () => {
    operation(sessionManager.get, parameters)
  }
}

object Operation {
  def apply(
      name: String,
      operation: TransactionalOperationWithParams,
      parameters: Seq[(String, Any)]
  )(implicit sessionManager: SessionManager): Operation =
    new Operation(name, operation, parameters)(sessionManager)

  def apply(name: String, operation: TransactionalOperation)(implicit sessionManager: SessionManager
  ): Operation = new Operation(
    name = name,
    operation = (s, _) => operation(s),
    Seq.empty
  )(sessionManager)
}
