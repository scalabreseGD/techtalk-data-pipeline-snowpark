package com.griddynamics.common.pipeline

import com.griddynamics.common.SnowflakeUtils.{
  TransactionalOperation,
  TransactionalOperationWithParams
}
import com.snowflake.snowpark.Session

class Operation(
    override val name: String,
    operation: TransactionalOperationWithParams,
    parameters: Seq[(String, Any)]
)(implicit session: Session)
    extends Node {
  override def children: Seq[Node] = Seq.empty
  override def execute: () => Any = () => operation(session, parameters)
}

object Operation {
  def apply(
      name: String,
      operation: TransactionalOperationWithParams,
      parameters: Seq[(String, Any)]
  )(implicit session: Session): Operation =
    new Operation(name, operation, parameters)(session)

  def apply(name: String, operation: TransactionalOperation)(implicit
      session: Session
  ): Operation = new Operation(
    name = name,
    operation = (s, _) => operation(s),
    Seq.empty
  )(session)
}
