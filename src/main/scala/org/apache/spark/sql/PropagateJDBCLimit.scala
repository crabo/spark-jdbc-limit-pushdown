package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc._

import scala.util.control.Breaks.break

/**
 * Rule to push down limit to JDBC relation.
 * This works for both `df.show()` or `df.limit(X)`.
 * Simply add this rule to spark extra optimizations sequence.
 */
object PropagateJDBCLimit extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case limit @ LocalLimit(
      limitValue @ IntegerLiteral(value),
      rel @ LogicalRelation(
        prev @ JDBCRelation(parts, jdbcOptions), _, table)) =>
      // this is done to preserve aliases for expressions
      val attr = rel.attributeMap.values.toSeq
      val updatedRel = LogicalRelation(
        JDBCRelationWithLimit(parts, jdbcOptions, value)(prev.sparkSession),
        Some(attr),
        table)
      LocalLimit(limitValue, updatedRel)


    case limit @ LocalLimit(
      limitValue @ IntegerLiteral(value),
      prj
    ) =>

      var prev = limit.child;
      while(prev.isInstanceOf[UnaryNode]){
        val child = prev.asInstanceOf[UnaryNode].child
        if(child==null)
          break
        else{
          prev = child
        }
      }
      val rel = prev.asInstanceOf[LogicalRelation]
      if(rel.relation.isInstanceOf[JDBCRelation]){
        val jdbc:JDBCRelation = rel.relation.asInstanceOf[JDBCRelation]
        if(jdbc!=null){
          val fieldX = rel.getClass.getDeclaredField("relation")
          fieldX.setAccessible(true)
          fieldX.set(rel,
            JDBCRelationWithLimit(
              jdbc.parts,jdbc.jdbcOptions,value)
            (jdbc.sparkSession)
          )
          return LocalLimit(limitValue, prj)
        }
      }
      limit
  }
}
