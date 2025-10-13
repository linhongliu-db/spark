/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.metricview.serde.canonical

import scala.util.{Success, Try}

import org.apache.spark.sql.metricview.serde.canonical.ColumnType.ColumnType
import org.apache.spark.sql.metricview.serde.common.Constants
import org.apache.spark.sql.metricview.serde.json.ColumnMetadata

case class Column[T <: Expression](
    name: String,
    expression: T,
    ordinal: Int) extends Validatable {
  override def validate(): Try[Unit] = {
    Success(())
  }

  def columnType: ColumnType = expression match {
    case _: DimensionExpression => ColumnType.Dimension
    case _: MeasureExpression => ColumnType.Measure
    case _ =>
      throw MetricViewValidationException(
        s"Unsupported expression type: ${expression.getClass.getName}"
      )
  }

  def getColumnMetadata: ColumnMetadata = {
    val truncatedExpr = expression.expr.take(Constants.MAXIMUM_PROPERTY_SIZE)
    ColumnMetadata(columnType.toString, truncatedExpr)
  }
}

object ColumnType extends Enumeration {
  type ColumnType = Value
  val Dimension: ColumnType = Value("dimension")
  val Measure: ColumnType = Value("measure")

  // Method to match case-insensitively and return the correct value
  def fromString(columnType: String): ColumnType = {
    values.find(_.toString.equalsIgnoreCase(columnType)).getOrElse {
      throw MetricViewFromProtoException(
        s"Unsupported column type: $columnType"
      )
    }
  }
}
