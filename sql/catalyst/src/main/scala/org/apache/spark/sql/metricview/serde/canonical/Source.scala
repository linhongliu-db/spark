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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.metricview.serde.canonical.SourceType.SourceType

object SourceType extends Enumeration {
  type SourceType = Value
  val ASSET, SQL = Value

  def fromString(sourceType: String): SourceType = {
    values.find(_.toString.equalsIgnoreCase(sourceType)).getOrElse {
      throw MetricViewFromProtoException(
        s"Unsupported source type: $sourceType"
      )
    }
  }
}

// Representation of a source in the Metric View
sealed trait Source extends Validatable {
  def sourceType: SourceType

  def validate(): Try[Unit]
}

// Asset source, representing a UC table, view, or Metric View, etc.
case class AssetSource(name: String) extends Source {
  val sourceType: SourceType = SourceType.ASSET

  def validate(): Try[Unit] = {
    if (name.isEmpty) {
      Failure(
        MetricViewValidationException("Source cannot be empty")
      )
    } else Success(())
  }

  override def toString: String = this.name
}

// SQL source, representing a SQL query
case class SQLSource(sql: String) extends Source {
  val sourceType: SourceType = SourceType.SQL

  def validate(): Try[Unit] = {
    if (sql.isEmpty) {
      Failure(
        MetricViewValidationException("Source cannot be empty")
      )
    } else Success(())
  }

  override def toString: String = this.sql
}

object Source {
  def apply(sourceText: String): Source = {
    if (sourceText.isEmpty) {
      throw MetricViewValidationException("Source cannot be empty")
    }
    Try(CatalystSqlParser.parseTableIdentifier(sourceText)) match {
      case Success(_) => AssetSource(sourceText)
      case Failure(_) =>
        Try(CatalystSqlParser.parseQuery(sourceText)) match {
          case Success(_) => SQLSource(sourceText)
          case Failure(queryEx) =>
            throw queryEx
        }
    }
  }
}
