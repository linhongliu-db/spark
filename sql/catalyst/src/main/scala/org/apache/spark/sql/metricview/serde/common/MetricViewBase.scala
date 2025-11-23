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

package org.apache.spark.sql.metricview.serde.common

import org.apache.spark.sql.metricview.serde.canonical.{MetricView => MetricViewCanonical, Source => SourceCanonical}
import org.apache.spark.sql.metricview.serde.v01.{MetricView => MetricViewV01}

trait MetricViewBase {
  def version: String
  def source: String
  def filter: Option[String]
  def dimensions: Seq[ColumnBase]
  def measures: Seq[ColumnBase]

  def toCanonical: MetricViewCanonical = {
    // Convert dimensions with proper ordinals (0 to dimensions.length-1)
    val dimensionsCanonical = dimensions.zipWithIndex.map {
      case (column, index) => column.toCanonical(index, isDimension = true)
    }
    // Convert measures with proper ordinals
    // (dimensions.length to dimensions.length + measures.length - 1)
    val measuresCanonical = measures.zipWithIndex.map {
      case (column, index) =>
        column.toCanonical(dimensions.length + index, isDimension = false)
    }
    MetricViewCanonical(
      version = version,
      from = SourceCanonical(source),
      where = filter,
      select = dimensionsCanonical ++ measuresCanonical
    )
  }
}

object MetricViewBase {
  /**
   * Factory method to create the appropriate version-specific MetricView from canonical form.
   * @param canonical The canonical MetricView to convert from
   * @return The appropriate version-specific MetricView
   */
  def fromCanonical(canonical: MetricViewCanonical): MetricViewBase = {
    canonical.version match {
      case "0.1" =>
        MetricViewV01.fromCanonical(canonical)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported version: ${canonical.version}")
    }
  }
}

