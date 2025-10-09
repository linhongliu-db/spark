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

package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.metricview.serde.MetricViewFactory
import org.apache.spark.sql.metricview.serde.canonical.{AssetSource, Column, DimensionExpression, MeasureExpression, MetricView}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}

class MetricViewSuite extends QueryTest with SQLTestUtils with SharedSparkSession {

  private def createMetricView(metricView: MetricView): Unit = {
    val yaml = MetricViewFactory.toYAML(metricView)
    sql(s"""
        |CREATE VIEW my_metric_view
        |WITH METRICS
        |LANGUAGE YAML
        |AS
        |$$$$
        |$yaml
        |$$$$
        |""".stripMargin)
  }

  test("basic") {
    val metricViewColumns = Seq(
      Column("d1", DimensionExpression("upper(a)"), 0),
      Column("m1", MeasureExpression("sum(a)"), 1)
    )
    val metricView = MetricView("0.1", AssetSource("my_table"), Some("a > 1"), metricViewColumns)
    createMetricView(metricView)
  }
}
