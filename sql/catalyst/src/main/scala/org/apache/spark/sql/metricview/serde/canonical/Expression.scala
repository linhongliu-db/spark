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

// Expression types in a Metric View
sealed trait Expression extends Validatable {
  def expr: String

  // Validate that expression is not empty
  def validate(): Try[Unit] = {
    if (expr.isEmpty) {
      Failure(MetricViewValidationException("expr cannot be empty"))
    } else Success(())
  }
}

// Dimension expression representing a scalar value
case class DimensionExpression(expr: String) extends Expression

// Measure expression representing an aggregated value
case class MeasureExpression(expr: String) extends Expression
