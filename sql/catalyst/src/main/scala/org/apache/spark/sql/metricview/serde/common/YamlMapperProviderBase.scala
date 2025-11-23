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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.{YAMLFactory, YAMLGenerator}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.yaml.snakeyaml.DumperOptions

trait YamlMapperProviderBase {
  def mapperWithAllFields: ObjectMapper = {
    val options = new DumperOptions()
    // Set flow style to BLOCK for better readability (each key-value pair on separate lines)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    // Set indentation to 2 spaces
    options.setIndent(2)
    // Set indicator indentation to 2 spaces for list/dict indicators
    options.setIndicatorIndent(2)
    // Enable indentation with indicators for better readability
    options.setIndentWithIndicator(true)
    // Disable pretty flow so that it doesn't add unnecessary newlines after dashes
    options.setPrettyFlow(false)

    val yamlFactory = YAMLFactory.builder()
      // Minimize quotes around strings when possible
      .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
      // Don't force numbers to be quoted as strings (preserve numeric types)
      .configure(YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS, false)
      // Don't write YAML document start marker (---)
      .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false)
      // Disable native type IDs and use explicit type instead
      .configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false)
      .dumperOptions(options)
      .build()

    val mapper = new ObjectMapper(yamlFactory)
      // Exclude null values from serialized output
      .setSerializationInclusion(JsonInclude.Include.NON_NULL)
      // Exclude empty collections/strings from serialized output
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)

    mapper.registerModule(DefaultScalaModule)
    mapper
  }
}
