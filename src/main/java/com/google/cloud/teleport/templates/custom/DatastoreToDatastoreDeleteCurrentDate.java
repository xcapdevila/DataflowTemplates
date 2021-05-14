/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.custom;

import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteEntityJson;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreDeleteOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.DatastoreReadOptions;
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 *
 * Supports GQL queries with custom NOW function:
 * - SELECT * FROM `Kind` WHERE timestamp < ${NOW-12h}
 * - SELECT * FROM `Kind` WHERE timestamp < ${NOW-2y} AND timestamp > ${NOW-5y}
 *
 * NOW format is ${NOW_Operation__Value__TimeUnit_}.
 * _Operation_:
 * - "+" to add
 * - "-" to subtract
 * _Value_:
 * - any integer greater than zero.
 * _TimeUnit_:
 * - "y" for years
 * - "m" for months
 * - "d" for days
 * - "h" for hours
 */
public class DatastoreToDatastoreDeleteCurrentDate {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreToDatastoreDeleteCurrentDate.class);
  private static final String NOW_PATTERN = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";
  private static final Pattern PATTERN = Pattern.compile(NOW_PATTERN);

  /**
   * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities to a Javascript UDF, and deletes all the Entities.
   *
   * <p>If the UDF returns value of undefined or null for a given Entity, then that Entity will not
   * be deleted.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {

    DatastoreToDatastoreDeleteOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DatastoreToDatastoreDeleteOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(ReadJsonEntities.newBuilder()
            .setGqlQuery(
                NestedValueProvider.of(
                    options.getDatastoreReadGqlQuery(),
                    DatastoreToDatastoreDeleteCurrentDate::parseGqlStatement)
            )
            .setProjectId(options.getDatastoreReadProjectId())
            .setNamespace(options.getDatastoreReadNamespace())
            .build())
        .apply(TransformTextViaJavascript.newBuilder()
            .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
            .setFunctionName(options.getJavascriptTextTransformFunctionName())
            .build())
        .apply(DatastoreDeleteEntityJson.newBuilder()
            .setProjectId(options.getDatastoreDeleteProjectId())
            .build());

    pipeline.run();
  }

  private static String parseGqlStatement(String gqlStatement) {
    final Matcher matcher = PATTERN.matcher(gqlStatement);
    while (matcher.find()) {
      final String parsedDate = DatetimeHelper
          .create()
          .withNow(LocalDateTime.now())
          .withOperation(matcher.group(1))
          .withValue(matcher.group(2))
          .withTimeUnit(matcher.group(3))
          .build()
          .calculate();
      LOG.info("parsedDate: {}", parsedDate);

      gqlStatement = gqlStatement.replaceFirst(PATTERN.pattern(), "'" + parsedDate + "'");
      LOG.info("gqlStatement: {}", gqlStatement);
    }
    return gqlStatement;
  }

  /**
   * Custom PipelineOptions.
   */
  public interface DatastoreToDatastoreDeleteOptions extends
      PipelineOptions,
      DatastoreReadOptions,
      JavascriptTextTransformerOptions,
      DatastoreDeleteOptions {

  }

}
