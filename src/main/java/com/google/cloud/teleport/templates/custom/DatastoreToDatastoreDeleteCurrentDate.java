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
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 */
public class DatastoreToDatastoreDeleteCurrentDate {

  private static String NOW_PATTERN = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";

  /**
   * Custom PipelineOptions.
   */
  public interface DatastoreToDatastoreDeleteOptions extends
      PipelineOptions,
      DatastoreReadOptions,
      JavascriptTextTransformerOptions,
      DatastoreDeleteOptions {}

  /**
   * Runs a pipeline which reads in Entities from datastore, passes in the JSON encoded Entities
   * to a Javascript UDF, and deletes all the Entities.
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

    final ValueProvider<String> datastoreReadGqlQuery = parseCustomValuesOnQuery(options.getDatastoreReadGqlQuery());
    pipeline
        .apply(ReadJsonEntities.newBuilder()
            .setGqlQuery(datastoreReadGqlQuery)
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

  private static ValueProvider<String> parseCustomValuesOnQuery(ValueProvider<String> datastoreReadGqlQuery) {
    String gqlStatement = datastoreReadGqlQuery.get();
    final Matcher matcher = Pattern.compile(NOW_PATTERN).matcher(gqlStatement);
    while (matcher.find()) {
      final String parsedDate = DatetimeHelper
          .create()
          .withNow(LocalDateTime.now())
          .withOperation(matcher.group(1))
          .withValue(matcher.group(2))
          .withTimeUnit(matcher.group(3))
          .build()
          .calculate();
      System.out.println(
          parsedDate);

      gqlStatement = gqlStatement.replaceFirst(NOW_PATTERN, parsedDate);
      System.out.println(gqlStatement);
    }
    return ValueProvider.StaticValueProvider.of(gqlStatement);
  }

}
