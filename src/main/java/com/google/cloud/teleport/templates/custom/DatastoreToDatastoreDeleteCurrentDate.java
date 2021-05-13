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
import com.google.cloud.teleport.templates.common.DatastoreConverters.ReadJsonEntities;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.TransformTextViaJavascript;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow template which deletes pulled Datastore Entities.
 */
public class DatastoreToDatastoreDeleteCurrentDate {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreToDatastoreDeleteCurrentDate.class);
  private static final String NOW_PATTERN = "\\$\\{NOW(\\+|\\-)(\\d*?)(y|m|d|h)\\}";
  private static final Pattern PATTERN = Pattern.compile(NOW_PATTERN);

  /**
   * Custom PipelineOptions.
   */
  public interface DatastoreToDatastoreDeleteOptions extends
      PipelineOptions,
      CustomDatastoreReadOptions,
      JavascriptTextTransformerOptions,
      DatastoreDeleteOptions {}

  /** Options for Reading Datastore Entities. */
  public interface CustomDatastoreReadOptions extends PipelineOptions {
    @Description("GQL Query which specifies what entities to grab")
    String getDatastoreReadGqlQuery();
    void setDatastoreReadGqlQuery(String datastoreReadGqlQuery);

    @Description("Fake property to extract the NOW based GQL query")
    @Default.String("DEFAULT")
    default String getParsedGqlStatement() {
      String gqlStatement = getDatastoreReadGqlQuery();
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

        gqlStatement = gqlStatement.replaceFirst(NOW_PATTERN, parsedDate);
        LOG.info("gqlStatement: {}", gqlStatement);
      }
      return gqlStatement;
    }
    void setParsedGqlStatement(String parsedGqlStatement);

    @Description("GCP Project Id of where the datastore entities live")
    ValueProvider<String> getDatastoreReadProjectId();
    void setDatastoreReadProjectId(ValueProvider<String> datastoreReadProjectId);

    @Description("Namespace of requested Entties. Set as \"\" for default namespace")
    ValueProvider<String> getDatastoreReadNamespace();
    void setDatastoreReadNamespace(ValueProvider<String> datstoreReadNamespace);
  }

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
            .setGqlQuery(ValueProvider.StaticValueProvider.of(options.getParsedGqlStatement()))
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

}
