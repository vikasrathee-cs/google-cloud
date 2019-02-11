/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.gcp.dlp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.privacy.dlp.v2.ByteContentItem;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cloud DLP.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(DLPSensitiveFilter.NAME)
@Description(DLPSensitiveFilter.DESCRIPTION)
public final class DLPSensitiveFilter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DLPSensitiveFilter.class);
  public static final String NAME = "DLPSensitiveFilter";
  public static final String DESCRIPTION = "Filters input records based on sensitivity of a given field.";

  // Stores the configuration passed to this class from user.
  private final Config config;

  private DlpServiceClient client;
  private InspectConfig inspectConfig;


  @VisibleForTesting
  public DLPSensitiveFilter(Config config) {
    this.config = config;
  }

  /**
   * Confiigure Pipeline.
   * @param pipelineConfigurer
   * @throws IllegalArgumentException
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro(config.getFieldName()) && inputSchema.getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified is not present in input schema");
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    // Macros are resolved by this time, so we check before runninig if the input field specified by user
    // is present in the input schema.
    if (context.getInputSchema().getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Input field not present in the input schema");
    }
  }

  /**
   * Initialize.
   *
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    SensitivityMapping sensitivityMapping = new SensitivityMapping();
    List<InfoType> sensitiveInfoTypes = sensitivityMapping.getSensitiveInfoTypes(config.getSensitiveTypes());
    client = DlpServiceClient.create(getSettings());
    inspectConfig =
      InspectConfig.newBuilder()
        .addAllInfoTypes(sensitiveInfoTypes)
        .setMinLikelihood(config.getFilterConfidence())
        .build();
  }

  /**
   * Transform.
   *
   * @param record
   * @param emitter
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Object object = record.get(config.getFieldName());
    if (object instanceof String) {
      ByteContentItem byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFromUtf8(String.valueOf(object)))
          .build();
      ContentItem contentItem = ContentItem.newBuilder().setByteItem(byteContentItem).build();

      InspectContentRequest request =
        InspectContentRequest.newBuilder()
          .setParent(ProjectName.of(config.getProject()).toString())
          .setInspectConfig(inspectConfig)
          .setItem(contentItem)
          .build();
      InspectContentResponse response = client.inspectContent(request);
      InspectResult result = response.getResult();

      if (result.getFindingsCount() > 0) {
        List<String> findingInfoTypes = new ArrayList<>();
        for (Finding finding : result.getFindingsList()) {
          if (canFilter(finding.getLikelihood(), config.getFilterConfidence())) {
            findingInfoTypes.add(finding.getInfoType().getName());
          }
        }
        if (findingInfoTypes.size() > 0) {
          List<String> dedup = Lists.newArrayList(Sets.newHashSet(findingInfoTypes));
          emitter.emitError(new InvalidEntry<>(dedup.size(), String.join(",", dedup), record));
          return;
        }
      }
      emitter.emit(record);
    }
  }

  /**
   * 0 1 false
   * 1 1 true
   * 2 1 true
   * 0 0 true
   * 1 0 true
   * 2 0 true
   * 0 2 false
   * 1 2 false
   * 2 2 true
   * @param inputLikelihood
   * @param configLikelihood
   * @return
   */
  private boolean canFilter(Likelihood inputLikelihood, Likelihood configLikelihood) {
    Map<String, Integer> mapping = ImmutableMap.of(
      "POSSIBLE", 0,
      "LIKELY", 1,
      "VERY_LIKELY", 2
    );

    int left = mapping.get(inputLikelihood.name());
    int right = mapping.get(configLikelihood.name());
    if (left >= right) {
      return true;
    }
    return false;
  }

  /**
   * Destroy.
   */
  @Override
  public void destroy() {
    super.destroy();
  }

  private DlpServiceSettings getSettings() throws IOException {
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    if (config.getServiceAccountFilePath() != null) {
      builder.setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
    }
    return builder.build();
  }

  /**
   * Configuration object.
   */
  public static class Config extends GCPConfig {
    @Macro
    @Name("field")
    @Description("Name of field to be inspected")
    private String field;

    @Macro
    @Name("filter-confidence")
    @Description("Confidence in types ")
    private String filterConfidence;

    @Macro
    @Name("sensitive-types")
    @Description("Information types to be matched")
    private String sensitiveTypes;

    public String getFieldName() {
      return field;
    }

    public String[] getSensitiveTypes() {
      String[] types = sensitiveTypes.split(",");
      return types;
    }

    public Likelihood getFilterConfidence() {
      if (filterConfidence.equalsIgnoreCase("low")) {
        return Likelihood.POSSIBLE;
      } else if (filterConfidence.equalsIgnoreCase("medium")) {
        return Likelihood.LIKELY;
      }
      return Likelihood.VERY_LIKELY;
    }
  }
}