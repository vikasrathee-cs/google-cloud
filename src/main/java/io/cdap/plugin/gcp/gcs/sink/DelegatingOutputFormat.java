/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * TODO: add
 */
public class DelegatingOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DelegatingOutputFormat.class);
  public static final String PARTITION_FIELD = "delegating.partition.field";
  private static final String DELEGATE_CLASS = "delegating.delegate";
  private static final String OUTPUT_PATH_BASE_DIR = "delegating.output.path.base";
  private static final String OUTPUT_PATH_SUFFIX = "delegating.output.path.suffix";
  private final Map<String, RecordWriter<NullWritable, StructuredRecord>> delegateMap;
  private final DelegatingOutputCommitter outputCommitter;

  public DelegatingOutputFormat() {
    this.delegateMap = new HashMap<>();
    this.outputCommitter = new DelegatingOutputCommitter();
  }

  /**
   * TODO: add
   */
  public static Map<String, String> configure(String delegateClassName, String filterField,
                                              String outputBaseDir, String outputSuffix) {
    Map<String, String> config = new HashMap<>();
    config.put(DELEGATE_CLASS, delegateClassName);
    config.put(PARTITION_FIELD, filterField);
    config.put(OUTPUT_PATH_BASE_DIR, outputBaseDir);
    config.put(OUTPUT_PATH_SUFFIX, outputSuffix);
    return config;
  }

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context) {
    Configuration hConf = context.getConfiguration();
    String partitionField = hConf.get(PARTITION_FIELD);

    return new DelegatingRecordWriter(context, partitionField, delegateMap, outputCommitter);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    //no-op
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
    return outputCommitter;
  }

  @SuppressWarnings("unchecked")
  private static OutputFormat getDelegateFormat(Configuration hConf) throws IOException {
    String delegateClassName = hConf.get(DELEGATE_CLASS);
    try {
      Class<OutputFormat<NullWritable, StructuredRecord>> delegateClass =
        (Class<OutputFormat<NullWritable, StructuredRecord>>) hConf.getClassByName(delegateClassName);
      return delegateClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Unable to instantiate output format for class " + delegateClassName, e);
    }
  }

  /**
   * Filters records before writing them out using a delegate.
   */
  public static class DelegatingRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
    private final TaskAttemptContext context;
    private final String partitionField;
    private final Map<String, RecordWriter<NullWritable, StructuredRecord>> delegateMap;
    private final DelegatingOutputCommitter delegatingOutputCommitter;

    DelegatingRecordWriter(TaskAttemptContext context, String partitionField,
                           Map<String, RecordWriter<NullWritable, StructuredRecord>> delegateMap,
                           DelegatingOutputCommitter delegatingOutputCommitter) {
      this.context = context;
      this.partitionField = partitionField;
      this.delegateMap = delegateMap;
      this.delegatingOutputCommitter = delegatingOutputCommitter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(NullWritable key, StructuredRecord record) throws IOException, InterruptedException {
      String val = record.get(partitionField);

      RecordWriter<NullWritable, StructuredRecord> delegate =
        delegateMap.computeIfAbsent(val, (partitionName) -> {
          try {
            //Set output directory
            context.getConfiguration()
              .set(FileOutputFormat.OUTDIR, buildOutputPath(context.getConfiguration(), partitionName));

            //Create output format for the new output directory and add it to our delegating output committer
            OutputFormat outputFormat = getDelegateFormat(context.getConfiguration());
            OutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(context));
            delegatingOutputCommitter.addCommitter(context, partitionName, gcsOutputCommitter);

            //Add record writer to delegate map.
            return outputFormat.getRecordWriter(context);
          } catch (IOException | InterruptedException e) {
            LOG.error("Unable to instantiate delegate class.", e);
            throw new RuntimeException(e);
          }
        });

      delegate.write(key, record);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      //Close all delegates
      for (RecordWriter<NullWritable, StructuredRecord> delegate : delegateMap.values()) {
        delegate.close(context);
      }
    }

    private String buildOutputPath(Configuration hConf, String context) {
      return String.format("%s/%s/%s", hConf.get(OUTPUT_PATH_BASE_DIR), context, hConf.get(OUTPUT_PATH_SUFFIX));
    }
  }
}
