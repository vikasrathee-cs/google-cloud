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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.cdap.plugin.gcp.gcs.sink.DelegatingOutputFormat.DelegatingRecordWriter.buildOutputPath;
import static io.cdap.plugin.gcp.gcs.sink.DelegatingOutputFormat.getDelegateFormat;

/**
 * TODO: add
 */
public class DelegatingOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(DelegatingOutputFormat.class);
  private static final Gson GSON = new Gson();

  Map<String, OutputCommitter> committerMap;

  public DelegatingOutputCommitter() {
    committerMap = new HashMap<>();
  }

  /**
   * TODO: add
   */
  public void addCommitter(TaskAttemptContext context,
                           String partitionName) throws IOException, InterruptedException {
    //Set output directory
    context.getConfiguration()
      .set(FileOutputFormat.OUTDIR, buildOutputPath(context.getConfiguration(), partitionName));

    //Create output format for the new output directory and add it to our delegating output committer
    OutputFormat outputFormat = getDelegateFormat(context.getConfiguration());
    GCSOutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(context));
    gcsOutputCommitter.setupJob(context);
    gcsOutputCommitter.setupTask(context);
    committerMap.put(partitionName, gcsOutputCommitter);
  }

  public void initCommitters(TaskAttemptContext context, String delegatePartitionNames) {
    try {
      Set<String> partitionNames = GSON.fromJson(delegatePartitionNames, new TypeToken<Set<String>>() {
      }.getType());
      for (String partition : partitionNames) {
        if (!committerMap.containsKey(partition)) {
          addCommitter(context, partition);
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.error("Exception when rebuilding committers.", e);
      throw new RuntimeException(e);
    }
  }

  public void addCommitter(TaskAttemptContext context,
                           String partitionName,
                           OutputCommitter committer) throws IOException {
    committerMap.putIfAbsent(partitionName, committer);
  }

  public boolean hasCommitter(String partitionName) {
    return committerMap.containsKey(partitionName);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    //no-op
    System.out.println(committerMap.toString());
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //no-op
    System.out.println(committerMap.toString());
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    if (taskAttemptContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES) != null) {
      initCommitters(taskAttemptContext,
                     taskAttemptContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES));
    }

    if (committerMap.isEmpty()) {
      return false;
    }

    boolean needsTaskCommit = true;

    for (OutputCommitter committer : committerMap.values()) {
      needsTaskCommit = needsTaskCommit && committer.needsTaskCommit(taskAttemptContext);
    }

    return needsTaskCommit;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    LOG.warn("Commit task:" +
               taskAttemptContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES));

    if (taskAttemptContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES) != null) {
      initCommitters(taskAttemptContext,
                     taskAttemptContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES));
    }

    for (OutputCommitter committer : committerMap.values()) {
      committer.commitTask(taskAttemptContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    LOG.warn("Commit Job:" + jobContext.getConfiguration().get(DelegatingOutputFormat.DELEGATE_PARTITION_NAMES));

    for (OutputCommitter committer : committerMap.values()) {
      committer.commitJob(jobContext);
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.abortTask(taskAttemptContext);
    }
  }
}
