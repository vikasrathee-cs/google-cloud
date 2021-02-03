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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * TODO: add
 */
public class DelegatingOutputCommitter extends OutputCommitter {

  Map<String, OutputCommitter> committerMap;

  public DelegatingOutputCommitter() {
    committerMap = new HashMap<>();
  }

  /**
   * TODO: add
   */
  public void addCommitter(TaskAttemptContext context,
                           String partitionName,
                           OutputCommitter committer) throws IOException {
    committerMap.put(partitionName, committer);
    committer.setupJob(context);
    committer.setupTask(context);
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
    for (OutputCommitter committer : committerMap.values()) {
      if (committer.needsTaskCommit(taskAttemptContext)) {
        committer.commitTask(taskAttemptContext);
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (OutputCommitter committer : committerMap.values()) {
      committer.abortTask(taskAttemptContext);
    }
  }
}
