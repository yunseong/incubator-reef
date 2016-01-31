/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverJobSubmissionDirectory;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.ClasspathProvider;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.runtime.yarn.client.parameters.JobQueue;
import org.apache.reef.runtime.yarn.client.uploader.JobFolder;
import org.apache.reef.runtime.yarn.client.uploader.JobUploader;
import org.apache.reef.runtime.yarn.driver.YarnDriverConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class YarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(YarnJobSubmissionHandler.class.getName());

  private final YarnConfiguration yarnConfiguration;
  private final JobJarMaker jobJarMaker;
  private final REEFFileNames fileNames;
  private final ClasspathProvider classpath;
  private final JobUploader uploader;
  private final double jvmSlack;
  private final String defaultQueueName;
  private final SecurityTokenProvider tokenProvider;

  @Inject
  YarnJobSubmissionHandler(
      final YarnConfiguration yarnConfiguration,
      final JobJarMaker jobJarMaker,
      final REEFFileNames fileNames,
      final ClasspathProvider classpath,
      final JobUploader uploader,
      @Parameter(JVMHeapSlack.class) final double jvmSlack,
      @Parameter(JobQueue.class) final String defaultQueueName,
      final SecurityTokenProvider tokenProvider) throws IOException {

    this.yarnConfiguration = yarnConfiguration;
    this.jobJarMaker = jobJarMaker;
    this.fileNames = fileNames;
    this.classpath = classpath;
    this.uploader = uploader;
    this.jvmSlack = jvmSlack;
    this.defaultQueueName = defaultQueueName;
    this.tokenProvider = tokenProvider;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    LOG.log(Level.FINEST, "Submitting job with ID [{0}]", jobSubmissionEvent.getIdentifier());

    try (final YarnSubmissionHelper submissionHelper =
             new YarnSubmissionHelper(this.yarnConfiguration, this.fileNames, this.classpath, this.tokenProvider)) {

      LOG.log(Level.FINE, "Assembling submission JAR for the Driver.");
      final Optional<String> userBoundJobSubmissionDirectory =
          getUserBoundJobSubmissionDirectory(jobSubmissionEvent.getConfiguration());
      final JobFolder jobFolderOnDfs = userBoundJobSubmissionDirectory.isPresent()
          ? this.uploader.createJobFolder(userBoundJobSubmissionDirectory.get())
          : this.uploader.createJobFolder(submissionHelper.getApplicationId());
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, jobFolderOnDfs.getPath());
      final File jobSubmissionFile = this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);
      final LocalResource driverJarOnDfs = jobFolderOnDfs.uploadAsLocalResource(jobSubmissionFile);

      submissionHelper
          .addLocalResource(this.fileNames.getREEFFolderName(), driverJarOnDfs)
          .setApplicationName(jobSubmissionEvent.getIdentifier())
          .setDriverMemory(jobSubmissionEvent.getDriverMemory().get())
          .setPriority(getPriority(jobSubmissionEvent))
          .setQueue(getQueue(jobSubmissionEvent))
          .setPreserveEvaluators(getPreserveEvaluators(jobSubmissionEvent))
          .setMaxApplicationAttempts(getMaxApplicationSubmissions(jobSubmissionEvent))
          .submit();

      LOG.log(Level.FINEST, "Submitted job with ID [{0}]", jobSubmissionEvent.getIdentifier());
    } catch (final YarnException | IOException e) {
      throw new RuntimeException("Unable to submit Driver to YARN.", e);
    }
  }

  /**
   * Assembles the Driver configuration.
   */
  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final Path jobFolderPath) throws IOException {
    return Configurations.merge(
        YarnDriverConfiguration.CONF
            .set(YarnDriverConfiguration.JOB_SUBMISSION_DIRECTORY, jobFolderPath.toString())
            .set(YarnDriverConfiguration.JOB_IDENTIFIER, jobSubmissionEvent.getIdentifier())
            .set(YarnDriverConfiguration.CLIENT_REMOTE_IDENTIFIER, jobSubmissionEvent.getRemoteId())
            .set(YarnDriverConfiguration.JVM_HEAP_SLACK, this.jvmSlack)
            .build(),
        jobSubmissionEvent.getConfiguration());
  }

  private static int getPriority(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getPriority().orElse(0);
  }

  /**
   * Extract the queue name from the jobSubmissionEvent or return default if none is set.
   */
  private String getQueue(final JobSubmissionEvent jobSubmissionEvent) {
    return getQueue(jobSubmissionEvent.getConfiguration());
  }

  /**
   * Extract the information on whether or not the job should preserve evaluators across job driver restarts.
   */
  private Boolean getPreserveEvaluators(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getPreserveEvaluators().orElse(false);
  }

  /**
   * Extract the number of maximum application attempts on the job.
   */
  private Integer getMaxApplicationSubmissions(final JobSubmissionEvent jobSubmissionEvent) {
    return jobSubmissionEvent.getMaxApplicationSubmissions().orElse(1);
  }

  /**
   * Extracts the queue name from the driverConfiguration or return default if none is set.
   *
   * @param driverConfiguration
   * @return the queue name from the driverConfiguration or return default if none is set.
   */
  private String getQueue(final Configuration driverConfiguration) {
    try {
      return Tang.Factory.getTang().newInjector(driverConfiguration).getNamedInstance(JobQueue.class);
    } catch (final InjectionException e) {
      return this.defaultQueueName;
    }
  }

  private static Optional<String> getUserBoundJobSubmissionDirectory(final Configuration configuration) {
    try {
      return Optional.ofNullable(Tang.Factory.getTang().newInjector(configuration)
          .getNamedInstance(DriverJobSubmissionDirectory.class));
    } catch (final InjectionException ex) {
      return Optional.empty();
    }

  }

}
