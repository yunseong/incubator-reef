﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro;
using Org.Apache.REEF.Client.Avro.YARN;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.Yarn
{
    internal sealed class YarnREEFClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.YarnJobSubmissionClient";

        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnREEFClient));
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly IJavaClientLauncher _javaClientLauncher;
        private readonly string _securityTokenKind;
        private readonly string _securityTokenService;
        private readonly string _jobSubmissionPrefix;
        private readonly REEFFileNames _fileNames;
        private readonly IYarnRMClient _yarnClient;

        [Inject]
        internal YarnREEFClient(IJavaClientLauncher javaClientLauncher,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            YarnCommandLineEnvironment yarn,
            IYarnRMClient yarnClient,
            [Parameter(typeof(SecurityTokenKindParameter))] string securityTokenKind,
            [Parameter(typeof(SecurityTokenServiceParameter))] string securityTokenService,
            [Parameter(typeof(JobSubmissionDirectoryPrefixParameter))] string jobSubmissionPrefix)
        {
            _jobSubmissionPrefix = jobSubmissionPrefix;
            _securityTokenKind = securityTokenKind;
            _securityTokenService = securityTokenService;
            _javaClientLauncher = javaClientLauncher;
            _javaClientLauncher.AddToClassPath(yarn.GetYarnClasspathList());
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _fileNames = fileNames;
            _yarnClient = yarnClient;
        }

        public void Submit(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobSubmission.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobSubmission, driverFolderPath);
        }

        public IJobSubmissionResult SubmitAndGetJobStatus(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobSubmission.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobSubmission, driverFolderPath);

            var pointerFileName = Path.Combine(driverFolderPath, _fileNames.DriverHttpEndpoint);
            var jobSubmitionResultImpl = new YarnJobSubmissionResult(this, pointerFileName);

            var msg = string.Format(CultureInfo.CurrentCulture,
                "Submitted the Driver for execution. Returned driverUrl is: {0}, appId is {1}.",
                jobSubmitionResultImpl.DriverUrl, jobSubmitionResultImpl.AppId);
            Logger.Log(Level.Info, msg);

            return jobSubmitionResultImpl;
        }

        /// <summary>
        /// Pull Job status from Yarn for the given appId
        /// </summary>
        /// <returns></returns>
        public async Task<FinalState> GetJobFinalStatus(string appId)
        {
            var application = await _yarnClient.GetApplicationAsync(appId);

            var msg = string.Format("application status {0}, Progress: {1}, trackingUri: {2}, Name: {3}, ApplicationId: {4}, State {5}.",
                application.FinalStatus, application.Progress, application.TrackingUI, application.Name, application.Id, application.State);
            Logger.Log(Level.Verbose, msg);

            return application.FinalStatus;
        }

        private void Launch(IJobSubmission jobSubmission, string driverFolderPath)
        {
            _driverFolderPreparationHelper.PrepareDriverFolder(jobSubmission, driverFolderPath);

            // TODO: Remove this when we have a generalized way to pass config to java
            var paramInjector = TangFactory.GetTang().NewInjector(jobSubmission.DriverConfigurations.ToArray());
                
            var avroJobSubmissionParameters = new AvroJobSubmissionParameters
            {
                jobId = jobSubmission.JobIdentifier,
                tcpBeginPort = paramInjector.GetNamedInstance<TcpPortRangeStart, int>(),
                tcpRangeCount = paramInjector.GetNamedInstance<TcpPortRangeCount, int>(),
                tcpTryCount = paramInjector.GetNamedInstance<TcpPortRangeTryCount, int>(),
                jobSubmissionFolder = driverFolderPath
            };

            var avroYarnJobSubmissionParameters = new AvroYarnJobSubmissionParameters
            {
                driverMemory = jobSubmission.DriverMemory,
                driverRecoveryTimeout = paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds, int>(),
                jobSubmissionDirectoryPrefix = _jobSubmissionPrefix,
                sharedJobSubmissionParameters = avroJobSubmissionParameters
            };

            var avroYarnClusterJobSubmissionParameters = new AvroYarnClusterJobSubmissionParameters
            {
                maxApplicationSubmissions = paramInjector.GetNamedInstance<DriverBridgeConfigurationOptions.MaxApplicationSubmissions, int>(),
                securityTokenKind = _securityTokenKind,
                securityTokenService = _securityTokenService,
                yarnJobSubmissionParameters = avroYarnJobSubmissionParameters
            };

            var submissionArgsFilePath = Path.Combine(driverFolderPath, _fileNames.GetJobSubmissionParametersFile());
            using (var argsFileStream = new FileStream(submissionArgsFilePath, FileMode.CreateNew))
            {
                var serializedArgs = AvroJsonSerializer<AvroYarnClusterJobSubmissionParameters>.ToBytes(avroYarnClusterJobSubmissionParameters);
                argsFileStream.Write(serializedArgs, 0, serializedArgs.Length);
            }

            // Submit the driver
            _javaClientLauncher.Launch(JavaClassName, submissionArgsFilePath);
            Logger.Log(Level.Info, "Submitted the Driver for execution." + jobSubmission.JobIdentifier);
        }

        /// <summary>
        /// Creates the temporary directory to hold the job submission.
        /// </summary>
        /// <returns>The path to the folder created.</returns>
        private string CreateDriverFolder(string jobId)
        {
            var timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId, timestamp)));
        }
    }
}