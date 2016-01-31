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
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Timer = System.Timers.Timer;

namespace Org.Apache.REEF.Tests.Functional
{
    public class ReefFunctionalTest
    {
        protected const string _stdout = "driver.stdout";
        protected const string _stderr = "driver.stderr";
        protected const string _cmdFile = "run.cmd";
        protected const string _binFolder = ".";

        protected static int TestNumber = 1;
        protected const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        private const string Local = "local";
        private const string YARN = "yarn";
        private const int SleepTime = 1000;

        private readonly static Logger Logger = Logger.GetLogger(typeof(ReefFunctionalTest));
        private const string StorageAccountKeyEnvironmentVariable = "REEFTestStorageAccountKey";
        private const string StorageAccountNameEnvironmentVariable = "REEFTestStorageAccountName";
        private bool _testSuccess = false;
        private bool _onLocalRuntime = false;

        private readonly bool _enableRealtimeLogUpload = false;

        protected string TestId { get; set; }

        protected Timer TestTimer { get; set; }

        protected Task TimerTask { get; set; }

        protected bool TestSuccess 
        {
            get { return _testSuccess; }
            set { _testSuccess = value; }
        }

        protected bool IsOnLocalRuntime
        {
            get { return _onLocalRuntime; }
            set { _onLocalRuntime = value; }
        }

        public void Init()
        {
            TestId = Guid.NewGuid().ToString("N").Substring(0, 8);
            Console.WriteLine("Running test " + TestId + ". If failed AND log uploaded is enabled, log can be find in " + Path.Combine(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), TestId));
            if (_enableRealtimeLogUpload)
            {
                TimerTask = new Task(() =>
                {
                    TestTimer = new Timer()
                    {
                        Interval = 1000,
                        Enabled = true,
                        AutoReset = true
                    };
                    TestTimer.Elapsed += PeriodicUploadLog;
                    TestTimer.Start();
                });
                TimerTask.Start(); 
            }
            
            ValidationUtilities.ValidateEnvVariable("JAVA_HOME");

            if (!Directory.Exists(_binFolder))
            {
                throw new InvalidOperationException(_binFolder + " not found in current directory, cannot init test");
            }
        }

        protected void CleanUp(string testFolder = DefaultRuntimeFolder)
        {
            Console.WriteLine("Cleaning up test.");

            if (TimerTask != null)
            {
                TestTimer.Stop();
                TimerTask.Dispose();
                TimerTask = null;
            }
            
            // Wait for file upload task to complete
            Thread.Sleep(500);

            string dir = Path.Combine(Directory.GetCurrentDirectory(), testFolder);
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, true);
                }
            }
            catch (IOException)
            {
                // do not fail if clean up is unsuccessful
            }   
        }

        protected void ValidateSuccessForLocalRuntime(int numberOfEvaluatorsToClose, int numberOfTasksToFail = 0, int numberOfEvaluatorsToFail = 0, string testFolder = DefaultRuntimeFolder)
        {
            const string successIndication = "EXIT: ActiveContextClr2Java::Close";
            const string failedTaskIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext";
            const string failedEvaluatorIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedEvaluatorHandlerOnNext";
            string[] lines = null;
            for (int i = 0; i < 60; i++)
            {
                try
                {
                    lines = File.ReadAllLines(GetLogFile(_stdout, testFolder));
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(SleepTime);
                }
            }

            if (lines != null)
            {
                Console.WriteLine("Lines read from log file : " + lines.Count());
                string[] successIndicators = lines.Where(s => s.Contains(successIndication)).ToArray();
                string[] failedTaskIndicators = lines.Where(s => s.Contains(failedTaskIndication)).ToArray();
                string[] failedIndicators = lines.Where(s => s.Contains(failedEvaluatorIndication)).ToArray();
                Assert.AreEqual(numberOfEvaluatorsToClose, successIndicators.Length);
                Assert.AreEqual(numberOfTasksToFail, failedTaskIndicators.Length);
                Assert.AreEqual(numberOfEvaluatorsToFail, failedIndicators.Length);
            }
            else
            {
                Console.WriteLine("Cannot read from log file");
                Assert.Fail();
            }
        }

        protected void ValidateMessageSuccessfullyLogged(string message, string testFolder, int numberOfoccurances = 1)
        {
            string[] lines = null;
            for (int i = 0; i < 60; i++)
            {
                try
                {
                    lines = File.ReadAllLines(GetLogFile(_stdout, testFolder));
                    break;
                }
                catch (Exception)
                {
                    Thread.Sleep(SleepTime);
                }
            }

            if (lines != null)
            {
                string[] successIndicators = lines.Where(s => s.Contains(message)).ToArray();
                Assert.AreEqual(numberOfoccurances, successIndicators.Count());
            }
            else
            {
                Console.WriteLine("Cannot read from log file");
                Assert.Fail();
            }
        }

        protected void PeriodicUploadLog(object source, ElapsedEventArgs e)
        {
            try
            {
                UploadDriverLog();
            }
            catch (Exception)
            {
                // log not available yet, ignore it
            }
        }

        protected string GetLogFile(string logFileName, string testFolder = DefaultRuntimeFolder)
        {
            string driverContainerDirectory = Directory.GetDirectories(Path.Combine(Directory.GetCurrentDirectory(), testFolder), "driver", SearchOption.AllDirectories).SingleOrDefault();
            Console.WriteLine("GetLogFile, driverContainerDirectory:" + driverContainerDirectory);

            if (string.IsNullOrWhiteSpace(driverContainerDirectory))
            {
                throw new InvalidOperationException("Cannot find driver container directory: " + driverContainerDirectory);
            }
            string logFile = Path.Combine(driverContainerDirectory, logFileName);
            if (!File.Exists(logFile))
            {
                throw new InvalidOperationException("Driver stdout file not found: " + logFile);
            }
            return logFile;
        }

        private void UploadDriverLog()
        {
            string driverStdout = GetLogFile(_stdout);
            string driverStderr = GetLogFile(_stderr);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(GetStorageConnectionString());
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));   
            container.CreateIfNotExists();

            CloudBlockBlob blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdOut"));
            FileStream fs = new FileStream(driverStdout, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStream(fs);
            fs.Close();

            blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdErr"));
            fs = new FileStream(driverStderr, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStream(fs);
            fs.Close();
        }

        /// <summary>
        /// Assembles the storage account connection string from the environment.
        /// </summary>
        /// <returns>the storage account connection string assembled from the environment.</returns>
        /// <exception cref="Exception">If the environment variables aren't set.</exception>
        private static string GetStorageConnectionString()
        {
            var accountName = GetEnvironmentVariable(StorageAccountNameEnvironmentVariable,
                "Please set " + StorageAccountNameEnvironmentVariable + " to the storage account name to be used for the tests");

            var accountKey = GetEnvironmentVariable(StorageAccountKeyEnvironmentVariable,
                "Please set " + StorageAccountKeyEnvironmentVariable + " to the key of the storage account to be used for the tests");

            var result = @"DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey;
            return result;
        }

        /// <summary>
        /// Fetch the value of the given environment variable
        /// </summary>
        /// <param name="variableName"></param>
        /// <param name="errorMessageIfNotAvailable"></param>
        /// <returns>the value of the given environment variable</returns>
        /// <exception cref="Exception">
        /// If the environment variables is not set. The message is taken from
        /// errorMessageIfNotAvailable
        /// </exception>
        private static string GetEnvironmentVariable(string variableName, string errorMessageIfNotAvailable)
        {
            var result = Environment.GetEnvironmentVariable(variableName);
            if (string.IsNullOrWhiteSpace(result))
            {
                Exceptions.Throw(new Exception(errorMessageIfNotAvailable), Logger);
            }
            return result;
        }

        protected void TestRun(IConfiguration driverConfig, Type globalAssemblyType, int numberOfEvaluator, string jobIdentifier = "myDriver", string runOnYarn = "local", string runtimeFolder = DefaultRuntimeFolder)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(runOnYarn, numberOfEvaluator, runtimeFolder));
            var reefClient = injector.GetInstance<IREEFClient>();
            var jobSubmissionBuilderFactory = injector.GetInstance<JobSubmissionBuilderFactory>();
            var jobSubmission = jobSubmissionBuilderFactory.GetJobSubmissionBuilder()
                .AddDriverConfiguration(driverConfig)
                .AddGlobalAssemblyForType(globalAssemblyType)
                .SetJobIdentifier(jobIdentifier)
                .Build();

            reefClient.SubmitAndGetJobStatus(jobSubmission);
        }

        private IConfiguration GetRuntimeConfiguration(string runOnYarn, int numberOfEvaluator, string runtimeFolder)
        {
            switch (runOnYarn)
            {
                case Local:
                    var dir = Path.Combine(".", runtimeFolder);
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfEvaluator.ToString())
                        .Set(LocalRuntimeClientConfiguration.RuntimeFolder, dir)
                        .Build();
                case YARN:
                    return YARNClientConfiguration.ConfigurationModule.Build();
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }
    }
}