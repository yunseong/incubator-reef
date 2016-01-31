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
using System.IO;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn
{
    /// <summary>
    /// Uploads assembled driver folder to DFS as an archive file.
    /// Uploading of DFS is done via calling into org.apache.reef.bridge.client.JobResourceUploader
    /// Uploading via Java is necessary to support Hadoop versions before v2.7.0 because
    /// we need to find modification time with accuracy of seconds to use archive as resource for YARNRM
    /// <see cref="Org.Apache.REEF.IO.FileSystem.Hadoop.HadoopFileSystem"/> implementation uses HDFS
    /// commandline shell which does not provide modification time with accuracy of seconds until v2.7.1
    /// </summary>
    internal sealed class LegacyJobResourceUploader : IJobResourceUploader
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(LegacyJobResourceUploader));

        private static readonly string JavaClassNameForResourceUploader =
            @"org.apache.reef.bridge.client.JobResourceUploader";

        private readonly IJavaClientLauncher _javaLauncher;
        private readonly IResourceArchiveFileGenerator _resourceArchiveFileGenerator;
        private readonly IFile _file;

        [Inject]
        private LegacyJobResourceUploader(
            IJavaClientLauncher javaLauncher,
            IResourceArchiveFileGenerator resourceArchiveFileGenerator,
            IFile file,
            IYarnCommandLineEnvironment yarn)
        {
            _file = file;
            _resourceArchiveFileGenerator = resourceArchiveFileGenerator;
            _javaLauncher = javaLauncher;
            _javaLauncher.AddToClassPath(yarn.GetYarnClasspathList());
        }

        public JobResource UploadJobResource(string driverLocalFolderPath, string jobSubmissionDirectory)
        {
            driverLocalFolderPath = driverLocalFolderPath.TrimEnd('\\') + @"\";
            string driverUploadPath = jobSubmissionDirectory.TrimEnd('/') + @"/";
            Log.Log(Level.Info, "DriverFolderPath: {0} DriverUploadPath: {1}", driverLocalFolderPath, driverUploadPath);

            var archivePath = _resourceArchiveFileGenerator.CreateArchiveToUpload(driverLocalFolderPath);

            var resourceDetailsOutputPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));
            _javaLauncher.Launch(JavaClassNameForResourceUploader,
                archivePath,
                driverUploadPath,
                resourceDetailsOutputPath);

            return ParseGeneratedOutputFile(resourceDetailsOutputPath);
        }

        private JobResource ParseGeneratedOutputFile(string resourceDetailsOutputPath)
        {
            if (!_file.Exists(resourceDetailsOutputPath))
            {
                Exceptions.Throw(
                    new FileNotFoundException("Could not find resource details file " + resourceDetailsOutputPath),
                    Log);
            }

            string fileContent = _file.ReadAllText(resourceDetailsOutputPath).Trim();

            Log.Log(Level.Info, "Java uploader returned content: " + fileContent);

            _file.Delete(resourceDetailsOutputPath);
            string[] tokens = fileContent.Split(';');

            return new JobResource
            {
                RemoteUploadPath = tokens[0],
                LastModificationUnixTimestamp = long.Parse(tokens[1]),
                ResourceSize = long.Parse(tokens[2])
            };
        }
    }
}