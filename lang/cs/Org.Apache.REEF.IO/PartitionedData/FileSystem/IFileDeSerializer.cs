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

namespace Org.Apache.REEF.IO.PartitionedData.FileSystem
{
    /// <summary>
    /// A interface for user to implement its deserializer.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IFileDeSerializer<T>
    {
        /// <summary>
        /// The input is a file folder which contains all input files in one partition.
        /// The output is of type T which is defined by the client
        /// If there is any IO error, IOException could be thrown.
        /// </summary>
        /// <param name="fileFolder"></param>
        /// <returns></returns>
        T Deserialize(string fileFolder);
    }
}
