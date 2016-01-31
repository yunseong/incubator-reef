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

using System.IO;
using Org.Apache.REEF.IO.PartitionedData.Random.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IO.PartitionedData.Random
{
    internal sealed class RandomInputPartitionDescriptor : IPartitionDescriptor
    {
        private readonly string _id;
        private readonly int _numberOfDoubles;

        internal RandomInputPartitionDescriptor(string id, int numberOfDoubles)
        {
            _id = id;
            _numberOfDoubles = numberOfDoubles;
        }

        public string Id
        {
            get { return _id; }
        }

        public IConfiguration GetPartitionConfiguration()
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(IInputPartition<Stream>), typeof(RandomInputPartition))
                .BindNamedParameter(typeof(PartitionId), _id)
                .BindNamedParameter(typeof(NumberOfDoublesPerPartition), _numberOfDoubles.ToString())
                .Build();
        }
    }
}