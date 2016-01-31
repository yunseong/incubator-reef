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
using System.Runtime.Serialization;

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// Default implementation of IEvaluatorRequest.
    /// </summary>
    [DataContract]
    internal class EvaluatorRequest : IEvaluatorRequest
    {
        internal EvaluatorRequest()
            : this(0, 0, 1, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        internal EvaluatorRequest(int number, int megaBytes)
            : this(number, megaBytes, 1, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, int core)
            : this(number, megaBytes, core, string.Empty, Guid.NewGuid().ToString("N"))
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, string rack)
            : this(number, megaBytes, 1, rack, Guid.NewGuid().ToString("N"))
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, int core, string rack)
            : this(number, megaBytes, core, rack, Guid.NewGuid().ToString("N"))
        {
        }

        internal EvaluatorRequest(int number, int megaBytes, int core, string rack, string evaluatorBatchId)
        {
            Number = number;
            MemoryMegaBytes = megaBytes;
            VirtualCore = core;
            Rack = rack;
            EvaluatorBatchId = evaluatorBatchId;
        }

        [DataMember]
        public string InstanceId { get; set; }

        [DataMember]
        public int MemoryMegaBytes { get; private set; }

        [DataMember]
        public int Number { get; private set; }

        [DataMember]
        public int VirtualCore { get; private set; }

        [DataMember]
        public string Rack { get; private set; }

        [DataMember]
        public string EvaluatorBatchId { get; private set; }

        internal static EvaluatorRequestBuilder NewBuilder()
        {
            return new EvaluatorRequestBuilder();
        }

        internal static EvaluatorRequestBuilder NewBuilder(EvaluatorRequest request)
        {
            return new EvaluatorRequestBuilder(request);
        }
    }
}