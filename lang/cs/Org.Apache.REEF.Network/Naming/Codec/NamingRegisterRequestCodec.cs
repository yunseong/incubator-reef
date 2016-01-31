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

using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming.Contracts;
using Org.Apache.REEF.Network.Naming.Events;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Naming.Codec
{
    internal sealed class NamingRegisterRequestCodec : ICodec<NamingRegisterRequest>
    {
        public byte[] Encode(NamingRegisterRequest obj)
        {
            AvroNamingRegisterRequest request = new AvroNamingRegisterRequest
            {
                id = obj.NameAssignment.Identifier,
                host = obj.NameAssignment.Endpoint.Address.ToString(),
                port = obj.NameAssignment.Endpoint.Port
            };
            return AvroUtils.AvroSerialize(request);
        }

        public NamingRegisterRequest Decode(byte[] data)
        {
            AvroNamingRegisterRequest request = AvroUtils.AvroDeserialize<AvroNamingRegisterRequest>(data);
            return new NamingRegisterRequest(new NameAssignment(request.id, request.host, request.port));
        }
    }
}
