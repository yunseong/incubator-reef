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
using System.Net;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Utilities
{
    public static class NetUtilities
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(NetUtilities));

        public static IPEndPoint ParseIpEndpoint(string ipWithPort)
        {
            string ip = ipWithPort.TrimStart().TrimEnd();
            if (char.IsDigit(ip[0]))
            {
                ip = @"socket://" + ip;
            }
            Uri uri = new Uri(ip);
            string driverAddress = uri.Host;
            int driverCommunicationPort = uri.Port;
            IPAddress ipAddress;
            IPAddress.TryParse(driverAddress, out ipAddress);
            if (ipAddress == null)
            {
                Exceptions.Throw(new FormatException("invalid format for ip: " + ipWithPort), LOGGER);
            }

            return new IPEndPoint(ipAddress, driverCommunicationPort);
        }
    }
}
