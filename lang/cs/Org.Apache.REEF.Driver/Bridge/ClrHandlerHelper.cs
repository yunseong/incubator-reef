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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    public static class ClrHandlerHelper
    {
        private const string DllExtension = ".dll";
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClrHandlerHelper));

        /// <summary>
        /// The set of REEF assemblies required for the Driver.
        /// </summary>
        [Obsolete("Deprecated in 0.14. Will be made internal in 0.15.")]
        public static string[] ReefAssemblies
        {
            get
            {
                return new[]
                {
                    "Microsoft.Hadoop.Avro.dll", 
                    "Org.Apache.REEF.Driver.dll", 
                    "Org.Apache.REEF.Common.dll", 
                    "Org.Apache.REEF.Utilities.dll", 
                    "Org.Apache.REEF.Network.dll", 
                    "Org.Apache.REEF.Tang.dll", 
                    "Org.Apache.REEF.Wake.dll", 
                    "Newtonsoft.Json.dll", 
                    "protobuf-net.dll"
                };
            }
        }

        /// <summary>
        /// The memory granularity in megabytes for the Evaluator Descriptors.
        /// </summary>
        internal static int MemoryGranularity { get; set; }

        /// <summary>
        /// Creates an InterOp handle for .NET EventHandlers.
        /// </summary>
        /// <param name="handler">The EventHandler</param>
        /// <returns>The InterOp handle</returns>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static ulong CreateHandler(object handler)
        {
            GCHandle gc = GCHandle.Alloc(handler);
            IntPtr intPtr = GCHandle.ToIntPtr(gc);
            ulong ul = (ulong)intPtr.ToInt64();
            return ul;
        }

        /// <summary>
        /// Frees a .NET handle.
        /// </summary>
        /// <param name="handle">The handle to free</param>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static void FreeHandle(ulong handle)
        {
            GCHandle gc = GCHandle.FromIntPtr((IntPtr)handle);
            gc.Free();
        }

        /// <summary>
        /// Sets the memory granularity in megabytes for the Evaluator Descriptors.
        /// </summary>
        /// <param name="granularity">The memory granularity in megabytes</param>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static void SetMemoryGranuality(int granularity)
        {
            if (granularity <= 0)
            {
                var e = new ArgumentException("Granularity must be a positive value, provided: " + granularity);
                Exceptions.Throw(e, LOGGER);
            }
            MemoryGranularity = granularity;
        }

        /// <summary>
        /// Returns the null handle not used on the Java side (i.e. 0).
        /// </summary>
        /// <returns>The null handle</returns>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static ulong CreateNullHandler()
        {
            return Constants.NullHandler;
        }

        /// <summary>
        /// Gets the command line arguments as specified in <see cref="DriverBridgeConfigurationOptions.ArgumentSets"/>.
        /// </summary>
        /// <returns>The set of command line arguments</returns>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15. Inject the CommandLineArguments in your Handler instead.")]
        public static ISet<string> GetCommandLineArguments()
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GetCommandLineArguments"))
            {
                CommandLineArguments arguments;
                try
                {                       
                    arguments = BridgeConfigurationProvider.GetBridgeInjector(null).GetInstance<CommandLineArguments>();
                }
                catch (InjectionException e)
                {
                    const string error = "Cannot inject command line arguments from driver bridge configuration.";
                    Exceptions.CaughtAndThrow(e, Level.Error, error, LOGGER);
                    throw;
                }
                return arguments.Arguments;
            }
        }

        /// <summary>
        /// Allows additional Java classes to be included into the classpath by the user.
        /// Generates a file named <see cref="Constants.GlobalUserSuppliedJavaLibraries"/> in the 
        /// executing directory containing classpath information.
        /// </summary>
        /// <param name="classPaths">classpaths to the Java classes</param>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static void SupplyAdditionalClassPath(params string[] classPaths)
        {
            var path = Path.Combine(Directory.GetCurrentDirectory(), Constants.GlobalUserSuppliedJavaLibraries);
            File.WriteAllText(path, string.Join(",", classPaths));
        }

        /// <summary>
        /// Generates the class hierarchy binary to the current directory into a file named
        /// <see cref="Constants.ClassHierarchyBin"/>.
        /// </summary>
        /// <param name="clrDlls">The set of DLLs generating the class hierarchy</param>
        [Obsolete("Deprecated in 0.14. Will be removed in 0.15.")]
        public static void GenerateClassHierarchy(ISet<string> clrDlls)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GenerateClassHierarchy"))
            {
                IClassHierarchy ns = TangFactory.GetTang().GetClassHierarchy(clrDlls.ToArray());
                ProtocolBufferClassHierarchy.Serialize(Constants.ClassHierarchyBin, ns);

                LOGGER.Log(Level.Info, "Class hierarchy written to [{0}].", Path.Combine(Directory.GetCurrentDirectory(), Constants.ClassHierarchyBin));
            }
        }

        /// <summary>
        /// Gets the list of assemblies needed for the REEF driver.
        /// </summary>
        /// <returns>A whitespace separated string consisting of all the required driver assemblies</returns>
        public static string GetAssembliesListForReefDriverApp()
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::GetAssembliesListForReefDriverApp"))
            {
                var executionDirectory = Directory.GetCurrentDirectory();
                var assemblies = new List<string>(
                    Directory.GetFiles(Path.Combine(executionDirectory, Constants.DriverAppDirectory), "*.dll")
                             .Select(f => string.Format(CultureInfo.InvariantCulture, "\"{0}\"", Constants.DriverAppDirectory + @"\" + Path.GetFileName(f))));

                foreach (var reefAssembly in ReefAssemblies)
                {
                    if (!File.Exists(reefAssembly))
                    {
                        var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Assembly [{0}] needed for REEF driver not found in {1}", reefAssembly, executionDirectory));
                        Exceptions.Throw(e, LOGGER);
                    }
                    File.Copy(reefAssembly, Path.Combine(executionDirectory, Constants.DriverAppDirectory, reefAssembly), overwrite: true);
                    assemblies.Add(string.Format(CultureInfo.InvariantCulture, "\"{0}\"", Constants.DriverAppDirectory + @"\" + reefAssembly));
                }
                return string.Join(" ", assemblies);
            }
        }

        /// <summary>
        /// Creates a new local directory <see cref="Constants.DriverAppDirectory"/> 
        /// at the current directory and copies all .dll files as specified by 
        /// the input parameter <see cref="dlls"/> from the current directory into
        /// the new directory.
        /// </summary>
        /// <param name="dlls">The set of DLLs to copy from the current directory</param>
        public static void CopyDllsToAppDirectory(ISet<string> dlls)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::CopyDllsToAppDirectory"))
            {
                var executionDirectory = Directory.GetCurrentDirectory();
                Directory.CreateDirectory(Path.Combine(executionDirectory, Constants.DriverAppDirectory));
                foreach (var dll in dlls)
                {
                    var dllFile = dll.EndsWith(DllExtension, StringComparison.OrdinalIgnoreCase) ? dll : dll + DllExtension;
                    if (!File.Exists(dllFile))
                    {
                        var e = new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, "Assembly [{0}] for REEF application not found in {1}", dllFile, executionDirectory));
                        Exceptions.Throw(e, LOGGER);
                    }
                    File.Copy(dllFile, Path.Combine(executionDirectory, Constants.DriverAppDirectory, dllFile), overwrite: true);
                }
            }
        }
    }
}