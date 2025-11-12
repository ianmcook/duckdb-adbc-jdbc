#!/bin/bash
# Copyright 2025 Columnar Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run the JDBC example

# Compile (suppress warnings)
mvn compile -q 2>&1 | grep -v "^WARNING:"

# Get the classpath
CP=$(mvn dependency:build-classpath -q -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout 2>&1 | grep -v "^WARNING:")

# Run the JDBC example with increased heap size
java -Xmx8g -Xms4g \
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
  --enable-native-access=ALL-UNNAMED \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=error \
  -cp "target/classes:$CP" tech.columnar.JdbcExample "$@" 2>&1 | grep -v "^WARNING:" | grep -v "^SLF4J"
