<!--
Copyright 2025 Columnar Technologies Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Comparing ADBC and JDBC query result transfer with DuckDB in Java

## Running the examples

### ADBC example

Run the ADBC example with default behavior to materialize results in a List of ArrowRecordBatch
```bash
./run-adbc.sh
```

Run with `--vectorschemaroot` flag to materialize all results in a VectorSchemaRoot:
```bash
./run-adbc.sh --vectorschemaroot
```

### JDBC example

Run the JDBC example with default behavior to materialize results to a CachedRowSet
```bash
./run-jdbc.sh
```

Run with `--arraylist` flag to materialize results to an ArrayList of Object arrays
```bash
./run-jdbc.sh --arraylist
```

Run with `--arrow` flag to use the Arrow interface that's included in the DuckDB JDBC driver
```bash
./run-jdbc.sh --arrow
```
