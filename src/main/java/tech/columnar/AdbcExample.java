/*
 * Copyright 2025 Columnar Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.columnar;

import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class AdbcExample {
    private static final String DRIVER_FACTORY = "org.apache.arrow.adbc.driver.jni.JniDriverFactory";

    public static void main(String[] args) throws Exception {
        // Check for --vectorschemaroot flag
        boolean useVectorSchemaRoot = args.length > 0 && args[0].equals("--vectorschemaroot");

        Map<String, Object> params = new HashMap<>();
        JniDriver.PARAM_DRIVER.set(params, "duckdb");
        params.put("path", "tpch_lineitem.duckdb");

        try (BufferAllocator allocator = new RootAllocator();
             AdbcDatabase db = AdbcDriverManager.getInstance().connect(DRIVER_FACTORY, allocator, params);
             AdbcConnection conn = db.connect();
             AdbcStatement stmt = conn.createStatement()) {
            stmt.setSqlQuery("SELECT * FROM lineitem;");

            Runtime runtime = Runtime.getRuntime();
            long startTime = System.nanoTime();

            int totalRows;

            if (useVectorSchemaRoot) {
                // Option A: Accumulate all results into a single VectorSchemaRoot
                try (var result = stmt.executeQuery();
                     var targetRoot = VectorSchemaRoot.create(result.getReader().getVectorSchemaRoot().getSchema(), allocator)) {

                    var reader = result.getReader();
                    targetRoot.allocateNew();
                    targetRoot.setRowCount(0);

                    while (reader.loadNextBatch()) {
                        VectorSchemaRootAppender.append(targetRoot, reader.getVectorSchemaRoot());
                    }

                    totalRows = targetRoot.getRowCount();

                    long endTime = System.nanoTime();
                    double durationMs = (endTime - startTime) / 1_000_000.0;
                    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

                    System.out.println("Result materialization: VectorSchemaRoot");
                    System.out.println("Query execution and result transfer time: " + String.format("%.2f", durationMs) + " ms");
                    System.out.println("Number of rows: " + totalRows);
                    System.out.println("Max JVM heap memory used: " + usedMemoryMB + " MB");
                }
            } else {
                // Option B: Materialize all batches into List<ArrowRecordBatch>
                try (var result = stmt.executeQuery()) {
                    var reader = result.getReader();
                    var root = reader.getVectorSchemaRoot();
                    var unloader = new VectorUnloader(root);

                    List<ArrowRecordBatch> batches = new ArrayList<>();
                    int numRows = 0;
                    while (reader.loadNextBatch()) {
                        numRows += root.getRowCount();
                        var batch = unloader.getRecordBatch();
                        batches.add(batch);
                        batch.close();
                    }

                    long endTime = System.nanoTime();
                    double durationMs = (endTime - startTime) / 1_000_000.0;
                    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
                    totalRows = numRows;

                    System.out.println("Result materialization: List<ArrowRecordBatch>");
                    System.out.println("Query execution and result transfer time: " + String.format("%.2f", durationMs) + " ms");
                    System.out.println("Number of rows: " + totalRows);
                    System.out.println("Max JVM heap memory used: " + usedMemoryMB + " MB");
                }
            }
        }
    }
}
