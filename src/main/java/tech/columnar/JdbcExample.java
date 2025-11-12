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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.duckdb.DuckDBResultSet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class JdbcExample {
    public static void main(String[] args) throws Exception {
        // Check for flags
        boolean useArrayList = args.length > 0 && args[0].equals("--arraylist");
        boolean useArrow = args.length > 0 && args[0].equals("--arrow");

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:tpch_lineitem.duckdb");
             Statement stmt = conn.createStatement()) {

            Runtime runtime = Runtime.getRuntime();
            long startTime = System.nanoTime();

            int rowCount;

            if (useArrow) {
                // Option A: Use DuckDB's Arrow interface
                try (var duckRS = (DuckDBResultSet) stmt.executeQuery("SELECT * FROM lineitem;");
                     var allocator = new RootAllocator();
                     var reader = (ArrowReader) duckRS.arrowExportStream(allocator, 65536);
                     var targetRoot = VectorSchemaRoot.create(reader.getVectorSchemaRoot().getSchema(), allocator)) {

                    targetRoot.allocateNew();
                    targetRoot.setRowCount(0);

                    // Load all result batches into a single VectorSchemaRoot
                    while (reader.loadNextBatch()) {
                        VectorSchemaRootAppender.append(targetRoot, reader.getVectorSchemaRoot());
                    }

                    rowCount = targetRoot.getRowCount();

                    long endTime = System.nanoTime();
                    double durationMs = (endTime - startTime) / 1_000_000.0;
                    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

                    System.out.println("Result materialization: DuckDBResultSet (Arrow VectorSchemaRoot)");
                    System.out.println("Query execution and result transfer time: " + String.format("%.2f", durationMs) + " ms");
                    System.out.println("Number of rows: " + rowCount);
                    System.out.println("Max JVM heap memory used: " + usedMemoryMB + " MB");
                }
                return;
            }

            if (useArrayList) {
                // Option B: Store in ArrayList of Object arrays
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM lineitem;")) {
                    List<Object[]> rows = new ArrayList<>();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    while (rs.next()) {
                        Object[] row = new Object[columnCount];
                        for (int i = 0; i < columnCount; i++) {
                            row[i] = rs.getObject(i + 1);
                        }
                        rows.add(row);
                    }

                    long endTime = System.nanoTime();
                    double durationMs = (endTime - startTime) / 1_000_000.0;

                    rowCount = rows.size();

                    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

                    System.out.println("Result materialization: ArrayList<Object[]>");
                    System.out.println("Query execution and result transfer time: " + String.format("%.2f", durationMs) + " ms");
                    System.out.println("Number of rows: " + rowCount);
                    System.out.println("Max JVM heap memory used: " + usedMemoryMB + " MB");
                }
            } else {
                // Option C: Store results in a CachedRowSet
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM lineitem;");
                     CachedRowSet cachedRowSet = RowSetProvider.newFactory().createCachedRowSet()) {
                    cachedRowSet.populate(rs);

                    long endTime = System.nanoTime();
                    double durationMs = (endTime - startTime) / 1_000_000.0;

                    rowCount = 0;
                    while (cachedRowSet.next()) {
                        rowCount++;
                    }

                    long usedMemoryMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

                    System.out.println("Result materialization: CachedRowSet");
                    System.out.println("Query execution and result transfer time: " + String.format("%.2f", durationMs) + " ms");
                    System.out.println("Number of rows: " + rowCount);
                    System.out.println("Max JVM heap memory used: " + usedMemoryMB + " MB");
                }
            }
        }
    }
}
