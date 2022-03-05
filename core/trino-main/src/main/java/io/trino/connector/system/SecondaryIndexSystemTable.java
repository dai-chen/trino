/*
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
package io.trino.connector.system;

import io.trino.metadata.MetadataUtil.TableMetadataBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class SecondaryIndexSystemTable
        implements SystemTable
{
    private static final ConnectorTableMetadata TABLE_DEFINITION = TableMetadataBuilder.tableMetadataBuilder(
            new SchemaTableName("metadata", "indexes"))
            .column("catalog_name", createUnboundedVarcharType())
            .column("schema_name", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("indexed_column", createUnboundedVarcharType())
            //.column("is_fresh", BOOLEAN)
            .column("index_location", createUnboundedVarcharType())
            //.column("index_type", createUnboundedVarcharType())
            .build();

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return TABLE_DEFINITION;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle,
                               ConnectorSession session,
                               TupleDomain<Integer> constraint)
    {
        return InMemoryRecordSet.builder(getTableMetadata())
                .addRow("hive", "tpch10g", "lineitem", "l_extendedprice",
                        "/Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes/tpch-q6-price-minmax")
                .build().cursor();
    }
}
