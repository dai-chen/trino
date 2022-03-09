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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.index.dataskipping.DataSkippingIndex;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.TableScanNode;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Optimizer rule that applies data skipping to the query if applicable.
 */
public class ApplyDataSkippingIndex
{
    private static final Logger log = Logger.get(ApplyDataSkippingIndex.class);

    private ApplyDataSkippingIndex() {}

    /**
     * Apply the index by analyzing the predicate and return data file list as domain of "$path" column
     * (which can be recognized by Hive connector to filter out splits in InternalHiveSplitFactory)
     */
    public static TupleDomain<ColumnHandle> applyDataSkippingIndex(
            Metadata metadata,
            Session session,
            TypeProvider types,
            TableScanNode node,
            DomainTranslator.ExtractionResult decomposedPredicate)
    {
        TupleDomain<ColumnHandle> currentDomain = decomposedPredicate.getTupleDomain().transformKeys(node.getAssignments()::get);
        if (currentDomain.getDomains().isEmpty() || currentDomain.getDomains().get().size() == 0) {
            return TupleDomain.all();
        }

        Symbol pathSymbol = new Symbol("$path");
        Type type = types.allTypes().get(pathSymbol);
        if (type == null) { // Underlying table doesn't have $path, ex. query on system table
            return TupleDomain.all();
        }

        Domain pathDomain = Domain.all(type);
        for (Map.Entry<ColumnHandle, Domain> entry : currentDomain.getDomains().get().entrySet()) {
            ColumnHandle column = entry.getKey();
            Domain predicate = entry.getValue();
            Optional<Path> indexLocation = getIndexLocation(session, metadata, node, column);
            if (indexLocation.isPresent()) {
                pathDomain = pathDomain.intersect(getPathDomainForDomain(indexLocation.get(), column, predicate, type));
            }
        }

        ColumnHandle pathColumnHandle = node.getAssignments().get(pathSymbol);
        if (pathColumnHandle == null) {
            pathColumnHandle = metadata.getColumnHandles(session, node.getTable()).get(pathSymbol.getName());
        }
        return TupleDomain.withColumnDomains(Collections.singletonMap(pathColumnHandle, pathDomain));
    }

    private static Optional<Path> getIndexLocation(Session session, Metadata metadata, TableScanNode node, ColumnHandle column)
    {
        QualifiedObjectName indexTableName = QualifiedObjectName.valueOf("system.metadata.indexes");
        SystemTable indexTable = metadata.getSystemTable(session, indexTableName).get();
        TableHandle indexTableHandle = metadata.getTableHandle(session, indexTableName).get();

        TableHandle tableHandle = node.getTable();
        TableSchema tableSchema = metadata.getTableSchema(session, tableHandle);

        log.info("Querying index system table for table %s column %s", tableSchema.getQualifiedName(), column.toString());

        RecordCursor cursor = indexTable.cursor(indexTableHandle.getTransaction(), session.toConnectorSession(), TupleDomain.all());
        while (cursor.advanceNextPosition()) {
            String catalogName = (String) cursor.getObject(0);
            String schemaName = (String) cursor.getObject(1);
            String tableName = (String) cursor.getObject(2);
            String columnName = (String) cursor.getObject(3);

            QualifiedObjectName indexedTableName = new QualifiedObjectName(catalogName, schemaName, tableName);
            if (indexedTableName.equals(tableSchema.getQualifiedName()) && column.toString().startsWith(columnName + ":")) { // HiveColumnHandle.toString
                log.info("Found skipping index at %s", cursor.getObject(4));
                return Optional.of(Path.of((String) cursor.getObject(4)));
            }
        }
        log.info("No data skipping index applicable to the query");
        return Optional.empty();
    }

    private static Domain getPathDomainForDomain(Path indexLocation, ColumnHandle column, Domain predicate, Type type)
    {
        DataSkippingIndex dataSkippingIndex = new DataSkippingIndex(indexLocation, TupleDomain.withColumnDomains(ImmutableMap.of(column, predicate)));
        if (log.isInfoEnabled()) {
            Set<String> allIncludeDataFiles = dataSkippingIndex.getAllIncludeDataFiles();
            log.info("Data files to search %s (first 5 in total %d)",
                    allIncludeDataFiles.stream().limit(5).collect(Collectors.toList()),
                    allIncludeDataFiles.size());
        }

        List<Slice> pathList = dataSkippingIndex.getAllIncludeDataFiles()
                .stream()
                .map(Slices::utf8Slice).collect(Collectors.toList());
        return pathList.isEmpty() ? Domain.all(type) : Domain.multipleValues(type, pathList);
    }
}
