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

import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ApplyDataSkippingIndex
{
    private ApplyDataSkippingIndex() {}

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
        Type type = types.get(pathSymbol);

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

        RecordCursor cursor = indexTable.cursor(indexTableHandle.getTransaction(), session.toConnectorSession(), TupleDomain.all());
        while (cursor.advanceNextPosition()) {
            String catalogName = (String) cursor.getObject(0);
            String schemaName = (String) cursor.getObject(1);
            String tableName = (String) cursor.getObject(2);
            String columnName = (String) cursor.getObject(3);

            QualifiedObjectName indexedTableName = new QualifiedObjectName(catalogName, schemaName, tableName);
            if (indexedTableName.equals(tableSchema.getQualifiedName()) && column.toString().startsWith(columnName + ":")) { // HiveColumnHandle.toString
                return Optional.of(Path.of((String) cursor.getObject(4)));
            }
        }
        return Optional.empty();
    }

    private static Domain getPathDomainForDomain(Path indexLocation, ColumnHandle column, Domain predicate, Type type)
    {
        DataSkippingIndex dataSkippingIndex = new DataSkippingIndex(indexLocation, TupleDomain.withColumnDomains(ImmutableMap.of(column, predicate)));
        List<Slice> pathList = dataSkippingIndex.getAllIncludeDataFiles()
                .stream()
                .map(path -> Slices.utf8Slice(getPath(path))).collect(Collectors.toList());
        return Domain.multipleValues(type, pathList); // TODO: handle pathList empty
    }

    private static String getPath(Path path)
    {
        try {
            return path.toUri().toURL().toString(); // file:/Users/...
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
