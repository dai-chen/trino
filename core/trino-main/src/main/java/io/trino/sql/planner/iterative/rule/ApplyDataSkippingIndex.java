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
import io.trino.spi.connector.ColumnHandle;
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
            Optional<Path> indexLocation = getIndexLocation(node, column);
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

    private static Optional<Path> getIndexLocation(TableScanNode node, ColumnHandle column)
    {
        // TODO: determine if any index on the column and find its location by system metadata table
        return Optional.of(Path.of("/Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes/tpch-q6-price-minmax"));
    }

    private static Domain getPathDomainForDomain(Path indexLocation, ColumnHandle column, Domain predicate, Type type)
    {
        DataSkippingIndex dataSkippingIndex = new DataSkippingIndex(indexLocation, TupleDomain.withColumnDomains(ImmutableMap.of(column, predicate)));
        List<Slice> pathList = dataSkippingIndex.getAllIncludeDataFiles()
                .stream()
                .map(path -> Slices.utf8Slice(getPath(path))).collect(Collectors.toList());
        return Domain.multipleValues(type, pathList);
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
