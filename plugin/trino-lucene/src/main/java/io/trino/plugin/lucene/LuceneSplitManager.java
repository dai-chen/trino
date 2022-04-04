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
package io.trino.plugin.lucene;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LuceneSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final LuceneClient exampleClient;

    @Inject
    public LuceneSplitManager(LuceneConnectorId connectorId, LuceneClient exampleClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.exampleClient = requireNonNull(exampleClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle tableHandle,
                                          SplitSchedulingStrategy splitSchedulingStrategy,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint)
    {
        LuceneTableHandle luceneTable = (LuceneTableHandle) tableHandle;

        // LuceneTableLayoutHandle layoutHandle = checkType(layout, LuceneTableLayoutHandle.class, "layout");
        // LuceneTableHandle tableHandle = layoutHandle.getTable();
        // LuceneTable table = exampleClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        // checkState(tableTable != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        //for (URI uri : luceneTable.getSources()) {
        splits.add(new LuceneSplit(connectorId, luceneTable.getSchemaName(), luceneTable.getTableName(), URI.create("localhost")));
        //}
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
