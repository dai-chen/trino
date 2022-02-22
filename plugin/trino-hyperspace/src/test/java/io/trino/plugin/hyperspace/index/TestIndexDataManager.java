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
package io.trino.plugin.hyperspace.index;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hyperspace.index.IndexDataManager.IndexDataEntry;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.testng.Assert.assertEquals;

public class TestIndexDataManager
{
    private final Path indexPath = new File(
            "src/test/resources/orders-minmax/v__=0/part-00000-0ddc1a94-ade4-48b6-910a-3c521a415aa4-c000.snappy.parquet").toPath();

    @Test
    public void testGetAllIndexData()
            throws IOException
    {
        IndexDataManager indexDataManager = new IndexDataManager(TupleDomain.all());
        assertEquals(indexDataManager.getIndexData(indexPath), ImmutableMap.of(
                0L, new IndexDataEntry(0, 4, 4),
                1L, new IndexDataEntry(1, 2, 2),
                2L, new IndexDataEntry(2, 1, 1),
                3L, new IndexDataEntry(3, 3, 3)));
    }

    @Test
    public void testGetIndexDataFiltered()
            throws IOException
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                new ColumnHandle() {},
                Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, 2L)), false)));

        IndexDataManager indexDataManager = new IndexDataManager(predicate);
        assertEquals(indexDataManager.getIndexData(indexPath), ImmutableMap.of(
                3, new IndexDataEntry(3, 3, 3),
                0, new IndexDataEntry(0, 4, 4)));
    }
}
