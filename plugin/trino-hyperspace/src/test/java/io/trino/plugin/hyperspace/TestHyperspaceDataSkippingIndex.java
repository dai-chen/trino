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
package io.trino.plugin.hyperspace;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class TestHyperspaceDataSkippingIndex
{
    private final static ColumnHandle COLUMN_HANDLE = new ColumnHandle() {};

    @Test
    public void testIsSplitIncluded()
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(Range.greaterThan(BigintType.BIGINT, 1L)), false)
        ));
        HyperspaceDataSkippingIndex index = new HyperspaceDataSkippingIndex("orders-minmax", predicate);

        assertTrue(index.isSplitIncluded("/Users/daichen/Temp/orders/region=US/part-00001-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet"));
        assertTrue(index.isSplitIncluded("/Users/daichen/Temp/orders/region=US/part-00002-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet"));
        assertTrue(index.isSplitIncluded("/Users/daichen/Temp/orders/region=US/part-00003-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet"));
    }
}
