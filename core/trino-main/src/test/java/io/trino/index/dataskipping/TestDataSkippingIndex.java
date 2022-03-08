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
package io.trino.index.dataskipping;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import org.testng.annotations.Test;

import java.nio.file.Path;

public class TestDataSkippingIndex
{
    private static final Path INDEX_ROOT_PATH = Path.of("/Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes/lineitem10g-price-minmax"); // Path.of("src/test/resources/orders-minmax").toAbsolutePath();

    private static final ColumnHandle COLUMN_HANDLE = new ColumnHandle() {};

    @Test
    public void testIsSplitIncludedWithEqualityPredicate()
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                COLUMN_HANDLE,
                Domain.create(ValueSet.ofRanges(Range.equal(DecimalType.createDecimalType(12, 2), 90200L)), false)));
        DataSkippingIndex index = new DataSkippingIndex(INDEX_ROOT_PATH, predicate);
    }
}
