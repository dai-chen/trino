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
package io.trino.index.dataskipping.sketch;

import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.generic.GenericRecord;

/**
 * Bloom filter sketch that determines data skipping based on bloom filter index.
 */
public class BloomFilterSketch
        extends Sketch
{
    public BloomFilterSketch(TupleDomain<?> predicate)
    {
        super(predicate);
    }

    @Override
    public boolean evaluate(GenericRecord record)
    {
        Object bf = record.get("BloomFilter_CAST_l_extendedprice_AS_STRING___0.01__25000__0");
        // BloomFilter<Integer> bloomFilter =
        return false;
    }
}
