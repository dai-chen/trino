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
 * Algorithm that implements the skipping, such as min-max, bloom filter, value list etc.
 */
public abstract class Sketch
{
    /**
     * Predicate given to be evaluated with each index data record
     */
    protected final TupleDomain<?> predicate;

    protected Sketch(TupleDomain<?> predicate)
    {
        this.predicate = predicate;
    }

    /**
     * Evaluate a given predicate on the record.
     */
    public abstract boolean evaluate(GenericRecord record);
}
