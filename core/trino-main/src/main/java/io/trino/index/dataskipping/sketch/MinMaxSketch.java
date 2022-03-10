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

import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import org.apache.avro.generic.GenericRecord;

/**
 * Min-max sketch that determines data skipping based on min-max index.
 */
public class MinMaxSketch
        extends Sketch
{
    public MinMaxSketch(TupleDomain<?> predicate)
    {
        super(predicate);
    }

    @Override
    public boolean evaluate(GenericRecord record)
    {
        long minValue = (long) record.get("MinMax_l_extendedprice__0"); // TODO: remove hardcoding name and type
        long maxValue = (long) record.get("MinMax_l_extendedprice__1");
        Domain range = Domain.create(ValueSet.ofRanges(
                Range.range(DecimalType.createDecimalType(12, 2), minValue, true, maxValue, true)), false);
        Domain expression = predicate.getDomains().get().entrySet().iterator().next().getValue();
        return !expression.intersect(range).isNone();
    }
}
