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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.lucene.search.MatchAllDocsQuery;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LuceneRecordSet
        implements RecordSet
{
    private final LuceneSplit split;
    private final List<LuceneColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public LuceneRecordSet(LuceneSplit split, List<LuceneColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.split = split;
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LuceneColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        LuceneRecordCursor lrc = null;
        try {
            lrc = new LuceneRecordCursor(split.getPath(), columnHandles, new MatchAllDocsQuery(), split.getPartNumber(), split.getTotalParts());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return lrc;
    }
}