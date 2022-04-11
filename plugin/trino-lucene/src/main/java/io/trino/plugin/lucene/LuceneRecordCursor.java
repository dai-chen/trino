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

import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.apache.lucene.search.Sort.INDEXORDER;

public class LuceneRecordCursor
        implements RecordCursor
{
    private final List<LuceneColumnHandle> columnHandles;

    private final List<String> fields = new ArrayList<>();

    private final List<DocIdSetIterator> docValueIterators = new ArrayList<>();

    private final ScoreDoc[] documents;
    private int current;
    private final int totalParts;

    public LuceneRecordCursor(URI path, List<LuceneColumnHandle> columnHandles) throws Exception
    {
        this(path, columnHandles, new MatchAllDocsQuery(), 0, 1);
    }

    public LuceneRecordCursor(URI path, List<LuceneColumnHandle> columnHandles, Query query, int partNumber, int totalParts) throws Exception
    {
        this.columnHandles = columnHandles;
        this.current = partNumber;
        this.totalParts = totalParts;

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of(path)));
        IndexSearcher searcher = new IndexSearcher(reader);
        documents = searcher.search(query, Integer.MAX_VALUE, INDEXORDER, false).scoreDocs;

        LeafReader leafReader = reader.leaves().get(0).reader();
        for (LuceneColumnHandle columnHandle : columnHandles) {
            if (columnHandle.getColumnType() == VarcharType.VARCHAR) {
                docValueIterators.add(DocValues.unwrapSingleton(DocValues.getSortedSet(leafReader, columnHandle.getColumnName())));
            }
            else {
                docValueIterators.add(DocValues.unwrapSingleton(DocValues.getSortedNumeric(leafReader, columnHandle.getColumnName())));
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (current >= documents.length) {
            return false;
        }

        try {
            fields.clear();
            int docId = documents[current].doc;
            current += totalParts;

            for (DocIdSetIterator iterator : docValueIterators) {
                /* Doesn't work ...
                if (iterator.advance(iterator.nextDoc()) == NO_MORE_DOCS) {
                    return false;
                }
                */
                iterator.advance(docId);
                if (iterator instanceof SortedDocValues) {
                    SortedDocValues stringIterator = (SortedDocValues) iterator;
                    BytesRef bytesRef = stringIterator.lookupOrd(stringIterator.ordValue());
                    fields.add(parseIP(bytesRef));
                }
                else {
                    fields.add(String.valueOf(((NumericDocValues) iterator).longValue()));
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private String getFieldValue(int field)
    {
        return fields.get(field);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }

    private String parseIP(BytesRef value)
    {
        byte[] bytes = Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
        InetAddress inet = InetAddressPoint.decode(bytes);
        return InetAddresses.toAddrString(inet);
    }
}
