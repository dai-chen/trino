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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class LuceneRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<LuceneColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final long totalBytes = 0;
    private List<String> fields;
    private IndexSearcher searcher;
    private int currentDocIdx;
    private int numDoc;

    private TopDocs docs;
    private List<SortedNumericDocValues> docValueIterators = new ArrayList<>();

    public LuceneRecordCursor(URI path, List<LuceneColumnHandle> columnHandles) throws Exception
    {
        this.columnHandles = columnHandles;

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Path.of(path)));
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        searcher = new IndexSearcher(reader);
        // this.numDoc = reader.maxDoc();

        this.docs = searcher.search(new MatchAllDocsQuery(), 10000);
        this.numDoc = docs.scoreDocs.length;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            LuceneColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();

            docValueIterators.add(DocValues.getSortedNumeric(reader.leaves().get(0).reader(), columnHandle.getColumnName()));
        }

        fields = new ArrayList<>();
    }

    // @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
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
        if (currentDocIdx == numDoc) {
            return false;
        }

        try {
            fields.clear();
            for (SortedNumericDocValues iterator : docValueIterators) {
                iterator.advance(docs.scoreDocs[currentDocIdx].doc);
                fields.add(String.valueOf(iterator.nextValue()));
            }

            currentDocIdx++;
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        /*
        try {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                LeafReader docValueReader = leaf.reader();

                for (LuceneColumnHandle columnHandle : columnHandles) {
                    if (columnHandle.getColumnType() == IntegerType.INTEGER) {
                        fields.add(String.valueOf(docValueReader.getNumericDocValues(columnHandle.getColumnName()).longValue()));
                    } else {
                        // fields.add(docValueReader.getBinaryDocValues(columnHandle.getColumnName()).binaryValue().utf8ToString());
                        //fields.add(docValueReader.getSortedDocValues(columnHandle.getColumnName()).);
                    }
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        */

        /*
        Document doc = null;
        try {
            doc = searcher.doc(currentDocIdx++);
            fields = parseDoc(doc);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        */
        return true;
    }

    private List<String> parseDoc(Document doc)
    {
        List<String> fds = new ArrayList<>();
        // String allFieldNames = "orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment";
        // String[] fieldNames = allFieldNames.split(",");
        for (int i = 0; i < columnHandles.size(); i++) {
            String fieldName = columnHandles.get(i).getColumnName();
            String columnValue = doc.get(fieldName);
            fds.add(columnValue);
        }
        return fds;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
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
}
