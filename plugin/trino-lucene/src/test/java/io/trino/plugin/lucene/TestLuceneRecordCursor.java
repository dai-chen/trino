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

import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class TestLuceneRecordCursor
{
    @Test
    public void test() throws Exception
    {
        URI path = URI.create("file:/Users/daichen/Temp/es-index-test-data/MM_gCu_7Ss-sAj3Jg1li0A/0/index");
        List<LuceneColumnHandle> columns = Arrays.asList(
                new LuceneColumnHandle("connector-1", "clientip", VarcharType.VARCHAR, 1),
                new LuceneColumnHandle("connector-1", "status", BigintType.BIGINT, 1),
                new LuceneColumnHandle("connector-1", "size", BigintType.BIGINT, 2));
        try (LuceneRecordCursor cursor = new LuceneRecordCursor(path, columns)) {
            for (int i = 0; i < 10; i++) {
                if (cursor.advanceNextPosition()) {
                    System.out.print("clientip: " + cursor.getSlice(0).toStringUtf8());
                    System.out.print(", status: " + cursor.getLong(1));
                    System.out.print(", size: " + cursor.getLong(2));
                    System.out.println();
                }
            }
        }
    }
}
