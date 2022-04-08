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
                new LuceneColumnHandle("connector-1", "status", BigintType.BIGINT, 0),
                new LuceneColumnHandle("connector-1", "size", BigintType.BIGINT, 1));
        try (LuceneRecordCursor cursor = new LuceneRecordCursor(path, columns)) {
            while (cursor.advanceNextPosition()) {
                System.out.println("status: " + cursor.getLong(0));
                System.out.println("size: " + cursor.getLong(1));
            }
        }
    }
}
