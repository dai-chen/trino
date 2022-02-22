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
package io.trino.plugin.hyperspace.index;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.testng.Assert.*;

public class TestIndexDataManager
{
    private final Path indexPath = new File(
            "src/test/resources/orders-minmax/v__=0/part-00000-0ddc1a94-ade4-48b6-910a-3c521a415aa4-c000.snappy.parquet").toPath();

    private final IndexDataManager indexDataManager = new IndexDataManager();

    @Test
    public void testLoad()
            throws IOException
    {
        indexDataManager.load(indexPath, null);
    }
}
