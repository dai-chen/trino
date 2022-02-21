package io.trino.plugin.hyperspace.index;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import static io.trino.plugin.hyperspace.index.IndexLogManager.*;
import static org.testng.Assert.*;

public class TestIndexLogManager
{
    private final Path indexPath = new File("src/test/resources/orders-minmax").toPath();

    private final IndexLogManager indexLogManager = new IndexLogManager(indexPath);

    @Test
    public void testGetLatestStableLog()
            throws IOException
    {
        IndexLogEntry latestStableLog = indexLogManager.getLatestStableLog();
        assertEquals(latestStableLog.logDataFilePath,
                "file://Users/daichen/Software/spark-3.1.2-bin-hadoop3.2/spark-warehouse/indexes/orders-minmax/v__=0/");
        assertEquals(latestStableLog.sourceIdTracker, ImmutableMap.of(
                "0", "file://Users/daichen/Temp/orders/region=US/part-00003-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                "1", "file://Users/daichen/Temp/orders/region=US/part-00002-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                "2", "file://Users/daichen/Temp/orders/region=US/part-00001-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet",
                "3", "file://Users/daichen/Temp/orders/region=EU/part-00000-dd26df1d-8bd4-4757-aa49-47d3a6bd8678.c000.snappy.parquet"));
    }
}
