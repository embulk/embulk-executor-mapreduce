package org.embulk.executor.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestEmbulkRecordReader
{
    @Test
    public void simpleTest()
    {
        int[] taskIndexes = new int[] {0, 1, 4, 6, 7};
        try (EmbulkRecordReader r = new EmbulkRecordReader(new EmbulkInputSplit(taskIndexes))) {
            int i = 0;
            while (r.nextKeyValue()) {
                assertEquals(taskIndexes[i], r.getCurrentKey().get());
                assertTrue(r.getCurrentValue() instanceof NullWritable);
                i++;
            }
            assertEquals(taskIndexes.length, i);
        }
    }
}
