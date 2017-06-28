package org.embulk.executor.mapreduce;

import org.embulk.spi.Buffer;
import org.embulk.spi.PageReader;

public interface Partitioner
{
    public Buffer newKeyBuffer();

    public PartitionKey updateKey(PageReader record);
}
