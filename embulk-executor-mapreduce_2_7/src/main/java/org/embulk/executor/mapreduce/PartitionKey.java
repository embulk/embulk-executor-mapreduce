package org.embulk.executor.mapreduce;

import org.embulk.spi.Buffer;

public interface PartitionKey
        extends Cloneable
{
    public void dump(Buffer buffer);

    public PartitionKey clone();
}
