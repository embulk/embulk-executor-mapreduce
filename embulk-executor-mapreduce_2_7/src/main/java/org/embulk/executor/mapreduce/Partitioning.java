package org.embulk.executor.mapreduce;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Schema;

public interface Partitioning
{
    public TaskSource configure(ConfigSource config, Schema schema, int outputTaskCount);

    public Partitioner newPartitioner(TaskSource taskSource);
}
