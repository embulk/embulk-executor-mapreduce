package org.embulk.executor.mapreduce;

import org.joda.time.DateTimeZone;
import com.google.common.base.Optional;

public class TimestampPartitioning
        implements Partitioning
{
    public interface PartitioningTask
            extends Task
    {
        @Config("column")
        public String getColumn();

        @Config("unit")
        public String getUnit();

        @Config("timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getTimeZone();

        @Config("unix_timestamp")
        @ConfigDefault("null")
        public Optional<String> getUnixTimestamp();
    }

    @Override
    public TaskSource configure(ConfigSource config, Schema schema, int outputTaskCount)
    {
        // TODO
        throw new UnsupportedOperationException("TimestampPartitioning is not implemented yet");
    }

    @Override
    public Partitioner newPartitioner(TaskSource taskSource)
    {
        // TODO
        throw new UnsupportedOperationException("TimestampPartitioning is not implemented yet");
    }
}
