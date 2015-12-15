package org.embulk.executor.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.joda.time.DateTimeZone;
import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.Buffer;

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

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        public String getUnixTimestamp();

        @Config("map_side_split_count")
        @ConfigDefault("1")
        public int getMapSideSplitCount();

        public Column getTargetColumn();
        public void setTargetColumn(Column column);
    }

    @VisibleForTesting
    static enum Unit
    {
        HOUR(60*60),
        DAY(24*60*60);
        //WEEK
        //MONTH,
        //YEAR;

        private final int unit;

        private Unit(int unit)
        {
            this.unit = unit;
        }

        public long utcPartition(long seconds)
        {
            return seconds / unit;
        }

        public static Unit of(String s)
        {
            switch (s) {
            case "hour": return HOUR;
            case "day": return DAY;
            //case "week": return WEEK;
            //case "month": return MONTH;
            //case "year": return YEAR;
            default:
                throw new ConfigException(
                        String.format("Unknown unit '%s'. Supported units are hour and day", s));
            }
        }
    }

    @VisibleForTesting
    static enum UnixTimestampUnit
    {
        SEC(1),
        MILLI(1000),
        MICRO(1000000),
        NANO(1000000000);

        private final int unit;

        private UnixTimestampUnit(int unit)
        {
            this.unit = unit;
        }

        public long toSeconds(long v)
        {
            return v / unit;
        }

        public static UnixTimestampUnit of(String s)
        {
            switch (s) {
            case "sec": return SEC;
            case "milli": return MILLI;
            case "micro": return MICRO;
            case "nano": return NANO;
            default:
                throw new ConfigException(
                        String.format("Unknown unix_timestamp_unit '%s'. Supported units are sec, milli, micro, and nano", s));
            }
        }
    }

    @Override
    public TaskSource configure(ConfigSource config, Schema schema, int outputTaskCount)
    {
        PartitioningTask task = config.loadConfig(PartitioningTask.class);
        Column column = findColumnByName(schema, task.getColumn());

        if (!task.getTimeZone().equals(DateTimeZone.UTC)) {
            // TODO
            throw new ConfigException("Timestamp partitioner supports only UTC time zone for now");
        }

        // validate unit
        Unit.of(task.getUnit());

        // validate type
        if (column.getType() instanceof TimestampType) {
            // ok
        } else if (column.getType() instanceof LongType) {
            // validate unix_timestamp_unit
            UnixTimestampUnit.of(task.getUnixTimestamp());
        } else {
            throw new ConfigException(
                    String.format("Partitioning column '%s' must be timestamp or long but got '%s'", column.getName(), column.getType()));
        }

        task.setTargetColumn(column);

        return task.dump();
    }

    private static Column findColumnByName(Schema schema, String columnName)
    {
        for (Column column : schema.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new ConfigException(
                String.format("Column '%s' is not found in schema", columnName));
    }

    @Override
    public Partitioner newPartitioner(TaskSource taskSource)
    {
        PartitioningTask task = taskSource.loadTask(PartitioningTask.class);

        Column column = task.getTargetColumn();
        if (column.getType() instanceof TimestampType) {
            return new TimestampPartitioner(column, Unit.of(task.getUnit()), task.getMapSideSplitCount());
        } else if (column.getType() instanceof LongType) {
            return new LongUnixTimestampPartitioner(column, Unit.of(task.getUnit()), UnixTimestampUnit.of(task.getUnixTimestamp()));
        } else {
            throw new AssertionError();
        }
    }

    private static class LongPartitionKey
            implements PartitionKey
    {
        public static Buffer newKeyBuffer()
        {
            Buffer buffer = Buffer.allocate(8);
            buffer.limit(8);
            return buffer;
        }

        private long value;

        public LongPartitionKey()
        { }

        private LongPartitionKey(long value)
        {
            this.value = value;
        }

        public void set(long value)
        {
            this.value = value;
        }

        @Override
        public void dump(Buffer buffer)
        {
            // TODO optimize
            buffer.array()[0] = (byte) (((int) (value >>>  0)) & 0xff);
            buffer.array()[1] = (byte) (((int) (value >>>  4)) & 0xff);
            buffer.array()[2] = (byte) (((int) (value >>>  8)) & 0xff);
            buffer.array()[3] = (byte) (((int) (value >>> 12)) & 0xff);
            buffer.array()[4] = (byte) (((int) (value >>> 16)) & 0xff);
            buffer.array()[5] = (byte) (((int) (value >>> 20)) & 0xff);
            buffer.array()[6] = (byte) (((int) (value >>> 24)) & 0xff);
            buffer.array()[7] = (byte) (((int) (value >>> 28)) & 0xff);
        }

        @Override
        public LongPartitionKey clone()
        {
            return new LongPartitionKey(value);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof LongPartitionKey)) {
                return false;
            }
            LongPartitionKey o = (LongPartitionKey) other;
            return value == o.value;
        }

        @Override
        public int hashCode()
        {
            return (int) (value ^ (value >>> 32));
        }
    }

    private static abstract class AbstractTimestampPartitioner
            implements Partitioner
    {
        protected final Column column;
        protected final Unit unit;
        private final LongPartitionKey key;

        public AbstractTimestampPartitioner(Column column, Unit unit)
        {
            this.column = column;
            this.unit = unit;
            this.key = new LongPartitionKey();
        }

        @Override
        public Buffer newKeyBuffer()
        {
            return LongPartitionKey.newKeyBuffer();
        }

        protected LongPartitionKey updateKey(long v)
        {
            key.set(v);
            return key;
        }
    }

    @VisibleForTesting
    static class TimestampPartitioner
            extends AbstractTimestampPartitioner
    {
        private int mapSideSplitCount;

        public TimestampPartitioner(Column column, Unit unit, int mapSideSplitCount)
        {
            super(column, unit);
            this.mapSideSplitCount = mapSideSplitCount;
        }

        @Override
        public PartitionKey updateKey(PageReader record)
        {
            Timestamp v = record.getTimestamp(column);
            return super.updateKey(unit.utcPartition(v.getEpochSecond()) + v.getEpochSecond() % mapSideSplitCount);
        }
    }

    @VisibleForTesting
    static class LongUnixTimestampPartitioner
            extends AbstractTimestampPartitioner
    {
        private final UnixTimestampUnit unixTimestampUnit;

        public LongUnixTimestampPartitioner(Column column, Unit unit,
                UnixTimestampUnit unixTimestampUnit)
        {
            super(column, unit);
            this.unixTimestampUnit = unixTimestampUnit;
        }

        @Override
        public PartitionKey updateKey(PageReader record)
        {
            long v = record.getLong(column);
            return super.updateKey(unit.utcPartition(unixTimestampUnit.toSeconds(v)));
        }
    }
}
