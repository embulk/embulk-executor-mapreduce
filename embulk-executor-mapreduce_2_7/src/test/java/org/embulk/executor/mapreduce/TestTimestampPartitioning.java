package org.embulk.executor.mapreduce;

import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.embulk.executor.mapreduce.TimestampPartitioning.LongUnixTimestampPartitioner;
import org.embulk.executor.mapreduce.TimestampPartitioning.TimestampPartitioner;
import org.embulk.executor.mapreduce.TimestampPartitioning.Unit;
import org.embulk.executor.mapreduce.TimestampPartitioning.UnixTimestampUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestTimestampPartitioning
{
    @Rule
    public MapReduceExecutorTestRuntime runtime = new MapReduceExecutorTestRuntime();

    private TimestampPartitioning tp;

    @Before
    public void createTimestampPartitioning()
    {
        tp = new TimestampPartitioning();
    }

    @Test
    public void validateConfigSource()
            throws IOException
    {
        { // specified column is not included in schema
            ConfigSource config = runtime.getExec().newConfigSource()
                    .set("column", "_c0").set("unit", "hour").set("timezone", "UTC");
            Schema schema = Schema.builder().add("not_included", Types.TIMESTAMP).build();

            try {
                tp.configure(config, schema, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // only UTC is supported now
            ConfigSource config = runtime.getExec().newConfigSource()
                    .set("column", "_c0").set("unit", "hour").set("timezone", "PDT");
            Schema schema = Schema.builder().add("_c0", Types.TIMESTAMP).build();

            try {
                tp.configure(config, schema, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // the unit is only 'hour' or 'day'
            ConfigSource config = runtime.getExec().newConfigSource()
                    .set("column", "_c0").set("unit", "invalid").set("timezone", "UTC");
            Schema schema = Schema.builder().add("_c0", Types.TIMESTAMP).build();

            try {
                tp.configure(config, schema, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // the column type is only timestamp or long
            ConfigSource config = runtime.getExec().newConfigSource()
                    .set("column", "_c0").set("unit", "hour").set("timezone", "UTC");
            Schema schema = Schema.builder().add("_c0", Types.STRING).build();

            try {
                tp.configure(config, schema, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }

        { // if the column type is long, unix_timestamp_unit is required
            ConfigSource config = runtime.getExec().newConfigSource()
                    .set("column", "_c0").set("unit", "hour").set("timezone", "UTC").set("unix_timestamp_unit", "invalid");
            Schema schema = Schema.builder().add("_c0", Types.LONG).build();

            try {
                tp.configure(config, schema, 0);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof ConfigException);
            }
        }
    }

    @Test
    public void comparePartitionKeys()
            throws Exception
    {
        List<PartitionKey> pks = new ArrayList<>();

        Column c0 = new Column(0, "c0", Types.LONG);
        Column c1 = new Column(1, "c1", Types.TIMESTAMP);
        Schema schema = new Schema(Arrays.asList(c0, c1));

        LongUnixTimestampPartitioner lp = new LongUnixTimestampPartitioner(c0, Unit.HOUR, 1, UnixTimestampUnit.SEC);
        TimestampPartitioner tp = new TimestampPartitioner(c1, Unit.HOUR, 1);

        long timeWindow = System.currentTimeMillis()/1000/3600*3600;
        PageReader r = new PageReader(schema);
        for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                timeWindow, Timestamp.ofEpochSecond(timeWindow),
                timeWindow+1, Timestamp.ofEpochSecond(timeWindow+1),
                timeWindow+3600, Timestamp.ofEpochSecond(timeWindow+3600),
                timeWindow+3600+1, Timestamp.ofEpochSecond(timeWindow+3600+1),
                timeWindow+2*3600, Timestamp.ofEpochSecond(timeWindow+2*3600),
                timeWindow+2*3600+1, Timestamp.ofEpochSecond(timeWindow+2*3600+1)
        )){
            r.setPage(page);
            while (r.nextRecord()) {
                pks.add(lp.updateKey(r).clone());
                pks.add(tp.updateKey(r).clone());
            }
        }

        for (int i = 0; i < pks.size(); i += 2) {
            assertTrue(pks.get(i).equals(pks.get(i+1))); // long(tw) == timestamp(tw)
        }
        for (int i = 0; i < pks.size() - 4; i += 4) {
            assertTrue(pks.get(i).equals(pks.get(i+2))); // long(tw) == long (tw+1)
        }
        for (int i = 0; i < pks.size() - 4; i += 4) {
            assertFalse(pks.get(i).equals(pks.get(i+4))); // long(tw) != long (tw+3600)
        }
    }

    @Test
    public void checkUnit()
    {
        long hourlyTimeWindow = System.currentTimeMillis() / 1000 / 3600 * 3600;
        long dailyTimeWindow = System.currentTimeMillis() / 1000 / 86400 * 86600;

        // hour
        {
            assertEquals(Unit.HOUR, Unit.of("hour"));
            assertTrue(Unit.HOUR.utcPartition(hourlyTimeWindow) == Unit.HOUR.utcPartition(hourlyTimeWindow + 1));
            assertTrue(Unit.HOUR.utcPartition(hourlyTimeWindow) != Unit.HOUR.utcPartition(hourlyTimeWindow + 3600));
        }

        // day
        {
            assertEquals(Unit.DAY, Unit.of("day"));
            assertTrue(Unit.DAY.utcPartition(dailyTimeWindow) == Unit.DAY.utcPartition(dailyTimeWindow + 1));
            assertTrue(Unit.DAY.utcPartition(dailyTimeWindow) != Unit.DAY.utcPartition(dailyTimeWindow + 86400));
        }

        // invalid_unit
        {
            try {
                Unit.of("invalid_unit");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof ConfigException);
            }
        }
    }

    @Test
    public void checkUnixTimestampUnit()
    {
        long currentNano = System.nanoTime();
        long currentSec = currentNano / 1000000000;

        // sec
        {
            assertEquals(UnixTimestampUnit.SEC, UnixTimestampUnit.of("sec"));
            assertEquals(currentSec, UnixTimestampUnit.SEC.toSeconds(currentNano / 1000000000));
        }

        // milli
        {
            assertEquals(UnixTimestampUnit.MILLI, UnixTimestampUnit.of("milli"));
            assertEquals(currentSec, UnixTimestampUnit.MILLI.toSeconds(currentNano / 1000000));
        }

        // micro
        {
            assertEquals(UnixTimestampUnit.MICRO, UnixTimestampUnit.of("micro"));
            assertEquals(currentSec, UnixTimestampUnit.MICRO.toSeconds(currentNano / 1000));
        }

        // nano
        {
            assertEquals(UnixTimestampUnit.NANO, UnixTimestampUnit.of("nano"));
            assertEquals(currentSec, UnixTimestampUnit.NANO.toSeconds(currentNano));
        }

        // invalid_unit
        {
            try {
                UnixTimestampUnit.of("invalid_unit");
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof ConfigException);
            }
        }
    }
}
