package org.embulk.executor.mapreduce;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.Progress;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.executor.mapreduce.EmbulkPartitioningMapReduce.EmbulkPartitioningMapper;
import org.embulk.executor.mapreduce.EmbulkPartitioningMapReduce.EmbulkPartitioningReducer;
import org.embulk.executor.mapreduce.MapReduceTestUtils.MockRecordWriter;
import org.embulk.spi.Column;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class TestEmbulkPartitioningMapReduce
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private MapReduceTestUtils utils;
    private Configuration conf;

    private MockRecordWriter recordWriter = new MockRecordWriter();

    @Before
    public void createUtilities()
            throws Exception
    {
        utils = new MapReduceTestUtils(runtime);

        ConfigSource systemConfig = utils.systemConfig();
        ConfigLoader loader = utils.configLoader();
        ConfigSource config = utils.config(loader, "src/test/resources/config/partitioning_mapred_config.yml");
        MapReduceExecutorTask task = utils.executorTask(config, loader, "src/test/resources/config/process_task.yml");
        setPartitioningTaskSource(loader, task);

        conf = utils.configuration(systemConfig, task);
    }

    @Test
    public void checkEmbulkPartitioningMapper()
            throws Exception
    {
        EmbulkPartitioningMapper mapper = new EmbulkPartitioningMapper();
        Mapper<IntWritable, NullWritable, BufferWritable, PageWritable>.Context mapContext = newMapContext(conf, recordWriter);

        mapper.setup(mapContext);
        mapper.map(new IntWritable(0), NullWritable.get(), mapContext);
        mapper.map(new IntWritable(1), NullWritable.get(), mapContext);
    }

    @Test
    public void checkEmbulkPartitioningReducer()
            throws Exception
    {
        EmbulkPartitioningReducer reducer = new EmbulkPartitioningReducer();
        Reducer<BufferWritable, PageWritable, NullWritable, NullWritable>.Context reduceContext = newReduceContext(conf);

        reducer.setup(reduceContext);
        Iterator<MockRecordWriter.Pair> pairs = recordWriter.iterator();
        while (pairs.hasNext()) {
            MockRecordWriter.Pair p = pairs.next();
            reducer.reduce(p.getBuffer(), Arrays.asList(p.getPage()), reduceContext);
        }
    }

    private void setPartitioningTaskSource(ConfigLoader loader, MapReduceExecutorTask execTask)
            throws IOException {
        ConfigSource config = loader.fromYamlFile(new File("src/test/resources/config/partitioning_task.yml"));
        TimestampPartitioning.PartitioningTask task = config.loadConfig(TimestampPartitioning.PartitioningTask.class);
        task.setTargetColumn(new Column(0, "date_code", Types.TIMESTAMP));
        TaskSource t = task.dump();

        execTask.setPartitioningType(Optional.of("timestamp"));
        execTask.setPartitioningTask(Optional.of(t));
    }

    private Mapper<IntWritable, NullWritable, BufferWritable, PageWritable>.Context newMapContext(
            Configuration conf, MockRecordWriter recordWriter)
    {
        TaskAttemptID taskID = TaskAttemptID.forName("attempt_200707121733_0003_m_000005_0");
        StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
        EmbulkInputSplit inputSplit = newInputSplit();
        EmbulkRecordReader recordReader = newRecordReader(inputSplit);
        MapContext<IntWritable, NullWritable, BufferWritable, PageWritable> mapContext =
                new MapContextImpl<>(conf, taskID, recordReader, recordWriter, null, reporter, inputSplit);
        Mapper<IntWritable, NullWritable, BufferWritable, PageWritable>.Context context =
                new WrappedMapper<IntWritable, NullWritable, BufferWritable, PageWritable>().getMapContext(mapContext);
        return context;
    }

    private Reducer<BufferWritable, PageWritable, NullWritable, NullWritable>.Context newReduceContext(Configuration conf)
            throws IOException, InterruptedException
    {
        TaskAttemptID taskID = TaskAttemptID.forName("attempt_200707121733_0003_r_000005_0");
        StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
        RawKeyValueIterator rawKeyValueIterator = new RawKeyValueIterator() {
            @Override
            public DataInputBuffer getKey() throws IOException {
                return null;
            }

            @Override
            public DataInputBuffer getValue() throws IOException {
                return null;
            }

            @Override
            public boolean next() throws IOException {
                return true;
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public Progress getProgress() {
                return null;
            }
        };
        ReduceContext<BufferWritable, PageWritable, NullWritable, NullWritable> reduceContext =
                new ReduceContextImpl<>(conf, taskID, rawKeyValueIterator, newDummyCounter(), newDummyCounter(),
                        null, null, reporter, null, BufferWritable.class, PageWritable.class);
        Reducer<BufferWritable, PageWritable, NullWritable, NullWritable>.Context context =
                new WrappedReducer<BufferWritable, PageWritable, NullWritable, NullWritable>().getReducerContext(reduceContext);
        return context;
    }

    private Counter newDummyCounter()
    {
        return new GenericCounter("", "");
    }
    private EmbulkInputSplit newInputSplit()
    {
        return new EmbulkInputSplit();
    }

    private EmbulkRecordReader newRecordReader(EmbulkInputSplit split)
    {
        return new EmbulkRecordReader(split);
    }


}
