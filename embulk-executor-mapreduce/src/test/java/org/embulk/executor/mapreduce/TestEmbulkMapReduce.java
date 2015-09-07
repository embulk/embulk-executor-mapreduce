package org.embulk.executor.mapreduce;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.executor.mapreduce.EmbulkMapReduce.EmbulkMapper;
import org.embulk.executor.mapreduce.MapReduceTestUtils.MockRecordWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class TestEmbulkMapReduce
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private MapReduceTestUtils utils;
    private Configuration conf;

    @Before
    public void createUtilities()
            throws Exception
    {
        utils = new MapReduceTestUtils(runtime);

        ConfigSource systemConfig = utils.systemConfig();
        ConfigLoader loader = utils.configLoader();
        ConfigSource config = utils.config(loader, "src/test/resources/config/mapred_config.yml");
        MapReduceExecutorTask task = utils.executorTask(config, loader, "src/test/resources/config/process_task.yml");
        setPartitioningTaskSource(loader, task);

        conf = utils.configuration(systemConfig, task);
    }

    @Test
    public void checkEmbulkMapper()
            throws Exception
    {
        EmbulkMapper mapper = new EmbulkMapper();
        Mapper<IntWritable, NullWritable, NullWritable, NullWritable>.Context context = newMapContext(conf);

        mapper.setup(context);
        mapper.map(new IntWritable(0), NullWritable.get(), context);
        mapper.map(new IntWritable(1), NullWritable.get(), context);
    }

    private void setPartitioningTaskSource(ConfigLoader loader, MapReduceExecutorTask execTask)
            throws IOException {
        execTask.setPartitioningType(Optional.<String>absent());
        execTask.setPartitioningTask(Optional.<TaskSource>absent());
    }

    private Mapper<IntWritable, NullWritable, NullWritable, NullWritable>.Context newMapContext(Configuration conf)
    {
        TaskAttemptID taskID = TaskAttemptID.forName("attempt_200707121733_0003_m_000005_0");
        StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
        EmbulkInputSplit inputSplit = new EmbulkInputSplit();
        EmbulkRecordReader recordReader = new EmbulkRecordReader(inputSplit);
        MapContext<IntWritable, NullWritable, NullWritable, NullWritable> mapContext =
                new MapContextImpl<>(conf, taskID, recordReader, null, null, reporter, inputSplit);
        Mapper<IntWritable, NullWritable, NullWritable, NullWritable>.Context context =
                new WrappedMapper<IntWritable, NullWritable, NullWritable, NullWritable>().getMapContext(mapContext);
        return context;
    }
}
