package org.embulk.executor.mapreduce;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.CommitReport;
import org.embulk.executor.mapreduce.EmbulkMapReduce.AttemptStateUpdateHandler;
import org.embulk.executor.mapreduce.EmbulkMapReduce.EmbulkMapper;
import org.embulk.executor.mapreduce.EmbulkMapReduce.EmbulkReducer;
import org.embulk.executor.mapreduce.EmbulkMapReduce.SessionRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.mrunit.mapreduce.MapReduceDriver.newMapReduceDriver;

public class TestEmbulkMapReduce
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private MapReduceDriver<IntWritable, NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> mapReduceDriver;

    @Before
    public void createResources()
    {
        EmbulkMapper mapper = new MockEmbulkMapper();
        EmbulkReducer reducer = new EmbulkReducer();
        mapReduceDriver = newMapReduceDriver(mapper, reducer);
    }

    private void setConfigure(Configuration conf)
    {
        conf.set("embulk.mapreduce.systemConfig", "{}");
        conf.set("embulk.mapreduce.stateDirectorypath", "/tmp/embulk/state");
        conf.set("embulk.mapreduce.task",
        "{" +
           "\"Partitioning\":{}," +
           "\"ExecConfig\":{" +
              "\"type\":\"mapreduce\"," +
              "\"config\":{},\"state_path\":\"/tmp/embulk/\"," +
              "\"partitioning\":{}," +
              "\"job_name\":\"embulk\"}," +
           "\"JobName\":\"embulk\"," +
           "\"StatePath\":\"/tmp/embulk/\"," +
           "\"ProcessTask\":{" +
              "\"inputType\":\"file\"," +
              "\"outputType\":\"stdout\"," +
              "\"filterTypes\":[]," +
              "\"inputTask\":{" +
                 "\"ParserTaskSource\":{\"SchemaConfig\":[{\"format\":\"%Y%m%d\",\"name\":\"ts\",\"type\":\"timestamp\"}],\"DefaultTimeZone\":\"UTC\"}," +
                 "\"DecoderTaskSources\":[]," +
                 "\"FileInputTaskSource\":{\"Files\":[\"src/test/resources/fixtures/csv/sample1.csv.gz\",\"src/test/resources/fixtures/csv/sample2.csv.gz\"],\"PathPrefix\":\"src/test/resources/fixtures/csv/\",\"LastPath\":null}," +
                 "\"ParserConfig\":{\"type\":\"csv\",\"columns\":[{\"name\":\"date_code\",\"type\":\"timestamp\",\"format\":\"%Y%m%d\"}]}," +
                 "\"DecoderConfigs\":[]}," +
              "\"outputTask\":{\"TimeZone\":\"UTC\"}," +
              "\"filterTasks\":[]," +
              "\"schemas\":[[{\"index\":0,\"name\":\"ts\",\"type\":\"timestamp\"}]]," +
              "\"executorSchema\":[{\"index\":0,\"name\":\"ts\",\"type\":\"timestamp\"}]," +
              "\"executorTask\":{}}," +
           "\"PartitioningTask\":{}," +
           "\"PartitioningType\":\"timestamp\"," +
           "\"Reducers\":null," +
           "\"Config\":{}," +
           "\"Libjars\":[]," +
           "\"ConfigFiles\":[]" +
        "}"
        );
    }

    @Test
    public void testMapReduce()
            throws Exception
    {
        setConfigure(mapReduceDriver.getConfiguration());

        mapReduceDriver
                .withInput(new IntWritable(1), NullWritable.get())
                .withInput(new IntWritable(2), NullWritable.get())
                .withOutput(NullWritable.get(), NullWritable.get());

       mapReduceDriver.run();
    }

    static class MockEmbulkMapper
            extends EmbulkMapper
    {
        @Override
        protected void restorePluginLoadPaths()
                throws IOException
        { }

        @Override
        protected AttemptStateUpdateHandler newAttemptStateUpdateHandler(SessionRunner runner, AttemptState attemptState)
        {
            return new MockAttemptStateUpdateHandler(runner, attemptState);
        }
    }

    static class MockAttemptStateUpdateHandler
            extends AttemptStateUpdateHandler
    {
        public MockAttemptStateUpdateHandler(SessionRunner runner, AttemptState state) {
            super(runner, state);
        }

        @Override
        public void started()
        { }

        @Override
        public void inputCommitted(CommitReport report)
        { }

        @Override
        public void outputCommitted(CommitReport report)
        { }

        public void setException(Throwable ex) throws IOException
        { }
    }
}
