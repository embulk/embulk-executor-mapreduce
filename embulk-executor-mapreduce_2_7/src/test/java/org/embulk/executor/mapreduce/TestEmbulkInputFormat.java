package org.embulk.executor.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEmbulkInputFormat
{
    @Rule
    public MapReduceExecutorTestRuntime runtime = new MapReduceExecutorTestRuntime();

    private Configuration conf;
    private EmbulkInputFormat format;

    @Before
    public void createResources()
    {
        conf = new Configuration();
        format = new EmbulkInputFormat();
    }

    @Test
    public void getSplits()
            throws Exception
    {
        checkNumOfSplits(0);

        for (int i = 0; i < 10; i++) {

            int split = runtime.getRandom().nextInt(10000);
            checkNumOfSplits(split);
        }
    }

    private void checkNumOfSplits(int split)
            throws Exception
    {
        conf.set("embulk.mapreduce.taskCount", Integer.toString(split));
        JobContext jobContext = newJobContext(conf);
        assertEquals(split, format.getSplits(jobContext).size());
    }

    private JobContext newJobContext(Configuration conf)
    {
        JobID jobID = new JobID("test", runtime.getRandom().nextInt());
        return new JobContextImpl(conf, jobID);
    }
}
