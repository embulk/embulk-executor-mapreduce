package org.embulk.executor.mapreduce;

import java.io.IOException;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.embulk.exec.ForSystemConfig;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.ProcessState;

public class MapReduceExecutor
        implements ExecutorPlugin
{
    private final ConfigSource systemConfig;

    public MapReduceExecutor(@ForSystemConfig ConfigSource systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    @Override
    public void transaction(final ConfigSource config, ExecutorPlugin.Control control)
    {
        final MapReduceExecutorTask task = config.loadConfig(MapReduceExecutorTask.class);

        control.transaction(new ExecutorPlugin.Executor() {
            public void execute(ProcessTask procTask, int taskCount, ProcessState state)
            {
                task.setExecConfig(config);
                task.setProcessTask(procTask);
                run(task, taskCount, state);
            }
        });
    }

    void run(MapReduceExecutorTask task, int taskCount, ProcessState state)
    {
        ModelManager modelManager = task.getModelManager();

        Job job;
        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        job.setJobName(task.getJobName());
        EmbulkMapReduce.setSystemConfig(job.getConfiguration(), modelManager, systemConfig);
        EmbulkMapReduce.setExecutorTask(job.getConfiguration(), modelManager, task);
        EmbulkMapReduce.setTaskCount(job.getConfiguration(), taskCount);

        job.setInputFormatClass(EmbulkInputFormat.class);
        job.setMapperClass(EmbulkMapReduce.EmbulkMapper.class);
        job.setReducerClass(EmbulkMapReduce.EmbulkReducer.class);

        // dummy
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // dummy
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        try {
            job.waitForCompletion(false);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }

        // TODO shutdown hook
    }
}
