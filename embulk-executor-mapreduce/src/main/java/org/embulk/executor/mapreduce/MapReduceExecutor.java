package org.embulk.executor.mapreduce;

import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.embulk.exec.ForSystemConfig;
import org.embulk.config.ConfigSource;
import org.embulk.config.ModelManager;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.ProcessState;

import java.util.Map;
import java.lang.reflect.Field;
import java.util.ServiceLoader;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;
import org.apache.hadoop.fs.FileSystem;

public class MapReduceExecutor
        implements ExecutorPlugin
{
    private final ConfigSource systemConfig;

    static {
        try {
            Field f = Cluster.class.getDeclaredField("frameworkLoader");
            f.setAccessible(true);
            f.set(null, ServiceLoader.load(ClientProtocolProvider.class, ClientProtocolProvider.class.getClassLoader()));
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            ex.printStackTrace(System.err);
            // TODO only warning
            throw Throwables.propagate(ex);
        }

        try {
            Field f = FileSystem.class.getDeclaredField("SERVICE_FILE_SYSTEMS");
            f.setAccessible(true);
            Map<String, Class<? extends FileSystem>> fileSystems = (Map<String, Class<? extends FileSystem>>) f.get(null);
            ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class, FileSystem.class.getClassLoader());
            for (FileSystem fs : serviceLoader) {
                System.out.println("fs: "+fs);
                fileSystems.put(fs.getScheme(), fs.getClass());
            }

            Field flag = FileSystem.class.getDeclaredField("FILE_SYSTEMS_LOADED");
            flag.setAccessible(true);
            flag.setBoolean(null, true);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            ex.printStackTrace(System.err);
            // TODO only warning
            throw Throwables.propagate(ex);
        }
    }

    @Inject
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

        Configuration conf = new Configuration();
        for (String path : task.getConfigFiles()) {
            conf.addResource(path);
        }

        Job job;
        try {
            //job = Job.getInstance(conf);
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
