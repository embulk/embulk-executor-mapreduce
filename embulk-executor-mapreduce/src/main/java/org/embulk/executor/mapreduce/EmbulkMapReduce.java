package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import com.google.inject.Injector;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.embulk.config.ModelManager;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigLoader;
import org.embulk.config.CommitReport;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.embulk.spi.util.Executors;
import org.embulk.EmbulkService;

public class EmbulkMapReduce
{
    private static final String CK_SYSTEM_CONFIG = "embulk.mapreduce.systemConfig";
    private static final String CK_STATE_DIRECTORY_PATH = "embulk.mapreduce.stateDirectorypath";
    private static final String CK_TASK_COUNT = "embulk.mapreduce.taskCount";
    private static final String CK_TASK = "embulk.mapreduce.task";

    public static void setSystemConfig(Configuration config, ModelManager modelManager, ConfigSource systemConfig)
    {
        config.set(CK_SYSTEM_CONFIG, modelManager.writeObject(systemConfig));
    }

    public static ConfigSource getSystemConfig(Configuration config)
    {
        try {
            ModelManager bootstrapModelManager = new ModelManager(null, new ObjectMapper());
            return new ConfigLoader(bootstrapModelManager).fromJson(
                    new JsonFactory().createParser(config.get(CK_SYSTEM_CONFIG)));  // TODO add fromJson(String)
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void setMapTaskCoun(Configuration config, int taskCount)
    {
        config.setInt(CK_TASK_COUNT, taskCount);
    }

    public static int getMapTaskCount(Configuration config)
    {
        return config.getInt(CK_TASK_COUNT, 0);
    }

    public static void setStateDirectoryPath(Configuration config, Path path)
    {
        config.set(CK_STATE_DIRECTORY_PATH, path.toString());
    }

    public static Path getStateDirectoryPath(Configuration config)
    {
        return new Path(config.get(CK_STATE_DIRECTORY_PATH));
    }

    public static void setExecutorTask(Configuration config, ModelManager modelManager, MapReduceExecutorTask task)
    {
        config.set(CK_TASK, modelManager.writeObject(task));
    }

    public static MapReduceExecutorTask getExecutorTask(Injector injector, Configuration config)
    {
        return injector.getInstance(ModelManager.class).readObject(MapReduceExecutorTask.class,
                config.get(CK_TASK));
    }

    public static Injector newEmbulkInstance(Configuration config)
    {
        ConfigSource systemConfig = getSystemConfig(config);
        return new EmbulkService(systemConfig).getInjector();
    }

    public static List<TaskAttemptID> listAttempts(Configuration config,
            Path stateDir) throws IOException
    {
        FileStatus[] stats = stateDir.getFileSystem(config).listStatus(stateDir);
        ImmutableList.Builder<TaskAttemptID> builder = ImmutableList.builder();
        for (FileStatus stat : stats) {
            String name = stat.getPath().getName();
            try {
                builder.add(TaskAttemptID.forName(name));
            } catch (IllegalArgumentException ex) {
                // ignore
            }
        }
        return builder.build();
    }

    public static AttemptState readAttemptStateFile(Configuration config,
            Path stateDir, TaskAttemptID id, ModelManager modelManager) throws IOException
    {
        Path path = new Path(stateDir, id.toString());
        try (FSDataInputStream in = path.getFileSystem(config).open(path)) {
            return AttemptState.readFrom(in, modelManager);
        }
    }

    public static void writeAttemptStateFile(Configuration config,
            Path stateDir, AttemptState state, ModelManager modelManager) throws IOException
    {
        Path path = new Path(stateDir, state.getAttemptId().toString());
        try (FSDataOutputStream out = path.getFileSystem(config).create(path, true)) {
            state.writeTo(out, modelManager);
        }
    }

    public static class SessionRunner
    {
        private final Injector injector;
        private final ModelManager modelManager;
        private final MapReduceExecutorTask task;
        private final ExecSession session;

        public SessionRunner(TaskAttemptContext context)
        {
            this.injector = newEmbulkInstance(context.getConfiguration());
            this.modelManager = injector.getInstance(ModelManager.class);
            this.task = getExecutorTask(injector, context.getConfiguration());
            this.session = new ExecSession(injector, task.getExecConfig());
        }

        public ModelManager getModelManager()
        {
            return modelManager;
        }

        public BufferAllocator getBufferAllocator()
        {
            return injector.getInstance(BufferAllocator.class);
        }

        public MapReduceExecutorTask getMapReduceExecutorTask()
        {
            return task;
        }

        public ExecSession getExecSession()
        {
            return session;
        }

        public <T> void execSession(ExecAction<T> action)
        {
            try {
                return Exec.doWith(session, action);
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
                throw Throwables.propagate(e.getCause());
            }
        }
    }

    public static class EmbulkMapper
            extends Mapper<IntWritable, NullWritable, NullWritable, NullWritable>
    {
        private Context context;
        private SessionRunner runner;

        @Override
        public void setup(Context context)
        {
            this.context = context;
            this.runner = new SessionRunner(context);
        }

        @Override
        public void map(IntWritable key, NullWritable value, final Context context) throws IOException, InterruptedException
        {
            final int taskIndex = key.get();

            runner.execSession(new ExecAction<Void>() {
                public Void run() throws Exception
                {
                    process(context, taskIndex);
                    return null;
                }
            });
        }

        private void process(final Context context, int taskIndex) throws IOException, InterruptedException
        {
            final Configuration config = context.getConfiguration();
            final Path stateDir = getStateDirectoryPath(config);
            final AttemptState state = new AttemptState(context.getTaskAttemptID(), taskIndex);
            final ModelManager modelManager = runner.getModelManager();

            ProcessTask task = runner.getMapReduceExecutorTask().getProcessTask();

            try {
                Executors.process(runner.getExecSession(), task.getProcessTask(), taskIndex, new Executors.ProcessStateCallback()
                {
                    public void started()
                    {
                        try {
                            writeAttemptStateFile(config, stateDir, state, modelManager);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public void inputCommitted(CommitReport report)
                    {
                        state.setInputCommitReport(report);
                        try {
                            writeAttemptStateFile(config, stateDir, state, modelManager);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public void outputCommitted(CommitReport report)
                    {
                        state.setOutputCommitReport(report);
                        try {
                            writeAttemptStateFile(config, stateDir, state, modelManager);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public void finished()
                    { }
                });
            } catch (Throwable ex) {
                try {
                    state.setException(ex);
                    writeAttemptStateFile(config, stateDir, state, modelManager);
                } catch (Throwable e) {
                    e.addSuppressed(ex);
                    throw e;
                }
                //if (task.getTaskRecovery()) {
                //    throw ex;
                //}
            }
        }
    }

    public static class EmbulkReducer
            extends Reducer<NullWritable, NullWritable, NullWritable, NullWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(NullWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException
        {
            // do nothing
        }
    }
}
