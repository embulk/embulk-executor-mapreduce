package org.embulk.executor.mapreduce;

import java.util.concurrent.ExecutionException;
import java.io.IOException;
import com.google.inject.Injector;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.embulk.config.ModelManager;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigLoader;
import org.embulk.config.CommitReport;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.embulk.spi.util.Executors;
import org.embulk.EmbulkService;

public class EmbulkMapReduce
{
    private static final String CK_SYSTEM_CONFIG = "embulk.mapreduce.systemConfig";
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

    public static void setTaskCount(Configuration config, int taskCount)
    {
        config.setInt(CK_TASK_COUNT, taskCount);
    }

    public static int getTaskCount(Configuration config)
    {
        return config.getInt(CK_TASK_COUNT, 0);
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

    public static void writeAttemptStateFile(JobContext context, AttemptState state) throws IOException
    {
        Path path = new Path(context.getWorkingDirectory(), state.getAttemptId().toString());
        try (FSDataOutputStream out = path.getFileSystem(context.getConfiguration()).create(path, true)) {
            state.writeTo(out);
        }
    }

    public static AttemptState readAttemptStateFile(JobContext context, TaskAttemptID id) throws IOException
    {
        Path path = new Path(context.getWorkingDirectory(), id.toString());
        try (FSDataInputStream in = path.getFileSystem(context.getConfiguration()).open(path)) {
            return AttemptState.readFrom(in);
        }
    }

    public static class EmbulkMapper
            extends Mapper<IntWritable, Text, Text, Text>
    {
        private Context context;
        private Injector injector;
        private MapReduceExecutorTask task;
        private ExecSession session;

        public void setup(Context context)
        {
            this.context = context;
            this.injector = newEmbulkInstance(context.getConfiguration());
            this.task = getExecutorTask(injector, context.getConfiguration());
            this.session = new ExecSession(injector, task.getExecConfig());
        }

        public void map(IntWritable key, Text value, final Context context) throws IOException, InterruptedException
        {
            final int taskIndex = key.get();

            try {
                Exec.doWith(session, new ExecAction<Void>() {
                    public Void run() throws Exception
                    {
                        process(context, taskIndex);
                        return null;
                    }
                });
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
                throw Throwables.propagate(e.getCause());
            }
        }

        private void process(final Context context, int taskIndex) throws IOException, InterruptedException
        {
            final AttemptState state = new AttemptState(context.getTaskAttemptID(), taskIndex);

            try {
                Executors.process(session, task.getProcessTask(), taskIndex, new Executors.ProcessStateCallback()
                {
                    public void started()
                    {
                        try {
                            writeAttemptStateFile(context, state);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public void inputCommitted(CommitReport report)
                    {
                        state.setInputCommitReport(report);
                        try {
                            writeAttemptStateFile(context, state);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public void outputCommitted(CommitReport report)
                    {
                        state.setOutputCommitReport(report);
                        try {
                            writeAttemptStateFile(context, state);
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
                    writeAttemptStateFile(context, state);
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
            extends Reducer<Text, Text, Text, Text>
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            // do nothing
        }
    }
}
