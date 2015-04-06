package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.embulk.config.ModelManager;
import org.embulk.config.CommitReport;
import org.embulk.config.ConfigDiff;
import org.embulk.config.TaskSource;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.embulk.spi.Schema;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageOutput;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.util.Filters;
import org.embulk.spi.util.Executors;
import org.embulk.executor.mapreduce.EmbulkMapReduce.SessionRunner;
import org.embulk.executor.mapreduce.BufferedPagePartitioner.PartitionedPageOutput;
import org.embulk.executor.mapreduce.EmbulkMapReduce.AttemptStateUpdateHandler;
import static org.embulk.executor.mapreduce.MapReduceExecutor.newPartitioning;

public class EmbulkPartitioningMapReduce
{
    public static class EmbulkPartitioningMapper
            extends Mapper<IntWritable, NullWritable, BufferWritable, PageWritable>
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
            ProcessTask task = runner.getMapReduceExecutorTask().getProcessTask();
            ExecSession exec = runner.getExecSession();

            // input and filters run at mapper
            InputPlugin inputPlugin = exec.newPlugin(InputPlugin.class, task.getInputPluginType());
            List<FilterPlugin> filterPlugins = Filters.newFilterPlugins(exec, task.getFilterPluginTypes());

            // output writes pages with partitioning key to the Context
            Partitioning partitioning = newPartitioning(runner.getMapReduceExecutorTask().getPartitioningType().get());
            final Partitioner partitioner = partitioning.newPartitioner(runner.getMapReduceExecutorTask().getPartitioningTask().get());
            OutputPlugin outputPlugin = new MapperOutputPlugin(
                    runner.getBufferAllocator(), partitioner,
                    128,  // TODO configurable
                    new PartitionedPageOutput() {
                        private final BufferWritable keyWritable = new BufferWritable();
                        private final PageWritable valueWritable = new PageWritable();

                        {
                            keyWritable.set(partitioner.newKeyBuffer());
                        }

                        @Override
                        public void add(PartitionKey key, Page value)
                        {
                            try {
                                key.dump(keyWritable.get());
                                valueWritable.set(value);
                                context.write(keyWritable, valueWritable);
                            } catch (IOException | InterruptedException ex) {
                                throw new RuntimeException(ex);
                            } finally {
                                value.release();
                            }
                        }

                        @Override
                        public void finish()
                        { }

                        @Override
                        public void close()
                        { }
                    });

            AttemptStateUpdateHandler handler = new AttemptStateUpdateHandler(runner,
                    new AttemptState(context.getTaskAttemptID(), Optional.of(taskIndex), Optional.<Integer>absent()));

            try {
                Executors.process(exec, taskIndex,
                    inputPlugin, task.getInputSchema(), task.getInputTaskSource(),
                    filterPlugins, task.getFilterSchemas(), task.getFilterTaskSources(),
                    outputPlugin, task.getOutputSchema(), task.getOutputTaskSource(),
                    handler);
            } catch (Throwable ex) {
                try {
                    handler.setException(ex);
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

    public static class EmbulkPartitioningReducer
            extends Reducer<BufferWritable, PageWritable, NullWritable, NullWritable>
    {
        private Context context;
        private SessionRunner runner;
        private AttemptStateUpdateHandler handler;
        private TransactionalPageOutput output;
        private boolean failed = false;

        @Override
        public void setup(final Context context) throws IOException, InterruptedException
        {
            System.out.println("reducer setup: "+context.getTaskAttemptID());
            this.context = context;
            this.runner = new SessionRunner(context);

            runner.execSession(new ExecAction<Void>() {
                public Void run() throws Exception
                {
                    int taskIndex = context.getTaskAttemptID().getTaskID().getId();

                    ProcessTask task = runner.getMapReduceExecutorTask().getProcessTask();
                    ExecSession exec = runner.getExecSession();
                    OutputPlugin outputPlugin = exec.newPlugin(OutputPlugin.class, task.getOutputPluginType());

                    handler = new AttemptStateUpdateHandler(runner,
                            new AttemptState(context.getTaskAttemptID(), Optional.<Integer>absent(), Optional.of(taskIndex)));

                    output = outputPlugin.open(task.getOutputTaskSource(), task.getExecutorSchema(), taskIndex);

                    handler.started();

                    return null;
                }
            });
        }

        @Override
        public void reduce(BufferWritable key, final Iterable<PageWritable> values, final Context context)
                throws IOException, InterruptedException
        {
            runner.execSession(new ExecAction<Void>() {
                public Void run() throws Exception
                {
                    process(context, values);
                    return null;
                }
            });
        }

        private void process(final Context context, Iterable<PageWritable> values) throws IOException, InterruptedException
        {
            try {
                for (PageWritable value : values) {
                    output.add(value.get());
                }
            } catch (Throwable ex) {
                failed = true;
                try {
                    handler.setException(ex);
                } catch (Throwable e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            runner.execSession(new ExecAction<Void>() {
                public Void run() throws Exception
                {
                    try {
                        if (!failed) {
                            output.finish();
                            CommitReport report = output.commit();
                            handler.outputCommitted(report);
                        }
                    } finally {
                        output.close();
                    }
                    return null;
                }
            });
        }
    }

    private static class MapperOutputPlugin
            implements OutputPlugin
    {
        private final BufferAllocator bufferAllocator;
        private final Partitioner partitioner;
        private final int maxPageBufferCount;
        private final PartitionedPageOutput output;

        public MapperOutputPlugin(BufferAllocator bufferAllocator,
                Partitioner partitioner, int maxPageBufferCount,
                PartitionedPageOutput output)
        {
            this.bufferAllocator = bufferAllocator;
            this.partitioner = partitioner;
            this.maxPageBufferCount = maxPageBufferCount;
            this.output = output;
        }

        public ConfigDiff transaction(ConfigSource config,
                Schema schema, int taskCount,
                OutputPlugin.Control control)
        {
            // won't be called
            throw new RuntimeException("");
        }

        public ConfigDiff resume(TaskSource taskSource,
                Schema schema, int taskCount,
                OutputPlugin.Control control)
        {
            // won't be called
            throw new RuntimeException("");
        }

        public void cleanup(TaskSource taskSource,
                Schema schema, int taskCount,
                List<CommitReport> successCommitReports)
        {
            // won't be called
            throw new RuntimeException("");
        }

        public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int taskIndex)
        {
            return new TransactionalPageOutput() {
                private final BufferedPagePartitioner bufferedPartitioner = new BufferedPagePartitioner(
                        bufferAllocator, schema, partitioner, maxPageBufferCount, output);
                private final PageReader reader = new PageReader(schema);

                public void add(Page page)
                {
                    reader.setPage(page);
                    while (reader.nextRecord()) {
                        bufferedPartitioner.add(reader);
                    }
                }

                public void finish()
                {
                    bufferedPartitioner.finish();
                }

                public void close()
                {
                    reader.close();
                    bufferedPartitioner.close();
                }

                public void abort()
                { }

                public CommitReport commit()
                {
                    return Exec.newCommitReport();
                }
            };
        }
    }
}
