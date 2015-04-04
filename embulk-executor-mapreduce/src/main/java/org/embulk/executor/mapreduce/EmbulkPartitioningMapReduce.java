package org.embulk.executor.mapreduce;

import static org.embulk.executor.mapreduce.newPartitioning;
import org.apache.hadoop.io.BytesWritable;

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
            final Configuration config = context.getConfiguration();
            final Path stateDir = getStateDirectoryPath(config);
            final AttemptState state = new AttemptState(context.getTaskAttemptID(), taskIndex);
            final BufferAllocator bufferAllocator = runner.getModelManager();
            final ModelManager modelManager = runner.getModelManager();

            ProcessTask task = runner.getMapReduceExecutorTask().getProcessTask();
            Partitioning partitioning = newPartitioning(runner.getMapReduceExecutorTask().getPartitioningType().get());
            ExecSession exec = runner.getExecSession();

            InputPlugin inputPlugin = exec.newPlugin(InputPlugin.class, task.getInputPluginType());
            List<FilterPlugin> filterPlugins = Filters.newFilterPlugins(exec, task.getFilterPluginTypes());

            final Partitioner partitioner = partitioning.newPartitioner(runner.getMapReduceExecutorTask().getPartitioningTask().get());
            final PageWritable keyWritable = new PageWritable();
            final BufferWritable valueWritable = new BufferWritable();
            valueWritable.set(partitioner.newKeyBuffer());

            OutputPlugin outputPlugin = new MapperOutputPlugin(
                    bufferAllocator,
                    partitioning.newPartitioner(runner.getMapReduceExecutorTask().getPartitioningTask().get()),
                    128,  // TODO configurable
                    new MapperOutputPlugin.Output() {
                        @Override
                        public void add(PartitionKey key, Page value)
                        {
                            try {
                                key.dump(keyWritable.get());
                                valueWritable.set(value);
                                context.write(keyWritable, valueWritable);
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

            try {
                Executors.process(ExecSession exec,
                    task, taskIndex,
                    inputPlugin, filterPlugins, outputPlugin,
                    new Executors.ProcessStateCallback()
                    {
                        public void started()
                        {
                            // TODO
                        }

                        public void inputCommitted(CommitReport report)
                        {
                            // TODO
                        }

                        public void outputCommitted(CommitReport report)
                        {
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

    public static class EmbulkPartitioningReducer
            extends Reducer<BufferWritable, PageWritable, NullWritable, NullWritable>
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
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
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
            ExecSession exec = runner.getExecSession();

            List<FilterPlugin> filterPlugins = Filters.newFilterPlugins(exec, task.getFilterPluginTypes());
            OutputPlugin outputPlugin = exec.newPlugin(OutputPlugin.class, task.getOutputPluginType());

            InputPlugin inputPulgin = new ReducerInputPlugin(new Input() {
                public Page poll()
                {
                }
            });

            try {
                Executors.process(ExecSession exec,
                    task, taskIndex,
                    inputPlugin, filterPlugins, outputPlugin,
                    new Executors.ProcessStateCallback()
                    {
                        public void started()
                        {
                        }

                        public void inputCommitted(CommitReport report)
                        {
                        }

                        public void outputCommitted(CommitReport report)
                        {
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
}
