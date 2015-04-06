package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.io.File;
import java.io.IOException;
import java.io.EOFException;
import java.net.URL;
import java.net.URLClassLoader;
import org.slf4j.Logger;
import org.joda.time.format.DateTimeFormat;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.jruby.embed.ScriptingContainer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.embulk.exec.ForSystemConfig;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskSource;
import org.embulk.config.ModelManager;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.ProcessState;
import org.embulk.spi.TaskState;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;

public class MapReduceExecutor
        implements ExecutorPlugin
{
    private final Logger log = Exec.getLogger(MapReduceExecutor.class);
    private final ConfigSource systemConfig;
    private final ScriptingContainer jruby;

    @Inject
    public MapReduceExecutor(@ForSystemConfig ConfigSource systemConfig,
            ScriptingContainer jruby)
    {
        this.systemConfig = systemConfig;
        this.jruby = jruby;
    }

    @Override
    public void transaction(ConfigSource config, Schema outputSchema, final int inputTaskCount,
            ExecutorPlugin.Control control)
    {
        final MapReduceExecutorTask task = config.loadConfig(MapReduceExecutorTask.class);
        task.setExecConfig(config);

        final int outputTaskCount;
        final int reduceTaskCount;

        if (task.getPartitioning().isPresent()) {
            reduceTaskCount = task.getReducers().or(inputTaskCount);
            if (reduceTaskCount <= 0) {
                throw new ConfigException("Reducers must be larger than 1 if partition: is set");
            }
            outputTaskCount = reduceTaskCount;
            ConfigSource partitioningConfig = task.getPartitioning().get();
            String partitioningType = partitioningConfig.get(String.class, "type");
            Partitioning partitioning = newPartitioning(partitioningType);
            TaskSource partitioningTask = partitioning.configure(partitioningConfig, outputSchema, reduceTaskCount);
            task.setPartitioningType(Optional.of(partitioningType));
            task.setPartitioningTask(Optional.of(partitioningTask));
        } else {
            reduceTaskCount = 0;
            outputTaskCount = inputTaskCount;
            task.setPartitioningType(Optional.<String>absent());
            task.setPartitioningTask(Optional.<TaskSource>absent());
        }

        control.transaction(outputSchema, outputTaskCount, new ExecutorPlugin.Executor() {
            public void execute(ProcessTask procTask, ProcessState state)
            {
                task.setProcessTask(procTask);

                // hadoop uses ServiceLoader using context classloader to load some implementations
                try (SetContextClassLoader closeLater = new SetContextClassLoader(MapReduceExecutor.class.getClassLoader())) {
                    run(task, inputTaskCount, reduceTaskCount, state);
                }
            }
        });
    }

    static Partitioning newPartitioning(String type)
    {
        switch (type) {
        case "timestamp":
            return new TimestampPartitioning();
        default:
            throw new ConfigException("Unknown partition type '"+type+"'");
        }
    }

    void run(MapReduceExecutorTask task,
            int mapTaskCount, int reduceTaskCount, ProcessState state)
    {
        ModelManager modelManager = task.getModelManager();

        Configuration conf = new Configuration();
        for (String path : task.getConfigFiles()) {
            conf.addResource(path);
        }

        String uniqueTransactionName = getTransactionUniqueName(Exec.session());
        Path stateDir = new Path(new Path(task.getStatePath()), uniqueTransactionName);

        Job job;
        try {
            //job = Job.getInstance(conf);
            job = Job.getInstance();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        job.setJobName(task.getJobName());

        // extra config
        for (Map.Entry<String, String> pair : task.getConfig().entrySet()) {
            job.getConfiguration().set(pair.getKey(), pair.getValue());
        }

        // framework config
        EmbulkMapReduce.setSystemConfig(job.getConfiguration(), modelManager, systemConfig);
        EmbulkMapReduce.setExecutorTask(job.getConfiguration(), modelManager, task);
        EmbulkMapReduce.setMapTaskCount(job.getConfiguration(), mapTaskCount);  // used by EmbulkInputFormat
        EmbulkMapReduce.setStateDirectoryPath(job.getConfiguration(), stateDir);

        // create state dir
        try {
            stateDir.getFileSystem(job.getConfiguration()).mkdirs(stateDir);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // archive plugins
        PluginArchive archive = new PluginArchive.Builder()
            .addLoadedRubyGems(jruby)
            .build();
        try {
            EmbulkMapReduce.writePluginArchive(job.getConfiguration(), stateDir, archive, modelManager);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // jar files
        Iterable<String> jars = collectJars(task.getLibjars());
        job.getConfiguration().set("tmpjars", StringUtils.join(",", jars));

        job.setInputFormatClass(EmbulkInputFormat.class);

        if (reduceTaskCount > 0) {
            job.setMapperClass(EmbulkPartitioningMapReduce.EmbulkPartitioningMapper.class);
            job.setMapOutputKeyClass(BufferWritable.class);
            job.setMapOutputValueClass(PageWritable.class);

            job.setReducerClass(EmbulkPartitioningMapReduce.EmbulkPartitioningReducer.class);

            job.setNumReduceTasks(reduceTaskCount);

        } else {
            job.setMapperClass(EmbulkMapReduce.EmbulkMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setReducerClass(EmbulkMapReduce.EmbulkReducer.class);

            job.setNumReduceTasks(0);
        }

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        try {
            job.submit();

            int interval = Job.getCompletionPollInterval(job.getConfiguration());
            while (!job.isComplete()) {
                //if (job.getState() == JobStatus.State.PREP) {
                //    continue;
                //}
                log.info(String.format("map %.1f%% reduce %.1f%%",
                            job.mapProgress() * 100, job.reduceProgress() * 100));
                Thread.sleep(interval);

                updateProcessState(job, mapTaskCount, stateDir, state, modelManager);
            }

            log.info(String.format("map %.1f%% reduce %.1f%%",
                        job.mapProgress() * 100, job.reduceProgress() * 100));
            updateProcessState(job, mapTaskCount, stateDir, state, modelManager);

            Counters counters = job.getCounters();
            if (counters != null) {
                log.info(counters.toString());
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Iterable<String> collectJars(List<String> extraJars)
    {
        Set<String> set = new HashSet<String>();

        ClassLoader cl1 = Exec.class.getClassLoader();
        if (cl1 instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader) cl1).getURLs()) {
                if (new File(url.getPath()).isFile()) {
                    // TODO log if not found
                    // TODO debug logging
                    set.add(url.getPath());
                }
            }
        }

        ClassLoader cl2 = MapReduceExecutor.class.getClassLoader();
        if (cl2 instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader) cl2).getURLs()) {
                if (new File(url.getPath()).isFile()) {
                    // TODO log if not found
                    // TODO debug logging
                    set.add(url.getPath());
                }
            }
        }

        set.addAll(extraJars);

        return set;
    }

    private static String getTransactionUniqueName(ExecSession session)
    {
        // TODO implement Exec.getTransactionUniqueName()
        Timestamp time = session.getTransactionTime();
        return DateTimeFormat.forPattern("yyyyMMdd_HHmmss_").withZoneUTC()
            .print(time.getEpochSecond() * 1000)
            + String.format("%09d", time.getNano());
    }

    private void updateProcessState(Job job, int mapTaskCount, Path stateDir,
            ProcessState state, ModelManager modelManager) throws IOException
    {
        List<AttemptReport> reports = getAttemptReports(job.getConfiguration(), stateDir, modelManager);

        for (AttemptReport report : reports) {
            if (report == null) {
                continue;
            }
            if (!report.isStarted()) {
                continue;
            }
            AttemptState attempt = report.getAttemptState();
            if (attempt.getInputTaskIndex().isPresent()) {
                updateState(state.getInputTaskState(attempt.getInputTaskIndex().get()), attempt, true);
            }
            if (attempt.getOutputTaskIndex().isPresent()) {
                updateState(state.getOutputTaskState(attempt.getOutputTaskIndex().get()), attempt, false);
            }
        }
    }

    private static void updateState(TaskState state, AttemptState attempt, boolean isInput)
    {
        state.start();
        if (attempt.getException().isPresent()) {
            if (!state.isCommitted()) {
                state.setException(new RemoteTaskFailedException(attempt.getException().get()));
            }
        } else if (
                (isInput && attempt.getInputCommitReport().isPresent()) ||
                (!isInput && attempt.getOutputCommitReport().isPresent())) {
            state.resetException();
        }
        if (isInput && attempt.getInputCommitReport().isPresent()) {
            state.setCommitReport(attempt.getInputCommitReport().get());
            state.finish();
        }
        if (!isInput && attempt.getOutputCommitReport().isPresent()) {
            state.setCommitReport(attempt.getOutputCommitReport().get());
            state.finish();
        }
    }

    private static class AttemptReport
    {
        private final TaskAttemptID attemptId;
        private final AttemptState attemptState;

        public AttemptReport(TaskAttemptID attemptId)
        {
            this(attemptId, null);
        }

        public AttemptReport(TaskAttemptID attemptId, AttemptState attemptState)
        {
            this.attemptId = attemptId;
            this.attemptState = attemptState;
        }

        public boolean isStarted()
        {
            return attemptState != null;
        }

        public boolean isFinished()
        {
            return true;  // TODO true if attempt is done
        }

        public boolean isFailed()
        {
            return isStarted() && attemptState.getException().isPresent();
        }

        public boolean isInputCommitted()
        {
            return attemptState != null && attemptState.getInputCommitReport().isPresent();
        }

        public boolean isOutputCommitted()
        {
            return attemptState != null && attemptState.getOutputCommitReport().isPresent();
        }

        public AttemptState getAttemptState()
        {
            return attemptState;
        }
    }

    private static final int TASK_EVENT_FETCH_SIZE = 100;

    private static List<AttemptReport> getAttemptReports(Configuration config,
            Path stateDir, ModelManager modelManager) throws IOException
    {
        ImmutableList.Builder<AttemptReport> builder = ImmutableList.builder();
        for (TaskAttemptID aid : EmbulkMapReduce.listAttempts(config, stateDir)) {
            try {
                AttemptState state = EmbulkMapReduce.readAttemptStateFile(config,
                        stateDir, aid, modelManager);
                builder.add(new AttemptReport(aid, state));
            } catch (EOFException ex) {  // plus Not Found exception
                builder.add(new AttemptReport(aid, null));
            }
        }
        return builder.build();
    }
}
