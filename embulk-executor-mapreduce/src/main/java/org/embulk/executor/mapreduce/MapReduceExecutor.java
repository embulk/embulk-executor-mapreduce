package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.io.File;
import java.io.IOException;
import java.io.EOFException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
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
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.MRJobConfig;
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
        // don't call conf.setQuietMode(false). Configuraiton has invalid resource names by default
        for (String path : task.getConfigFiles()) {
            File file = new File(path);
            if (!file.isFile()) {
                throw new ConfigException(String.format("Config file '%s' does not exist", file));
            }
            try {
                // use URL here. Configuration assumes String is a path of a resource in a ClassLoader
                conf.addResource(file.toURI().toURL());
            } catch (MalformedURLException ex) {
                throw new RuntimeException(ex);
            }
        }

        String uniqueTransactionName = getTransactionUniqueName(Exec.session());
        Path stateDir = new Path(new Path(task.getStatePath()), uniqueTransactionName);

        Job job;
        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        job.setJobName(task.getJobName());

        // create a dedicated classloader for this yarn application.
        // allow task.getConfig to overwrite this parameter
        job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER, "true");  // mapreduce.job.classloader
        job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CLASSLOADER_SYSTEM_CLASSES, "java.,org.apache.hadoop.");  // mapreduce.job.classloader.system.classes

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
        Iterable<Path> jars = collectJars(task.getLibjars());
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

                updateProcessState(job, mapTaskCount, stateDir, state, modelManager, true);
            }

            // Here sets skipUnavailable=false to updateProcessState method because race
            // condition of AttemptReport.readFrom and .writeTo does not happen here.
            log.info(String.format("map %.1f%% reduce %.1f%%",
                        job.mapProgress() * 100, job.reduceProgress() * 100));
            updateProcessState(job, mapTaskCount, stateDir, state, modelManager, false);

            Counters counters = job.getCounters();
            if (counters != null) {
                log.info(counters.toString());
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Iterable<Path> collectJars(List<String> extraJars)
    {
        Set<Path> set = new HashSet<Path>();

        collectURLClassLoaderJars(set, Exec.class.getClassLoader());
        collectURLClassLoaderJars(set, MapReduceExecutor.class.getClassLoader());

        for (String extraJar : extraJars) {
            URI uri;
            try {
                uri = new URI(extraJar);
            } catch (URISyntaxException ex) {
                throw new ConfigException(String.format("Invalid jar path '%s'", extraJar), ex);
            }
            if (uri.getScheme() == null) {
                set.add(localFileToLocalPath(new File(extraJar)));
            } else {
                set.add(new Path(uri));
            }
        }

        return set;
    }

    private static void collectURLClassLoaderJars(Set<Path> set, ClassLoader cl)
    {
        if (cl instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader) cl).getURLs()) {
                File file = new File(url.getPath());
                if (file.isFile()) {
                    // TODO log if not found
                    // TODO debug logging
                    set.add(localFileToLocalPath(file));
                }
            }
        }
    }

    private static Path localFileToLocalPath(File file)
    {
        Path cwd = new Path(java.nio.file.Paths.get("").toAbsolutePath().toString()).makeQualified(FsConstants.LOCAL_FS_URI, new Path("/"));
        return new Path(file.toString()).makeQualified(FsConstants.LOCAL_FS_URI, cwd);
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
            ProcessState state, ModelManager modelManager, boolean skipUnavailable) throws IOException
    {
        List<AttemptReport> reports = getAttemptReports(job.getConfiguration(), stateDir, modelManager);

        for (AttemptReport report : reports) {
            if (report == null) {
                continue;
            }
            if (!report.isAvailable()) {
                if (skipUnavailable) {
                    continue;
                } else {
                    throw report.getUnavailableException();
                }
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
        private final IOException unavailableException;

        public AttemptReport(TaskAttemptID attemptId, AttemptState attemptState)
        {
            this.attemptId = attemptId;
            this.attemptState = attemptState;
            this.unavailableException = null;
        }

        public AttemptReport(TaskAttemptID attemptId, IOException unavailableException)
        {
            this.attemptId = attemptId;
            this.attemptState = null;
            this.unavailableException = unavailableException;
        }

        public boolean isAvailable()
        {
            return attemptState != null;
        }

        public IOException getUnavailableException()
        {
            return unavailableException;
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
            } catch (IOException ex) {
                // Either of:
                //   * race condition of AttemptReport.writeTo and .readFrom
                //   * FileSystem is not working
                // See also comments on MapReduceExecutor.readAttemptStateFile.isRetryableException.
                builder.add(new AttemptReport(aid, ex));
            }
        }
        return builder.build();
    }
}
