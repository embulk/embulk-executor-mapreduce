package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.Collection;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.embulk.exec.ForSystemConfig;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
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

        if (task.getPartitioning().isPresent() && inputTaskCount > 0) { // here can disable partitioning and force set reduceTaskCount and outputTaskCount to 0 if inputTaskCount is 0
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

    private static class TaskReportSet
    {
        private Map<Integer, AttemptReport> inputTaskReports = new HashMap<>();
        private Map<Integer, AttemptReport> outputTaskReports = new HashMap<>();

        private final JobID runningJobId;

        public TaskReportSet(JobID runningJobId)
        {
            this.runningJobId = runningJobId;
        }

        public Collection<AttemptReport> getLatestInputAttemptReports()
        {
            return inputTaskReports.values();
        }

        public Collection<AttemptReport> getLatestOutputAttemptReports()
        {
            return outputTaskReports.values();
        }

        public void update(AttemptReport report)
        {
            if (report.getInputTaskIndex().isPresent()) {
                int taskIndex = report.getInputTaskIndex().get();
                AttemptReport past = inputTaskReports.get(taskIndex);
                if (past == null || checkOverwrite(past, report)) {
                    inputTaskReports.put(taskIndex, report);
                }
            }
            if (report.getOutputTaskIndex().isPresent()) {
                int taskIndex = report.getOutputTaskIndex().get();
                AttemptReport past = outputTaskReports.get(taskIndex);
                if (past == null || checkOverwrite(past, report)) {
                    outputTaskReports.put(taskIndex, report);
                }
            }
        }

        private boolean checkOverwrite(AttemptReport past, AttemptReport report)
        {
            // if already committed successfully, use it
            if (!past.isOutputCommitted() && report.isOutputCommitted()) {
                return true;
            }

            // Here expects that TaskAttemptID.compareTo returns <= 0 if attempt is started later.
            // However, it returns unexpected result if 2 jobs run on different JobTrackers because
            // JobID includes start time of a JobTracker with sequence number in the JobTracker
            // rather than start time of a job. To mitigate this problem, this code assumes that
            // attempts of the running job is always newer.
            boolean pastRunning = past.getTaskAttempId().getJobID().equals(runningJobId);
            boolean reportRunning = report.getTaskAttempId().getJobID().equals(runningJobId);
            if (!pastRunning && reportRunning) {
                return true;
            }
            return past.getTaskAttempId().compareTo(report.getTaskAttempId()) <= 0;
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

        // archive plugins (also create state dir)
        PluginArchive archive = new PluginArchive.Builder()
            .addLoadedRubyGems(jruby)
            .build();
        try {
            EmbulkMapReduce.writePluginArchive(job.getConfiguration(), stateDir, archive, modelManager);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        // jar files
        Iterable<Path> jars = collectJars(task.getLibjars(), task.getExcludedjars());
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
            TaskReportSet reportSet = new TaskReportSet(job.getJobID());

            int interval = Job.getCompletionPollInterval(job.getConfiguration());
            while (true) {
                EmbulkMapReduce.JobStatus status = EmbulkMapReduce.getJobStatus(job);
                if (status.isComplete()) {
                    break;
                }
                log.info(String.format("map %.1f%% reduce %.1f%%",
                            status.getMapProgress() * 100, status.getReduceProgress() * 100));

                //if (job.getState() == JobStatus.State.PREP) {
                //    continue;
                //}
                Thread.sleep(interval);

                updateProcessState(job, reportSet, stateDir, state, modelManager, true);
            }

            EmbulkMapReduce.JobStatus status = EmbulkMapReduce.getJobStatus(job);
            log.info(String.format("map %.1f%% reduce %.1f%%",
                        status.getMapProgress() * 100, status.getReduceProgress() * 100));
            // Here sets inProgress=false to updateProcessState method to tell that race
            // condition of AttemptReport.readFrom and .writeTo does not happen here.
            updateProcessState(job, reportSet, stateDir, state, modelManager, false);

            Counters counters = EmbulkMapReduce.getJobCounters(job);
            if (counters != null) {
                log.info(counters.toString());
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private static Iterable<Path> collectJars(List<String> extraJars, List<String> excludedJars)
    {
        Set<Path> set = new HashSet<Path>();

        collectURLClassLoaderJars(set, Exec.class.getClassLoader(), excludedJars);
        collectURLClassLoaderJars(set, MapReduceExecutor.class.getClassLoader(), excludedJars);

        for (String extraJar : extraJars) {
            if (isExcludedJar(extraJar, excludedJars)) {
                continue;
            }

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

    private static boolean isExcludedJar(String jarName, List<String> excludedJars)
    {
        for (String excludedJar : excludedJars) {
            if (jarName.endsWith(excludedJar)) {
                return true;
            }
        }
        return false;
    }

    private static void collectURLClassLoaderJars(Set<Path> set, ClassLoader cl, List<String> excludedJars)
    {
        if (cl instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader) cl).getURLs()) {
                File file = new File(url.getPath());
                if (file.isFile() && !isExcludedJar(file.getName(), excludedJars)) {
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

    private void updateProcessState(Job job, TaskReportSet reportSet, Path stateDir,
            ProcessState state, ModelManager modelManager, boolean inProgress) throws IOException
    {
        List<AttemptReport> reports = getAttemptReports(job.getConfiguration(), stateDir, modelManager,
                inProgress, job.getJobID());

        for (AttemptReport report : reports) {
            if (report.isAvailable()) {
                reportSet.update(report);
            }
        }

        for (AttemptReport report : reportSet.getLatestInputAttemptReports()) {
            updateTaskState(state.getInputTaskState(report.getInputTaskIndex().get()), report.getAttemptState(), true);
        }

        for (AttemptReport report : reportSet.getLatestOutputAttemptReports()) {
            updateTaskState(state.getOutputTaskState(report.getOutputTaskIndex().get()), report.getAttemptState(), false);
        }
    }

    private static void updateTaskState(TaskState state, AttemptState attempt, boolean isInput)
    {
        state.start();
        Optional<TaskReport> taskReport = isInput ? attempt.getInputTaskReport() : attempt.getOutputTaskReport();
        boolean committed = taskReport.isPresent();
        if (attempt.getException().isPresent()) {
            if (!state.isCommitted()) {
                state.setException(new RemoteTaskFailedException(attempt.getException().get()));
            }
        }
        if (taskReport.isPresent()) {
            state.setTaskReport(taskReport.get());
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

        public Optional<Integer> getInputTaskIndex()
        {
            return attemptState == null ? Optional.<Integer>absent() : attemptState.getInputTaskIndex();
        }

        public Optional<Integer> getOutputTaskIndex()
        {
            return attemptState == null ? Optional.<Integer>absent() : attemptState.getOutputTaskIndex();
        }

        public boolean isInputCommitted()
        {
            return attemptState != null && attemptState.getInputTaskReport().isPresent();
        }

        public boolean isOutputCommitted()
        {
            return attemptState != null && attemptState.getOutputTaskReport().isPresent();
        }

        public TaskAttemptID getTaskAttempId()
        {
            return attemptId;
        }

        public AttemptState getAttemptState()
        {
            return attemptState;
        }
    }

    private static List<AttemptReport> getAttemptReports(Configuration config,
            Path stateDir, ModelManager modelManager,
            boolean jobIsRunning, JobID runningJobId) throws IOException
    {
        ImmutableList.Builder<AttemptReport> builder = ImmutableList.builder();
        for (TaskAttemptID aid : EmbulkMapReduce.listAttempts(config, stateDir)) {
            boolean concurrentWriteIsPossible = aid.getJobID().equals(runningJobId) && jobIsRunning;
            try {
                AttemptState state = EmbulkMapReduce.readAttemptStateFile(config,
                        stateDir, aid, modelManager, concurrentWriteIsPossible);
                builder.add(new AttemptReport(aid, state));
            } catch (IOException ex) {
                // See comments on readAttemptStateFile for the possible error causes.
                if (!concurrentWriteIsPossible) {
                    if (!(ex instanceof EOFException)) {
                        // f) HDFS is broken. This is critical problem which should throw an exception
                        throw new RuntimeException(ex);
                    }
                    // HDFS is working but file is corrupted. It is always possible that the directly
                    // contains corrupted file created by past attempts of retried task or job. Ignore it.
                }
                // if concurrentWriteIsPossible, there're no ways to tell the cause. Ignore it.
                builder.add(new AttemptReport(aid, ex));
            }
        }
        return builder.build();
    }
}
