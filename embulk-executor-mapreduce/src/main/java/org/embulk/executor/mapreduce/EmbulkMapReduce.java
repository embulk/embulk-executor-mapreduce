package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.io.File;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InterruptedIOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import com.google.inject.Injector;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jruby.embed.ScriptingContainer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.MRConfig;
import org.embulk.config.ModelManager;
import org.embulk.config.ConfigSource;
import org.embulk.config.ConfigLoader;
import org.embulk.config.DataSourceImpl;
import org.embulk.config.TaskReport;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecAction;
import org.embulk.spi.ExecSession;
import org.embulk.spi.ProcessTask;
import org.embulk.spi.util.Executors;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.EmbulkEmbed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.xml.sax.InputSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public class EmbulkMapReduce
{
    private static final Logger systemLogger = LoggerFactory.getLogger(EmbulkMapReduce.class);

    private static final String EMBULK_FACTORY_CLASS = "embulk_factory_class";
    private static final String SYSTEM_CONFIG_SERVICE_CLASS = "mapreduce_service_class";

    private static final String CK_SYSTEM_CONFIG = "embulk.mapreduce.systemConfig";
    private static final String CK_STATE_DIRECTORY_PATH = "embulk.mapreduce.stateDirectorypath";
    private static final String CK_TASK_COUNT = "embulk.mapreduce.taskCount";
    private static final String CK_RETRY_TASKS = "embulk.mapreduce.retryTasks";
    private static final String CK_TASK = "embulk.mapreduce.task";
    private static final String CK_PLUGIN_ARCHIVE_SPECS = "embulk.mapreduce.pluginArchive.specs";

    private static final String PLUGIN_ARCHIVE_FILE_NAME = "gems.zip";

    public static void setSystemConfig(Configuration config, ModelManager modelManager, ConfigSource systemConfig)
    {
        config.set(CK_SYSTEM_CONFIG, modelManager.writeObject(systemConfig));
    }

    public static ConfigSource getSystemConfig(Configuration config)
    {
        try {
            ModelManager bootstrapModelManager = new ModelManager(null, new ObjectMapper());
            String json = config.get(CK_SYSTEM_CONFIG);
            try (InputStream in = new ByteArrayInputStream(json.getBytes(UTF_8))) {
                return new ConfigLoader(bootstrapModelManager).fromJson(in);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void setMapTaskCount(Configuration config, int taskCount)
    {
        config.setInt(CK_TASK_COUNT, taskCount);
    }

    public static int getMapTaskCount(Configuration config)
    {
        return config.getInt(CK_TASK_COUNT, 0);
    }

    public static void setRetryTasks(Configuration config, boolean enabled)
    {
        config.setBoolean(CK_RETRY_TASKS, enabled);
    }

    public static boolean getRetryTasks(Configuration config)
    {
        return config.getBoolean(CK_RETRY_TASKS, false);
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

    public static EmbulkEmbed.Bootstrap newEmbulkBootstrap(Configuration config)
    {
        ConfigSource systemConfig = getSystemConfig(config);

        // for warnings of old versions
        if (!systemConfig.get(String.class, SYSTEM_CONFIG_SERVICE_CLASS, "org.embulk.EmbulkService").equals("org.embulk.EmbulkService")) {
            throw new RuntimeException("System config 'mapreduce_service_class' is not supported any more. Please use 'embulk_factory_class' instead");
        }

        String factoryClassName = systemConfig.get(String.class, EMBULK_FACTORY_CLASS, DefaultEmbulkFactory.class.getName());

        try {
            Class<?> factoryClass = Class.forName(factoryClassName);
            Object factory = factoryClass.newInstance();

            Object bootstrap;
            try {
                // factory.bootstrap(ConfigSource masterSystemConfig, ConfigSource executorParams)
                Method method = factoryClass.getMethod("bootstrap", ConfigSource.class, ConfigSource.class);
                Map<String, String> hadoopConfig = config.getValByRegex("");
                ConfigSource executorParams = new DataSourceImpl(new ModelManager(null, new ObjectMapper())).set("hadoopConfig", hadoopConfig).getNested("hadoopConfig");  // TODO add a method to embulk that creates an empty DataSource instance
                bootstrap = method.invoke(factory, systemConfig, executorParams);
            }
            catch (NoSuchMethodException ex) {
                // factory.bootstrap(ConfigSource masterSystemConfig)
                bootstrap = factoryClass.getMethod("bootstrap", ConfigSource.class).invoke(factory, systemConfig);
            }

            return (EmbulkEmbed.Bootstrap) bootstrap;

        }
        catch (InvocationTargetException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | IllegalArgumentException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public static class JobStatus
    {
        private final boolean completed;
        private final float mapProgress;
        private final float reduceProgress;

        public JobStatus(boolean completed, float mapProgress, float reduceProgress)
        {
            this.completed = completed;
            this.mapProgress = mapProgress;
            this.reduceProgress = reduceProgress;
        }

        public boolean isComplete()
        {
            return completed;
        }

        public float getMapProgress()
        {
            return mapProgress;
        }

        public float getReduceProgress()
        {
            return reduceProgress;
        }
    }

    public static JobStatus getJobStatus(final Job job) throws IOException
    {
        return hadoopOperationWithRetry("Getting job status", new Callable<JobStatus>() {
            public JobStatus call() throws IOException
            {
                return new JobStatus(job.isComplete(), job.mapProgress(), job.reduceProgress());
            }
        });
    }

    public static Counters getJobCounters(final Job job) throws IOException
    {
        return hadoopOperationWithRetry("Getting job counters", new Callable<Counters>() {
            public Counters call() throws IOException
            {
                return job.getCounters();
            }
        });
    }

    static Path attemptsDirectory(Path stateDir)
    {
        return new Path(stateDir, "attempts");
    }

    static Path attemptStateFilePath(Path stateDir, TaskAttemptID attemptId)
    {
        return new Path(attemptsDirectory(stateDir), attemptId.toString());
    }

    static Path logsDirectory(Path stateDir)
    {
        return new Path(stateDir, "logs");
    }

    static Path logFilePath(Path stateDir, TaskAttemptID attemptId)
    {
        return new Path(logsDirectory(stateDir), attemptId.toString());
    }

    public static List<TaskAttemptID> listAttempts(final Configuration config,
            final Path stateDir) throws IOException
    {
        final Path dir = attemptsDirectory(stateDir);
        return hadoopOperationWithRetry("Getting list of attempt state files on "+dir, new Callable<List<TaskAttemptID>>() {
            public List<TaskAttemptID> call() throws IOException
            {
                FileStatus[] stats = dir.getFileSystem(config).listStatus(dir);
                ImmutableList.Builder<TaskAttemptID> builder = ImmutableList.builder();
                for (FileStatus stat : stats) {
                    if (stat.getPath().getName().startsWith("attempt_") && stat.isFile()) {
                        String name = stat.getPath().getName();
                        TaskAttemptID id;
                        try {
                            id = TaskAttemptID.forName(name);
                        } catch (Exception ex) {
                            // ignore this file
                            continue;
                        }
                        builder.add(id);
                    }
                }
                return builder.build();
            }
        });
    }

    public static Map<TaskAttemptID, FileStatus> listLogFiles(final Configuration config,
            final Path stateDir) throws IOException
    {
        final Path dir = logsDirectory(stateDir);
        return hadoopOperationWithRetry("Getting list of log files on "+dir, new Callable<Map<TaskAttemptID, FileStatus>>() {
            public Map<TaskAttemptID, FileStatus> call() throws IOException
            {
                FileStatus[] stats = dir.getFileSystem(config).listStatus(dir);
                ImmutableMap.Builder<TaskAttemptID, FileStatus> builder = ImmutableMap.builder();
                for (FileStatus stat : stats) {
                    if (stat.getPath().getName().startsWith("attempt_") && stat.isFile()) {
                        String name = stat.getPath().getName();
                        TaskAttemptID id;
                        try {
                            id = TaskAttemptID.forName(name);
                        } catch (Exception ex) {
                            // ignore this file
                            continue;
                        }
                        builder.put(id, stat);
                    }
                }
                return builder.build();
            }
        });
    }

    public static void writePluginArchive(final Configuration config, final Path stateDir,
            final PluginArchive archive, final ModelManager modelManager) throws IOException
    {
        final Path path = new Path(stateDir, PLUGIN_ARCHIVE_FILE_NAME);
        hadoopOperationWithRetry("Writing plugin archive to "+path, new Callable<Void>() {
            public Void call() throws IOException
            {
                try (FSDataOutputStream out = path.getFileSystem(config).create(path, true)) {
                    List<PluginArchive.GemSpec> specs = archive.dump(out);
                    config.set(CK_PLUGIN_ARCHIVE_SPECS, modelManager.writeObject(specs));
                }
                return null;
            }
        });
    }

    public static class GemSpecListType extends ArrayList<PluginArchive.GemSpec>
    { }

    public static PluginArchive readPluginArchive(final File localDirectory, final Configuration config,
            Path stateDir, final ModelManager modelManager) throws IOException
    {
        final Path path = new Path(stateDir, PLUGIN_ARCHIVE_FILE_NAME);
        return hadoopOperationWithRetry("Reading plugin archive file from "+path, new Callable<PluginArchive>() {
                public PluginArchive call() throws IOException
                {
                    List<PluginArchive.GemSpec> specs = modelManager.readObject(
                            GemSpecListType.class,
                            config.get(CK_PLUGIN_ARCHIVE_SPECS));
                    try (FSDataInputStream in = path.getFileSystem(config).open(path)) {
                        return PluginArchive.load(localDirectory, specs, in);
                    }
                }
        });
    }

    public static void writeAttemptStateFile(final Configuration config,
            Path stateDir, final AttemptState state, final ModelManager modelManager) throws IOException
    {
        final Path path = attemptStateFilePath(stateDir, state.getAttemptId());
        hadoopOperationWithRetry("Writing attempt state file to "+path, new Callable<Void>() {
            public Void call() throws IOException
            {
                try (FSDataOutputStream out = path.getFileSystem(config).create(path, true)) {
                    state.writeTo(out, modelManager);
                }
                return null;
            }
        });
    }

    public static AttemptState readAttemptStateFile(final Configuration config,
            Path stateDir, TaskAttemptID attemptId, final ModelManager modelManager,
            final boolean concurrentWriteIsPossible) throws IOException
    {
        final Logger log = Exec.getLogger(EmbulkMapReduce.class);
        final Path path = attemptStateFilePath(stateDir, attemptId);
        try {
            return retryExecutor()
                    .withRetryLimit(5)
                    .withInitialRetryWait(2 * 1000)
                    .withMaxRetryWait(20 * 1000)
                    .runInterruptible(new Retryable<AttemptState>() {
                        @Override
                        public AttemptState call() throws IOException
                        {
                            try (FSDataInputStream in = path.getFileSystem(config).open(path)) {
                                return AttemptState.readFrom(in, modelManager);
                            }
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            // AttemptState.readFrom throws 4 types of exceptions:
                            //
                            //   concurrentWriteIsPossible == true:
                            //      a) EOFException: race between readFrom and writeTo. See comments on AttemptState.readFrom.
                            //      b) EOFException: file exists but its format is invalid because this task is retried and last job/attempt left corrupted files (such as empty, partially written, etc)
                            //      c) IOException "Cannot obtain block length for LocatedBlock": HDFS-1058. See https://github.com/embulk/embulk-executor-mapreduce/pull/3
                            //      d) IOException: FileSystem is not working
                            //   concurrentWriteIsPossible == false:
                            //      e) EOFException: file exists but its format is invalid because this task is retried and last job/attempt left corrupted files (such as empty, partially written, etc)
                            //      f) IOException: FileSystem is not working
                            //
                            if (exception instanceof EOFException) {
                                // a) and b) don't need retrying. See MapReduceExecutor.getAttemptReports that ignores EOFException.
                                // e) is not recoverable.
                                return false;
                            }
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            log.warn("Reading a state file failed. Retrying {}/{} after {} seconds. Message: {}",
                                    retryCount, retryLimit, retryWait, exception.getMessage(),
                                    retryCount % 3 == 0 ? exception : null);
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        { }
                    });
        } catch (RetryGiveupException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
            throw Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    private static <T> T hadoopOperationWithRetry(final String message, final Callable<T> callable) throws IOException
    {
        return hadoopOperationWithRetry(Exec.getLogger(EmbulkMapReduce.class), message, callable);
    }

    private static <T> T hadoopOperationWithRetry(final Logger log,
            final String message, final Callable<T> callable) throws IOException
    {
        try {
            return retryExecutor()
                    .withRetryLimit(5)
                    .withInitialRetryWait(2 * 1000)
                    .withMaxRetryWait(20 * 1000)
                    .runInterruptible(new Retryable<T>() {
                        @Override
                        public T call() throws Exception
                        {
                            return callable.call();
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                throws RetryGiveupException
                        {
                            log.warn("{} failed. Retrying {}/{} after {} seconds. Message: {}",
                                    message, retryCount, retryLimit, retryWait, exception.getMessage(),
                                    retryCount % 3 == 0 ? exception : null);
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException)
                                throws RetryGiveupException
                        { }
                    });
        } catch (RetryGiveupException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
            throw Throwables.propagate(e.getCause());
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    public static class SessionRunner
    {
        private final Configuration config;
        private final TaskAttemptID attemptId;
        private final EmbulkEmbed embed;
        private final MapReduceExecutorTask task;
        private final ExecSession session;
        private final File localGemPath;

        public SessionRunner(TaskAttemptContext context)
        {
            this.config = context.getConfiguration();
            this.attemptId = context.getTaskAttemptID();
            this.embed = newEmbulkBootstrap(context.getConfiguration()).initialize();  // TODO use initializeCloseable?
            this.task = getExecutorTask(embed.getInjector(), context.getConfiguration());
            this.session = ExecSession.builder(embed.getInjector()).fromExecConfig(task.getExecConfig()).build();

            try {
                // LocalDirAllocator allocates a directory for a job. Here adds attempt id to the path
                // so that attempts running on the same machine don't conflict each other.
                LocalDirAllocator localDirAllocator = new LocalDirAllocator(MRConfig.LOCAL_DIR);
                String dirName = attemptId.toString() + "/embulk_gems";
                Path destPath = localDirAllocator.getLocalPathForWrite(dirName, config);
                this.localGemPath = new File(destPath.toString());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public PluginArchive readPluginArchive() throws IOException
        {
            localGemPath.mkdirs();
            return EmbulkMapReduce.readPluginArchive(localGemPath, config, getStateDirectoryPath(config), embed.getModelManager());
        }

        public Configuration getConfiguration()
        {
            return config;
        }

        public ModelManager getModelManager()
        {
            return embed.getModelManager();
        }

        public BufferAllocator getBufferAllocator()
        {
            return embed.getBufferAllocator();
        }

        public ScriptingContainer getScriptingContainer()
        {
            return embed.getInjector().getInstance(ScriptingContainer.class);
        }

        public MapReduceExecutorTask getMapReduceExecutorTask()
        {
            return task;
        }

        public ExecSession getExecSession()
        {
            return session;
        }

        public <T> T execSession(boolean run, ExecAction<T> action) throws IOException, InterruptedException
        {
            try {
                FileSystemAttemptLogAppender logger = openLoggerIfSupported(run);
                if (logger == null) {
                    return Exec.doWith(session, action);
                }
                else {
                    try {
                        ThreadLocalLogger.instance.set(logger);
                        return Exec.doWith(session, action);
                    }
                    finally {
                        ThreadLocalLogger.instance.set(null);
                        logger.close();
                    }
                }
            } catch (ExecutionException e) {
                Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
                throw Throwables.propagate(e.getCause());
            }
        }

        private FileSystemAttemptLogAppender openLoggerIfSupported(boolean run)
            throws IOException
        {
            try {
                Path path = logFilePath(getStateDirectoryPath(config), attemptId);
                return FileSystemAttemptLogAppender.openIfSupported(config, path, run);
            }
            catch (Exception ex) {
                // show also to stderr because logger is likely broken
                systemLogger.error("Failed to hook logger. Task logging is disabled.", ex);
                System.err.println("Failed to hook logger. Task logging is disabled.");
                ex.printStackTrace(System.err);
                return null;
            }
        }

        public void deleteTempFiles()
        {
            // TODO delete localGemPath
        }
    }

    public static class AttemptStateUpdateHandler
            implements Executors.ProcessStateCallback
    {
        private final Configuration config;
        private final Path stateDir;
        private final ModelManager modelManager;
        private final AttemptState state;

        public AttemptStateUpdateHandler(Configuration config, ModelManager modelManager, AttemptState state)
        {
            this.config = config;
            this.stateDir = getStateDirectoryPath(config);
            this.state = state;
            this.modelManager = modelManager;
        }

        @Override
        public void started()
        {
            try {
                writeAttemptStateFile(config, stateDir, state, modelManager);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void inputCommitted(TaskReport report)
        {
            state.setInputTaskReport(report);
            try {
                writeAttemptStateFile(config, stateDir, state, modelManager);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void outputCommitted(TaskReport report)
        {
            state.setOutputTaskReport(report);
            try {
                writeAttemptStateFile(config, stateDir, state, modelManager);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void setException(Throwable ex) throws IOException
        {
            state.setException(ex);
            writeAttemptStateFile(config, stateDir, state, modelManager);
        }
    }

    public static class EmbulkMapper
            extends Mapper<IntWritable, NullWritable, NullWritable, NullWritable>
    {
        private Context context;
        private SessionRunner runner;
        private boolean retryTasks;

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            this.context = context;
            this.runner = new SessionRunner(context);
            this.retryTasks = EmbulkMapReduce.getRetryTasks(context.getConfiguration());

            setupThreadLocalLoggerHook();

            runner.execSession(false, new ExecAction<Void>() {  // for Exec.getLogger
                public Void run() throws IOException
                {
                    runner.readPluginArchive().restoreLoadPathsTo(runner.getScriptingContainer());
                    return null;
                }
            });
        }

        @Override
        public void map(IntWritable key, NullWritable value, final Context context) throws IOException, InterruptedException
        {
            final int taskIndex = key.get();

            runner.execSession(true, new ExecAction<Void>() {
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

            AttemptStateUpdateHandler handler = new AttemptStateUpdateHandler(
                    runner.getConfiguration(), runner.getModelManager(),
                    new AttemptState(context.getTaskAttemptID(), Optional.of(taskIndex), Optional.of(taskIndex)));

            try {
                Executors.process(runner.getExecSession(), task, taskIndex, handler);
            }
            catch (Exception ex) {
                try {
                    handler.setException(ex);
                } catch (Throwable e) {
                    e.addSuppressed(ex);
                    throw e;
                }
                if (retryTasks) {
                    throw ex;
                }
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

    static void setupThreadLocalLoggerHook()
    {
        try {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            context.reset();

            String xml =  "" +
                "<configuration>" +
                  "<appender name=\"embulk-context\" class=\"org.embulk.executor.mapreduce.LogbackThreadLocalLoggerAdapter\">" +
                  "</appender>" +
                  "<root level=\"DEBUG\">" +
                    "<appender-ref ref=\"embulk-context\"/>" +
                  "</root>" +
                "</configuration>";
            configurator.doConfigure(new InputSource(new StringReader(xml)));

            // This actually fails to setup LogbackThreadLocalLoggerAdapter because context.getClass().getClassLoader()
            // can't lookup org.embulk.executor.mapreduce.LogbackThreadLocalLoggerAdapter, plus an unknown reason.
            // But it suceeds to create only one logger. So here overrides it to use
            // LogbackThreadLocalLoggerAdapter programmably.

            LogbackThreadLocalLoggerAdapter adapter = new LogbackThreadLocalLoggerAdapter();
            adapter.setContext(context);
            adapter.start();

            Logger rootLogger = LoggerFactory.getLogger("");
            while (true) {
                Field f = ch.qos.logback.classic.Logger.class.getDeclaredField("parent");
                f.setAccessible(true);
                Logger parent = (Logger) f.get(rootLogger);
                if (parent == null) {
                    break;
                }
                else {
                    rootLogger = parent;
                }
            }

            ((ch.qos.logback.classic.Logger) rootLogger).addAppender(adapter);
        }
        catch (Exception ex) {
            // show also to stderr because logger is likely broken
            systemLogger.error("Failed to hook logger. Task logging is disabled.", ex);
            System.err.println("Failed to hook logger. Task logging is disabled.");
            ex.printStackTrace(System.err);
        }
    }
}
