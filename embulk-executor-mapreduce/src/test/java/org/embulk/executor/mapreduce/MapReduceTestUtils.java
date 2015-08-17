package org.embulk.executor.mapreduce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.DataSourceImpl;
import org.embulk.config.ModelManager;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.Page;
import org.embulk.spi.ProcessTask;
import org.jruby.embed.ScriptingContainer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapReduceTestUtils
{
    private EmbulkTestRuntime runtime;

    public MapReduceTestUtils(EmbulkTestRuntime runtime)
    {
        this.runtime = runtime;
    }

    public ConfigSource systemConfig()
    {
        ModelManager bootstrapModelManager = new ModelManager(null, new ObjectMapper());
        return new DataSourceImpl(bootstrapModelManager);
    }

    public ConfigLoader configLoader()
    {
        return new ConfigLoader(runtime.getModelManager());
    }

    public ConfigSource config(ConfigLoader loader, String yamlFilePath)
            throws IOException
    {
        return loader.fromYamlFile(new File(yamlFilePath));
    }

    public ProcessTask processTask(ConfigLoader loader, String yamlFilePath)
            throws IOException
    {
        ConfigSource taskSource = loader.fromYamlFile(new File(yamlFilePath));
        return taskSource.loadConfig(ProcessTask.class);
    }

    public MapReduceExecutorTask executorTask(ConfigSource config, ConfigLoader loader, String processTaskFilePath)
            throws IOException
    {
        MapReduceExecutorTask task = config.loadConfig(MapReduceExecutorTask.class);
        task.setExecConfig(config);

        task.setPartitioningType(Optional.<String>absent());
        task.setPartitioningTask(Optional.<TaskSource>absent());
        task.setProcessTask(processTask(loader, processTaskFilePath));
        return task;
    }

    public Configuration configuration(ConfigSource systemConfig, MapReduceExecutorTask task)
    {
        Configuration conf = new Configuration();

        Path stateDir = new Path(task.getStatePath()); // TODO
        int mapTaskCount = 1;

        EmbulkMapReduce.setSystemConfig(conf, runtime.getModelManager(), systemConfig);
        EmbulkMapReduce.setExecutorTask(conf, runtime.getModelManager(), task);
        EmbulkMapReduce.setMapTaskCount(conf, mapTaskCount);
        EmbulkMapReduce.setStateDirectoryPath(conf, stateDir);

        // archive plugins (also create state dir)
        PluginArchive archive = new PluginArchive.Builder()
                .addLoadedRubyGems(runtime.getInstance(ScriptingContainer.class))
                .build();
        try {
            EmbulkMapReduce.writePluginArchive(conf, stateDir, archive, runtime.getModelManager());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        return conf;
    }

    static class MockRecordWriter
            extends RecordWriter<BufferWritable, PageWritable>
    {
        List<Buffer> keys = new ArrayList<>();
        List<Page> values = new ArrayList<>();

        @Override
        public void write(BufferWritable key, PageWritable value)
                throws IOException, InterruptedException
        {
            keys.add(key.get());
            values.add(value.get());
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException
        { }

        public class Pair
        {
            private BufferWritable key = new BufferWritable();
            private PageWritable value = new PageWritable();

            Pair(Buffer k, Page v) {
                key.set(k);
                value.set(v);
            }

            public BufferWritable getBuffer()
            {
                return key;
            }

            public PageWritable getPage()
            {
                return value;
            }
        }

        public Iterator<Pair> iterator() {
            return new Iterator<Pair>() {
                Iterator<Buffer> ks = keys.iterator();
                Iterator<Page> vs = values.iterator();

                @Override
                public boolean hasNext() {
                    return ks.hasNext();
                }

                @Override
                public Pair next() {
                    return new Pair(ks.next(), vs.next());
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException("remove");
                }
            };
        }
    }
}
