package org.embulk.executor.mapreduce;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.util.Modules;
import org.embulk.EmbulkEmbed;
import org.embulk.RandomManager;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.UserDataExceptions;
import org.embulk.exec.PartialExecutionException;
import org.embulk.spi.ExecutorPlugin;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.impl.Log4jLoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.embulk.plugin.InjectedPluginSource.registerPluginTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// this tests use Hadoop's standalone mode
public class TestMapReduceExecutor
{
    private EmbulkEmbed embulk;
    private Random random = new RandomManager(System.currentTimeMillis()).getRandom();

    @Before
    public void createResources()
    {
        EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();

        ConfigSource systemConfig = bootstrap.getSystemConfigLoader().newConfigSource();

        if (random.nextBoolean()) {
            systemConfig.set("embulk_factory_class", MapReduceEmbulkFactory.class.getName());
        } else {
            systemConfig.set("embulk_factory_class", MapReduceEmbulkFactory2.class.getName());
        }

        bootstrap.setSystemConfig(systemConfig);
        bootstrap.overrideModules(getModuleOverrides(systemConfig));
        embulk = bootstrap.initialize();
    }

    @Test
    public void testEmbulkMapper()
            throws Exception
    {
        ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_config.yml");
        embulk.run(config);
        assertFileContent(
                Lists.newArrayList(
                        "fixtures/csv/sample1.csv",
                        "fixtures/csv/sample1.csv"),
                Lists.newArrayList(
                        "fixtures/csv/embulk_mapred_output.000.00.csv",
                        "fixtures/csv/embulk_mapred_output.001.00.csv"));
    }

    @Test
    public void testEmbulkPartitioningMapperReducer()
            throws Exception
    {
        ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_partitioning_config.yml");
        embulk.run(config);
        assertFileContent(
                Lists.newArrayList(
                        "fixtures/csv/sample1.csv",
                        "fixtures/csv/sample1.csv"),
                Lists.newArrayList(
                        "fixtures/csv/embulk_mapred_partitioning_output.000.00.csv",
                        "fixtures/csv/embulk_mapred_partitioning_output.001.00.csv"));
    }

    @Test
    public void testInvalidConfigFiles()
            throws Exception
    {
        try {
            ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_invalid_config_files_config.yml");
            embulk.run(config);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof PartialExecutionException);
            assertTrue(t.getCause() instanceof ConfigException);
        }
    }

    @Test
    public void testInvalidPartitioning()
            throws Exception
    {
        try {
            ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_invalid_partitioning_config.yml");
            embulk.run(config);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof PartialExecutionException);
            assertTrue(t.getCause() instanceof ConfigException);
        }
    }

    @Test
    public void testInvalidReducers()
            throws Exception
    {
        try {
            ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_invalid_reducers_config.yml");
            embulk.run(config);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof PartialExecutionException);
            assertTrue(t.getCause() instanceof ConfigException);
        }
    }

    @Test
    public void testInvalidLibjars()
            throws Exception
    {
        try {
            ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_invalid_libjars_config.yml");
            embulk.run(config);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof PartialExecutionException);
            assertTrue(t.getCause() instanceof RuntimeException);
            assertTrue(t.getCause().getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void testStopOnInvalidRecord()
            throws Exception
    {
        try {
            ConfigSource config = loadConfigSource(embulk.newConfigLoader(), "config/embulk_mapred_stop_on_invalid_record_config.yml");
            embulk.run(config);
            fail();
        }
        catch (Throwable t) {
            t.printStackTrace();
            assertTrue(t instanceof PartialExecutionException);
            assertTrue(UserDataExceptions.isUserDataException(t.getCause()));
        }
    }

    private static ConfigSource loadConfigSource(ConfigLoader configLoader, String yamlFile)
            throws IOException
    {
        return configLoader.fromYaml(TestMapReduceExecutor.class.getClassLoader().getResourceAsStream(yamlFile));
    }

    private static Function<List<Module>, List<Module>> getModuleOverrides(final ConfigSource systemConfig)
    {
        return new Function<List<Module>, List<Module>>()
        {
            public List<Module> apply(List<Module> modules)
            {
                return overrideModules(modules, systemConfig);
            }
        };
    }

    private static List<Module> overrideModules(List<Module> modules, ConfigSource systemConfig)
    {
        return ImmutableList.of(Modules.override(Iterables.concat(modules, getAdditionalModules(systemConfig)))
                .with(getOverrideModules(systemConfig)));
    }

    private static List<Module> getAdditionalModules(ConfigSource systemConfig)
    {
        return ImmutableList.<Module>of(new ExecutorPluginApplyModule());
    }

    private static List<Module> getOverrideModules(ConfigSource systemConfig)
    {
        return ImmutableList.<Module>of(new LoggerOverrideModule());
    }

    static class ExecutorPluginApplyModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            registerPluginTo(binder, ExecutorPlugin.class, "mapreduce", MapReduceExecutor.class);
        }
    }

    static class LoggerOverrideModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(ILoggerFactory.class).toProvider(new Provider<ILoggerFactory>()
            {
                @Override
                public ILoggerFactory get()
                {
                    return new Log4jLoggerFactory(); // YARN has used log4j.
                }
            });
        }
    }

    public static class MapReduceEmbulkFactory
    {
        public EmbulkEmbed.Bootstrap bootstrap(final ConfigSource systemConfig)
        {
            EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
            bootstrap.setSystemConfig(systemConfig);

            // add modules
            //bootstrap.addModules(ImmutableList.<Module>of());

            // override modules
            bootstrap.overrideModules(new Function<List<Module>, List<Module>>()
            {
                public List<Module> apply(List<Module> modules)
                {
                    return ImmutableList.of(Modules.override(modules).with(new LoggerOverrideModule()));
                }
            });

            return bootstrap;
        }
    }

    public static class MapReduceEmbulkFactory2
    {
        public EmbulkEmbed.Bootstrap bootstrap(final ConfigSource systemConfig, final ConfigSource executorParams)
        {
            EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
            bootstrap.setSystemConfig(systemConfig);

            // add modules
            //bootstrap.addModules(ImmutableList.<Module>of());

            // override modules
            bootstrap.overrideModules(new Function<List<Module>, List<Module>>()
            {
                public List<Module> apply(List<Module> modules)
                {
                    return ImmutableList.of(Modules.override(modules).with(new LoggerOverrideModule()));
                }
            });

            return bootstrap;
        }
    }

    private static void assertFileContent(List<String> inputFiles, List<String> outputFiles)
    {
        List<List<String>> inputRecords = getRecords(inputFiles);
        Collections.sort(inputRecords, new RecordComparator());

        List<List<String>> outputRecords = getRecords(outputFiles);
        Collections.sort(outputRecords, new RecordComparator());

        assertEquals(inputRecords, outputRecords);
    }

    private static class RecordComparator
            implements Comparator<List<String>>
    {
        @Override
        public int compare(List<String> r1, List<String> r2)
        {
            return r1.get(0).compareTo(r2.get(0));
        }
    }

    private static List<List<String>> getRecords(List<String> files)
    {
        List<List<String>> records = new ArrayList<>();

        try {
            for (String file : files) {
                try (BufferedReader r = newReader(file)) {
                    r.readLine(); // header
                    records.addAll(getRecords(r)); // contents
                }
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return records;
    }

    private static List<List<String>> getRecords(BufferedReader reader)
            throws IOException
    {
        List<List<String>> records = new ArrayList<>();

        String line;
        while (!Strings.isNullOrEmpty(line = reader.readLine())) {
            String[] record = line.split(",");
            records.add(Lists.newArrayList(record));
        }

        return records;
    }

    private static BufferedReader newReader(String filePath)
    {
        InputStream in = new BufferedInputStream(TestMapReduceExecutor.class.getClassLoader().getResourceAsStream(filePath));
        return new BufferedReader(new InputStreamReader(in));
    }
}
