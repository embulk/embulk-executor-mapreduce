package org.embulk.executor.mapreduce;

public class MapperOutputPlugin
        implements OutputPlugin
{
    private static interface Output
    {
        public void add(PartitionKey key, Page value);

        public void finish();

        public void close();
    }

    private final BufferAllocator bufferAllocator;
    private final Partitioner partitioner;
    private final int maxPageBufferCount;
    private final Output output;

    public MapperOutputPlugin(BufferAllocator bufferAllocator,
            Partitioner partitioner, int maxPageBufferCount,
            Output output)
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
