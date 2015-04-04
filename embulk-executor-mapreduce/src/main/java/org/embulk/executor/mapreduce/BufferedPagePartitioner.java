package org.embulk.executor.mapreduce;

public class BufferedPagePartitioner
{
    private final BufferAllocator bufferAllocator;
    private final Schema schema;
    private final Partitioner partitioner;
    private final int maxPageBufferCount;
    private final MapperOutputPlugin.Output output;

    private final Map<PartitionKey, PageBuilder> hash = new HashMap<Partitioner, PageBuilder>();

    public BufferedPagePartitioner(BufferAllocator bufferAllocator, Schema schema,
            Partitioner partitioner, int maxPageBufferCount, MapperOutputPlugin.Output output)
    {
        this.bufferAllocator = bufferAllocator;
        this.schema = schema;
        this.partitioner = partitioner;
        this.maxPageBufferCount = maxPageBufferCount;
        this.output = output;
    }

    public void add(PageReader record)
    {
        PartitionKey searchKey = partitioner.updateKey(record);
        PageBuilder builder = hash.get(searchKey);
        if (builder != null) {
            builder.add(record);
        } else {
            if (hash.size() >= maxPageBufferCount) {
                try (PageBuilder builder = removeMostUnsed(hash)) {
                    builder.finish();
                }
            }
            final PartitionKey key = searchKey.clone();
            hash.put(key, new PageBuilder(bufferAllocator, schema, new PageOutput() {
                public void add(Page page)
                {
                    output.add(key, page);
                }

                public void finish()
                { }

                public void close()
                { }
            }));
        }
    }

    private PageBuilder removeMostUnsed(Map<PartitionKey, PageBuilder> hash)
    {
        // TODO remove the largest buffer
        Map.Iterator<PartitionKey, PageBuilder> ite = hash.iterator();
        PageBuilder builder = ite.next().getValue();
        ite.remove();
        return builder;
    }

    public void finish()
    {
        for (PageBuilder builder : hash.values()) {
            builder.finish();
        }
        output.finish();
    }

    public void close()
    {
        Map.Iterator<PartitionKey, PageBuilder> ite = hash.iterator();
        while (ite.hasNext()) {
            PageBuilder builder = ite.next().getValue();
            builder.close();
            ite.remove();
        }
        output.close();
    }
}
