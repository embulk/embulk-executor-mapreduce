package org.embulk.executor.mapreduce;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.BufferAllocator;

public class BufferedPagePartitioner
{
    public static interface PartitionedPageOutput
    {
        public void add(PartitionKey key, Page value);

        public void finish();

        public void close();
    }

    private static class ForwardRecordColumnVisitor
            implements ColumnVisitor
    {
        private final PageReader source;
        private final PageBuilder destination;

        public ForwardRecordColumnVisitor(PageReader source, PageBuilder destination)
        {
            this.source = source;
            this.destination = destination;
        }

        public void booleanColumn(Column column)
        {
            if (source.isNull(column)) {
                destination.setNull(column);
            } else {
                destination.setBoolean(column, source.getBoolean(column));
            }
        }

        public void longColumn(Column column)
        {
            if (source.isNull(column)) {
                destination.setNull(column);
            } else {
                destination.setLong(column, source.getLong(column));
            }
        }

        public void doubleColumn(Column column)
        {
            if (source.isNull(column)) {
                destination.setNull(column);
            } else {
                destination.setDouble(column, source.getDouble(column));
            }
        }

        public void stringColumn(Column column)
        {
            if (source.isNull(column)) {
                destination.setNull(column);
            } else {
                destination.setString(column, source.getString(column));
            }
        }

        public void timestampColumn(Column column)
        {
            if (source.isNull(column)) {
                destination.setNull(column);
            } else {
                destination.setTimestamp(column, source.getTimestamp(column));
            }
        }
    }

    private final BufferAllocator bufferAllocator;
    private final Schema schema;
    private final Partitioner partitioner;
    private final int maxPageBufferCount;
    private final PartitionedPageOutput output;

    private final Map<PartitionKey, PageBuilder> hash = new HashMap<PartitionKey, PageBuilder>();

    public BufferedPagePartitioner(BufferAllocator bufferAllocator, Schema schema,
            Partitioner partitioner, int maxPageBufferCount, PartitionedPageOutput output)
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
        if (builder == null) {
            if (hash.size() >= maxPageBufferCount) {
                try (PageBuilder b = removeMostUnsed(hash)) {
                    b.finish();
                }
            }
            final PartitionKey key = searchKey.clone();
            builder = new PageBuilder(bufferAllocator, schema, new PageOutput() {
                public void add(Page page)
                {
                    output.add(key, page);
                }

                public void finish()
                { }

                public void close()
                { }
            });
            hash.put(key, builder);
        }
        builder.getSchema().visitColumns(new ForwardRecordColumnVisitor(record, builder));
        builder.addRecord();
    }

    private PageBuilder removeMostUnsed(Map<PartitionKey, PageBuilder> hash)
    {
        // TODO remove the largest buffer
        Iterator<Map.Entry<PartitionKey, PageBuilder>> ite = hash.entrySet().iterator();
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
        Iterator<Map.Entry<PartitionKey, PageBuilder>> ite = hash.entrySet().iterator();
        while (ite.hasNext()) {
            PageBuilder builder = ite.next().getValue();
            builder.close();
            ite.remove();
        }
        output.close();
    }
}
