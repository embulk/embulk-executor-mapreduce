package org.embulk.executor.mapreduce;

import org.embulk.spi.Buffer;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Types;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestPageBufferWritable
{
    @Rule
    public MapReduceExecutorTestRuntime runtime = new MapReduceExecutorTestRuntime();

    @Test
    public void writeAndRead() throws IOException
    {
        Schema schema = Schema.builder()
                .add("c0", Types.BOOLEAN)
                .add("c1", Types.LONG)
                .add("c2", Types.DOUBLE)
                .add("c3", Types.STRING)
                .add("c4", Types.TIMESTAMP)
                .build();

        for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(), schema,
                true, 2L, 3.0D, "45", Timestamp.ofEpochMilli(678L),
                true, 2L, 3.0D, "45", Timestamp.ofEpochMilli(678L))) {

            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                try (DataOutputStream dout = new DataOutputStream(out)) {
                    PageWritable pw1 = new PageWritable();
                    pw1.set(page);
                    pw1.write(dout);

                    BufferWritable bw1 = new BufferWritable();
                    bw1.set(page.buffer());
                    bw1.write(dout);
                    dout.flush();

                    try (DataInputStream din = new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
                        PageWritable pw2 = new PageWritable();
                        pw2.readFields(din);

                        BufferWritable bw2 = new BufferWritable();
                        bw2.readFields(din);

                        assertPageWritableEquals(pw1, pw2);
                        assertBufferWritableEquals(bw1, bw2);
                    }
                }
            }
        }
    }

    static void assertPageWritableEquals(PageWritable pw1, PageWritable pw2)
    {
        Page p1 = pw1.get();
        Page p2 = pw2.get();

        assertEquals(p1.getStringReferences(), p2.getStringReferences());
        assertBufferEquals(p1.buffer(), p2.buffer());
    }

    static void assertBufferWritableEquals(BufferWritable bw1, BufferWritable bw2)
    {
        assertBufferEquals(bw1.get(), bw2.get());
    }

    static void assertBufferEquals(Buffer b1, Buffer b2)
    {
        assertEquals(b1, b2);
    }
}
