package org.embulk.executor.mapreduce;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparator;
import org.embulk.spi.Buffer;

public class BufferWritable
        implements WritableComparable<BufferWritable>
{
    private Buffer buffer;

    public BufferWritable() { }

    public void set(Buffer buffer)
    {
        this.buffer = buffer;
    }

    public Buffer get()
    {
        return buffer;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        WritableUtils.writeVInt(out, buffer.limit());
        out.write(buffer.array(), buffer.offset(), buffer.limit());
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int size = WritableUtils.readVInt(in);
        byte[] bytes = new byte[size];  // TODO usa buffer allocator?
        in.readFully(bytes, 0, size);
        Buffer newBuffer = Buffer.wrap(bytes);
        if (buffer != null) {
            buffer.release();
        }
        buffer = newBuffer;
    }

    @Override
    public int compareTo(BufferWritable o)
    {
        return WritableComparator.compareBytes(
                buffer.array(), buffer.offset(), buffer.limit(),
                o.buffer.array(), o.buffer.offset(), o.buffer.limit());
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof BufferWritable)) {
            return false;
        }
        BufferWritable o = (BufferWritable) other;
        if (buffer == null) {
            return o.buffer == null;
        }
        return buffer.equals(o.buffer);
    }

    @Override
    public int hashCode()
    {
        return buffer.hashCode();
    }
}
