package org.embulk.executor.mapreduce;

import org.embulk.spi.Buffer;

public class BufferWritable
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
        out.write(buffer.get(), buffer.offset(), buffer.limit());
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int size = WritableUtils.readVInt(in);
        byte[] bytes = new byte[size];  // TODO usa buffer allocator?
        in.readFully(bytes, 0, size);
        return Buffer.wrap(bytes);
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
