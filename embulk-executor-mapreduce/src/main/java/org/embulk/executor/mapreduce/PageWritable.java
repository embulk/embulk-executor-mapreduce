package org.embulk.executor.mapreduce;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.embulk.spi.Page;

public class PageWritable
{
    private Page page;

    public PageWritable() { }

    public void set(Page page)
    {
        this.page = page;
    }

    public Page get()
    {
        return Page;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        Buffer buffer = page.buffer();
        out.writeInt(buffer.limit());
        out.write(buffer.get(), buffer.offset(), buffer.limit());

        List<String> stringReferences = page.getStringReferences();
        WritableUtils.writeVInt(out, stringReferences.size());
        for (String s : stringReferences) {
            out.writeUTF(s);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int bufferSize = in.readInt();
        byte[] bytes = new byte[size];  // TODO usa buffer allocator?
        in.readFully(bytes, 0, size);
        Buffer buffer = Buffer.wrap(bytes);

        int stringCount = WritableUtils.readVInt(in);
        List<String> strings = new ArrayList<String>(stringCount);
        for (int i=0; i < stringCount; i++) {
            strings.add(in.readUTF());
        }

        return Page.wrap(buffer).setStringReferences(strings);
    }
}
