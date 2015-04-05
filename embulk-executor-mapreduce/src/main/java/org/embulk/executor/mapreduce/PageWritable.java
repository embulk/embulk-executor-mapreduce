package org.embulk.executor.mapreduce;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.embulk.spi.Buffer;
import org.embulk.spi.Page;
import static java.nio.charset.StandardCharsets.UTF_8;

public class PageWritable
        implements Writable
{
    private Page page;

    public PageWritable() { }

    public void set(Page page)
    {
        this.page = page;
    }

    public Page get()
    {
        return page;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        Buffer buffer = page.buffer();
        out.writeInt(buffer.limit());
        out.write(buffer.array(), buffer.offset(), buffer.limit());

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
        byte[] bytes = new byte[bufferSize];  // TODO usa buffer allocator?
        in.readFully(bytes, 0, bufferSize);
        Buffer buffer = Buffer.wrap(bytes);

        int stringCount = WritableUtils.readVInt(in);
        List<String> strings = new ArrayList<String>(stringCount);
        for (int i=0; i < stringCount; i++) {
            strings.add(in.readUTF());
        }

        Page newPage = Page.wrap(buffer);
        newPage.setStringReferences(strings);
        if (page != null) {
            page.release();
        }
        page = newPage;
    }
}
