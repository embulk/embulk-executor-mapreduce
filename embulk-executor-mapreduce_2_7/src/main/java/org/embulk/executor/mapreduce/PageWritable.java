package org.embulk.executor.mapreduce;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.DataOutputOutputStream;
import org.msgpack.value.Value;
import org.msgpack.value.ImmutableValue;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBuffer;
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

        List<ImmutableValue> valueReferences = page.getValueReferences();
        WritableUtils.writeVInt(out, valueReferences.size());
        for (Value value : valueReferences) {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();  // TODO reuse allocated buffer
            value.writeTo(packer);
            List<MessageBuffer> buffers = packer.toBufferList();
            int size = 0;
            for (MessageBuffer b : buffers) {
                size += b.size();
            }
            WritableUtils.writeVInt(out, size);
            for (MessageBuffer b : buffers) {
                out.write(b.array(), b.arrayOffset(), b.size());
            }
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
        List<String> strings = new ArrayList<>(stringCount);
        for (int i=0; i < stringCount; i++) {
            strings.add(in.readUTF());
        }

        int valueCount = WritableUtils.readVInt(in);
        List<ImmutableValue> values = new ArrayList<>(valueCount);
        byte[] b = new byte[32 * 1024];
        for (int i=0; i < valueCount; i++) {
            int size = WritableUtils.readVInt(in);
            if (b.length < size) {
                int ns = b.length;
                while (ns < size) {
                    ns *= 2;
                }
                b = new byte[ns];
            }
            in.readFully(b, 0, size);
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(b, 0, size);
            values.add(unpacker.unpackValue());
        }

        Page newPage = Page.wrap(buffer);
        newPage.setStringReferences(strings);
        newPage.setValueReferences(values);
        if (page != null) {
            page.release();
        }
        page = newPage;
    }
}
