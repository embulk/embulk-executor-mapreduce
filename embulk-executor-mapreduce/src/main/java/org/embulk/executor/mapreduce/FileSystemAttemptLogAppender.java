package org.embulk.executor.mapreduce;

import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.nio.charset.StandardCharsets.UTF_8;

public class FileSystemAttemptLogAppender
    implements ThreadLocalLogger
{
    private static final Logger systemLogger = LoggerFactory.getLogger(FileSystemAttemptLogAppender.class);

    public static FileSystemAttemptLogAppender openIfSupported(Configuration config, Path path, boolean run)
        throws IOException
    {
        FSDataOutputStream out;
        try {
            out = path.getFileSystem(config).append(path);
        }
        catch (IOException ex) {
            if (ex.getMessage().contains("Not supported")) {
                if (run) {
                    // ballback to create
                    out = path.getFileSystem(config).create(path);
                }
                else {
                    // show also to stderr because logger is likely broken
                    systemLogger.error("FileSystem doesn't support append mode.");
                    System.err.println("FileSystem doesn't support append mode.");
                    return null;
                }
            }
            else {
                throw ex;
            }
        }
        return new FileSystemAttemptLogAppender(out);
    }

    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    private final OutputStream out;
    private final ObjectMapper mapper = new ObjectMapper();
    private ByteBuffer buffer = ByteBuffer.wrap(new byte[32*1024]);

    public FileSystemAttemptLogAppender(OutputStream out)
    {
        this.out = out;
    }

    @Override
    public synchronized void log(LogMessage message)
    {
        try {
            byte[] data = mapper.writeValueAsBytes(message);
            int maxPossibleSize = 4 + compressor.maxCompressedLength(data.length);  // 4 for bytes header

            if (buffer.capacity() < maxPossibleSize) {
                int allocSize = buffer.capacity();
                do {
                    allocSize *= 2;
                } while (allocSize < maxPossibleSize);
                buffer = ByteBuffer.wrap(new byte[allocSize]);
            }

            int len = compressor.compress(data, 0, data.length, buffer.array(), 4);
            buffer.putInt(0, len);

            out.write(buffer.array(), 0, 4 + len);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public void close()
        throws IOException
    {
        out.close();
    }
}
