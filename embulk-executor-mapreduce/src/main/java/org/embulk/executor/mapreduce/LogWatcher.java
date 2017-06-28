package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.HashMap;
import java.io.IOException;
import java.text.SimpleDateFormat;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.embulk.executor.mapreduce.EmbulkMapReduce.listLogFiles;

public class LogWatcher
{
    private final Logger logger = LoggerFactory.getLogger(LogWatcher.class);

    private static class LastState
    {
        private int failCount = 0;
        private long offset = 0;
        private long lastSize = 0;

        public boolean isReadable()
        {
            return failCount < 10;
        }

        public boolean isGrown(long newSize)
        {
            return lastSize < newSize;
        }

        public long getOffset()
        {
            return offset;
        }

        public void update(long offset, long newSize)
        {
            this.offset = offset;
            this.lastSize = newSize;
        }

        public void fail()
        {
            failCount++;
        }
    }

    private final Configuration config;
    private final Path stateDir;
    private final Map<TaskAttemptID, LastState> files = new HashMap<>();

    private byte[] readBuffer = new byte[32*1024];
    private final byte[] decompBuffer = new byte[64*1024];

    private final SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final ObjectMapper mapper = new ObjectMapper();

    private final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

    public LogWatcher(Configuration config, Path stateDir)
    {
        this.config = config;
        this.stateDir = stateDir;
    }

    public void update()
        throws IOException
    {
        for (Map.Entry<TaskAttemptID, FileStatus> pair : listLogFiles(config, stateDir).entrySet()) {
            LastState lastState = files.get(pair.getKey());
            if (lastState == null) {
                lastState = new LastState();
                files.put(pair.getKey(), lastState);
            }
            else if (!lastState.isReadable()) {
                continue;
            }

            long newSize = pair.getValue().getLen();
            if (lastState.isGrown(newSize)) {
                try {
                    long newOffset = showMoreLogs(pair.getValue().getPath(), lastState.getOffset(), newSize);
                    lastState.update(newOffset, newSize);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    logger.warn("Failed to read log file " + pair.getValue().getPath(), ex);
                    lastState.fail();
                }
            }
        }
    }

    private long showMoreLogs(Path path, long offset, long fileSize)
        throws IOException
    {
        FSDataInputStream in = path.getFileSystem(config).open(path);
        in.seek(offset);
        while (true) {
            if (fileSize < offset + 4) {
                return offset;
            }
            int n = in.readInt();
            if (fileSize < offset + 4 + n) {
                return offset;
            }

            if (readBuffer.length < n) {
                int allocSize = readBuffer.length;
                do {
                    allocSize *= 2;
                } while (allocSize < n);
                readBuffer = new byte[allocSize];
            }
            in.readFully(readBuffer, 0, n);

            try {
                int dec = decompressor.decompress(readBuffer, 0, n, decompBuffer, 0);
                LogMessage message = mapper.readValue(decompBuffer, 0, dec, LogMessage.class);
                logMessage(message);
            }
            catch (LZ4Exception ex) {
                ex.printStackTrace();
                logger.warn("Skipping too long log message");
            }

            offset += 4 + n;
        }
    }

    private String lastLoggerName = "";
    private Logger lastLogger = LoggerFactory.getLogger(lastLoggerName);

    private void logMessage(LogMessage message)
    {
        Thread currentThread = Thread.currentThread();
        String savedThreadName = currentThread.getName();
        try {
            // fake thread name
            currentThread.setName(message.getThreadName());

            // fake logger name
            String loggerName = message.getLoggerName();
            if (!lastLoggerName.equals(loggerName)) {
                lastLogger = LoggerFactory.getLogger(loggerName);
                lastLoggerName = loggerName;
            }

            // slf4j doesn't allow to override log timestamp. include time in message
            String timestamp = timeFormatter.format(new Date(message.getTimestamp()));

            switch (message.getLevel()) {
            case TRACE:
                lastLogger.trace("{}: {}", message.getMessage(), timestamp);
            case DEBUG:
                lastLogger.debug("{}: {}", message.getMessage(), timestamp);
            case INFO:
                lastLogger.info("{}: {}", message.getMessage(), timestamp);
            case WARN:
                lastLogger.warn("{}: {}", message.getMessage(), timestamp);
            case ERROR:
                lastLogger.error("{}: {}", message.getMessage(), timestamp);
            }
        }
        finally {
            currentThread.setName(savedThreadName);
        }
    }
}
