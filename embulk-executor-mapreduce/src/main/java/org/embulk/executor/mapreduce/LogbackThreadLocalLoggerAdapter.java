package org.embulk.executor.mapreduce;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.classic.Level;
import static ch.qos.logback.classic.Level.ERROR_INT;
import static ch.qos.logback.classic.Level.WARN_INT;
import static ch.qos.logback.classic.Level.INFO_INT;
import static ch.qos.logback.classic.Level.DEBUG_INT;
import static ch.qos.logback.classic.Level.TRACE_INT;

public class LogbackThreadLocalLoggerAdapter
    extends UnsynchronizedAppenderBase<ILoggingEvent>
{
    @Override
    public void start()
    {
        if (isStarted()) {
            return;
        }

        super.start();
    }

    @Override
    protected void append(ILoggingEvent event)
    {
        ThreadLocalLogger threadLocal = ThreadLocalLogger.instance.get();
        if (threadLocal != null) {
            LogMessage message = new LogMessage(
                    event.getTimeStamp(),
                    logLevel(event.getLevel()),
                    event.getLoggerName(),
                    event.getThreadName(),
                    event.getFormattedMessage());
            threadLocal.log(message);
        }
    }

    private static LogMessage.Level logLevel(Level level)
    {
        int lv = level.toInt();
        if (lv >= ERROR_INT) {
            return LogMessage.Level.ERROR;
        }
        else if (lv >= WARN_INT) {
            return LogMessage.Level.WARN;
        }
        else if (lv >= INFO_INT) {
            return LogMessage.Level.INFO;
        }
        else if (lv >= DEBUG_INT) {
            return LogMessage.Level.DEBUG;
        }
        else {
            return LogMessage.Level.TRACE;
        }
    }
}
