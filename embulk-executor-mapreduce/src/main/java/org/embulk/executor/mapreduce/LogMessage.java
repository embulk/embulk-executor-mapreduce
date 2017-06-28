package org.embulk.executor.mapreduce;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

public class LogMessage
{
    public static enum Level
    {
        ERROR("error"),
        WARN("warn"),
        INFO("info"),
        DEBUG("debug"),
        TRACE("trace");

        private final String name;

        private Level(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static Level of(String name)
        {
            switch (name) {
            case "error":
                return ERROR;
            case "warn":
                return WARN;
            case "info":
                return INFO;
            case "debug":
                return DEBUG;
            case "trace":
                return TRACE;
            default:
                throw new IllegalArgumentException("Unknown log level: " + name);
            }
        }

        @JsonValue
        public String toString()
        {
            return name;
        }
    }

    private final long timestamp;
    private final Level level;
    private final String loggerName;
    private final String threadName;
    private final String message;

    @JsonCreator
    public LogMessage(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("level") Level level,
            @JsonProperty("logger") String loggerName,
            @JsonProperty("thread") String threadName,
            @JsonProperty("message") String message)
    {
        this.timestamp = timestamp;
        this.level = level;
        this.loggerName = loggerName;
        this.threadName = threadName;
        this.message = message;
    }

    @JsonProperty("timestamp")
    public long getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty("level")
    public Level getLevel()
    {
        return level;
    }

    @JsonProperty("logger")
    public String getLoggerName()
    {
        return loggerName;
    }

    @JsonProperty("thread")
    public String getThreadName()
    {
        return threadName;
    }

    @JsonProperty("message")
    public String getMessage()
    {
        return message;
    }
}
