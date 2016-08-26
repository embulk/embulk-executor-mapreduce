package org.embulk.executor.mapreduce;

public interface ThreadLocalLogger
{
    public static final InheritableThreadLocal<ThreadLocalLogger> instance = new InheritableThreadLocal<ThreadLocalLogger>();

	void log(LogMessage message);
}
