package org.embulk.executor.mapreduce;

public class SetContextClassLoader
        implements AutoCloseable
{
    private final ClassLoader original;

    public SetContextClassLoader(ClassLoader classLoader)
    {
        this.original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
    }

    @Override
    public void close()
    {
        Thread.currentThread().setContextClassLoader(original);
    }
}
