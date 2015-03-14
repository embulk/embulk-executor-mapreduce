package org.embulk.executor.mapreduce;

public class RemoteTaskFailedException
        extends RuntimeException
{
    public RemoteTaskFailedException(String message)
    {
        super(message);
    }
}
