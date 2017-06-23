package org.embulk.executor.mapreduce;

import org.embulk.spi.DataException;

public class RemoteTaskFailedDataException
        extends DataException
{
    public RemoteTaskFailedDataException(String message)
    {
        super(message);
    }
}
