package org.embulk.executor.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class EmbulkInputSplit
        extends InputSplit
        implements Writable
{
    private int[] taskIndexes;

    public EmbulkInputSplit()
    {
        this(new int[0]);
    }

    public EmbulkInputSplit(int[] taskIndexes)
    {
        this.taskIndexes = taskIndexes;
    }

    public int[] getTaskIndexes()
    {
        return taskIndexes;
    }

    @Override
    public long getLength()
    {
        return taskIndexes.length;
    }

    @Override
    public String[] getLocations()
    {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(taskIndexes.length);
        for (int taskIndex : taskIndexes) {
            out.writeInt(taskIndex);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int c = in.readInt();
        int[] taskIndexes = new int[c];
        for (int i=0; i < c; i++) {
            taskIndexes[i] = in.readInt();
        }
        this.taskIndexes = taskIndexes;
    }
}
