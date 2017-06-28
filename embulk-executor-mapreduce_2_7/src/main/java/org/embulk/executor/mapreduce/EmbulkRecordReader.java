package org.embulk.executor.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;

public class EmbulkRecordReader
        extends RecordReader<IntWritable, NullWritable>
{
    private final int[] taskIndexes;
    private int offset;

    private final IntWritable currentKey = new IntWritable();

    public EmbulkRecordReader(EmbulkInputSplit split)
    {
        this.taskIndexes = split.getTaskIndexes();
        this.offset = -1;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
    { }

    @Override
    public boolean nextKeyValue()
    {
        offset++;
        if (taskIndexes.length <= offset) {
            return false;
        }
        currentKey.set(taskIndexes[offset]);
        return true;
    }

    @Override
    public float getProgress()
    {
        if (taskIndexes.length == 0) {
            return (float) 1.0;
        }
        return offset / (float) taskIndexes.length;
    }

    @Override
    public IntWritable getCurrentKey()
    {
        return currentKey;
    }

    @Override
    public NullWritable getCurrentValue()
    {
        return NullWritable.get();
    }

    @Override
    public void close()
    {
    }
}
