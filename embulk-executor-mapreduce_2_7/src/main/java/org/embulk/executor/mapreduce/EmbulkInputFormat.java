package org.embulk.executor.mapreduce;

import java.util.List;
import java.io.IOException;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;

public class EmbulkInputFormat
        extends InputFormat<IntWritable, NullWritable>
{
    @Override
    public List<InputSplit> getSplits(JobContext context)
        throws IOException, InterruptedException
    {
        // TODO combining multiple tasks to one mapper is not implemented yet.
        int taskCount = EmbulkMapReduce.getMapTaskCount(context.getConfiguration());
        ImmutableList.Builder<InputSplit> builder = ImmutableList.builder();
        for (int i=0; i < taskCount; i++) {
            builder.add(new EmbulkInputSplit(new int[] { i }));
        }
        return builder.build();
    }

    @Override
    public RecordReader<IntWritable, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException
    {
        return new EmbulkRecordReader((EmbulkInputSplit) split);
    }
}
