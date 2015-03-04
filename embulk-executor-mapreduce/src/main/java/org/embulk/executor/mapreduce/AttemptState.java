package org.embulk.executor.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.embulk.config.CommitReport;

// ProcessState.start := state file exists
// ProcessState.finish := attempt is done at hadoop
// ProcessState.setException := read from state file
// ProcessState.setInputCommitReport := read from state file
// ProcessState.setOutputCommitReport := read from state file

public class AttemptState
{
    private final TaskAttemptID attemptId;
    private final int taskIndex;
    private String exception;
    private CommitReport inputCommitReport;
    private CommitReport outputCommitReport;

    public AttemptState(TaskAttemptID attemptId, int taskIndex)
    {
        this.attemptId = attemptId;
        this.taskIndex = taskIndex;
    }

    public TaskAttemptID getAttemptId()
    {
        return attemptId;
    }

    public int getTaskIndex()
    {
        return taskIndex;
    }

    public void setException(Throwable exception)
    {
        setException(exception.toString());  // TODO save stacktrace
    }

    public void setException(String exception)
    {
        this.exception = exception;
    }

    public String getException()
    {
        return exception;
    }

    public CommitReport getInputCommitReport()
    {
        return inputCommitReport;
    }

    public CommitReport getOutputCommitReport()
    {
        return outputCommitReport;
    }

    public void setInputCommitReport(CommitReport inputCommitReport)
    {
        this.inputCommitReport = inputCommitReport;
    }

    public void setOutputCommitReport(CommitReport outputCommitReport)
    {
        this.outputCommitReport = outputCommitReport;
    }

    public void writeTo(DataOutput out) throws IOException
    {
        // TODO
    }

    public static AttemptState readFrom(DataInput out) throws IOException
    {
        // TODO
        return null;
    }
}
