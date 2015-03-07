package org.embulk.executor.mapreduce;

import java.util.Scanner;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.embulk.config.ModelManager;
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
    private Optional<String> exception;
    private CommitReport inputCommitReport;
    private CommitReport outputCommitReport;

    public AttemptState(TaskAttemptID attemptId, int taskIndex)
    {
        this.attemptId = attemptId;
        this.taskIndex = taskIndex;
    }

    @JsonCreator
    public AttemptState(
            @JsonProperty("attempt") TaskAttemptID attemptId,
            @JsonProperty("taskIndex") int taskIndex,
            @JsonProperty("exception") Optional<String> exception,
            @JsonProperty("inputCommitReport") CommitReport inputCommitReport,
            @JsonProperty("outputCommitReport") CommitReport outputCommitReport)
    {
        this.attemptId = attemptId;
        this.taskIndex = taskIndex;
        this.exception = exception;
        this.inputCommitReport = inputCommitReport;
        this.outputCommitReport = outputCommitReport;
    }

    @JsonIgnore
    public TaskAttemptID getAttemptId()
    {
        return attemptId;
    }

    @JsonProperty("attempt")
    public String getAttemptIdString()
    {
        return attemptId.toString();
    }

    @JsonProperty("taskIndex")
    public int getTaskIndex()
    {
        return taskIndex;
    }

    public void setException(Throwable exception)
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(os, false, "UTF-8")) {
            exception.printStackTrace(ps);
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        setException(new String(os.toByteArray(), StandardCharsets.UTF_8));
    }

    public void setException(String exception)
    {
        this.exception = Optional.of(exception);
    }

    @JsonProperty("exception")
    public Optional<String> getException()
    {
        return exception;
    }

    @JsonProperty("inputCommitReport")
    public CommitReport getInputCommitReport()
    {
        return inputCommitReport;
    }

    @JsonProperty("outputCommitReport")
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

    public void writeTo(OutputStream out, ModelManager modelManager) throws IOException
    {
        String s = modelManager.writeObject(this);
        out.write(s.getBytes(StandardCharsets.UTF_8));
    }

    public static AttemptState readFrom(InputStream in, ModelManager modelManager) throws IOException
    {
        Scanner s = new Scanner(in, "UTF-8").useDelimiter("\\0");
        if (s.hasNext()) {
            return modelManager.readObject(AttemptState.class, s.next());
        } else {
            throw new EOFException("JSON is not included in the attempt state file.");
        }
    }
}
