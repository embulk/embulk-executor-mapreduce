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
    private final Optional<Integer> inputTaskIndex;
    private final Optional<Integer> outputTaskIndex;
    private Optional<String> exception;
    private Optional<CommitReport> inputCommitReport;
    private Optional<CommitReport> outputCommitReport;

    public AttemptState(TaskAttemptID attemptId, Optional<Integer> inputTaskIndex, Optional<Integer> outputTaskIndex)
    {
        this.attemptId = attemptId;
        this.inputTaskIndex = inputTaskIndex;
        this.outputTaskIndex = outputTaskIndex;
    }

    @JsonCreator
    AttemptState(
            @JsonProperty("attempt") String attemptId,
            @JsonProperty("inputTaskIndex") Optional<Integer> inputTaskIndex,
            @JsonProperty("outputTaskIndex") Optional<Integer> outputTaskIndex,
            @JsonProperty("exception") Optional<String> exception,
            @JsonProperty("inputCommitReport") Optional<CommitReport> inputCommitReport,
            @JsonProperty("outputCommitReport") Optional<CommitReport> outputCommitReport)
    {
        this(TaskAttemptID.forName(attemptId),
                inputTaskIndex, outputTaskIndex, exception,
                inputCommitReport, outputCommitReport);
    }

    public AttemptState(
            TaskAttemptID attemptId,
            Optional<Integer> inputTaskIndex,
            Optional<Integer> outputTaskIndex,
            Optional<String> exception,
            Optional<CommitReport> inputCommitReport,
            Optional<CommitReport> outputCommitReport)
    {
        this.attemptId = attemptId;
        this.inputTaskIndex = inputTaskIndex;
        this.outputTaskIndex = outputTaskIndex;
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

    @JsonProperty("inputTaskIndex")
    public Optional<Integer> getInputTaskIndex()
    {
        return inputTaskIndex;
    }

    @JsonProperty("outputTaskIndex")
    public Optional<Integer> getOutputTaskIndex()
    {
        return outputTaskIndex;
    }

    @JsonIgnore
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

    @JsonIgnore
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
    public Optional<CommitReport> getInputCommitReport()
    {
        return inputCommitReport;
    }

    @JsonProperty("outputCommitReport")
    public Optional<CommitReport> getOutputCommitReport()
    {
        return outputCommitReport;
    }

    @JsonIgnore
    public void setInputCommitReport(CommitReport inputCommitReport)
    {
        this.inputCommitReport = Optional.of(inputCommitReport);
    }

    @JsonIgnore
    public void setOutputCommitReport(CommitReport outputCommitReport)
    {
        this.outputCommitReport = Optional.of(outputCommitReport);
    }

    public void writeTo(OutputStream out, ModelManager modelManager) throws IOException
    {
        String s = modelManager.writeObject(this);
        out.write(s.getBytes(StandardCharsets.UTF_8));
    }

    public static AttemptState readFrom(InputStream in, ModelManager modelManager) throws IOException
    {
        Scanner s = new Scanner(in, "UTF-8").useDelimiter("\\A");  // TODO
        if (s.hasNext()) {
            return modelManager.readObject(AttemptState.class, s.next());
        } else {
            throw new EOFException("JSON is not included in the attempt state file.");
        }
    }
}
