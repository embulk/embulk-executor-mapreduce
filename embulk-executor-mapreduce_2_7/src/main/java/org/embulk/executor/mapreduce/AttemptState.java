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
import org.embulk.config.TaskReport;
import org.embulk.config.UserDataExceptions;

public class AttemptState
{
    private final TaskAttemptID attemptId;
    private final Optional<Integer> inputTaskIndex;
    private final Optional<Integer> outputTaskIndex;
    private Optional<String> exception;
    private boolean userDataException;
    private Optional<TaskReport> inputTaskReport;
    private Optional<TaskReport> outputTaskReport;

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
            @JsonProperty("userDataException") boolean userDataException,
            @JsonProperty("inputTaskReport") Optional<TaskReport> inputTaskReport,
            @JsonProperty("outputTaskReport") Optional<TaskReport> outputTaskReport)
    {
        this(TaskAttemptID.forName(attemptId),
                inputTaskIndex, outputTaskIndex,
                exception, userDataException,
                inputTaskReport, outputTaskReport);
    }

    public AttemptState(
            TaskAttemptID attemptId,
            Optional<Integer> inputTaskIndex,
            Optional<Integer> outputTaskIndex,
            Optional<String> exception,
            boolean userDataException,
            Optional<TaskReport> inputTaskReport,
            Optional<TaskReport> outputTaskReport)
    {
        this.attemptId = attemptId;
        this.inputTaskIndex = inputTaskIndex;
        this.outputTaskIndex = outputTaskIndex;
        this.exception = exception;
        this.userDataException = userDataException;
        this.inputTaskReport = inputTaskReport;
        this.outputTaskReport = outputTaskReport;
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
        setException(
                new String(os.toByteArray(), StandardCharsets.UTF_8),
                UserDataExceptions.isUserDataException(exception));
    }

    @JsonIgnore
    public void setException(String exception, boolean userDataException)
    {
        this.exception = Optional.of(exception);
        this.userDataException = userDataException;
    }

    @JsonProperty("exception")
    public Optional<String> getException()
    {
        return exception;
    }

    @JsonProperty("userDataException")
    public boolean isUserDataException()
    {
        return userDataException;
    }

    @JsonProperty("inputTaskReport")
    public Optional<TaskReport> getInputTaskReport()
    {
        return inputTaskReport;
    }

    @JsonProperty("outputTaskReport")
    public Optional<TaskReport> getOutputTaskReport()
    {
        return outputTaskReport;
    }

    @JsonIgnore
    public void setInputTaskReport(TaskReport inputTaskReport)
    {
        this.inputTaskReport = Optional.of(inputTaskReport);
    }

    @JsonIgnore
    public void setOutputTaskReport(TaskReport outputTaskReport)
    {
        this.outputTaskReport = Optional.of(outputTaskReport);
    }

    public void writeTo(OutputStream out, ModelManager modelManager) throws IOException
    {
        String s = modelManager.writeObject(this);
        out.write(s.getBytes(StandardCharsets.UTF_8));
    }

    public static AttemptState readFrom(InputStream in, ModelManager modelManager) throws IOException
    {
        // If InputStream contains partial JSON (like '{"key":"va'), this method throws EOFException
        Scanner s = new Scanner(in, "UTF-8").useDelimiter("\\A");  // TODO
        if (s.hasNext()) {
            return modelManager.readObject(AttemptState.class, s.next());
        } else {
            throw new EOFException("JSON is not included in the attempt state file.");
        }
    }
}
