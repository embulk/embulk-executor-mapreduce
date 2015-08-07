package org.embulk.executor.mapreduce;

import com.google.common.base.Optional;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimeAttemptState
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void readAndWrite()
            throws IOException {
        TaskAttemptID attemptId = TaskAttemptID.forName("attempt_200707121733_0003_m_000005_0");
        int inputTaskIndex = 1;
        int outputTaskIndex = 2;
        Exception ex = new Exception();

        AttemptState attemptState = new AttemptState(attemptId, Optional.of(inputTaskIndex), Optional.of(outputTaskIndex));
        attemptState.setException(ex);
        attemptState.setInputCommitReport(runtime.getExec().newCommitReport());
        attemptState.setOutputCommitReport(runtime.getExec().newCommitReport());

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            attemptState.writeTo(out, runtime.getModelManager());

            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                assertAttemptStateEquals(attemptState, AttemptState.readFrom(in, runtime.getModelManager()));
            }
        }
    }

    private static void assertAttemptStateEquals(AttemptState s1, AttemptState s2)
    {
        assertEquals(s1.getAttemptId(), s2.getAttemptId());
        assertEquals(s1.getInputTaskIndex(), s2.getInputTaskIndex());
        assertEquals(s1.getOutputTaskIndex(), s2.getOutputTaskIndex());
        assertEquals(s1.getException(), s2.getException());
        assertEquals(s1.getInputCommitReport(), s2.getInputCommitReport());
        assertEquals(s1.getOutputCommitReport(), s2.getOutputCommitReport());
    }

    @Test
    public void throwEOFIfInvalidJsonString()
            throws IOException {
        String json = "{\"key\":\"va";
        // TODO
    }
}
