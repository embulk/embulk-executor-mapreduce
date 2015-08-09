package org.embulk.executor.mapreduce;

import com.google.common.base.Optional;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestAttemptState
{
    @Rule
    public MapReduceExecutorTestRuntime runtime = new MapReduceExecutorTestRuntime();

    @Test
    public void readAndWrite()
            throws IOException {
        TaskAttemptID attemptId = TaskAttemptID.forName("attempt_200707121733_0003_m_000005_0");
        int inputTaskIndex = 1;
        int outputTaskIndex = 2;
        Exception ex = new Exception();

        AttemptState attemptState = new AttemptState(attemptId, Optional.of(inputTaskIndex), Optional.of(outputTaskIndex));
        attemptState.setException(ex);
        attemptState.setInputTaskReport(runtime.getExec().newTaskReport());
        attemptState.setOutputTaskReport(runtime.getExec().newTaskReport());

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
        assertEquals(s1.getInputTaskReport(), s2.getInputTaskReport());
        assertEquals(s1.getOutputTaskReport(), s2.getOutputTaskReport());
    }

    @Test
    public void throwEOFIfInvalidJsonString()
            throws IOException {
        String json = "{\"key\":\"va";
        // TODO
    }
}
