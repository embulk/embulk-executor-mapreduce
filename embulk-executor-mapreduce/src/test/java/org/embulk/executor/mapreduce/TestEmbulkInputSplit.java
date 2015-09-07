package org.embulk.executor.mapreduce;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestEmbulkInputSplit
{
    @Test
    public void readAndWrite()
            throws IOException
    {
        readAndWrite(new EmbulkInputSplit());
        readAndWrite(new EmbulkInputSplit(new int[] {0, 1, 2, 3}));
    }

    private void readAndWrite(EmbulkInputSplit is) throws IOException
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (DataOutputStream dout = new DataOutputStream(out)) {
                is.write(dout);
                dout.flush();

                try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
                    EmbulkInputSplit newIs = new EmbulkInputSplit();
                    newIs.readFields(in);
                    assertEmbulkInputSplitEquals(is, newIs);
                }
            }
        }
    }

    private static void assertEmbulkInputSplitEquals(EmbulkInputSplit is1, EmbulkInputSplit is2)
    {
        assertArrayEquals(is1.getTaskIndexes(), is2.getTaskIndexes());
        assertEquals(is1.getLength(), is2.getLength());
        assertArrayEquals(is1.getLocations(), is2.getLocations());
    }
}
