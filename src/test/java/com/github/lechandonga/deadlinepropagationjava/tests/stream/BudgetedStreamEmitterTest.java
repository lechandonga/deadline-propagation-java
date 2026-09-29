package com.github.lechandonga.deadlinepropagationjava.tests.stream;

import com.github.lechandonga.deadlinepropagationjava.budget.RequestBudget;
import com.github.lechandonga.deadlinepropagationjava.budget.TerminationReason;
import com.github.lechandonga.deadlinepropagationjava.stream.BudgetedStreamEmitter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BudgetedStreamEmitterTest {

    @Test
    void terminatorWrittenExactlyOnceAndNoContentAfter() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10));
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BudgetedStreamEmitter emitter = new BudgetedStreamEmitter(sink, budget);

        assertTrue(emitter.writeLine("tick 0"));
        assertTrue(emitter.finish(TerminationReason.DEADLINE_EXCEEDED));

        // After finish: nothing repeats and nothing extra appears.
        assertFalse(emitter.finish(TerminationReason.CLIENT_CANCELLED));
        assertFalse(emitter.writeLine("tick 1"));

        String body = sink.toString(StandardCharsets.UTF_8);
        assertEquals("tick 0\n" + BudgetedStreamEmitter.TERMINATOR_PREFIX
                + "DEADLINE_EXCEEDED\n", body);
        assertEquals(1, countOccurrences(body, BudgetedStreamEmitter.TERMINATOR_PREFIX));
    }

    @Test
    void linesStopOnceBudgetTerminatedAndTerminatorCarriesReason() throws Exception {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10));
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        BudgetedStreamEmitter emitter = new BudgetedStreamEmitter(sink, budget);

        assertTrue(emitter.writeLine("tick 0"));
        budget.cancel(TerminationReason.CLIENT_CANCELLED);
        assertFalse(emitter.writeLine("tick 1"), "no bytes after cancellation");
        assertTrue(emitter.finish(budget.terminationReason()));

        String body = sink.toString(StandardCharsets.UTF_8);
        assertTrue(body.endsWith(BudgetedStreamEmitter.TERMINATOR_PREFIX
                + "CLIENT_CANCELLED\n"));
        assertTrue(body.contains("tick 0"));
        assertFalse(body.contains("tick 1"));
    }

    @Test
    void clientDisconnectMarksCancelledAndInventsNoBytes() {
        RequestBudget budget = RequestBudget.newBudget(Duration.ofSeconds(10));
        OutputStream broken = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("connection reset by peer");
            }
        };
        BudgetedStreamEmitter emitter = new BudgetedStreamEmitter(broken, budget);

        assertThrows(IOException.class,
                () -> emitter.finish(TerminationReason.DEADLINE_EXCEEDED));
        assertEquals(TerminationReason.CLIENT_CANCELLED, budget.terminationReason(),
                "a write failure is the client disconnecting");
        assertTrue(emitter.isFinished());
        // Second finish must not attempt another write.
        assertFalse(safeFinish(emitter));
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    private static boolean safeFinish(BudgetedStreamEmitter emitter) {
        try {
            return emitter.finish(TerminationReason.DEADLINE_EXCEEDED);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
