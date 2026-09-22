package dev.reliableevent.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventFailureSummaryTest {

    @Test
    void includesExceptionTypeAndMessage() {
        String summary = EventFailureSummary.from(
                new IllegalStateException("broker unavailable")
        );

        assertThat(summary)
                .isEqualTo("java.lang.IllegalStateException: broker unavailable");
    }

    @Test
    void omitsSeparatorWhenMessageIsMissing() {
        String summary = EventFailureSummary.from(new IllegalStateException((String) null));

        assertThat(summary).isEqualTo("java.lang.IllegalStateException");
    }

    @Test
    void truncatesByCodePointWithoutSplittingSurrogatePairs() {
        String summary = EventFailureSummary.from(
                new IllegalStateException("😀".repeat(1_100))
        );

        assertThat(summary.codePointCount(0, summary.length())).isEqualTo(1_024);
        assertThat(Character.isLowSurrogate(summary.charAt(summary.length() - 1))).isTrue();
        assertThat(Character.isHighSurrogate(summary.charAt(summary.length() - 2))).isTrue();
    }
}
