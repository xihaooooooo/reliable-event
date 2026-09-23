package dev.reliableevent.jdbc.internal.cycle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PublicationCycleResultTest {

    @Test
    void acceptsNonNegativeCounts() {
        PublicationCycleResult empty = new PublicationCycleResult(0, 0);
        PublicationCycleResult populated = new PublicationCycleResult(2, 3);

        assertThat(empty.recoveredCount()).isZero();
        assertThat(empty.publishedCount()).isZero();
        assertThat(populated.recoveredCount()).isEqualTo(2);
        assertThat(populated.publishedCount()).isEqualTo(3);
    }

    @Test
    void rejectsNegativeCounts() {
        assertThatThrownBy(() -> new PublicationCycleResult(-1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveredCount");
        assertThatThrownBy(() -> new PublicationCycleResult(0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("publishedCount");
    }
}
