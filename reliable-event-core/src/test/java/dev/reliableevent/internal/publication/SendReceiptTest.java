package dev.reliableevent.internal.publication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SendReceiptTest {

    @Test
    void requiresANonBlankMessageId() {
        assertThat(new SendReceipt("message-1").messageId()).isEqualTo("message-1");
        assertThatThrownBy(() -> new SendReceipt(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageId");
        assertThatThrownBy(() -> new SendReceipt(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("messageId");
    }
}
