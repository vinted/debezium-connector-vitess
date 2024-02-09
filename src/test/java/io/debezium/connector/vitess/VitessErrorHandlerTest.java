package io.debezium.connector.vitess;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VitessErrorHandlerTest  {

    VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
    ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>().build();
    VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(config, queue);

    @Test
    public void isRetriable_returnsTrueForConnectionRefusedExceptionThat() {
        boolean result = vitessErrorHandler.isRetriable(createConnectionRefusedException());
        assertTrue(result);
    }

    @Test
    public void isRetriable_ReturnsFalseAfter100Retries() {
        for (int i = 0; i < 100; i++) {
            vitessErrorHandler.isRetriable(createConnectionRefusedException());
        }
        assertFalse(vitessErrorHandler.isRetriable(createConnectionRefusedException()));
    }

    private StatusRuntimeException createConnectionRefusedException() {
        Throwable connectionRefused = new AnnotatedConnectException("Connection refused");
        return Status.UNAVAILABLE.withDescription("io exception").withCause(connectionRefused).asRuntimeException();
    }

    // mocks com.ververica.cdc.connectors.vitess.shaded.io.netty.channel.AbstractChannel$AnnotatedConnectException
    static class AnnotatedConnectException extends RuntimeException {
        public AnnotatedConnectException(String message) {
            super(message);
        }
    }
}