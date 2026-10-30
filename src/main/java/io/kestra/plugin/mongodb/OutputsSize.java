package io.kestra.plugin.mongodb;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.kestra.core.serializers.JacksonMapper;

final class OutputsSize {
    // the default of kestra.queue.message-protection.limit, which rejects larger task results since Kestra 2.0
    static final long WARNING_BYTES = 1024 * 1024;

    private long bytes;

    void add(Object row) throws JsonProcessingException {
        // JSON is what the queue measures, ObjectIds and dates take more room there than in BSON. Measuring stops past the limit.
        if (this.bytes <= WARNING_BYTES) {
            this.bytes += JacksonMapper.ofJson().writeValueAsBytes(row).length;
        }
    }

    void warnIfLarge(Logger logger, long rows, String storeOption) {
        if (this.bytes > WARNING_BYTES) {
            logger.warn(
                "The {} rows kept in the outputs take more than 1 MiB. Kestra rejects task results above 'kestra.queue.message-protection.limit' (1 MiB by default since 2.0). Set `{}` to write the rows to internal storage instead.",
                rows,
                storeOption
            );
        }
    }
}
