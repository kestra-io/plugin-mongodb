package io.kestra.plugin.mongodb;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.slf4j.Logger;

/**
 * Measures the documents kept in the outputs, to warn when they get too large for them.
 */
final class OutputsSize {
    // Kestra 2.0 rejects a task result above 1 MiB with its default settings, and 1.3 keeps the whole result in the execution
    static final long WARNING_BYTES = 1024 * 1024;

    private static final BsonDocumentCodec CODEC = new BsonDocumentCodec();

    private long bytes;

    void add(BsonDocument document) {
        // measuring stops past the threshold, so a large result is encoded a second time for its first MiB at most
        if (this.bytes <= WARNING_BYTES) {
            this.bytes += new RawBsonDocument(document, CODEC).getByteBuffer().remaining();
        }
    }

    void warnIfLarge(Logger logger, long rows, String storeOption) {
        if (this.bytes > WARNING_BYTES) {
            logger.warn(
                "The {} rows kept in the outputs take more than 1 MiB. Outputs are held in memory and saved in the execution, and Kestra 2.0 rejects them above 1 MiB by default; set `{}` to write the rows to internal storage instead.",
                rows,
                storeOption
            );
        }
    }
}
