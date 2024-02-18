/*
 * Copyright (c) 2021-2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.sync;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.motility.zbench.generator.PerfMessage;
import se.motility.ziploq.api.BasicEntry;
import se.motility.ziploq.api.Source;
import se.motility.ziploq.api.Ziploq;

import static se.motility.zbench.sync.Reader.getStream;

/**
 * A simple example of a {@link Source} implementation for a single file containing
 * {@link PerfMessage}s. It handles uncompressed and compressed (gzip) files dynamically.
 *
 * @author M Tegling
 */
public class FileSourceImpl implements Source<PerfMessage> {

    private static final String FILE_SUFFIX = ".messages";
    private static final String GZIP_SUFFIX = ".gz";
    private static final Logger LOG = LoggerFactory.getLogger(FileSourceImpl.class);

    private final JsonFactory factory = new JsonFactory();
    private final String filename;
    private final long maxMessages;
    private long messageCount;

    // Keeps the file open during its whole lifecycle
    private InputStream in;
    private JsonParser parser;

    public FileSourceImpl(String path, String filename, long maxMessages) {
        String fullName = path + filename + FILE_SUFFIX;
        File file = new File(fullName);
        if (!file.exists()) {
            fullName = fullName + GZIP_SUFFIX;
            file = new File(fullName);
            if (!file.exists()) {
                throw new IllegalArgumentException("File does not exist: " + fullName);
            }
        }
        this.filename = fullName;
        this.maxMessages = maxMessages;
    }

    /**
     * Must be called before starting the Ziploq machinery
     * @throws IOException if the associated file could not be opened
     */
    public void init() throws IOException {
        in = getStream(filename);
        parser = factory.createParser(in);
    }

    @Override
    public BasicEntry<PerfMessage> emit() {
        //TODO: proper exception handling
        for (;;) {
            try {
                if (messageCount++ < maxMessages && parser.nextToken() != null) {
                    PerfMessage message = Mapper.readPerfMessage(parser);
                    return new PerfEntry(message, message.getTimestamp());
                } else {
                    close();
                    return Ziploq.getEndSignal();
                }
            } catch (JsonGenerationException e) {
                e.printStackTrace();
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void close() {
        try {
            parser.close();
            in.close();
        } catch (IOException e) {
            LOG.warn("Failed to close file {}", filename, e);
        }
    }

}
