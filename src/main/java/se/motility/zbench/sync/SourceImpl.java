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
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.Source;
import se.motility.ziploq.api.Ziploq;

import static se.motility.zbench.sync.Reader.getStream;

public class SourceImpl implements Source<PerfMessage> {

    private static final String FILE_SUFFIX = ".messages";
    private static final String GZIP_SUFFIX = ".gz";
    private static final Logger LOG = LoggerFactory.getLogger(SourceImpl.class);

    private final JsonFactory factory = new JsonFactory();
    private final String filename;

    private InputStream in;
    private JsonParser parser;

    public SourceImpl(String path, String filename) {
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
    }

    public void init() throws IOException {
        in = getStream(filename);
        parser = factory.createParser(in);
    }

    @Override
    public Entry<PerfMessage> emit() {
        for (;;) {
            try {
                if (parser.nextToken() != null) {
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

    private void close() {
        try {
            parser.close();
            in.close();
        } catch (IOException e) {
            LOG.warn("Failed to close file {}", filename, e);
        }
    }

}
