package se.motility.mgen.sync;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.motility.mgen.generator.PerfMessage;
import se.motility.ziploq.api.SynchronizedConsumer;

public class Reader {

    private static final String FILE_SUFFIX = ".messages";
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

    public final ObjectReader reader = new ObjectMapper()
            .readerFor(PerfMessage.class);

    public void read(String filename, SynchronizedConsumer<PerfMessage> consumer) {

        String line;
        PerfMessage msg;
        String fullName = "data/" + filename + FILE_SUFFIX;
        try (BufferedReader r = new BufferedReader(new FileReader(fullName))) {
            while ((line = r.readLine()) != null) {
                msg = reader.readValue(line);
                consumer.onEvent(msg, msg.getT());
            }
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }

        consumer.complete();

    }

}
