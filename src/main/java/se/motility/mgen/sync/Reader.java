package se.motility.mgen.sync;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

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

    public void readTreeBased(String filename, SynchronizedConsumer<PerfMessage> consumer) {
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

    public void readStreamingLines(String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String line;
        PerfMessage msg;
        String fullName = "data/" + filename + FILE_SUFFIX;

        Deserializer deserializer = new Deserializer();
        try (BufferedReader r = new BufferedReader(new FileReader(fullName))) {
            while ((line = r.readLine()) != null) {
                msg = deserializer.readMessage(line);
                consumer.onEvent(msg, msg.getT());
            }
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }

        consumer.complete();
    }

    public void readFullStream(String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String fullName = "data/" + filename + FILE_SUFFIX;

        StreamDeserializer d = new StreamDeserializer(consumer);
        // TODO: Add dynamical identification of GZIP files
        try (InputStream in = new BufferedInputStream(new FileInputStream(fullName))){
            d.readMessages(in);
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }
        consumer.complete();
    }

    public void readFileStream(String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String fullName = "data/" + filename + FILE_SUFFIX;
        StreamDeserializer d = new StreamDeserializer(consumer);
        d.readFile(fullName);
        consumer.complete();
    }



}
