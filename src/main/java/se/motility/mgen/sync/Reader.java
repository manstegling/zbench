package se.motility.mgen.sync;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.motility.mgen.generator.PerfMessage;
import se.motility.ziploq.api.SynchronizedConsumer;

public class Reader {

    private static final String FILE_SUFFIX = ".messages";
    private static final String GZIP_SUFFIX = ".gz";
    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

    public final ObjectReader reader = new ObjectMapper()
            .readerFor(PerfMessage.class);

    public void readTreeBased(String path, String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String line;
        PerfMessage msg;
        String fullName = path + filename + FILE_SUFFIX;

        try (BufferedReader r = new BufferedReader(new FileReader(fullName))) {
            while ((line = r.readLine()) != null) {
                msg = reader.readValue(line);
                consumer.onEvent(msg, msg.getTimestamp());
            }
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }

        consumer.complete();
    }

    public void readStreamingLines(String path, String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String line;
        PerfMessage msg;
        String fullName = path + filename + FILE_SUFFIX;

        Deserializer deserializer = new Deserializer();
        try (BufferedReader r = new BufferedReader(new FileReader(fullName))) {
            while ((line = r.readLine()) != null) {
                msg = deserializer.readMessage(line);
                consumer.onEvent(msg, msg.getTimestamp());
            }
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }

        consumer.complete();
    }

    public void readFullStream(String path, String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String fullName = path + filename + FILE_SUFFIX;
        File file = new File(fullName);
        if (!file.exists()) {
            fullName = fullName + GZIP_SUFFIX;
            file = new File(fullName);
            if (!file.exists()) {
                throw new IllegalArgumentException("File does not exist: " + fullName);
            }
        }
        StreamDeserializer d = new StreamDeserializer(consumer);
        try (InputStream in = getStream(fullName)) {
            d.readMessages(in);
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }
        consumer.complete();
    }

    public void readStreamAsReader(String path, String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String fullName = path + filename + FILE_SUFFIX;
        File file = new File(fullName);
        if (!file.exists()) {
            fullName = fullName + GZIP_SUFFIX;
            file = new File(fullName);
            if (!file.exists()) {
                throw new IllegalArgumentException("File does not exist: " + fullName);
            }
        }
        StreamDeserializer d = new StreamDeserializer(consumer);
        try (BufferedReader r = getReader(fullName)) {
            d.readMessages(r);
        } catch (IOException e) {
            LOG.error("Exception when accessing file '{}'", fullName, e);
        }
        consumer.complete();
    }

    public void readFileStream(String path, String filename, SynchronizedConsumer<PerfMessage> consumer) {
        String fullName = path + filename + FILE_SUFFIX;
        StreamDeserializer d = new StreamDeserializer(consumer);
        d.readFile(fullName);
        consumer.complete();
    }

    private static InputStream getStream(String filename) throws IOException {
        InputStream fis = new FileInputStream(filename);
        if (filename.endsWith(GZIP_SUFFIX)) {
            fis = new GZIPInputStream(fis, 2048);
        }
        return new BufferedInputStream(fis);
    }

    private static BufferedReader getReader(String filename) throws IOException {
        InputStream fis = new FileInputStream(filename);
        if (filename.endsWith(GZIP_SUFFIX)) {
            fis = new GZIPInputStream(fis, 2048);
        }
        InputStreamReader decoder = new InputStreamReader(fis, StandardCharsets.UTF_8);
        return new BufferedReader(decoder);
    }



}
