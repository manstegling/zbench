/*
 * Copyright (c) 2021-2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.generator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Locale;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A command-line app for creating test data. Test data consists of deterministically random
 * newline-delimited json files with {@link PerfMessage} messages. Message content is distributed
 * with a decreasing number of messages per file, such that the first file has {@code m} messages,
 * the second has {@code m/2} messages, the third {@code m/3} messages, and so on. That means you
 * can always read a fixed number of messages from {@code k} files resulting in {@code m} messages
 * in total, using a minimal disk footprint. The data generator keeps track on what has already
 * been generated through a file "filegen.meta", so that if you want to add more data later,
 * it does not have to recreate already produced data.
 *
 * The messages are partially ordered within each file, based on 'timestamp' and 'sequence'.
 * Those values are not guaranteed to increase so there might be multiple messages having the same
 * timestamp, same sequence, or both. This forces any user relying on total ordering to be careful
 * when selecting tie-breakers and perhaps provide their own sorting.
 *
 * @author M Tegling
 */
public class FileGenerator {

    static {
        System.setProperty("log4j.configurationFile", "config/log4j2.xml");
        Locale.setDefault(Locale.ENGLISH);
    }

    private static final int VERSION = 1;
    private static final String[] IDS = {"AAAA", "BBBB", "XYZ", "TEST1", "RANDOM", "O", "B", "B2", "OPAQ"};
    private static final String DATA_PATH = "data/";
    private static final String META_FILE = "filegen.meta";
    private static final Logger LOG = LoggerFactory.getLogger(FileGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter WRITER = MAPPER.writerFor(PerfMessage.class);

    public static void main(String[] args) {
        int files = args.length != 0 ? Integer.parseInt(args[0]) : 5;
        String path = args.length > 1 ? args[1] : DATA_PATH;
        long messages = args.length > 2 ? Integer.parseInt(args[2]) : 15_000_000;
        if(new File(path).mkdirs()) {
            LOG.info("Created folder {}", path);
        }

        int first = findStartFile(path, files, messages);
        for (int f = first; f < files; f++) {
            long start = System.currentTimeMillis();
            long messagesWritten = writeFile(path, f, messages);
            long duration = System.currentTimeMillis() - start;
            LOG.info("Completed '{}'. Took {} ms, tps: {}", f, duration, messagesWritten * 1000 / duration);
        }
        writeMeta(path, Math.max(files, first), messages);
    }

    private static int findStartFile(String path, int files, long messages) {
        MetaData meta = readMeta(path);
        if (meta != null && meta.getVersion() == VERSION && meta.getMessages() >= messages) {
            int start = meta.getFiles();
            LOG.info("{} files already present. Only last {} files will be written", start, Math.max(0, files - start));
            return start;
        } else {
            LOG.info("No matching prior files detected. All {} files will be written", files);
            return 0;
        }
    }

    private static long writeFile(String path, int file, long messages) {
        long currentTime = 0L;
        long sequence = 0L;
        long fileSpecificMessages = messages / (file + 1);
        String filename = file + ".messages";
        Random r = new Random(5139047L + filename.hashCode());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter( path + filename))) {
            for (int i = 0; i < fileSpecificMessages; i++) {
                currentTime += r.nextDouble() < 0.05 ? 1 : 0;
                sequence += r.nextInt(5);
                writer.write(WRITER.writeValueAsString(new PerfMessage(
                        currentTime,
                        sequence,
                        r.nextInt(1_000_000),
                        r.nextBoolean(),
                        IDS[r.nextInt(IDS.length)],
                        r.nextInt() + "_S1",
                        r.nextInt() + "_S2",
                        r.nextInt() + "_S3",
                        r.nextInt() + "_S4")));
                writer.write('\n');
            }
            writer.flush();
            return fileSpecificMessages;
        } catch (Exception e) {
            LOG.error("Fail: {}", e.getMessage(), e);
            return -1L;
        }
    }

    private static MetaData readMeta(String path) {
        String filename = path + META_FILE;
        if (!new File(filename).exists()) {
            LOG.info("No meta file found in {}", path);
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line = reader.readLine();
            return MAPPER
                    .readerFor(MetaData.class)
                    .readValue(line);
        } catch (Exception e) {
            LOG.warn("Could not read meta file: {}", e.getMessage(), e);
            return null;
        }
    }

    private static void writeMeta(String path, int files, long messages) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path + META_FILE))) {
            ObjectWriter w = MAPPER.writerFor(MetaData.class);
            MetaData meta = new MetaData(VERSION, files, messages);
            writer.write(w.writeValueAsString(meta));
            writer.write('\n');
            writer.flush();
        } catch (Exception e) {
            LOG.warn("Could not produce meta file: {}", e.getMessage(), e);
        }
    }

}
