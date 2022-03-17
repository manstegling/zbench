package se.motility.zbench.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Locale;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileGenerator {

    static {
        System.setProperty("log4j.configurationFile", "config/log4j2.xml");
        Locale.setDefault(Locale.ENGLISH);
    }

    private static final String[] IDS = {"AAAA", "BBBB", "XYZ", "TEST1", "RANDOM", "O", "B", "B2", "OPAQ"};
    private static final String DATA_PATH = "data/";
    private static final Logger LOG = LoggerFactory.getLogger(FileGenerator.class);
    private static final ObjectWriter MAPPER = new ObjectMapper()
            .writerFor(PerfMessage.class);

    private static final Random R = new Random(13377331L);

    public static void main(String[] args) {
        int messages = 10_000_000;
        int files = args.length != 0 ? Integer.parseInt(args[0]) : 5;
        String path = args.length > 1 ? args[1] : DATA_PATH;
        File data = new File(path);
        data.mkdirs();
        PerfMessage msg;
        for (int f = 0; f < files; f++) {
            long start = System.currentTimeMillis();
            long currentTime = 0L;
            long sequence = 0L;
            try (BufferedWriter writer = new BufferedWriter(new FileWriter( path + f + ".messages"))) {
                for (int i = 0; i < messages; i++) {
                    currentTime += R.nextDouble() < 0.05 ? 1 : 0;
                    sequence += R.nextInt(5);
                    msg = new PerfMessage(
                            currentTime,
                            sequence,
                            R.nextInt(1_000_000),
                            R.nextBoolean(),
                            IDS[R.nextInt(IDS.length)],
                            R.nextInt() + "_S1",
                            R.nextInt() + "_S2",
                            R.nextInt() + "_S3",
                            R.nextInt() + "_S4");
                    writer.write(MAPPER.writeValueAsString(msg));
                    writer.write('\n');
                }
                writer.flush();
                long duration = System.currentTimeMillis() - start;
                LOG.info("Completed '{}. Took {} ms, tps: {}'", f, duration, messages * 1000 / duration);
            } catch (Exception e) {
                LOG.error("Fail: {}", e.getMessage(), e);
            }

        }

    }


}
