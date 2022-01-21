package se.motility.mgen.sync;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.motility.mgen.generator.PerfMessage;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;

public class SyncApp {

    static {
        System.setProperty("log4j.configurationFile", "config/log4j2.xml");
        Locale.setDefault(Locale.ENGLISH);
    }

    public static final Comparator<PerfMessage> COMPARATOR = Comparator
            .comparingLong(PerfMessage::getSequence)
            .thenComparing(PerfMessage::getId);
    private static final Logger LOG = LoggerFactory.getLogger(SyncApp.class);

    public static void main(String... args) {
        int sources = args.length != 0 ? Integer.parseInt(args[0]) : 3;
        int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String path = args.length > 2 ? args[2] : "data/";

        LOG.info("Setting up pipeline: {} runs with {} sources from '{}'", iterations, sources, path);
        for (int iter = 0; iter < iterations; iter++) {

            Ziploq<PerfMessage> ziploq = ZiploqFactory.create(Optional.of(COMPARATOR));
            Reader r = new Reader();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < sources; i++) {
                String file = String.valueOf(i);
                SynchronizedConsumer<PerfMessage> c = ziploq
                        .registerOrdered(8196, BackPressureStrategy.BLOCK, file);
                Thread t = new Thread(() -> r.readFileStream(path, file, c));
                threads.add(t);
            }

            LOG.info("Starting run {}...", iter+1);
            threads.forEach(Thread::start);

            long start = System.currentTimeMillis();
            try {
                long messages = process(ziploq);
                for (Thread t : threads) {
                    t.join();
                }
                long duration = System.currentTimeMillis() - start;
                LOG.info("Messages: {}, duration: {} ms, tps: {}", messages, duration, messages * 1000 / duration);
            } catch (InterruptedException e) {
                LOG.error("Fail: {}", e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("{} runs completed.", iterations);
    }

    private static long process(Ziploq<PerfMessage> ziploq) throws InterruptedException {
        long messages = 0L;
        long checksum = 0L;
        long prev = 0L;
        Entry<PerfMessage> msg;
        Entry<PerfMessage> end = Ziploq.getEndSignal();
        while ((msg = ziploq.take()) != end) {
            messages++;
            // path-dependent checksum to prevent rearrangements
            checksum += (msg.getMessage().getI() - prev) * (msg.getMessage().getI() - prev);
            prev = msg.getMessage().getI();
        }
        LOG.info("Checksum: {}", checksum);
        return messages;
    }


}