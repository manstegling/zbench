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

    public static final Comparator<PerfMessage> COMPARATOR = Comparator.comparingLong(PerfMessage::getL);
    private static final Logger LOG = LoggerFactory.getLogger(SyncApp.class);

    public static void main(String... args) {
        int sources = args.length != 0 ? Integer.parseInt(args[0]) : 3;
        LOG.info("Setting up pipeline to process data from {} sources", sources);

//        for (int iter = 0; iter < 5; iter++) {

            Ziploq<PerfMessage> ziploq = ZiploqFactory.create(Optional.of(COMPARATOR));
            Reader r = new Reader();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < sources; i++) {
                String file = String.valueOf(i);
                SynchronizedConsumer<PerfMessage> c = ziploq
                        .registerOrdered(8196, BackPressureStrategy.BLOCK, file);
                Thread t = new Thread(() -> r.readFullStream(file, c));
                threads.add(t);
            }

            LOG.info("Starting to process data...");
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
//        }

    }

    private static long process(Ziploq<PerfMessage> ziploq) throws InterruptedException {
        long messages = 0L;
        long checksum = 0L;
        Entry<PerfMessage> msg;
        Entry<PerfMessage> end = Ziploq.getEndSignal();
        while ((msg = ziploq.take()) != end) {
            messages++;
            checksum += msg.getMessage().getI();
        }
        LOG.info("Checksum: {}", checksum);
        return messages;
    }


}