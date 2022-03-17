package se.motility.zbench.sync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import se.motility.zbench.generator.PerfMessage;
import se.motility.ziploq.api.BackPressureStrategy;
import se.motility.ziploq.api.Entry;
import se.motility.ziploq.api.SynchronizedConsumer;
import se.motility.ziploq.api.Ziploq;
import se.motility.ziploq.api.ZiploqFactory;
import se.motility.ziploq.impl.WaitStrategy;
import se.motility.ziploq.impl.ZiploqImpl;

public class SyncApp {

    static {
        System.setProperty("log4j.configurationFile", "config/log4j2.xml");
        Locale.setDefault(Locale.ENGLISH);
    }

    public static final char SEMICOLON = ';';
    public static final Comparator<PerfMessage> COMPARATOR = Comparator
            .comparingLong(PerfMessage::getSequence)
            .thenComparing(PerfMessage::getId);
    private static final Logger LOG = LoggerFactory.getLogger(SyncApp.class);
    private static final Marker STAT_MARKER = MarkerFactory.getMarker("STAT");

    public static void main(String... args) {
        long startTime = System.currentTimeMillis();
        int sources = args.length != 0 ? Integer.parseInt(args[0]) : 3;
        int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        String path = args.length > 2 ? args[2] : "data/";
        int threadPool = args.length > 3 ? Math.max(Integer.parseInt(args[3]), 0) : 0;
        WaitStrategy.Mode waitMode = args.length > 4 ? WaitStrategy.Mode.valueOf(args[4]) : WaitStrategy.Mode.BACKOFF;

        LOG.info("Setting up pipeline: {} iterations, {} sources ('{}'), {}",
                iterations, sources, path, threadPool > 0 ? "thread pool " + threadPool : "individual threads");
        LOG.info(STAT_MARKER, "Start Time;Timestamp;Total messages;Duration;TPS;Checksum;Sources;Iterations;Thread Pool;Path;WaitMode;Wait Stats");

        Parameters p = new Parameters(sources, iterations, path, threadPool, waitMode, startTime);
        for (int iter = 0; iter < iterations; iter++) {
            LOG.info("Starting iteration {}...", iter+1);
            if (threadPool <= 0) {
                if (waitMode != WaitStrategy.Mode.BACKOFF) {
                    LOG.warn("Only BACKOFF is implemented for thread-per-source. Ignoring strategy {}", waitMode);
                }
                runThreadPerSource(sources, path, p);
            } else {
                runWithThreadPool(sources, path, threadPool, p);
            }

        }
        LOG.info("{} runs completed.", iterations);
    }

    private static void runThreadPerSource(int sources, String path, Parameters parameters) {
        Ziploq<PerfMessage> ziploq = ZiploqFactory.create(COMPARATOR);
        Reader r = new Reader();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < sources; i++) {
            String file = String.valueOf(i);
            SynchronizedConsumer<PerfMessage> c = ziploq
                    .registerOrdered(8196, BackPressureStrategy.BLOCK, file);
            Thread t = new Thread(() -> r.readFileStream(path, file, c));
            threads.add(t);
        }

        threads.forEach(Thread::start);

        long start = System.currentTimeMillis();
        try {
            Result result = process(ziploq);
            for (Thread t : threads) {
                t.join();
            }
            long duration = System.currentTimeMillis() - start;
            LOG.info("Messages: {}, duration: {} ms, tps: {}", result.messages, duration, result.messages * 1000 / duration);
            LOG.info(STAT_MARKER, formatStats(parameters, System.currentTimeMillis(), duration, result));
        } catch (InterruptedException e) {
            LOG.error("Fail: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    private static void runWithThreadPool(int sources, String path, int threadPool,
            Parameters parameters) {
        ZiploqFactory.ZiploqServiceBuilder<PerfMessage> ziploq = ZiploqFactory
                .serviceBuilder(COMPARATOR)
                .setWaitMode(parameters.waitMode)
                .setThreads(threadPool);

        for (int i = 0; i < sources; i++) {
            String file = String.valueOf(i);
            SourceImpl source = new SourceImpl(path, file);
            try {
                source.init();
                ziploq.registerOrdered(source, 8196, file);
            } catch (IOException e) {
                LOG.warn("Could not add source '{}' from {}", file, path);
            }
        }

        long start = System.currentTimeMillis();
        try {
            Result result = process(ziploq.create());
            long duration = System.currentTimeMillis() - start;
            LOG.info("Messages: {}, duration: {} ms, tps: {}", result.messages, duration, result.messages * 1000 / duration);
            LOG.info(STAT_MARKER, formatStats(parameters, System.currentTimeMillis(), duration, result));
        } catch (InterruptedException e) {
            LOG.error("Fail: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }

    }

    private static Result process(Ziploq<PerfMessage> ziploq) throws InterruptedException {
        long messages = 0L;
        long checksum = 0L;
        long prev = 0L;
        Entry<PerfMessage> msg;
        while ((msg = ziploq.take()) != Ziploq.<PerfMessage>getEndSignal()) {
            messages++;
            // path-dependent checksum to prevent rearrangements
            checksum += (msg.getMessage().getI() - prev) * (msg.getMessage().getI() - prev);
            prev = msg.getMessage().getI();
        }
        int[] delayStats = ((ZiploqImpl<PerfMessage>) ziploq).delayStats();
        LOG.info("Checksum: {}, Delay stats: {}", checksum, delayStats);
        return new Result(messages, checksum, delayStats);
    }

    private static String formatStats(Parameters parameters, long timestamp, long duration, Result result) {
        return String.valueOf(parameters.startTime) +
               SEMICOLON +
               timestamp +
               SEMICOLON +
               result.messages +
               SEMICOLON +
               duration +
               SEMICOLON +
               result.messages * 1000 / duration +
               SEMICOLON +
               result.checksum +
               SEMICOLON +
               parameters.sources +
               SEMICOLON +
               parameters.iterations +
               SEMICOLON +
               parameters.threadPool +
               SEMICOLON +
               parameters.path +
               SEMICOLON +
               parameters.waitMode +
               SEMICOLON +
               Arrays.toString(result.delayStats);
    }

    private static class Parameters {
        private final int sources;
        private final int iterations;
        private final String path;
        private final int threadPool;
        private final WaitStrategy.Mode waitMode;
        private final long startTime;
        public Parameters(int sources, int iterations, String path, int threadPool,
                WaitStrategy.Mode waitMode, long startTime) {
            this.sources = sources;
            this.iterations = iterations;
            this.path = path;
            this.threadPool = threadPool;
            this.waitMode = waitMode;
            this.startTime = startTime;
        }
    }

    private static class Result {
        private final long messages;
        private final long checksum;
        private final int[] delayStats;
        public Result(long messages, long checksum, int[] delayStats) {
            this.messages = messages;
            this.checksum = checksum;
            this.delayStats = delayStats;
        }
    }
}