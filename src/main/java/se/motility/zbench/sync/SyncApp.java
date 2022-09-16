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
import se.motility.ziploq.api.Sequencer;
import se.motility.ziploq.impl.ZiploqImpl;

public class SyncApp {

    static {
        System.setProperty("log4j.configurationFile", "config/log4j2.xml");
        Locale.setDefault(Locale.ENGLISH);
    }

    public static final Comparator<PerfMessage> COMPARATOR = Comparator
            .comparingLong(PerfMessage::getSequence)
            .thenComparing(PerfMessage::getId)
            .thenComparingLong(PerfMessage::getI);
    public static final char SEMICOLON = ';';
    private static final Logger LOG = LoggerFactory.getLogger(SyncApp.class);
    private static final Marker STAT_MARKER = MarkerFactory.getMarker("STAT");

    public static void main(String... args) {
        long startTime = System.currentTimeMillis();
        int sources = args.length != 0 ? Integer.parseInt(args[0]) : 3;
        int iterations = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        long messages = args.length > 2 ? Long.parseLong(args[2]) : 1_000_000L;
        String path = args.length > 3 ? args[3] : "data/";
        int poolSize = args.length > 4 ? Integer.parseInt(args[4]) : 0;
        int schedulerType = args.length > 5 ? Integer.parseInt(args[5]) : 1;
        int bufSize = args.length > 6 ? Integer.parseInt(args[6]) : 8192;

        LOG.info("Setting up pipeline: {} iterations, {} sources ('{}'), {}, scheduler={}, buffer={}",
                iterations, sources, path, poolSize > 0 ? "pool size: " + poolSize : "dedicated threads",
                schedulerType, bufSize);
        LOG.info(STAT_MARKER, "Start Time;Timestamp;Total messages;Duration;TPS;Checksum;Sources;Iteration;" +
                              "Iterations;Thread Pool;Path;Scheduler;Buffer size;Wait Stats");

        Parameters p = new Parameters(sources, iterations, messages, path, poolSize, schedulerType, startTime, bufSize);
        for (int iter = 0; iter < iterations; iter++) {
            LOG.info("Starting iteration {}...", iter+1);
            Result result = poolSize <= 0
                    ? runWithDedicatedThreads(sources, path, p)
                    : runWithThreadPool(sources, path, poolSize, p);
            if (result != null) {
                LOG.info("Messages: {}, duration: {} ms, tps: {}",
                        result.messages, result.duration, result.messages * 1000 / result.duration);
                LOG.info(STAT_MARKER, formatStats(p, System.currentTimeMillis(), iter, result));
            }
        }
        LOG.info("{} runs completed.", iterations);
    }

    private static Result runWithDedicatedThreads(int sources, String path, Parameters parameters) {
        long messagesPerSource = parameters.messages / sources;
        Ziploq<PerfMessage> ziploq = ZiploqFactory.create(COMPARATOR);
        Reader r = new Reader();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < sources; i++) {
            String file = String.valueOf(i);
            SynchronizedConsumer<PerfMessage> c = ziploq
                    .registerOrdered(parameters.bufferSz, BackPressureStrategy.BLOCK, file);
            Thread t = new Thread(() -> r.readFileStream(path, file, messagesPerSource, c));
            threads.add(t);
        }

        threads.forEach(Thread::start);

        try {
            Result result = process(ziploq);
            for (Thread t : threads) {
                t.join();
            }
            return result;
        } catch (InterruptedException e) {
            LOG.error("Fail: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static Result runWithThreadPool(int sources, String path, int poolSize,
            Parameters parameters) {
        ZiploqFactory.ZiploqServiceBuilder<PerfMessage> ziploq = ZiploqFactory
                .serviceBuilder(COMPARATOR)
                .setSchedulerType(parameters.schedulerType)
                .setPoolSize(poolSize);

        long messagesPerSource = parameters.messages / sources;
        for (int i = 0; i < sources; i++) {
            String file = String.valueOf(i);
            SourceImpl source = new SourceImpl(path, file, messagesPerSource);
            try {
                source.init();
                ziploq.registerOrdered(source, parameters.bufferSz, file);
            } catch (IOException e) {
                LOG.warn("Could not add source '{}' from {}", file, path);
            }
        }

        try {
            return process(ziploq.create());
        } catch (InterruptedException e) {
            LOG.error("Fail: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return null;
        }

    }

    private static Result process(Sequencer<PerfMessage> sequencer) throws InterruptedException {
        long start = System.currentTimeMillis();
        long messages = 0L;
        long checksum = 0L;
        long prev = 0L;
        Entry<PerfMessage> msg;
        while ((msg = sequencer.take()) != Ziploq.<PerfMessage>getEndSignal()) {
            messages++;
            // path-dependent checksum to prevent rearrangements
            checksum += (msg.getMessage().getI() - prev) * (msg.getMessage().getI() - prev);
            prev = msg.getMessage().getI();
        }
        int[] delayStats = ((ZiploqImpl<PerfMessage>) sequencer).delayStats();
        LOG.info("Checksum: {}, Delay stats: {}", checksum, delayStats);
        return new Result(messages, checksum, delayStats, System.currentTimeMillis() - start);
    }

    private static String formatStats(Parameters parameters, long timestamp, int iter, Result result) {
        return format(
                Long.toString(parameters.startTime),
                Long.toString(timestamp),
                Long.toString(result.messages),
                Long.toString(result.duration),
                Long.toString(result.messages * 1000 / result.duration),
                Long.toString(result.checksum),
                Long.toString(parameters.sources),
                Integer.toString(iter + 1),
                Integer.toString(parameters.iterations),
                parameters.threadPool > 0 ? Long.toString(parameters.threadPool) : "0",
                parameters.path,
                Integer.toString(parameters.schedulerType),
                Integer.toString(parameters.bufferSz),
                Arrays.toString(result.delayStats));
    }

    private static String format(String... fields) {
        StringBuilder sb = new StringBuilder();
        boolean init = false;
        for (String f : fields) {
            if (init) {
                sb.append(SEMICOLON);
            }
            sb.append(f);
            init = true;
        }
        return sb.toString();
    }

    private static class Parameters {
        private final int sources;
        private final int iterations;
        private final long messages;
        private final String path;
        private final int threadPool;
        private final long startTime;
        private final int schedulerType;
        private final int bufferSz;
        public Parameters(int sources, int iterations, long messages, String path, int threadPool,
                int schedulerType, long startTime, int bufferSz) {
            this.sources = sources;
            this.iterations = iterations;
            this.messages = messages;
            this.path = path;
            this.threadPool = threadPool;
            this.schedulerType = schedulerType;
            this.startTime = startTime;
            this.bufferSz = bufferSz;
        }
    }

    private static class Result {
        private final long messages;
        private final long checksum;
        private final int[] delayStats;
        private final long duration;
        public Result(long messages, long checksum, int[] delayStats, long duration) {
            this.messages = messages;
            this.checksum = checksum;
            this.delayStats = delayStats;
            this.duration = duration;
        }
    }
}