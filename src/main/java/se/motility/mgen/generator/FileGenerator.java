package se.motility.mgen.generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class FileGenerator {

    private static final ObjectWriter MAPPER = new ObjectMapper()
            .writerFor(PerfMessage.class);

    private static final Random R = new Random(13377331L);

    public static void main(String[] args) {
        int files = args.length != 0 ? Integer.parseInt(args[0]) : 5;
        long currentTime = 0L;
        PerfMessage msg;
        for (int f = 0; f < files; f++) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter( "data/" + f + ".messages"))) {
                for (int i = 0; i < 10_000_000; i++) {
                    currentTime += R.nextDouble() < 0.05 ? 1 : 0;
                    msg = new PerfMessage(
                            currentTime,
                            R.nextLong(), R.nextInt(), R.nextBoolean(),
                            R.nextInt() + "_S1", R.nextInt() + "_S2", R.nextInt() + "_S3", R.nextInt() + "_S4");
                    writer.write(MAPPER.writeValueAsString(msg));
                    writer.newLine();
                }
            } catch (Exception e) {

            }
        }





    }


}
