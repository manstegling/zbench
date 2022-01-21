package se.motility.mgen.sync;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import se.motility.mgen.generator.PerfMessage;

public class Deserializer {

    private final JsonFactory factory = new JsonFactory();

    public PerfMessage readMessage(String json) {
        try (JsonParser parser = factory.createParser(json)) {
          return Mapper.readPerfMessage(parser);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalStateException("Could not read: " + json);
    }

}
