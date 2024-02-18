/*
 * Copyright (c) 2021 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.sync;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import se.motility.zbench.generator.PerfMessage;

/**
 * A class for deserializing PerfMessages one-by-one from String to PerfMessage by creating a new {@link JsonParser}
 * for each message (albeit, reusing a single {@link JsonFactory} instance). This is not as efficient as opening
 * an InputStream once and stream through all messages of the file continuously using a single JsonParser.
 *
 * The actual deserialization is done in a streaming fashion by {@link Mapper}, which is somewhat faster than
 * using Jackson annotations, especially since the PerfMessage structure if flat.
 *
 * @author M Tegling
 */
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
