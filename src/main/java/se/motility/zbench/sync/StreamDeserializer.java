/*
 * Copyright (c) 2021-2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.sync;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import se.motility.zbench.generator.PerfMessage;
import se.motility.ziploq.api.SynchronizedConsumer;

public class StreamDeserializer {

    private final JsonFactory factory = new JsonFactory();
    private final SynchronizedConsumer<PerfMessage> consumer;

    public StreamDeserializer(SynchronizedConsumer<PerfMessage> consumer) {
        this.consumer = consumer;
    }

    // Uses a single JsonParser instance for the whole file,
    // deserializing in a highly efficient streaming fashion
    public void readMessages(Reader reader) {
        //TODO: Proper exception handling
        PerfMessage message;
        try (JsonParser parser = factory.createParser(reader)) {
            while (parser.nextToken() != null) {
                message = Mapper.readPerfMessage(parser);
                consumer.onEvent(message, message.getTimestamp());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readMessages(InputStream input) {
        PerfMessage message;
        try (JsonParser parser = factory.createParser(input)) {
            while (parser.nextToken() != null) {
                message = Mapper.readPerfMessage(parser);
                consumer.onEvent(message, message.getTimestamp());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Variation of #readMessages in which Jackson manages the underlying resource.
    // Used for performance testing but not desired for production. Fortunately,
    // this does not really improve performance over using an InputStream.
    public void readFile(String filename, long maxMessages) {
        long messages = 0L;
        PerfMessage message;
        try (JsonParser parser = factory.createParser(new File(filename))) {
            while (messages++ < maxMessages && parser.nextToken() != null) {
                message = Mapper.readPerfMessage(parser);
                consumer.onEvent(message, message.getTimestamp());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
