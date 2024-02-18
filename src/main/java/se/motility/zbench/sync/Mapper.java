/*
 * Copyright (c) 2021-2024 Måns Tegling
 *
 * Use of this source code is governed by the MIT license that can be found in the LICENSE file.
 */
package se.motility.zbench.sync;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import se.motility.zbench.generator.PerfMessage;

public abstract class Mapper {

    /**
     * Fast streaming deserializer aware of the message structure, e.g. no nested objects
     * @param parser
     * @return a deserialized {@link PerfMessage}
     * @throws IOException
     */
    public static PerfMessage readPerfMessage(JsonParser parser) throws IOException {
        PerfMessage message = new PerfMessage();

        String field;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            field = parser.getCurrentName();
            switch (field) {
                case "timestamp":
                    parser.nextToken();
                    message.setTimestamp(parser.getLongValue());
                    break;
                case "sequence":
                    parser.nextToken();
                    message.setSequence(parser.getLongValue());
                    break;
                case "i":
                    parser.nextToken();
                    message.setI(parser.getIntValue());
                    break;
                case "b":
                    parser.nextToken();
                    message.setB(parser.getBooleanValue());
                    break;
                case "id":
                    parser.nextToken();
                    message.setId(parser.getText());
                    break;
                case "s1":
                    parser.nextToken();
                    message.setS1(parser.getText());
                    break;
                case "s2":
                    parser.nextToken();
                    message.setS2(parser.getText());
                    break;
                case "s3":
                    parser.nextToken();
                    message.setS3(parser.getText());
                    break;
                case "s4":
                    parser.nextToken();
                    message.setS4(parser.getText());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field: " + field);
            }
        }
        return message;
    }

}