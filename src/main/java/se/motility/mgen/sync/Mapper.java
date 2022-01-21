package se.motility.mgen.sync;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import se.motility.mgen.generator.PerfMessage;

public abstract class Mapper {

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