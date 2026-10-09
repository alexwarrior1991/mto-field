package com.alejandro.mtofield.application.mapper;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Los mensajes del contrato tal como se guardan: el JSON canonico de protobuf. Lo que se reproduce
 * en una reanudacion es exactamente lo que se envio; lo desconocido se ignora al leer, para que un
 * campo nuevo en el contrato no rompa lo ya guardado.
 */
public final class ProtoJson {

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser().ignoringUnknownFields();

    private ProtoJson() {
    }

    public static String print(Message message) {
        try {
            return PRINTER.print(message);
        } catch (InvalidProtocolBufferException exception) {
            throw new IllegalArgumentException("Cannot print " + message.getClass().getSimpleName() + " as JSON", exception);
        }
    }

    public static <B extends Message.Builder> B parse(String json, B builder) {
        try {
            PARSER.merge(json, builder);
            return builder;
        } catch (InvalidProtocolBufferException exception) {
            throw new IllegalArgumentException("Stored JSON is not a valid " + builder.getDescriptorForType().getName(), exception);
        }
    }
}
