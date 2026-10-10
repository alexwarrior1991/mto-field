package com.alejandro.mtofield.configuration.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * Firma los BYTES que de verdad viajan en un mensaje que este servicio publica: la contraparte de
 * {@link MessagePayloadSignatureVerifier}, con el mismo secreto y los mismos nombres de algoritmo.
 *
 * <p>El {@code messageHash} que va dentro del payload se calcula sobre el objeto antes de
 * serializarlo, asi que un consumidor no puede recomprobarlo sin reserializar, y esa ida y vuelta
 * no conserva la identidad ({@code 1.50} vuelve como {@code 1.5}). Firmando los bytes recibidos no
 * hay nada que reserializar: el consumidor calcula sobre exactamente lo mismo que calculo el
 * emisor. Por eso la firma viaja en una cabecera y no dentro del payload.</p>
 *
 * <p>Con secreto es HMAC-SHA256 y protege de manipulacion. Sin secreto es un SHA-256 simple: detecta
 * corrupcion pero no manipulacion, porque quien altere el mensaje puede recalcularlo. El algoritmo
 * usado viaja en su propia cabecera para que el consumidor no tenga que adivinarlo.</p>
 */
public class MessagePayloadSignature {

    public static final String HEADER_SIGNATURE = "messageSignature";
    public static final String HEADER_SIGNATURE_ALGORITHM = "messageSignatureAlgorithm";

    // Los mismos nombres que el verificador de los consumidores (MessagePayloadSignatureVerifier en
    // mto-notification): aqui solo se firma, asi que viven en el firmante.
    static final String HMAC_ALGORITHM = "HmacSHA256";
    static final String DIGEST_ALGORITHM = "SHA-256";
    static final String HMAC_ALGORITHM_HEADER_VALUE = "HMAC-SHA256";
    static final String DIGEST_ALGORITHM_HEADER_VALUE = "SHA-256";

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagePayloadSignature.class);

    private final MessageSignatureProperties properties;

    public MessagePayloadSignature(MessageSignatureProperties properties) {
        this.properties = properties;

        if (!properties.hasSecret()) {
            LOGGER.info("Messages are signed with plain {}, which detects corruption but not tampering. "
                    + "Set app.messaging.signature.secret (the same value as in the consumers) to use HMAC.",
                    DIGEST_ALGORITHM);
        }
    }

    /** Nombre del algoritmo, tal y como viaja en la cabecera y lo espera el verificador. */
    public String algorithm() {
        return properties.hasSecret()
                ? HMAC_ALGORITHM_HEADER_VALUE
                : DIGEST_ALGORITHM_HEADER_VALUE;
    }

    public String sign(byte[] payload) {
        if (payload == null) {
            throw new IllegalArgumentException("The payload to sign cannot be null");
        }

        return properties.hasSecret() ? hmac(payload) : digest(payload);
    }

    public String sign(String payload) {
        return sign(payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Comparacion en tiempo constante: comparar firmas con equals filtra informacion. */
    public boolean verify(byte[] payload, String signature) {
        if (payload == null || signature == null) {
            return false;
        }

        return MessageDigest.isEqual(
                sign(payload).getBytes(StandardCharsets.UTF_8),
                signature.getBytes(StandardCharsets.UTF_8));
    }

    private String hmac(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(
                    properties.secret().getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM));
            return HexFormat.of().formatHex(mac.doFinal(payload));
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot sign the message", exception);
        }
    }

    private String digest(byte[] payload) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance(DIGEST_ALGORITHM).digest(payload));
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot compute the message digest", exception);
        }
    }
}
