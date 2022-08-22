package org.kr.intp.lgenerator;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by kr on 01.04.2014.
 */
public final class LicenseGenerator {

    private LicenseGenerator() {}

    public static String generate(String instanceId, String name, char type, int intpSize,  long expiration,
                                  int tablesCount, String hardwareKey, String systemNo) throws IOException {
        final LicenseGenerator licenseGenerator = new LicenseGenerator();
        final Map<String, String> data = new HashMap<>();

        data.put("01", hardwareKey);
        data.put("02", systemNo);
        data.put("03", instanceId);
        data.put("04", String.valueOf(intpSize));
        data.put("05", String.valueOf(type));
        data.put("06", name);
        data.put("07", String.valueOf(expiration));
        data.put("08", String.valueOf(tablesCount));
        data.putAll(generateRandomParams(data.size()));
        return licenseGenerator.generate(data);
    }

    private static final Base64.Encoder encoder = Base64.getEncoder();
    private static final Random random = ThreadLocalRandom.current();

    private static Map<String, String> generateRandomParams(int startIndex) {
        final int count = random.nextInt(10) + 10;
        final Map<String, String> randomData = new HashMap<>(count);
        final byte[] data = new byte[30];
        for (int i = 0; i < count; i++) {
            final String key = padRight(String.valueOf(startIndex + i), 1);
            random.nextBytes(data);
            randomData.put(key, String.valueOf(random.nextDouble()) + encoder.encodeToString(data));
        }
        return randomData;
    }

    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    private String generate(Map<String, String> data) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final LicenseCrypter licenseCrypter = LicenseCrypter.newInstance();
        final String info = objectMapper.writeValueAsString(data);
        final String encrypted = licenseCrypter.encryptData(info);
        final StringBuilder license = new StringBuilder();
        for (int i = 0; i < encrypted.length(); i++) {
            license.append(encrypted.charAt(i));
            if (i > 0 && i % 80 == 0)
                license.append("\n");
        }
        return license.toString();
    }
}