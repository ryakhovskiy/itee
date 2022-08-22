package org.kr.intp;


import org.kr.intp.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class IntpMessages {

    private static final String RESOURCE_BUNDLE_NAME = "intp_messages";

    private final Logger log = Logger.getLogger(IntpMessages.class);
    private final ResourceBundle bundle;

    public IntpMessages(Locale locale) {
        bundle = initResourceBundle(locale);
    }

    private ResourceBundle initResourceBundle(Locale locale) {
        try {
            return ResourceBundle.getBundle(RESOURCE_BUNDLE_NAME, locale, new UTF8Control());
        } catch (Exception e) {
            log.error("Error while loading bundle [" + RESOURCE_BUNDLE_NAME + "] for locale [" + locale + "]");
            return null;
        }
    }

    public String getString(String key, String defaultValue) {
        if (null != bundle && bundle.containsKey(key)) {
            return bundle.getString(key);
        } else {
            return defaultValue;
        }
    }

    private class UTF8Control extends ResourceBundle.Control {

        public ResourceBundle newBundle(String baseName, Locale locale, String format,
                                        ClassLoader loader, boolean reload)
                throws IllegalAccessException, InstantiationException, IOException {
            String bundleName = toBundleName(baseName, locale);
            String resourceName = toResourceName(bundleName, "properties");
            ResourceBundle bundle = null;
            InputStream stream;
            if (reload) {
                stream = reloadResource(loader, resourceName);
            } else {
                stream = loader.getResourceAsStream(resourceName);
            }
            if (null == stream)
                return null;

            try {
                bundle = new PropertyResourceBundle(new InputStreamReader(stream, "UTF-8"));
            } finally {
                stream.close();
            }
            return bundle;
        }

        private InputStream reloadResource(ClassLoader loader, String resourceName) throws IOException {
            URL url = loader.getResource(resourceName);
            if (null == url)
                return null;
            URLConnection connection = url.openConnection();
            if (null == connection)
                return null;
            connection.setUseCaches(false);
            return connection.getInputStream();
        }
    }
}
