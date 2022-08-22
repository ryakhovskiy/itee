package org.kr.intp;

import org.junit.Test;

import java.util.Locale;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class IntpMessageTest {

    @Test
    public void testGetString() throws Exception {
        IntpMessages messages = new IntpMessages(Locale.getDefault());
        assertNotNull(messages);
        String value = messages.getString("test" + System.currentTimeMillis(), "default");
        assertEquals(value, "default");
        value = messages.getString("test.key", "default");
        assertEquals(value, "test.value");
        value = messages.getString("test.data", "default");
        assertEquals(value, "some value to be tested");
    }

    @Test
    public void testGetStringRus() {
        Locale locale = new Locale("ru", "RU");
        IntpMessages messages = new IntpMessages(locale);
        assertNotNull(messages);
        String value = messages.getString("test" + System.currentTimeMillis(), "default");
        assertEquals(value, "default");
        value = messages.getString("test.key", "default");
        assertEquals(value, "тест значение");
        value = messages.getString("test.data", "default");
        assertEquals(value, "какое-то значение для теста");
    }
}