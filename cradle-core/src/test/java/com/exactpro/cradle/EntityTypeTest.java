package com.exactpro.cradle;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class EntityTypeTest {
    @Test
    public void testFrom() {
        assertEquals(EntityType.from((byte) 1), EntityType.MESSAGE);
        assertEquals(EntityType.from((byte) 2), EntityType.EVENT);
    }
}