package au.org.ala.images.ffm;

import java.lang.foreign.*;

/**
 * FFM compatibility shim for Java 21 (Preview).
 */
public class FFMShim {
    public static MemorySegment allocateFrom(Arena arena, String str) {
        return arena.allocateUtf8String(str);
    }

    public static String getString(MemorySegment segment, long offset) {
        return segment.getUtf8String(offset);
    }

    public static MemorySegment allocateFrom(Arena arena, ValueLayout.OfDouble layout, double[] array) {
        return arena.allocateArray(layout, array);
    }
}
