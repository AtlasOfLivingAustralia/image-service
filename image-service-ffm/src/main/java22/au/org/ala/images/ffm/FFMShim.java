package au.org.ala.images.ffm;

import java.lang.foreign.*;

/**
 * FFM compatibility shim for Java 22 (Stable).
 */
public class FFMShim {
    public static MemorySegment allocateFrom(Arena arena, String str) {
        return arena.allocateFrom(str);
    }

    public static String getString(MemorySegment segment, long offset) {
        return segment.getString(offset);
    }

    public static MemorySegment allocateFrom(Arena arena, ValueLayout.OfDouble layout, double[] array) {
        return arena.allocateFrom(layout, array);
    }
}
