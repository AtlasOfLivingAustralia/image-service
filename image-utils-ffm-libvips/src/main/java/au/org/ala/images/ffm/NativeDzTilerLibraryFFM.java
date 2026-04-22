package au.org.ala.images.ffm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * FFM wrapper for the native deep-zoom tiler bridge.
 */
public class NativeDzTilerLibraryFFM {

    private static final Logger log = LoggerFactory.getLogger(NativeDzTilerLibraryFFM.class);

    @FunctionalInterface
    public interface TileCallback {
        int invoke(int level, int x, int y, String contentType, MemorySegment data, long length, MemorySegment userData);
    }

    private static final FunctionDescriptor FD_TILES_FROM_SOURCE = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS
    );

    private static final FunctionDescriptor FD_TILE_CALLBACK = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,
            ValueLayout.ADDRESS,
            ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS
    );

    private static final FunctionDescriptor FD_FREE_ERROR = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

    private final Linker linker;
    private final MethodHandle tilesFromSource;
    private final MethodHandle freeError;

    public NativeDzTilerLibraryFFM(SymbolLookup bridgeLookup) {
        this.linker = Linker.nativeLinker();

        MemorySegment tilesSymbol = bridgeLookup.find("ala_vips_google_tms_tiles_from_source")
                .or(() -> bridgeLookup.find("ala_vips_google_tms_tiles_from_file"))
                .orElseThrow(() -> new UnsatisfiedLinkError("Could not find native dz bridge entrypoint symbol"));
        MemorySegment freeErrorSymbol = bridgeLookup.find("ala_vips_google_tms_free_error")
                .orElseThrow(() -> new UnsatisfiedLinkError("Could not find native dz bridge free-error symbol"));

        this.tilesFromSource = linker.downcallHandle(tilesSymbol, FD_TILES_FROM_SOURCE);
        this.freeError = linker.downcallHandle(freeErrorSymbol, FD_FREE_ERROR);
    }

    public void alaVipsGoogleTmsTilesFromSource(
            MemorySegment inputSource,
            int[] subsamples,
            int levelCount,
            int tileSize,
            int minLevel,
            int maxLevel,
            String suffix,
            int jpegQuality,
            int pngCompression,
            TileCallback callback,
            MemorySegment userData
    ) throws IOException {
        try (Arena callArena = Arena.ofConfined()) {
            MemorySegment subsamplesSegment = FFMShim.allocateArray(callArena, ValueLayout.JAVA_INT, subsamples);
            MemorySegment suffixSegment = suffix != null ? FFMShim.allocateFrom(callArena, suffix) : MemorySegment.NULL;
            MemorySegment errorOut = callArena.allocate(ValueLayout.ADDRESS);
            errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            CallbackAdapter callbackAdapter = new CallbackAdapter(callback);
            MethodHandle callbackHandle = MethodHandles.lookup()
                    .findVirtual(
                            CallbackAdapter.class,
                            "invoke",
                            MethodType.methodType(
                                    int.class,
                                    int.class,
                                    int.class,
                                    int.class,
                                    MemorySegment.class,
                                    MemorySegment.class,
                                    long.class,
                                    MemorySegment.class
                            )
                    )
                    .bindTo(callbackAdapter);
            MemorySegment callbackStub = linker.upcallStub(callbackHandle, FD_TILE_CALLBACK, callArena);

            int rc = (int) tilesFromSource.invokeExact(
                    inputSource,
                    subsamplesSegment,
                    levelCount,
                    tileSize,
                    minLevel,
                    maxLevel,
                    suffixSegment,
                    jpegQuality,
                    pngCompression,
                    callbackStub,
                    userData,
                    errorOut
            );

            if (rc != 0) {
                MemorySegment errPtr = errorOut.get(ValueLayout.ADDRESS, 0);
                if (errPtr != null && errPtr.address() != 0) {
                    String msg = FFMShim.getString(errPtr, 0);
                    freeError.invokeExact(errPtr);
                    throw new IOException(msg);
                }
                throw new IOException("native dz tiler failed");
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("native dz tiler bridge invocation failed", t);
        }
    }

    private static class CallbackAdapter {
        private final TileCallback callback;

        private CallbackAdapter(TileCallback callback) {
            this.callback = callback;
        }

        @SuppressWarnings("unused")
        private int invoke(int level, int x, int y, MemorySegment contentType, MemorySegment data, long length, MemorySegment userData) {
            try {
                String contentTypeString = contentType != null && contentType.address() != 0
                        ? FFMShim.getString(contentType, 0)
                        : "application/octet-stream";
                return callback.invoke(level, x, y, contentTypeString, data, length, userData);
            } catch (RuntimeException e) {
                log.error("Native tile callback failed level={}/x={}/y={}", level, x, y, e);
                return -1;
            }
        }
    }
}

