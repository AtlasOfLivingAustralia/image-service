package au.org.ala.images.util;

import com.google.common.io.ByteSink;

import java.io.IOException;

public interface ByteSinkFactory {
    void prepare() throws IOException;
    ByteSink getByteSinkForNames(String... names);
}
