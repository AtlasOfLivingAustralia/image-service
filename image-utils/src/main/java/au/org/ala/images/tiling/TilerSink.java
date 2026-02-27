package au.org.ala.images.tiling;

import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSink;

import java.io.IOException;

public interface TilerSink {

    LevelSink getLevelSink(int level);

    interface LevelSink {
        ColumnSink getColumnSink(int col, int stripIndex, int _maxColsPerStrip);
    }

    interface ColumnSink {
        ByteSink getTileSink(int row);
    }

    class PathBasedTilerSink implements TilerSink {

        private final ByteSinkFactory byteSinkFactory;

        public PathBasedTilerSink(ByteSinkFactory byteSinkFactory) throws IOException {
            this.byteSinkFactory = byteSinkFactory;
            this.byteSinkFactory.prepare();
        }

        public class LevelSink implements TilerSink.LevelSink {

            private final int level;

            LevelSink(int level) {
                this.level = level;
            }

            @Override
            public ColumnSink getColumnSink(int col, int stripIndex, int _maxColsPerStrip) {
                return new ColumnSink(col, stripIndex, _maxColsPerStrip);
            }

            public class ColumnSink implements TilerSink.ColumnSink {

                private final int col;
                private final int stripIndex;
                private final int maxColsPerStrip;

                ColumnSink(int col, int stripIndex, int maxColsPerStrip) {
                    this.col = col;
                    this.stripIndex = stripIndex;
                    this.maxColsPerStrip = maxColsPerStrip;
                }

                @Override
                public ByteSink getTileSink(int row) {
                    return byteSinkFactory.getByteSinkForNames(Integer.toString(level), Integer.toString(col + (stripIndex * maxColsPerStrip)), row + ".png");
                }
            }
        }

        @Override
        public LevelSink getLevelSink(int level) {
            return new LevelSink(level);
        }
    }
}
