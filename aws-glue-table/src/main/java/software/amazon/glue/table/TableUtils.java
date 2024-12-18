package software.amazon.glue.table;

import com.google.common.annotations.VisibleForTesting;

public class TableUtils {

    @VisibleForTesting
    private TableUtils() {
        throw new IllegalStateException("Utility class");
    }

    protected static boolean validateLowercase(final String name) {
        return name.equals(name.toLowerCase());
    }

}
