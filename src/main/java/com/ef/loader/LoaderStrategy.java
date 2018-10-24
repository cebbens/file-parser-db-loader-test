package com.ef.loader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for loader strategy implementations.
 */

public interface LoaderStrategy {

    /**
     * Loads the file content of the given filePath using the given {@link Connection} into a database table.
     *
     * @param connection Connection to the database.
     * @param filePath File path to load.
     * @return Loaded entry count.
     * @throws SQLException
     * @throws IOException
     */
    int load(Connection connection, String filePath) throws SQLException, IOException;
}
