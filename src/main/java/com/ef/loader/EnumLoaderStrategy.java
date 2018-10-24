package com.ef.loader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Enum implementation of the {@link LoaderStrategy} interface.
 */
public enum EnumLoaderStrategy implements LoaderStrategy {

    /**
     * Inserts an entry for each line using simple JDBC insert statements.
     */
    JDBC_SIMPLE {
        @Override
        public int load(Connection connection, String filePath) throws SQLException, IOException {
            String sql = "INSERT INTO parser.log_entries(date, ip, request, status, user_agent) VALUES (?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql); Stream<String> linesStream = Files.lines(Paths.get(filePath))) {

                AtomicInteger count = new AtomicInteger();

                linesStream.forEach(line -> {
                    try {
                        this.setPrepareStatementParameters(preparedStatement, line);
                        preparedStatement.executeUpdate();
                        count.incrementAndGet();
                    }
                    catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                });

                return count.get();
            }
        }
    },

    /**
     * Adds a batch entry for each line using JDBC batch mechanism and the inserts them in one call. Performs better
     * than {@link #JDBC_SIMPLE} loader strategy.
     */
    JDBC_BATCH {
        @Override
        public int load(Connection connection, String filePath) throws SQLException, IOException {
            String sql = "INSERT INTO parser.log_entries(date, ip, request, status, user_agent) VALUES (?, ?, ?, ?, ?)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql); Stream<String> linesStream = Files.lines(Paths.get(filePath))) {

                linesStream.forEach(line -> {
                    try {
                        this.setPrepareStatementParameters(preparedStatement, line);
                        preparedStatement.addBatch();
                    }
                    catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                });

                return Arrays.stream(preparedStatement.executeBatch()).sum();
            }
        }
    },

    /**
     * Loads the whole file using MySQL proprietary (non-standard) <a href=https://dev.mysql.com/doc/refman/8.0/en/load-data.html>LOAD DATA LOCAL INFILE</a>
     * mechanism. Performs around ten times better than {@link #JDBC_SIMPLE} and {@link #JDBC_BATCH} loader strategies.
     */
    MYSQL_LOAD_DATA_INFILE {
        @Override
        public int load(Connection connection, String filePath) throws SQLException, IOException {
            String sql = "LOAD DATA LOCAL INFILE ? INTO TABLE log_entries FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' (date, ip, request, status, user_agent)";

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                preparedStatement.setString(1, filePath);
                return preparedStatement.executeUpdate();
            }
        }
    };


    private static final Logger logger = LogManager.getLogger();
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Sets parameters used by both {@link #JDBC_SIMPLE} and {@link #JDBC_BATCH} strategies.
     *
     * @param preparedStatement PreparedStatement to set parameters to.
     * @param line Line to extract the parameters' value from.
     * @throws SQLException
     */
    protected void setPrepareStatementParameters(PreparedStatement preparedStatement, String line) throws SQLException {
        String[] logEntry = line.replace("\"", "").split("\\|");

        preparedStatement.setObject(1, LocalDateTime.parse(logEntry[0], dateTimeFormatter));
        preparedStatement.setString(2, logEntry[1]);
        preparedStatement.setString(3, logEntry[2]);
        preparedStatement.setInt(4, Integer.valueOf(logEntry[3]));
        preparedStatement.setString(5, logEntry[4]);
    }

    public abstract int load(Connection connection, String accessLog) throws SQLException, IOException;
}
