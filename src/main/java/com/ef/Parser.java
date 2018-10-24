package com.ef;

import com.ef.loader.EnumLoaderStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

/**
 * Parser application which parses a web server access log file, loads the log to MySQL and checks if a given IP makes
 * more than a certain number of requests for the given duration.
 *
 * It contains the entry point along with the parameter objects to hold the Parser parameters' values passed as command
 * line arguments.
 *
 * <a href="https://github.com/remkop/picocli">Picocli</a> library is used to manage command line arguments. It usage
 * offloads the application from arguments type checking, error handling, printing usage information, among others.
 * It implements {@link Runnable} for easing the integration with Picocli.
 *
 * The database connection parameters could be optionally configurable via command-line parameters. The host and port,
 * if not specified, defaults to 'localhost' and '3306', respectively. User and password default to the ones configured
 * in the schema.sql DDL script, 'parser' for both (if others are specified, then a corresponding user and its
 * privileges should be created into the database).
 */
@Command(name = "Parser", version = "1.0", mixinStandardHelpOptions = true)
class Parser implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    @Option(names = "--accesslog", required = true, description = "Access log file full path.")
    private String accessLog;

    @Option(names = "--startDate", required = true, description = "Starting date. Format: yyyy-MM-dd.HH:mm:ss.")
    private String startDate;

    @Option(names = "--duration", required = true, description = "Duration from the start date. Possible values: 'hourly' or 'daily'.")
    private Duration duration;

    @Option(names = "--threshold", required = true, description = "Minimum number of requests to look for. Integer.")
    private int threshold;

    @Option(names = "--loaderStrategy", defaultValue = "MYSQL_LOAD_DATA_INFILE", description = "Log entries loader strategy. Possible values: 'JDBC_SIMPLE', 'JDBC_BATCH' or 'MYSQL_LOAD_DATA_INFILE'.")
    private EnumLoaderStrategy loaderStrategy;

    @Option(names = "--dbHost", defaultValue = "localhost", description = "Database host.")
    private String dbHost;

    @Option(names = "--dbPort", defaultValue = "3306", description = "Database port.")
    private String dbPort;

    @Option(names = "--dbUser", defaultValue = "parser", description = "Database user.")
    private String dbUser;

    @Option(names = "--dbPassword", defaultValue = "parser", description = "Database password.")
    private String dbPassword;


    /**
     * Parser entry point.
     *
     * @param args Parser command-line arguments.
     */
    public static void main(String[] args) {
        CommandLine.run(new Parser(), args);
    }

    /**
     * Parser main logic. It loads the given {@link #accessLog} file with the given {@link #loaderStrategy}. Then, it
     * selects the IPs with more requests than the given {@link #threshold} between the given {@link #startDate} and
     * {@link #duration}. Finally, it logs them, along with the request count, to the console and store them into
     * the 'blocked_ips' table.
     */
    @Override
    public void run() {
        try (Connection connection = this.getConnection(); Statement statement = connection.createStatement()) {
            // Deletes previously stored data from the tables, if any
            statement.execute("TRUNCATE log_entries");
            statement.execute("TRUNCATE blocked_ips");

            // Turns off auto-commit for performance gaining (hence, explicits commits are required)
            connection.setAutoCommit(false);

            logger.info("Loading log entries...");
            Instant startInstant = Instant.now();

            // Loads the given accessLog file with the given connection and loaderStrategy
            int loadedCount = loaderStrategy.load(connection, accessLog);

            // Commits the loaded entries into the 'log_entries' table
            connection.commit();

            // Profiles the loaderStrategy loading time (substring removes unnecessary first two characters of Duration.between.toString output)
            logger.info(String.format("%d log entries were loaded in %s", loadedCount, java.time.Duration.between(startInstant, Instant.now()).toString().substring(2).toLowerCase()));

            String selectSql = "SELECT ip, COUNT(ip) FROM parser.log_entries WHERE date BETWEEN ? AND ? GROUP BY ip HAVING COUNT(ip) > ? ORDER BY COUNT(ip)";

            try (PreparedStatement selectPreparedStatement = connection.prepareStatement(selectSql)) {
                LocalDateTime start = LocalDateTime.parse(startDate, DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss"));
                LocalDateTime end = duration == Duration.hourly ? start.plusHours(1) : start.plusDays(1);

                selectPreparedStatement.setObject(1, start);
                selectPreparedStatement.setObject(2, end);
                selectPreparedStatement.setInt(3, threshold);

                logger.debug(selectPreparedStatement.toString());

                String insertSql = "INSERT INTO parser.blocked_ips(ip, request_count, reason) VALUES (?, ?, ?)";

                // selects the IPs with more requests than the given threshold between the given startDate and duration
                try (ResultSet resultSet = selectPreparedStatement.executeQuery(); PreparedStatement insertPreparedStatement = connection.prepareStatement(insertSql)) {
                    String ip;
                    String reason = duration.name().substring(0, 1).toUpperCase() + duration.name().substring(1) + " request threshold exceeded";
                    int requestCount;

                    while (resultSet.next()) {
                        ip = resultSet.getString(1);
                        requestCount = resultSet.getInt(2);

                        // Logs the selected IPs, along with the request count, to the console
                        logger.info(String.format("IP: %15s | Request count: %4d  ->  %s", ip, requestCount, reason));

                        // Inserts the selected IPs, along with the request count, into the 'blocked_ips' table
                        insertPreparedStatement.setString(1, ip);
                        insertPreparedStatement.setInt(2, requestCount);
                        insertPreparedStatement.setString(3, reason);
                        insertPreparedStatement.executeUpdate();
                    }

                    // Commits the selected blocked IPs entries into the 'blocked_ips' table
                    connection.commit();
                }
            }
        }
        catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Creates a {@link Connection Connection} object with the given user and password, and sets required connection properties:
     * <ul>
     *     <li>useSSL=false: Disables SSL.</li>
     *     <li>allowPublicKeyRetrieval=true: Enabled because it is required for the first run somehow, otherwise it fails.</li>
     * </ul>
     *
     * @see Connection
     * @return A new <code>Connection</code> object.
     * @throws SQLException
     */
    private Connection getConnection() throws SQLException {
        Properties properties = new Properties();
        properties.put("user", dbUser);
        properties.put("password", dbPassword);
        properties.put("useSSL", "false");
        properties.put("allowPublicKeyRetrieval", "true");

        return DriverManager.getConnection(String.format("jdbc:mysql://%s:%s/parser", dbHost, dbPort), properties);
    }

    /**
     * Represents the possible values for the duration command-line parameter.
     */
    enum Duration {
        hourly,
        daily
    }
}
