import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * A useful jdbc connection wrapper.
 * See example usage at the bottom of this file.
 *
 * For best results, use "rewriteBatchedStatements=true":
 * Connection c = DriverManager.getConnection("jdbc:mysql://host:3306/db?useServerPrepStmts=false&rewriteBatchedStatements=true", "username", "password");
 *
 * @author Luke Terheyden (terheyden@gmail.com)
 */
public class JdbcConnector {

    private static final Logger log = LoggerFactory.getLogger(JdbcConnector.class);

    private Connection conn;
    private DataSource ds;
    private PreparedStatement ps;
    private ResultSet rs;
    private int argNum = 1;
    private boolean autoCloseConnection;

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    // Opening a connection
    //////////////////////////////////////////////////////////////////////

    /**
     * Specify a Connection we should use for our DB interactions.
     * Creating this obj and setting a connection never throws,
     * so it can be done above the try..catch..finally block.
     * Call closeConnection() in a finally block.
     */
    public void setConnection(Connection conn, boolean autoCloseConnection) {
        this.conn = conn;
        this.autoCloseConnection = autoCloseConnection;
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    // Performing operations
    //////////////////////////////////////////////////////////////////////

    /**
     * Start a SQL statement.
     * Use setString(), setInt(), etc. in builder style,
     * to set prepared statement variables.
     *
     * Use executeQuery() or executeUpdate(), etc., to complete the build statement.
     * This will auto-close any previous results.
     */
    public JdbcConnector setSql(String sql) throws SQLException {

        // Make sure the last result set is closed.
        closeResults();

        try {
            ps = conn.prepareStatement(sql);
            argNum = 1;

        } catch (Exception e) {
            log.error("Exception preparing statement.", e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setString(String arg) throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to set for. Use setSql() first.");
        }

        try {
            ps.setString(argNum, arg);
            argNum++;

        } catch (Exception e) {
            log.error("Exception setting string arg #" + argNum, e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setInt(int arg) throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to set for. Use setSql() first.");
        }

        try {
            ps.setInt(argNum, arg);
            argNum++;

        } catch (Exception e) {
            log.error("Exception setting int arg #" + argNum, e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setLong(long arg) throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to set for. Use setSql() first.");
        }

        try {
            ps.setLong(argNum, arg);
            argNum++;

        } catch (Exception e) {
            log.error("Exception setting long arg #" + argNum, e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setDate(java.util.Date arg) throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to set for. Use setSql() first.");
        }

        try {
            ps.setDate(argNum, new java.sql.Date(arg.getTime()));
            argNum++;

        } catch (Exception e) {
            log.error("Exception setting date arg #" + argNum, e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setDate(DateTime arg) throws SQLException {
        return setDate(arg.toDate());
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setTimestamp(java.util.Date arg) throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to set for. Use setSql() first.");
        }

        try {
            ps.setTimestamp(argNum, new Timestamp(arg.getTime()));
            argNum++;

        } catch (Exception e) {
            log.error("Exception setting timestamp arg #" + argNum, e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Set the next PreparedStatement SQL variable.
     * Only call this after calling setSql() to start a SQL statement builder.
     */
    public JdbcConnector setTimestamp(DateTime arg) throws SQLException {
        return setTimestamp(arg.toDate());
    }

    /**
     * Add a batch statement. Complete with executeAllBatches().
     */
    public JdbcConnector addBatch() throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to execute. Use setSql() first.");
        }

        try {

            ps.addBatch();
            ps.clearParameters();

        } catch (Exception e) {
            log.error("Exception adding to batch.", e);
            closeAndThrow(e);
        }

        return this;
    }

    /**
     * Start a sql query with setSql(), and execute it with this.
     * Set SQL variables using setString(), setInt(), etc.
     *
     * Use next(), getString(), etc. to get results.
     */
    public void executeQuery() throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to execute. Use setSql() first.");
        }

        try {
            rs = ps.executeQuery();
        } catch (Exception e) {
            rs = null;
            log.error("Exception executing query.", e);
            closeAndThrow(e);
        }
    }

    /**
     * Start a sql update with setSql(), then execute it with this.
     * Set SQL variables using setString(), setInt(), etc.
     */
    public void executeUpdate() throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to execute. Use setSql() first.");
        }

        try {

            ps.executeUpdate();

        } catch (Exception e) {
            log.error("Exception executing query.", e);
            closeAndThrow(e);
        }

        closeResults();
    }

    /**
     * Start a sql update with setSql(), add batches, then execute it with this.
     * Set SQL variables using setString(), setInt(), etc.
     */
    public void executeBatch() throws SQLException {

        if (ps == null) {
            closeAndThrow("There is no SQL query to execute. Use setSql() first.");
        }

        try {

            ps.executeBatch();

        } catch (Exception e) {
            log.error("Exception executing query.", e);
            closeAndThrow(e);
        }

        closeResults();
    }

    /**
     * Call after executeQuery().
     * Use getString(), getInt(), etc. while there are results.
     */
    public boolean next() throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get 'next' for. Use executeQuery() first.");
        }

        try {

            boolean res = rs.next();

            // If no more results, go ahead and close stuff.
            if (!res) {
                closeResults();
            }

            return res;

        } catch (Exception e) {
            log.error("Exception fetching next result.", e);
            closeAndThrow(e);
            closeResults();
            return false;
        }
    }

    /**
     * Get results after calling executeQuery().
     */
    public String getString(String colName) throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get from. Use executeQuery() first.");
        }

        try {

            return rs.getString(colName);

        } catch (Exception e) {
            log.error("Exception getting string value of: " + colName, e);
            closeAndThrow(e);
            return null;
        }
    }

    /**
     * Get results after calling executeQuery().
     */
    public int getInt(String colName) throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get from. Use executeQuery() first.");
        }

        try {

            return rs.getInt(colName);

        } catch (Exception e) {
            log.error("Exception getting int value of: " + colName, e);
            closeAndThrow(e);
            return Integer.MIN_VALUE;
        }
    }

    /**
     * Get results after calling executeQuery().
     */
    public long getLong(String colName) throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get from. Use executeQuery() first.");
        }

        try {

            return rs.getLong(colName);

        } catch (Exception e) {
            log.error("Exception getting long value of: " + colName, e);
            closeAndThrow(e);
            return Long.MIN_VALUE;
        }
    }

    /**
     * Get results after calling executeQuery().
     */
    public java.util.Date getDate(String colName) throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get from. Use executeQuery() first.");
        }

        try {

            return rs.getDate(colName);

        } catch (Exception e) {
            log.error("Exception getting date value of: " + colName, e);
            closeAndThrow(e);
            return null;
        }
    }

    /**
     * Get results after calling executeQuery().
     */
    public Timestamp getTimestamp(String colName) throws SQLException {

        if (rs == null) {
            closeAndThrow("There are no results to get from. Use executeQuery() first.");
        }

        try {

            return rs.getTimestamp(colName);

        } catch (Exception e) {
            log.error("Exception getting timestamp value of: " + colName, e);
            closeAndThrow(e);
            return null;
        }
    }

    //////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////
    // Closing a connection
    //////////////////////////////////////////////////////////////////////

    /**
     * Closes the open Connection.
     * Closes any PreparedStatements and ResultSets also.
     * You should always call this in a finally block.
     */
    public void closeConnection() {

        closeResults();

        // DO NOT try to close the connection unless asked to.
        if (conn == null || !autoCloseConnection) {
            return;
        }

        try {
            conn.close();
        } catch (Exception e) {
            // Ignore.
        }

        conn = null;
    }

    /**
     * Closes PreparedStatements and ResultSets,
     * leaving the connection open for further use.
     */
    private void closeResults() {

        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                // It's fine.
            }
            rs = null;
        }

        if (ps != null) {
            try {
                ps.close();
            } catch (Exception e) {
                // Whatever.
            }
            ps = null;
        }
    }

    /**
     * We got a SQL exception.
     * Close results and throw it.
     * This will keep the connection open in case you want to keep going.
     * Remember that closeConnection() should always be called in a finally block...
     */
    private void closeAndThrow(Exception e) throws SQLException {

        closeResults();
        throw new SQLException(e);
    }

    private void closeAndThrow(String errorMsg) throws SQLException {
        closeAndThrow(new RuntimeException(errorMsg));
    }

    //////////////////////////////////////////////////////////////////////
    // Some useful static helper methods.

    public static void closeQuietly(Connection conn) {

        if (conn == null) {
            return;
        }

        try {
            conn.close();
        } catch (Exception e) {
            // Quiet!
        }
    }

    public static void closeQuietly(PreparedStatement ps) {

        if (ps == null) {
            return;
        }

        try {
            ps.close();
        } catch (Exception e) {
            // Quiet!
        }
    }

    public static void closeQuietly(ResultSet rs) {

        if (rs == null) {
            return;
        }

        try {
            rs.close();
        } catch (Exception e) {
            // Quiet!
        }
    }
}

/* === EXAMPLE USAGE ===

    public void jdbcExample() {

        // Creating will never throw, so it can be done outside of the try..catch.
        JdbcConnector conn = new JdbcConnector();

        try {

            conn.setConnection(dbConn, true);
            Set<Integer> userIdSet = new HashSet<Integer>();

            conn.setSql("SELECT * FROM users WHERE name = ?;")
                .setString("Luke")
                .executeQuery();

            // The connection obj can also be used like a ResultSet.
            // When next() == false, the results will automatically be closed.
            while (conn.next()) {
                int userId = conn.getInt("id");
                userIdSet.add(userId);
            }

            // Starting a new SQL statement also auto-closes the last one.
            conn.setSql("DELETE FROM users")
                .executeUpdate();

        } catch (Exception e) {
            log.error("Exception", e);
        } finally {

            // Always finally close the connection.
            conn.closeConnection();
        }
    }

 */
