JdbcConnector
=============

A useful java class for working with JDBC.  
Makes your code cleaner and easier to use.  
Handles opening and closing statements and things for you.

Let's look at an example:

```java
    /**
     * A typical JDBC example. Verbose object / connection tracking
     * with lots of extraneous try..catches required.
     */
    public void jdbcExample() {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {

            conn = getConnection();
            ps = conn.prepareStatement("INSERT INTO users (ID, name, age, last_modified) VALUES (?, ?, ?, NOW());");

            ps.setString(1, "1234");
            ps.setString(2, "Luke");
            ps.setInt(3, 29);
            ps.executeUpdate();

            ps = conn.prepareStatement("SELECT age FROM users WHERE name = ?");
            ps.setString(1, "Luke");
            rs = ps.executeQuery();

            if (!rs.next()) {
                throw new RuntimeException("No row found!");
            }

            log.info(rs.getString("name") + " is " + rs.getInt("age") + " years old.");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                // Ignore exceptions while trying to close connections.
            }
        }
    }

    /**
     * The difference when using JdbcConnector is you don't have to
     * worry about tracking and closing objects, or excessive try..catch blocks.
     */
    public void jdbcConnectorExample() {

        // Creating will never throw, so it can be done outside of the try..catch.
        JdbcConnector conn = new JdbcConnector();

        try {

            conn.setConnection(getConnection(), true);

            conn.setSql("INSERT INTO users (ID, name, age, last_modified) VALUES (?, ?, ?, NOW());")
                .setString("1234")
                .setString("Luke")
                .setInt(29)
                .executeUpdate();

            conn.setSql("SELECT age FROM users WHERE name = ?")
                .setString("Luke")
                .executeQuery();

            if (!conn.next()) {
                throw new RuntimeException("No row found!");
            }

            log.info(conn.getString("name") + " is " + conn.getInt("age") + " years old.");

        } catch (Exception e) {
            log.error("Exception", e);
        } finally {

            // Always finally close the connection.
            conn.closeConnection();
        }
    }
```
