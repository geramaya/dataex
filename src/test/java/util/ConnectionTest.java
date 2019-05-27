package util;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.util.Map;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultIterable;
import org.junit.Test;

import de.aspera.dataexport.dao.DatabaseException;
import de.aspera.dataexport.util.JDBCConnection;

/**
 * 
 * @author Victoria Schneider
 *
 */
public class ConnectionTest {

	@Test
	public void testConnection() throws DatabaseException {
		Connection connection = JDBCConnection.getConnection("jdbc:mysql://127.0.0.1:3306/slc_dev", "root", "root");
		assertNotNull("No connection with database available", connection);
	}

	@Test
	public void testConnectionJdbi() throws DatabaseException {

		Connection connection = JDBCConnection.getConnection("jdbc:mysql://127.0.0.1:3306/slc_dev", "root", "root");
		Handle handle = Jdbi.open(connection);
		ResultIterable<Map<String, Object>> fooMap = handle.createQuery("SELECT * FROM SLC_USER ").mapToMap();
		handle.close();

	}

}
