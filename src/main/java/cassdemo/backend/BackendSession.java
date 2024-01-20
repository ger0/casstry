package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.Collections;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect();
			// session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		setupTables(keyspace);
		prepareStatements();
	}

	private static PreparedStatement SELECT_ALL_FROM_LISTS;
	private static PreparedStatement SELECT_ALL_FROM_PROPOSALS;
	private static PreparedStatement INSERT_INTO_LISTS;
	private static PreparedStatement INSERT_INTO_PROPOSALS;
	private static PreparedStatement DELETE_ALL_FROM_LISTS;
	private static PreparedStatement DELETE_ALL_FROM_PROPOSALS;

	private static final String LIST_FORMAT = "- %-10s %-16s\n";
	private static final String PROPOSAL_FORMAT = "-%-10s  %-16s %-10s %-10s\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {

		try {
			SELECT_ALL_FROM_LISTS 		= session.prepare("SELECT * FROM lists;");
			SELECT_ALL_FROM_PROPOSALS 	= session.prepare("SELECT * FROM proposals;");

			INSERT_INTO_LISTS = session
					.prepare("INSERT INTO lists (name, max_size, students)" +
							"VALUES (?, ?, ?);");
			INSERT_INTO_PROPOSALS = session
					.prepare("INSERT INTO proposals (student_id, list_name, placements, sending_time)" +
							"VALUES (?, ?, [], ?);");

			DELETE_ALL_FROM_LISTS 		= session.prepare("TRUNCATE lists;");
			DELETE_ALL_FROM_PROPOSALS 	= session.prepare("TRUNCATE proposals;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	private void setupTables(String keyspace) throws BackendException {
		try {
			session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
					" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }; "
			);
			session.execute("USE " + keyspace + ";");
			session.execute(
					"CREATE TABLE IF NOT EXISTS Lists (" +
							"name varchar," +
							"max_size int," +
							"students list<tuple<int, timestamp>>," +
							"PRIMARY KEY (name)); "
			);
			session.execute(
					"CREATE TABLE IF NOT EXISTS Proposals (" +
							" student_id int,		 " +
							" list_name varchar,	 " +
							" placements list<int>,  " +
							" sending_time timestamp," +
							" PRIMARY KEY(student_id, list_name));"
			);
		} catch (Exception e) {
			throw new BackendException("Failed to initialise tables. " + e.getMessage() + ".", e);
		}

		logger.info("Tables initialised");
	}

	public String selectAllLists() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_LISTS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String name 	= row.getString("name");
			int max_size 	= row.getInt("max_size");
			// List students 	= row.getList("students");

			builder.append(String.format(LIST_FORMAT, name, max_size));
		}

		return builder.toString();
	}
	public String selectAllProposals() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_PROPOSALS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			int student_id = row.getInt("student_id");
			String name = row.getString("list_name");
			String placements = row.getString("placements");
			String timestamp = row.getString("sending_time");

			builder.append(String.format(PROPOSAL_FORMAT, student_id, name, placements, timestamp));
		}

		return builder.toString();
	}

	public void upsertList(String name, int max_size) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_LISTS);
		//bs.bind(name, max_size, "[]");
		bs.bind().setString(0, name).setInt(1, max_size).setList(2, Collections.emptyList());

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert on list. " + e.getMessage() + ".", e);
		}

		logger.info("List " + name + " upserted");
	}

	public void deleteAllLists() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_LISTS);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation on lists. " + e.getMessage() + ".", e);
		}

		logger.info("All lists deleted");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
