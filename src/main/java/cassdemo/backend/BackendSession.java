package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import cassdemo.Statistics;
import cassdemo.ToStringer;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private Statistics statistics;

	private Session session;

	public BackendSession(String contactPoint, String keyspace, Statistics statistics) throws BackendException {
		this.statistics = statistics;

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
	private static PreparedStatement SELECT_FROM_PROPOSALS;
	private static PreparedStatement INSERT_INTO_LISTS;
	private static PreparedStatement INSERT_INTO_PROPOSALS;
	private static PreparedStatement DELETE_ALL_FROM_LISTS;
	private static PreparedStatement DELETE_ALL_FROM_PROPOSALS;
	private static PreparedStatement INCLUDE_PROPOSAL_INTO_LIST;
	private static PreparedStatement SELECT_OCCUPIER;
	private static PreparedStatement SELECT_ALL_PROPOSALS_TO_LIST;

	private void prepareStatements() throws BackendException {

		try {
			SELECT_ALL_FROM_LISTS = session.prepare("SELECT * FROM lists;");
			SELECT_ALL_FROM_PROPOSALS = session.prepare("SELECT * FROM proposals;");
			SELECT_FROM_PROPOSALS = session.prepare("SELECT * FROM proposals where student_id = ? and list_name = ?;");

			INSERT_INTO_LISTS = session
					.prepare("INSERT INTO lists (name, max_size, students, timestamps)" +
							"VALUES (?, ?, ?, ?);");
			INSERT_INTO_PROPOSALS = session
					.prepare("INSERT INTO proposals (student_id, list_name, placements, sending_time)" +
							"VALUES (?, ?, ?, ?) IF NOT EXISTS;");

			DELETE_ALL_FROM_LISTS = session.prepare("TRUNCATE lists;");
			DELETE_ALL_FROM_PROPOSALS = session.prepare("TRUNCATE proposals;");

			INCLUDE_PROPOSAL_INTO_LIST = session
					.prepare(
							"UPDATE lists set students[?] = ?, timestamps[?]= ? where name = ? if timestamps[?] > ?;");

			SELECT_OCCUPIER = session.prepare("SELECT students[?] as student FROM lists where name = ?;");

			SELECT_ALL_PROPOSALS_TO_LIST = session
					.prepare("select student_id from proposals where list_name = ? ALLOW FILTERING;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	private void setupTables(String keyspace) throws BackendException {
		try {
			session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
					" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }; ");
			session.execute("USE " + keyspace + ";");
			session.execute(
					"CREATE TABLE IF NOT EXISTS Lists (" +
							"name varchar," +
							"max_size int," +
							"students map<int, int>," +
							"timestamps map<int, timestamp>, " +
							"PRIMARY KEY (name)); ");
			session.execute(
					"CREATE TABLE IF NOT EXISTS Proposals (" +
							" student_id int,		 " +
							" list_name varchar,	 " +
							" placements list<int>,  " +
							" sending_time timestamp," +
							" PRIMARY KEY(student_id, list_name));");
		} catch (Exception e) {
			throw new BackendException("Failed to initialise tables. " + e.getMessage() + ".", e);
		}

		logger.info("Tables initialised");
	}

	public String selectAllLists() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_LISTS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return ToStringer.listsToString(rs);
	}

	public String selectAllProposals() throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_PROPOSALS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		return ToStringer.proposalsToString(rs);
	}

	private Row selectFromProposals(int student_id, String listName) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_FROM_PROPOSALS);
		bs.bind().setInt(0, student_id).setString(1, listName);

		ResultSet rs = null;
		Row row = null;
		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		row = rs.one();
		return row;
	}

	public void insertList(String name, int max_size) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_LISTS);
		// bs.bind(name, max_size, "[]");
		bs.bind().setString(0, name).setInt(1, max_size).setMap(2, initialStudentsMap(max_size)).setMap(3,
				initialTimestampMap(max_size));

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert on list. " + e.getMessage() + ".", e);
		}

		logger.info("List " + name + " upserted");
	}

	public void insertProposal(int studentId, String listName, List<Integer> placements) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_PROPOSALS);
		// bs.bind(name, max_size, "[]");
		Date timestamp = new Date(System.currentTimeMillis());
		includeProposalIntoList(studentId, listName, placements, timestamp);
		bs.bind().setInt(0, studentId).setString(1, listName).setList(2, placements).setTimestamp(3, timestamp);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert on list. " + e.getMessage() + ".", e);
		}

		logger.info("Student: " + Integer.toString(studentId) + " made proposal into: " + listName);
	}

	public void includeProposalIntoList(int student_id, String listName, List<Integer> placements, Date timestamp)
			throws BackendException {
		BoundStatement bs = new BoundStatement(INCLUDE_PROPOSAL_INTO_LIST);
		for (int placement : placements) {
			Integer replaced = selectOccupier(listName, placement);
			if (replaced != null) {
				if (replaced == student_id) {
					return; // this student already holds this place
				}
			}
			bs.bind().setInt(0, placement).setInt(1, student_id).setInt(2, placement).setTimestamp(3, timestamp)
					.setString(4, listName).setInt(5, placement).setTimestamp(6, timestamp);
			ResultSet rs = null;
			try {
				rs = session.execute(bs);
			} catch (Exception e) {
				throw new BackendException("Could not include proposal. " + e.getMessage() + ".", e);
			}
			for (Row row : rs) {
				if (row.getBool("[applied]")) {
					if (replaced != null) {
						reapplyProposal(replaced, listName);
						statistics.increasePreemptionCount();
					}
					return;
				}
			}
		}
		logger.info("Applied " + student_id + "'s proposal into " + listName);
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

	public ResultSet selectAllProposalsToList(String listName) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_ALL_PROPOSALS_TO_LIST);
		bs.bind().setString(0, listName);
		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}
		return rs;
	}

	public void reapplyProposalsToOneList(String listName) throws BackendException {
		ResultSet rs = selectAllProposalsToList(listName);
		for (Row row : rs) {
			reapplyProposal(row.getInt("student_id"), listName);
		}
	}

	public void reapplyProposal(int student_id, String listName) throws BackendException {
		Row proposal = selectFromProposals(student_id, listName);
		if (proposal != null) {
			includeProposalIntoList(student_id, listName, proposal.getList("placements", Integer.class),
					proposal.getTimestamp("sending_time"));
		}
	}

	private Integer selectOccupier(String listName, int listPlace) throws BackendException {
		BoundStatement bs = new BoundStatement(SELECT_OCCUPIER);
		bs.bind().setInt(0, listPlace).setString(1, listName);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		Integer ret = null;
		for (Row row : rs) {
			ret = row.getInt("student");
		}

		return ret;
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

	private Map<Integer, Integer> initialStudentsMap(int max_size) {
		return Collections.emptyMap();
	}

	private Map<Integer, Date> initialTimestampMap(int max_size) {
		HashMap<Integer, Date> ret = new HashMap<>();
		Date maxDate = new Date(Long.MAX_VALUE);
		for (int i = 1; i <= max_size; ++i) {
			ret.put(i, maxDate);
		}
		return ret;
	}

	public void increaseBackendExcepionCount() {
		statistics.increaseBackendExcepionCount();
	}
}
