package com.cj.databus2.producers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.MBeanServer;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.cj.event.Cell;
import com.cj.event.DmlEvent;
import com.cj.event.Event;
import com.cj.event.TxEvent;
import com.cj.event.parser.AntlrBasedParser;
import com.cj.event.parser.PgParser;
import com.cj.pg.dao.ChangeSetDTO;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.linkedin.databus.core.DatabusThreadBase;
import com.linkedin.databus.core.DbusConstants;
import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.DbusOpcode;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.AbstractEventProducer;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.db.EventReaderSummary;
import com.linkedin.databus2.producers.db.EventSourceStatisticsIface;
import com.linkedin.databus2.producers.db.ReadEventCycleSummary;
import com.linkedin.databus2.producers.ds.DbChangeEntry;
import com.linkedin.databus2.producers.ds.KeyPair;
import com.linkedin.databus2.producers.ds.PrimaryKeySchema;
import com.linkedin.databus2.producers.ds.Transaction;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;
import com.linkedin.databus2.schemas.VersionedSchema;
import com.linkedin.databus2.schemas.utils.SchemaHelper;

public class PgLogicalDecodingEventProducer extends AbstractEventProducer {

	private static final int GENERATE_RANGE = 1000;

	private static final String REGRESSION_SLOT = "regression_slot";

	private static final String LOGICAL_DECODING_CONSUME = "SELECT * from pg_logical_slot_get_changes(?, NULL, ?, 'include-timestamp', 'on')";
	
	private static final String LOGICAL_DECODING_PEEK = "SELECT * from pg_logical_slot_peek_changes(?, NULL, ?, 'include-timestamp', 'on')";
	
	private final Logger logger = Logger.getLogger(getClass());

	private final MaxSCNReaderWriter _maxSCNReaderWriter;
	private final PhysicalSourceStaticConfig _physicalSourceStaticConfig;
	private final String _physicalSourceName;
	private final Map<Integer, PgAvroEventFactory> _eventFactoryMap;
	private final SchemaRegistryService _schemaRegistryService;
	private final PgParser _parser;
	private final DataSource _datasource;
	private final Map<String, Short> _tableUriToSrcIdMap;
	private final Map<String, String> _tableUriToSrcNameMap;

	private EventProducerThread _producerThread;
	private boolean shutdown = false;

	private Connection _eventSelectConnection;

	// private final Logger _log;
	// private EventProducerThread _producerThread;

	public PgLogicalDecodingEventProducer(
			List<PgAvroEventFactory> eventFactories,
			DbusEventBufferAppendable eventBuffer, DataSource dataSource,
			MaxSCNReaderWriter maxScnReaderWriter,
			PhysicalSourceStaticConfig physicalSourceConfig,
			MBeanServer mbeanServer, SchemaRegistryService schemaRegistryService) {
		super(eventBuffer, maxScnReaderWriter, physicalSourceConfig,
				mbeanServer);
		_maxSCNReaderWriter = maxScnReaderWriter;
		_physicalSourceStaticConfig = physicalSourceConfig;
		_physicalSourceName = physicalSourceConfig.getName();

		_eventFactoryMap = new HashMap<Integer, PgAvroEventFactory>();
		for (PgAvroEventFactory s : eventFactories) {
			_eventFactoryMap.put(Integer.valueOf(s.getSourceId()), s);
		}
		_parser = new AntlrBasedParser();
		_datasource = dataSource;
		_schemaRegistryService = schemaRegistryService;
		_tableUriToSrcIdMap = new HashMap<String, Short>();
		_tableUriToSrcNameMap = new HashMap<String, String>();
		for (LogicalSourceStaticConfig l : _physicalSourceStaticConfig
				.getSources()) {
			_tableUriToSrcIdMap.put(l.getUri().toLowerCase(), l.getId());
			_tableUriToSrcNameMap.put(l.getUri().toLowerCase(), l.getName());
		}

		/*
		 * _log = (null != log) ? log :
		 * Logger.getLogger("com.linkedin.databus2.producers.or_" +
		 * _physicalSourceName);
		 */
	}

	@Override
	public synchronized void start(long sinceSCN) {
		long sinceSCNToUse = 0;

		if (sinceSCN > 0) {
			sinceSCNToUse = sinceSCN;
		} else {
			/*
			 * If the maxScn is persisted in maxSCN file and is greater than 0,
			 * then honor it. Else use sinceSCN passed in from above which is -1
			 * or 0 Currently, both of them mean to start from the first binlog
			 * file with filenum 000001, and offset 4
			 */
			if (_maxSCNReaderWriter != null) {
				try {
					long scn = _maxSCNReaderWriter.getMaxScn();
					if (scn > 0) {
						sinceSCNToUse = scn;
					}
				} catch (DatabusException e) {
					_log.warn("Could not read saved maxScn: Defaulting to startSCN="
							+ sinceSCNToUse);
				}
			}
		}
		_producerThread = new EventProducerThread(_physicalSourceName,
				sinceSCNToUse);
		_producerThread.start();
	}
	
	
	private void slotCommit(int size) {
		PreparedStatement pstmt = null;
		//int count = 0;
		try {
			pstmt = _eventSelectConnection
					.prepareStatement(LOGICAL_DECODING_CONSUME);
			pstmt.setString(1, REGRESSION_SLOT);
			pstmt.setInt(2, GENERATE_RANGE);
			pstmt.executeQuery();
			//TODO check
		} catch (Exception e) {
			logger.error("slotCommit error", e);
		}
	}
	
	@Override
	protected ReadEventCycleSummary readEventsFromAllSources(long sinceSCN)
			throws DatabusException, EventCreationException,
			UnsupportedKeyException {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int generateSize = 0;
		Collection<ChangeSetDTO> changes = new ArrayList<ChangeSetDTO>();
		try {
			pstmt = _eventSelectConnection
					.prepareStatement(LOGICAL_DECODING_PEEK);
			pstmt.setString(1, REGRESSION_SLOT);
			pstmt.setInt(2, GENERATE_RANGE);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				generateSize ++;
				ChangeSetDTO changeset = new ChangeSetDTO();
				changeset.setData(rs.getString("data"));
				changeset.setLocation(rs.getString("location"));
				changeset.setTransactionId(rs.getLong("xid"));
				changes.add(changeset);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		long endScn = 0l;
		Collection<DmlEvent> events = new ArrayList<DmlEvent>();
		Map<Long, Long> transactionCommitTimes = new HashMap<Long, Long>();
		for (ChangeSetDTO change : changes) {
			Event event = _parser.parseLogLine(change.getData());
			event.setTransactionId(change.getTransactionId());
			if (event instanceof DmlEvent) {
				events.add((DmlEvent) event);
			} else if (event instanceof TxEvent) {
				endScn = event.getTransactionId();
				if (event.getCommitTime() > 0) {
					transactionCommitTimes.put(event.getTransactionId(),
							event.getCommitTime());
				}
			}
		}

		// add transaction time to dmlevent
		for (DmlEvent event : events) {
			event.setCommitTime(transactionCommitTimes.get(event
					.getTransactionId()));
		}

		// ���˼���
		Predicate<DmlEvent> predicate = new Predicate<DmlEvent>() {
			@Override
			public boolean apply(DmlEvent input) {
				return _tableUriToSrcNameMap.containsKey(input.getSchemaName() + "." + input.getTableName());
			}
		};
		Collection<DmlEvent> relevantEvents = Collections2.filter(events,
				predicate);
		List<EventReaderSummary> summarys = new ArrayList<EventReaderSummary>();
		int totalEventSize = relevantEvents.size();
		if (totalEventSize > 0) {
			//判断当前eventBuffer(断言init or ended)状态并置于start状态,将currentPosition置于_tail尾部
			_eventBuffer.startEvents();
			if (sinceSCN <= 0) {
				//将当前SCN值写入_prevSCN
				_eventBuffer.setStartSCN(endScn);
			}
			//將dmlEvent格式avro調用appendEvent写入bytebuffer
			for (DmlEvent event : relevantEvents) {
				summarys.add(transforDmlEvent2DataBus(event));
			}
			_eventBuffer.endEvents(endScn, null);
		}
		
		slotCommit(generateSize);
		return null;
	}

	private EventReaderSummary transforDmlEvent2DataBus(DmlEvent dmlEvent)
			throws NoSuchSchemaException, DatabusException,
			EventCreationException, UnsupportedKeyException {
		Transaction transaction = new Transaction();
		// TODO
		transaction.setIgnoredSourceScn(0);
		long timestampInNanos = dmlEvent.getCommitTime() * 1000000L;
		long scn = dmlEvent.getTransactionId();
		// TODO
		transaction.setSizeInBytes(0);
		transaction.setTxnReadLatencyNanos(timestampInNanos);
		transaction.setTxnNanoTimestamp(timestampInNanos);
		DbusOpcode doc = null;

		// ��ݱ����õ�����ĸ�ʽ����Ϣ
		VersionedSchema vs = _schemaRegistryService
				.fetchLatestVersionedSchemaBySourceName(_tableUriToSrcNameMap
						.get(dmlEvent.getSchemaName() + "."
								+ dmlEvent.getTableName()));
		Schema schema = vs.getSchema();
		switch (dmlEvent.getType().name()) {
		case "insert":
			doc = DbusOpcode.UPSERT;
			break;
		case "update":
			doc = DbusOpcode.UPSERT;
			break;
		case "delete":
			doc = DbusOpcode.DELETE;
			break;
		default:
			doc = DbusOpcode.UPSERT;
		}
		final boolean isReplicated = false;
		List<KeyPair> kps = generateKeyPair(dmlEvent, schema);
		GenericRecord gr = new GenericData.Record(schema);

		generateAvroEvent(gr, dmlEvent, schema);

		// ��ɸ�ʽ���¼�������buffer
		DbChangeEntry db = new DbChangeEntry(scn, timestampInNanos, gr, doc,
				isReplicated, schema, kps);
		_eventFactoryMap.get((int)
				_tableUriToSrcIdMap.get( dmlEvent.getSchemaName() + "."
						+ dmlEvent.getTableName())).createAndAppendEvent(db,
				_eventBuffer, false, null);
		return null;
	}

	private void generateAvroEvent(GenericRecord gr, DmlEvent dmlEvent,
			Schema schema) throws DatabusException {
		List<Schema.Field> orderedFields = SchemaHelper
				.getOrderedFieldsByMetaField(schema, "dbFieldPosition",
						new Comparator<String>() {

							@Override
							public int compare(String o1, String o2) {
								Integer pos1 = Integer.parseInt(o1);
								Integer pos2 = Integer.parseInt(o2);

								return pos1.compareTo(pos2);
							}
						});

		// Build Map<AvroFieldType, Columns>
		/*
		 * if (orderedFields.size() > dmlEvent.getNewValues().size()) { throw
		 * new DatabusException("Mismatch in db schema vs avro schema"); }
		 */

		List<Cell> cellList = null;
		gr.put("op_type", dmlEvent.getType().name());
		if (dmlEvent.getType().equals(DmlEvent.Type.delete)) {
			cellList = dmlEvent.getOldValues();
		} else {
			cellList = dmlEvent.getNewValues();
		}
		for (Cell cell : cellList) {
			//未在schema定义的字段不予处理。
			if (cell.getTypeValue() != null && gr.getSchema().getField(cell.getName()) != null) {
				gr.put(cell.getName(), cell.getTypeValue());
			}
		}
	}

	private List<KeyPair> generateKeyPair(DmlEvent dmlEvent, Schema schema)
			throws DatabusException {
		Object o = null;
		Schema.Type st = null;

		// Build PrimaryKeySchema
		String pkFieldName = SchemaHelper.getMetaField(schema, "pk");
		if (pkFieldName == null) {
			throw new DatabusException("No primary key specified in the schema");
		}

		PrimaryKeySchema pkSchema = new PrimaryKeySchema(pkFieldName);
		List<Schema.Field> fields = schema.getFields();
		List<KeyPair> kpl = new ArrayList<KeyPair>();
		int cnt = 0;
		for (Schema.Field field : fields) {
			if (pkSchema.isPartOfPrimaryKey(field)) {
				if (dmlEvent.getType().equals(DmlEvent.Type.delete)) {
					o = dmlEvent.getOldValues().get(cnt).getTypeValue();
				} else {
					o = dmlEvent.getNewValues().get(cnt).getTypeValue();
				}
				st = field.schema().getType();
				KeyPair kp = new KeyPair(o, st);
				kpl.add(kp);
			}
			cnt++;
		}

		return kpl;
	}

	private Object obtainKey(DbChangeEntry dbChangeEntry)
			throws DatabusException {
		if (null == dbChangeEntry) {
			throw new DatabusException("DBUpdateImage is null");
		}
		List<KeyPair> pairs = dbChangeEntry.getPkeys();
		if (null == pairs || pairs.size() == 0) {
			throw new DatabusException("There do not seem to be any keys");
		}

		if (pairs.size() == 1) {
			Object key = pairs.get(0).getKey();
			Schema.Type pKeyType = pairs.get(0).getKeyType();
			Object keyObj = null;
			if (pKeyType == Schema.Type.INT) {
				if (key instanceof Integer) {
					keyObj = key;
				} else {
					throw new DatabusException(
							"Schema.Type does not match actual key type (INT) "
									+ key.getClass().getName());
				}

			} else if (pKeyType == Schema.Type.LONG) {
				if (key instanceof Long) {
					keyObj = key;
				} else {
					throw new DatabusException(
							"Schema.Type does not match actual key type (LONG) "
									+ key.getClass().getName());
				}

				keyObj = key;
			} else {
				keyObj = key;
			}

			return keyObj;
		} else {
			// Treat multiple keys as a separate case to avoid unnecessary casts
			Iterator<KeyPair> li = pairs.iterator();
			StringBuilder compositeKey = new StringBuilder();
			while (li.hasNext()) {
				KeyPair kp = li.next();
				Schema.Type pKeyType = kp.getKeyType();
				Object key = kp.getKey();
				if (pKeyType == Schema.Type.INT) {
					if (key instanceof Integer)
						compositeKey.append(kp.getKey().toString());
					else
						throw new DatabusException(
								"Schema.Type does not match actual key type (INT) "
										+ key.getClass().getName());
				} else if (pKeyType == Schema.Type.LONG) {
					if (key instanceof Long)
						compositeKey.append(key.toString());
					else
						throw new DatabusException(
								"Schema.Type does not match actual key type (LONG) "
										+ key.getClass().getName());
				} else {
					compositeKey.append(key);
				}

				if (li.hasNext()) {
					// Add the delimiter for all keys except the last key
					compositeKey.append(DbusConstants.COMPOUND_KEY_DELIMITER);
				}
			}
			return compositeKey.toString();
		}
	}

	@Override
	public List<? extends EventSourceStatisticsIface> getSources() {
		// TODO Auto-generated method stub
		return null;
	}

	void initPg(long scn) throws SQLException {
		if (_eventSelectConnection == null || _eventSelectConnection.isClosed()) {
			resetConnections();
		}
	}

	private void resetConnections() throws SQLException {
		_eventSelectConnection = _datasource.getConnection();
	}

	public class EventProducerThread extends DatabusThreadBase {

		// The scn with which the event buffer is started
		private final AtomicLong _startPrevScn = new AtomicLong(-1);
		private final long _sinceScn;

		public EventProducerThread(String sourceName, long sinceScn) {
			super("PgLogicalDecoding_" + sourceName);
			_sinceScn = sinceScn;
		}

		@Override
		public void run() {
			//TODO demind to remove 
			_eventBuffer.start(_sinceScn);
			_startPrevScn.set(_sinceScn);

			while (!shutdown) {
				try {
					initPg(_sinceScn);
					readEventsFromAllSources(_sinceScn);
					//_log.info("Event Producer Thread done");
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
					_eventBuffer.rollbackEvents();
				}

			}

			/*
			 * try { boolean started = false; while (!started) { try {
			 * _or.start(); started = true; } catch (Exception e) {
			 * _log.error("Failed to start OpenReplicator: " + e);
			 * _log.warn("Sleeping for 1000 ms"); Thread.sleep(1000); } }
			 * _orListener.start(); } catch (Exception e) { _log.error(
			 * "failed to start open replicator: " + e.getMessage(), e); return;
			 * }
			 */

			//_log.info("Event Producer Thread done");
			doShutdownNotify();
		}

	}

}
