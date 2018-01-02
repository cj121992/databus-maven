package com.cj.databus2.producers;

import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.DbusEventBufferAppendable;
import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus.core.monitoring.mbean.DbusEventsStatisticsCollector;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.core.seq.MaxSCNReaderWriter;
import com.linkedin.databus2.producers.ConstantPartitionFunction;
import com.linkedin.databus2.producers.EventCreationException;
import com.linkedin.databus2.producers.EventProducer;
import com.linkedin.databus2.producers.PartitionFunction;
import com.linkedin.databus2.relay.config.LogicalSourceStaticConfig;
import com.linkedin.databus2.relay.config.PhysicalSourceStaticConfig;
import com.linkedin.databus2.schemas.NoSuchSchemaException;
import com.linkedin.databus2.schemas.SchemaRegistryService;

public class PgEventProducerFactory {

  public static final String MODULE = PgEventProducerFactory.class.getName();
	
	public static final Logger LOG = Logger.getLogger(MODULE);

	private final Logger _log = Logger.getLogger(getClass());

	public EventProducer buildEventProducer(
			PhysicalSourceStaticConfig physicalSourceConfig,
			SchemaRegistryService schemaRegistryService,
			DbusEventBufferAppendable dbusEventBuffer, MBeanServer mbeanServer,
			DbusEventsStatisticsCollector dbusEventsStatisticsCollector,
			MaxSCNReaderWriter maxScnReaderWriter)
			throws InvalidConfigException {
		// Parse each one of the logical sources
		List<PgAvroEventFactory> eventFactories = new ArrayList<PgAvroEventFactory>();

		String uri = physicalSourceConfig.getUri();
		if (!uri.startsWith("jdbc:postgresql")) {
			try {
				throw new InvalidConfigException("Invalid source URI ("
						+ physicalSourceConfig.getUri()
						+ "). Only jdbc:postgresql: URIs are supported.");
			} catch (InvalidConfigException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		DataSource ds = null;
		try {
			ds = PgJarUtils.createPgDataSource(uri);
		} catch (Exception e) {
			String errMsg = "Pg URI likely not supported. Trouble creating PgDataSource";
			_log.error(errMsg, e);
			throw new InvalidConfigException(errMsg + e.getMessage());
		}

		PartitionFunction partitionFunction = null;
		short sourceId = 0;
		// TODO
		for (LogicalSourceStaticConfig sourceConfig : physicalSourceConfig
				.getSources()) {
			PgAvroEventFactory factory = null;
			try {
				factory = buildEventFactory(sourceConfig, physicalSourceConfig,
						schemaRegistryService);
			} catch (Exception ex) {
				LOG.error(
						"Got exception while building monitored sources for config :"
								+ sourceConfig, ex);
				throw new InvalidConfigException(ex);
			}
			eventFactories.add(factory);
			
			//partitionFunction = buildPartitionFunction(sourceConfig);
			//sourceId = sourceConfig.getId();
		}

		EventProducer eventProducer = new PgLogicalDecodingEventProducer(eventFactories,
				dbusEventBuffer, ds, maxScnReaderWriter, physicalSourceConfig,
				mbeanServer, schemaRegistryService);
		return eventProducer;
	}

	public PgAvroEventFactory buildEventFactory(
			LogicalSourceStaticConfig sourceConfig,
			PhysicalSourceStaticConfig pConfig,
			SchemaRegistryService schemaRegistryService)
			throws DatabusException, EventCreationException,
			UnsupportedKeyException, InvalidConfigException {
		String schema = null;
		try {
			schema = schemaRegistryService
					.fetchLatestSchemaBySourceName(sourceConfig.getName());
		} catch (NoSuchSchemaException e) {
			throw new InvalidConfigException(
					"Unable to load the schema for source ("
							+ sourceConfig.getName() + ").");
		}

		if (schema == null) {
			throw new InvalidConfigException(
					"Unable to load the schema for source ("
							+ sourceConfig.getName() + ").");
		}

		String eventViewSchema;
		String eventView;

		if (sourceConfig.getUri().indexOf('.') != -1) {
			String[] parts = sourceConfig.getUri().split("\\.");
			eventViewSchema = parts[0];
			eventView = parts[1];
		} else {
			eventViewSchema = null;
			eventView = sourceConfig.getUri();
		}

		PartitionFunction partitionFunction = buildPartitionFunction(sourceConfig);
		PgAvroEventFactory factory = createEventFactory(eventViewSchema,
				eventView, sourceConfig, pConfig, schema, partitionFunction);
		return factory;
	}

	private PgAvroEventFactory createEventFactory(String eventViewSchema,
			String eventView, LogicalSourceStaticConfig sourceConfig,
			PhysicalSourceStaticConfig pConfig, String schema,
			PartitionFunction partitionFunction) throws DatabusException {
		return new PgAvroEventFactory(sourceConfig.getId(),
                (short)pConfig.getId(),
                schema,
                partitionFunction);
	}

	public PartitionFunction buildPartitionFunction(
			LogicalSourceStaticConfig sourceConfig)
			throws InvalidConfigException {
		String partitionFunction = sourceConfig.getPartitionFunction();
		if (partitionFunction.startsWith("constant:")) {
			try {
				String numberPart = partitionFunction.substring(
						"constant:".length()).trim();
				short constantPartitionNumber = Short.valueOf(numberPart);
				return new ConstantPartitionFunction(constantPartitionNumber);
			} catch (Exception ex) {
				// Could be a NumberFormatException, IndexOutOfBoundsException
				// or other exception when trying to parse the partition number.
				throw new InvalidConfigException(
						"Invalid partition configuration ("
								+ partitionFunction
								+ "). "
								+ "Could not parse the constant partition number.");
			}
		} else {
			throw new InvalidConfigException(
					"Invalid partition configuration (" + partitionFunction
							+ ").");
		}
	}

}
