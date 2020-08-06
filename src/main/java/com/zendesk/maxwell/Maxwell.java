package com.zendesk.maxwell;

import com.djdch.log4j.StaticShutdownCallbackRegistry;
import com.github.shyiko.mysql.binlog.network.ServerException;
import com.ola.dataplatform.maxwell.configmanager.ConfigClient;
import com.ola.dataplatform.maxwell.configmanager.ConfigType;
import com.olacabs.dp.kaas.client.kafka.producer.impl.DefaultPartitionKeyGenerator;
import com.olacabs.dp.kaas.client.kafka.producer.impl.KaasProducerImpl;
import com.olacabs.dp.utils.KafkaTopicMapper;
import com.olacabs.dp.utils.SchemaSynchronizer;
import com.zendesk.maxwell.bootstrap.BootstrapController;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.recovery.Recovery;
import com.zendesk.maxwell.recovery.RecoveryInfo;
import com.zendesk.maxwell.replication.BinlogConnectorReplicator;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.schema.*;
import com.zendesk.maxwell.schema.columndef.ColumnDefCastException;
import com.zendesk.maxwell.util.Logging;
import com.zendesk.maxwell.util.MaxwellOptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Maxwell implements Runnable {
	protected MaxwellConfig config;
	protected MaxwellContext context;
	protected Replicator replicator;
	private Schema schema;
	private KafkaTopicMapper kafkaTopicMapper;
	private SchemaSynchronizer schemaSynchronizer;
	private KaasProducerImpl kaasProducer;
	private static final UUID uuid = UUID.randomUUID();

	static final Logger LOGGER = LoggerFactory.getLogger(Maxwell.class);

	public Maxwell(MaxwellConfig config) throws SQLException, URISyntaxException {
		this(new MaxwellContext(config));
	}

	protected Maxwell(MaxwellContext context) throws SQLException, URISyntaxException {
		this.config = context.getConfig();
		this.context = context;
	}

	public void run() {
		try {
			start();
		} catch (Exception e) {
			LOGGER.error("maxwell encountered an exception", e);
		}
	}

	public void terminate() {
		Thread terminationThread = this.context.terminate();
		if (terminationThread != null) {
			try {
				terminationThread.join();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	private Position attemptMasterRecovery() throws Exception {
		HeartbeatRowMap recoveredHeartbeat = null;
		MysqlPositionStore positionStore = this.context.getPositionStore();
		RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

		if ( recoveryInfo != null ) {
			Recovery masterRecovery = new Recovery(
				config.replicationMysql,
				config.databaseName,
				this.context.getReplicationConnectionPool(),
				this.context.getCaseSensitivity(),
				recoveryInfo
			);

			recoveredHeartbeat = masterRecovery.recover();

			if (recoveredHeartbeat != null) {
				// load up the schema from the recovery position and chain it into the
				// new server_id
				MysqlSchemaStore oldServerSchemaStore = new MysqlSchemaStore(
					context.getMaxwellConnectionPool(),
					context.getReplicationConnectionPool(),
					context.getSchemaConnectionPool(),
					recoveryInfo.serverID,
					recoveryInfo.position,
					context.getCaseSensitivity(),
					config.filter,
					false
				);

				// Note we associate this schema to the start position of the heartbeat event, so that
				// we pick it up when resuming at the event after the heartbeat.
				oldServerSchemaStore.clone(context.getServerID(), recoveredHeartbeat.getPosition());
				return recoveredHeartbeat.getNextPosition();
			}
		}
		return null;
	}

	private void logColumnCastError(ColumnDefCastException e) throws SQLException, SchemaStoreException {
		try ( Connection conn = context.getSchemaConnectionPool().getConnection() ) {
			LOGGER.error("checking for schema inconsistencies in " + e.database + "." + e.table);
			SchemaCapturer capturer = new SchemaCapturer(conn, context.getCaseSensitivity(), e.database, e.table);
			Schema recaptured = capturer.capture();
			Table t = this.replicator.getSchema().findDatabase(e.database).findTable(e.table);
			List<String> diffs = new ArrayList<>();

			t.diff(diffs, recaptured.findDatabase(e.database).findTable(e.table), "old", "new");
			if ( diffs.size() == 0 ) {
				LOGGER.error("no differences found");
			} else {
				for ( String diff : diffs )
					LOGGER.error(diff);
			}
		}
	}

	protected Position getInitialPosition() throws Exception {
		/* first method:  do we have a stored position for this server? */
		Position initial = this.context.getInitialPosition();

		if (initial == null) {

			/* second method: are we recovering from a master swap? */
			if ( config.masterRecovery )
				initial = attemptMasterRecovery();

			/* third method: is there a previous client_id?
			   if so we have to start at that position or else
			   we could miss schema changes, see https://github.com/zendesk/maxwell/issues/782 */

			if ( initial == null ) {
				initial = this.context.getOtherClientPosition();
				if ( initial != null ) {
					LOGGER.info("Found previous client position: " + initial);
				}
			}

			/* fourth method: capture the current master position. */
			if ( initial == null ) {
				try ( Connection c = context.getReplicationConnection() ) {
					initial = Position.capture(c, config.gtidMode);
				}
			}

			/* if the initial position didn't come from the store, store it */
			context.getPositionStore().set(initial);
		}

		if (config.masterRecovery) {
			this.context.getPositionStore().cleanupOldRecoveryInfos();
		}

		return initial;
	}

	public String getMaxwellVersion() {
		String packageVersion = getClass().getPackage().getImplementationVersion();
		if ( packageVersion == null )
			return "??";
		else
			return packageVersion;
	}

	static String bootString = "Maxwell v%s is booting (%s), starting at %s";
	private void logBanner(AbstractProducer producer, Position initialPosition) {
		String producerName = producer.getClass().getSimpleName();
		LOGGER.info(String.format(bootString, getMaxwellVersion(), producerName, initialPosition.toString()));
	}

	protected void onReplicatorStart() {}
	protected void onReplicatorEnd() {}

	private void start() throws Exception {
		try {
			startInner();
		} catch ( Exception e) {
			this.context.terminate(e);
		} finally {
			onReplicatorEnd();
			this.terminate();
		}

		Exception error = this.context.getError();
		if (error != null) {
			throw error;
		}
	}

	private void startInner() throws Exception {
		try ( Connection connection = this.context.getReplicationConnection();
		      Connection rawConnection = this.context.getRawMaxwellConnection() ) {
			MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
			MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
			if (config.gtidMode) {
				MaxwellMysqlStatus.ensureGtidMysqlState(connection);
			}

			SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

			try ( Connection schemaConnection = this.context.getMaxwellConnection() ) {
				SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
			}
		}


		AbstractProducer producer = this.context.getProducer();
		/**
		 * For first run, it will get position from the replication connection
		 * For further runs, it will get position from the maxwell positions table
		 */
		Position initPosition = getInitialPosition();
		logBanner(producer, initPosition);
		this.context.setPosition(initPosition);

		MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
		BootstrapController bootstrapController = this.context.getBootstrapController(mysqlSchemaStore.getSchemaID());

		if (config.recaptureSchema) {
			mysqlSchemaStore.captureAndSaveSchema();
		}

		schema=mysqlSchemaStore.getSchema();// trigger schema to load / capture before we start the replicator.
		LOGGER.info(schema.getDatabaseNames().toString());

		kafkaTopicMapper = new KafkaTopicMapper(this.context);
		schemaSynchronizer = new SchemaSynchronizer(context,schema,kafkaTopicMapper);

		mysqlSchemaStore.setSchemaSynchronizer(schemaSynchronizer);

		schemaSynchronizer.init();

		producer.setKafkaTopicMapper(kafkaTopicMapper);
		producer.setSchemaSynchronizer(schemaSynchronizer);

		this.replicator = new BinlogConnectorReplicator(
			mysqlSchemaStore,
			producer,
			bootstrapController,
			config.replicationMysql,
			config.replicaServerID,
			config.databaseName,
			context.getMetrics(),
			initPosition,
			false,
			config.clientID,
			context.getHeartbeatNotifier(),
			config.scripting,
			context.getFilter(),
			config.outputConfig,
			config.bufferMemoryUsage
		);

		context.setReplicator(replicator);
		this.context.start();

		replicator.startReplicator();
		this.onReplicatorStart();

		try {
			replicator.runLoop();
		} catch ( ColumnDefCastException e ) {
			logColumnCastError(e);
		}
	}

	public static void main(String[] args) {
		try {
			Logging.setupLogBridging();

			MaxwellConfig config = new MaxwellConfig();
			MaxwellOptionParser parser = config.buildOptionParser();
			OptionSet options = parser.parse(args);

			ConfigType configType= ConfigType.valueOf((String) options.valueOf("config_type"));
			String kaasConfigFile = (String) options.valueOf("kaas_config");
			String zookeeperTimeout = (String) options.valueOf("zk_timeout");

			KaasProducerImpl kaasProducer = KaasProducerImpl.<String, String>fileBuilder().configFilePath(kaasConfigFile).partitionKeyGenerator(new DefaultPartitionKeyGenerator()).fileBuild();
			try {
				kaasProducer.initialize();
			} catch (Exception e) {
				LOGGER.error("Unable to initialize kaas client. Error is {}", e.getMessage());
				System.exit(1);
			}

			String configFileName=null;
			for(int attempt=0;attempt<3;attempt++){
				try {
					ConfigClient client = ConfigClient.getInstance(kaasProducer.getPrimaryZkList(), Integer.parseInt(zookeeperTimeout), uuid.toString(), configType);
					configFileName = client.getConfig();
					break;
				} catch (Exception e) {
					LOGGER.error("{}", e.getMessage());
				}
				LOGGER.warn("Couldn't get config to proceed. Sleeping and retrying after {} ms ", zookeeperTimeout);
				Thread.sleep(Integer.parseInt(zookeeperTimeout));
			}

			if(configFileName==null){
				LOGGER.info("No config file found from Config Service !!");
				System.exit(1);
			}

//			configFileName ="config.properties";
			LOGGER.info("Received configuration fileName :  {} ", configFileName);
			config.parse(configFileName);
			if ( config.log_level != null )
				Logging.setLevel(config.log_level);

			MaxwellContext context = new MaxwellContext(config);
			context.setKaasProducer(kaasProducer);
			final Maxwell maxwell = new Maxwell(context);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					maxwell.terminate();
					StaticShutdownCallbackRegistry.invoke();
				}
			});

			maxwell.start();
		} catch ( SQLException e ) {
			// catch SQLException explicitly because we likely don't care about the stacktrace
			LOGGER.error("SQLException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch ( URISyntaxException e ) {
			// catch URISyntaxException explicitly as well to provide more information to the user
			LOGGER.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
			LOGGER.error("URISyntaxException: " + e.getLocalizedMessage());
			System.exit(1);
		} catch ( ServerException e ) {
			LOGGER.error("Maxwell couldn't find the requested binlog, exiting...");
			System.exit(2);
		} catch ( Exception e ) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
