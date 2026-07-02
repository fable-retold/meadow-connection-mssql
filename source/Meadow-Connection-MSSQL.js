/**
* Meadow MSSQL Provider Fable Service
* @author Steven Velozo <steven@velozo.com>
*/
const libFableServiceProviderBase = require('fable-serviceproviderbase');

const libMSSQL = require('mssql');

const libMeadowSchemaMSSQL = require('./Meadow-Schema-MSSQL.js');
const libRetry = require('./Meadow-MSSQL-Retry.js');

// Default timeouts and retry behavior.  All configurable per-provider via
// the MSSQL options block in fable settings.  Defaults lean generous for
// slow WAN links / firewalled customer networks — better to wait a minute
// than to false-fail a real sync.
const DEFAULT_REQUEST_TIMEOUT_MS    = 120000;  // 2 min per query
const DEFAULT_CONNECTION_TIMEOUT_MS = 60000;   // 1 min to establish a connection
const DEFAULT_CONNECT_MAX_ATTEMPTS  = 5;
const DEFAULT_CONNECT_INITIAL_DELAY = 3000;
const DEFAULT_CONNECT_MAX_DELAY     = 30000;

/*
	Das alt muster:

	{
		connectionLimit: _Fable.settings.MSSQL.ConnectionPoolLimit,
		server: _Fable.settings.MSSQL.Server,
		// TODO: Not yet mapped.
		port: _Fable.settings.MSSQL.Port,
		user: _Fable.settings.MSSQL.User,
		password: _Fable.settings.MSSQL.Password,
		database: _Fable.settings.MSSQL.Database
	}
*/

class MeadowConnectionMSSQL extends libFableServiceProviderBase
{
	constructor(pFable, pManifest, pServiceHash)
	{
		super(pFable, pManifest, pServiceHash);

		this.serviceType = 'MeadowConnectionMSSQL';

		this.connected = false;

		// Resolve MSSQL connection settings from options or fable settings
		if (typeof(this.options.MSSQL) == 'object')
		{
			// Options were passed in with an MSSQL sub-object; coerce PascalCase to lowercase
			if (!this.options.MSSQL.hasOwnProperty('server') && this.options.MSSQL.hasOwnProperty('Server'))
			{
				this.options.MSSQL.server = this.options.MSSQL.Server;
			}
			if (!this.options.MSSQL.hasOwnProperty('port') && this.options.MSSQL.hasOwnProperty('Port'))
			{
				this.options.MSSQL.port = this.options.MSSQL.Port;
			}
			if (!this.options.MSSQL.hasOwnProperty('user') && this.options.MSSQL.hasOwnProperty('User'))
			{
				this.options.MSSQL.user = this.options.MSSQL.User;
			}
			if (!this.options.MSSQL.hasOwnProperty('password') && this.options.MSSQL.hasOwnProperty('Password'))
			{
				this.options.MSSQL.password = this.options.MSSQL.Password;
			}
			if (!this.options.MSSQL.hasOwnProperty('database') && this.options.MSSQL.hasOwnProperty('Database'))
			{
				this.options.MSSQL.database = this.options.MSSQL.Database;
			}
		}
		else if (typeof(this.options.server) === 'string')
		{
			// Options were passed in flat (already has server, user, etc.)
			this.options.MSSQL = (
				{
					server: this.options.server,
					port: this.options.port,
					user: this.options.user,
					password: this.options.password,
					database: this.options.database,
					ConnectionPoolLimit: this.options.ConnectionPoolLimit,
					// Reliability tuning — forward through so it ends up on
					// options.MSSQL where _buildConnectionSettings and
					// _connectRetryOptions look for it.
					RequestTimeoutMs: this.options.RequestTimeoutMs,
					ConnectionTimeoutMs: this.options.ConnectionTimeoutMs,
					ConnectRetryOptions: this.options.ConnectRetryOptions,
					DDLRetryOptions: this.options.DDLRetryOptions,
					LegacyPagination: this.options.LegacyPagination
				});
		}
		else if (typeof(this.fable.settings.MSSQL) == 'object')
		{
			// Fall back to fable settings
			let tmpSettings = this.fable.settings.MSSQL;
			this.options.MSSQL = (
				{
					server: tmpSettings.server || tmpSettings.Server,
					port: tmpSettings.port || tmpSettings.Port,
					user: tmpSettings.user || tmpSettings.User,
					password: tmpSettings.password || tmpSettings.Password,
					database: tmpSettings.database || tmpSettings.Database,
					ConnectionPoolLimit: tmpSettings.ConnectionPoolLimit,
					RequestTimeoutMs: tmpSettings.RequestTimeoutMs,
					ConnectionTimeoutMs: tmpSettings.ConnectionTimeoutMs,
					ConnectRetryOptions: tmpSettings.ConnectRetryOptions,
					DDLRetryOptions: tmpSettings.DDLRetryOptions,
					LegacyPagination: tmpSettings.LegacyPagination
				});
		}

		// Schema provider handles DDL operations (create, drop, index, etc.)
		// Give it a back-reference so it can trigger pool recycling via the
		// retry helper when it detects a degraded pool.
		this._SchemaProvider = new libMeadowSchemaMSSQL(this.fable, this.options, `${this.Hash}-Schema`);
		this._SchemaProvider.setConnectionProvider(this);
	}

	get schemaProvider()
	{
		return this._SchemaProvider;
	}

	generateDropTableStatement(pTableName)
	{
		return this._SchemaProvider.generateDropTableStatement(pTableName);
	}

	generateCreateTableStatement(pMeadowTableSchema)
	{
		return this._SchemaProvider.generateCreateTableStatement(pMeadowTableSchema);
	}

	createTables(pMeadowSchema, fCallback)
	{
		return this._SchemaProvider.createTables(pMeadowSchema, fCallback);
	}

	createTable(pMeadowTableSchema, fCallback)
	{
		return this._SchemaProvider.createTable(pMeadowTableSchema, fCallback);
	}

	getIndexDefinitionsFromSchema(pMeadowTableSchema)
	{
		return this._SchemaProvider.getIndexDefinitionsFromSchema(pMeadowTableSchema);
	}

	generateCreateIndexScript(pMeadowTableSchema)
	{
		return this._SchemaProvider.generateCreateIndexScript(pMeadowTableSchema);
	}

	generateCreateIndexStatements(pMeadowTableSchema)
	{
		return this._SchemaProvider.generateCreateIndexStatements(pMeadowTableSchema);
	}

	createIndex(pIndexStatement, fCallback)
	{
		return this._SchemaProvider.createIndex(pIndexStatement, fCallback);
	}

	dropIndex(pTableName, pIndexName, fCallback)
	{
		return this._SchemaProvider.dropIndex(pTableName, pIndexName, fCallback);
	}

	createIndices(pMeadowTableSchema, fCallback)
	{
		return this._SchemaProvider.createIndices(pMeadowTableSchema, fCallback);
	}

	createAllIndices(pMeadowSchema, fCallback)
	{
		return this._SchemaProvider.createAllIndices(pMeadowSchema, fCallback);
	}

	// Database Introspection delegation

	listTables(fCallback)
	{
		return this._SchemaProvider.listTables(fCallback);
	}

	introspectTableColumns(pTableName, fCallback)
	{
		return this._SchemaProvider.introspectTableColumns(pTableName, fCallback);
	}

	introspectTableIndices(pTableName, fCallback)
	{
		return this._SchemaProvider.introspectTableIndices(pTableName, fCallback);
	}

	introspectTableForeignKeys(pTableName, fCallback)
	{
		return this._SchemaProvider.introspectTableForeignKeys(pTableName, fCallback);
	}

	introspectTableSchema(pTableName, fCallback)
	{
		return this._SchemaProvider.introspectTableSchema(pTableName, fCallback);
	}

	introspectDatabaseSchema(fCallback)
	{
		return this._SchemaProvider.introspectDatabaseSchema(fCallback);
	}

	generateMeadowPackageFromTable(pTableName, fCallback)
	{
		return this._SchemaProvider.generateMeadowPackageFromTable(pTableName, fCallback);
	}

	connect()
	{
		this.log.warn('The non-async Meadow-MSSQL connect() was called and the Microsoft SQL node driver has an asynchronous connection method; although this may function it will likely cause a race condition.');
		this.connectAsync();
	}

	/**
	 * Build a node-mssql connection settings object from this provider's
	 * configured options.  Centralised so both connectAsync and
	 * recyclePool produce identical settings.
	 *
	 * @returns {Object}
	 */
	_buildConnectionSettings()
	{
		let tmpMSSQLSettings = this.options.MSSQL || {};

		// Timeouts are configurable — slow WAN links / firewalled customer
		// networks may need much longer than the driver defaults.
		let tmpRequestTimeoutMs = tmpMSSQLSettings.RequestTimeoutMs
			|| tmpMSSQLSettings.requestTimeoutMs
			|| DEFAULT_REQUEST_TIMEOUT_MS;
		let tmpConnectionTimeoutMs = tmpMSSQLSettings.ConnectionTimeoutMs
			|| tmpMSSQLSettings.connectionTimeoutMs
			|| DEFAULT_CONNECTION_TIMEOUT_MS;

		return (
			{
				server: tmpMSSQLSettings.server,
				user: tmpMSSQLSettings.user,
				password: tmpMSSQLSettings.password,
				database: tmpMSSQLSettings.database,
				requestTimeout: tmpRequestTimeoutMs,
				connectionTimeout: tmpConnectionTimeoutMs,
				port: tmpMSSQLSettings.port,
				pool:
				{
					max: tmpMSSQLSettings.ConnectionPoolLimit || 10,
					min: 0,
					idleTimeoutMillis: 30000,
					// Cap how long pool.query() waits for an available
					// connection before giving up with a ResourceRequest
					// timeout.  Default matches connectionTimeout so the
					// error classifier sees a recognizable shape.
					acquireTimeoutMillis: tmpConnectionTimeoutMs
				},
				options:
				{
					useUTC: false,
					trustServerCertificate: true // change to true for local dev / self-signed customer certs
				},
			});
	}

	/**
	 * Resolve retry options from this provider's config, falling back to
	 * sensible defaults tuned for DDL over a slow network.
	 *
	 * @returns {Object}
	 */
	_connectRetryOptions()
	{
		let tmpMSSQLSettings = this.options.MSSQL || {};
		let tmpRetry = tmpMSSQLSettings.ConnectRetryOptions || {};
		let tmpConnectionTimeoutMs = tmpMSSQLSettings.ConnectionTimeoutMs
			|| tmpMSSQLSettings.connectionTimeoutMs
			|| DEFAULT_CONNECTION_TIMEOUT_MS;
		return (
			{
				OperationName: `Meadow-MSSQL connect to [${tmpMSSQLSettings.server}:${tmpMSSQLSettings.port || 1433}]`,
				MaxAttempts: tmpRetry.MaxAttempts || DEFAULT_CONNECT_MAX_ATTEMPTS,
				InitialDelayMs: tmpRetry.InitialDelayMs || DEFAULT_CONNECT_INITIAL_DELAY,
				MaxDelayMs: tmpRetry.MaxDelayMs || DEFAULT_CONNECT_MAX_DELAY,
				BackoffFactor: tmpRetry.BackoffFactor || 2,
				// Hard wall-clock backstop per connect attempt.  The driver's
				// connectionTimeout normally fires first, but a wedged TCP
				// handshake can leave the connect callback pending forever; give
				// the guard the driver timeout plus headroom so the driver still
				// reports first in the normal case.
				OperationTimeoutMs: tmpRetry.OperationTimeoutMs || (tmpConnectionTimeoutMs + 30000)
				// Connection-establishment retries never recycle a pool
				// (there isn't one yet).  OnRecyclePool is intentionally
				// absent here.
			});
	}

	connectAsync(fCallback)
	{
		let tmpCallback = fCallback;
		if (typeof (tmpCallback) !== 'function')
		{
			this.log.error(`Meadow MSSQL connect() called without a callback; this could lead to connection race conditions.`);
			tmpCallback = () => { };
		}

		if (this._ConnectionPool)
		{
			let tmpCleansedLogSettings = JSON.parse(JSON.stringify(this._buildConnectionSettings()));
			// No leaking passwords!
			tmpCleansedLogSettings.password = '*****************';
			this.log.error(`Meadow-Connection-MSSQL trying to connect to MSSQL but is already connected - skipping the generation of extra connections.`, tmpCleansedLogSettings);
			return tmpCallback(null, this._ConnectionPool);
		}

		let tmpConnectionSettings = this._buildConnectionSettings();
		this.log.info(`Meadow-Connection-MSSQL connecting to [${tmpConnectionSettings.server} : ${tmpConnectionSettings.port}] as ${tmpConnectionSettings.user} for database ${tmpConnectionSettings.database} at a connection limit of ${tmpConnectionSettings.pool.max} (connectionTimeout: ${(tmpConnectionSettings.connectionTimeout / 1000).toFixed(0)}s, requestTimeout: ${(tmpConnectionSettings.requestTimeout / 1000).toFixed(0)}s)`);

		// Retry with exponential backoff on transient connect failures
		// (network errors, timeouts).  Hard failures like authentication
		// errors get classified as ServerError and propagate immediately.
		libRetry.runWithRetry(this.log, this._connectRetryOptions(),
			(fAttemptDone) =>
			{
				libMSSQL.connect(tmpConnectionSettings)
					.then((pConnectionPool) => fAttemptDone(null, pConnectionPool))
					.catch((pError) => fAttemptDone(pError));
			},
			(pError, pConnectionPool) =>
			{
				if (pError)
				{
					this.log.error(`Meadow-Connection-MSSQL final connection failure to [${tmpConnectionSettings.server} : ${tmpConnectionSettings.port}] as ${tmpConnectionSettings.user} for database ${tmpConnectionSettings.database}: ${libRetry.extractErrorMessage(pError)}`);
					return tmpCallback(pError);
				}

				this.log.info(`Meadow-Connection-MSSQL successfully connected to MSSQL at [${tmpConnectionSettings.server} : ${tmpConnectionSettings.port}] as ${tmpConnectionSettings.user} for database ${tmpConnectionSettings.database} at a connection limit of ${tmpConnectionSettings.pool.max}.`);
				this._ConnectionPool = pConnectionPool;
				this.connected = true;
				this._SchemaProvider.setConnectionPool(this._ConnectionPool);
				return tmpCallback(null, this._ConnectionPool);
			});
	}

	/**
	 * Destroy the current pool and create a fresh one.  Used by the retry
	 * helper when a failure mode (RequestTimeout, PoolDegraded) suggests
	 * the pooled connection is in a bad state that new queries on the
	 * same pool won't recover from.
	 *
	 * Idempotent: safe to call even if there is no pool.  Always resolves
	 * (even on error) so callers can proceed with the retry regardless.
	 *
	 * @param {Function} fCallback - (err) => ... (err is informational only)
	 */
	recyclePool(fCallback)
	{
		let tmpCallback = (typeof (fCallback) === 'function') ? fCallback : () => {};

		this.log.info(`Meadow-Connection-MSSQL: recycling connection pool...`);

		// Close the existing pool.  mssql's pool.close() is a promise; handle
		// both shapes (some versions are callback-based).
		let fCloseAndReconnect = () =>
		{
			this._ConnectionPool = false;
			this.connected = false;
			this._SchemaProvider.setConnectionPool(false);
			this.connectAsync((pConnectError) =>
			{
				if (pConnectError)
				{
					this.log.warn(`Meadow-Connection-MSSQL: pool recycle reconnect failed — ${libRetry.extractErrorMessage(pConnectError)}`);
					return tmpCallback(pConnectError);
				}
				this.log.info(`Meadow-Connection-MSSQL: pool recycle complete.`);
				return tmpCallback();
			});
		};

		if (this._ConnectionPool && typeof (this._ConnectionPool.close) === 'function')
		{
			let tmpCloseResult;
			try
			{
				tmpCloseResult = this._ConnectionPool.close();
			}
			catch (pCloseError)
			{
				this.log.warn(`Meadow-Connection-MSSQL: pool.close() threw — ${pCloseError.message || pCloseError} — continuing with reconnect anyway`);
				return fCloseAndReconnect();
			}

			if (tmpCloseResult && typeof (tmpCloseResult.then) === 'function')
			{
				tmpCloseResult
					.then(() => fCloseAndReconnect())
					.catch((pCloseError) =>
					{
						this.log.warn(`Meadow-Connection-MSSQL: pool.close() rejected — ${pCloseError.message || pCloseError} — continuing with reconnect anyway`);
						fCloseAndReconnect();
					});
				return;
			}
		}

		fCloseAndReconnect();
	}

	/**
	 * Resolve the configured per-request (per-query) timeout in milliseconds.
	 * @returns {number}
	 */
	_requestTimeoutMs()
	{
		let tmpMSSQLSettings = this.options.MSSQL || {};
		return tmpMSSQLSettings.RequestTimeoutMs
			|| tmpMSSQLSettings.requestTimeoutMs
			|| DEFAULT_REQUEST_TIMEOUT_MS;
	}

	get preparedStatement()
	{
		if (this.connected && this._ConnectionPool)
		{
			// Bound the per-statement request timeout for all DML via the
			// documented constructor override — mssql applies it to the prepare,
			// execute, and unprepare operations.  The driver only reads this
			// `overrides` argument; assigning ps.requestTimeout after construction
			// has no effect.  Absent an override the statement would inherit the
			// pool's requestTimeout; setting it here keeps the DML ceiling explicit
			// and independent of pool config.  A wedged socket that the driver's own
			// timer can't service is caught by the headless stall detector.
			return new libMSSQL.PreparedStatement(this._ConnectionPool, { requestTimeout: this._requestTimeoutMs() });
		}
		else
		{
			throw new Error('The Meadow Microsoft SQL provider could not create a prepared statement; disconnected or no valid connection pool.');
		}
	}

	get MSSQL()
	{
		return libMSSQL;
	}

	/**
	 * The configured connection pool size (max connections).  Exposed so
	 * pool-aware consumers (e.g. the sync engine's per-record fan-out) can
	 * default their concurrency to the pool size and avoid oversubscribing it.
	 * @returns {number}
	 */
	get connectionPoolLimit()
	{
		let tmpMSSQLSettings = this.options.MSSQL || {};
		return tmpMSSQLSettings.ConnectionPoolLimit || 10;
	}

	get pool()
	{
		return this._ConnectionPool;
	}
}

module.exports = MeadowConnectionMSSQL;
