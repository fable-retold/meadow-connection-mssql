/**
* Meadow MSSQL Schema Provider
*
* Handles table creation, dropping, and DDL generation for Microsoft SQL Server.
* Separated from the connection provider to allow independent extension
* for indexing, foreign keys, and other schema operations.
*
* @author Steven Velozo <steven@velozo.com>
*/
const libFableServiceProviderBase = require('fable-serviceproviderbase');

const libRetry = require('./Meadow-MSSQL-Retry.js');
const libMSSQL = require('mssql');

// Default retry behavior for DDL operations.  CREATE TABLE and CREATE
// INDEX against a heavily-used MSSQL instance can hit schema locks held
// by unrelated transactions; retry with exponential backoff so a busy
// server window doesn't kill a whole deploy.
// A schema change is not a query. CREATE INDEX over a large, already-populated
// table legitimately runs for many minutes, and it only has to succeed once --
// so DDL gets its own ceiling instead of inheriting the pool's per-query one.
const DEFAULT_DDL_REQUEST_TIMEOUT_MS = 1800000;  // 30 min per DDL statement
// How often a long-running DDL statement reports that it is still alive. Silence
// is indistinguishable from a wedge to anything watching the log (see the data
// cloner's pipeline watchdog), and an operator staring at a 20 minute gap has no
// way to tell either.
const DDL_PROGRESS_LOG_INTERVAL_MS = 60000;

const DEFAULT_DDL_MAX_ATTEMPTS   = 5;
const DEFAULT_DDL_INITIAL_DELAY  = 3000;
const DEFAULT_DDL_MAX_DELAY      = 30000;

class MeadowSchemaMSSQL extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);

		this.serviceType = 'MeadowSchemaMSSQL';

		// Reference to the connection pool, set by the connection provider
		this._ConnectionPool = false;

		// Back-reference to the MeadowConnectionMSSQL that owns this
		// schema provider.  Used to request a pool recycle when a DDL
		// failure mode suggests the pooled connection is in a bad state.
		this._ConnectionProvider = null;
	}

	/**
	 * Set the connection pool reference for executing DDL statements.
	 * @param {object} pConnectionPool - MSSQL connection pool
	 * @returns {MeadowSchemaMSSQL} this (for chaining)
	 */
	setConnectionPool(pConnectionPool)
	{
		this._ConnectionPool = pConnectionPool;
		return this;
	}

	/**
	 * Set the back-reference to the connection provider.  The retry
	 * helper uses this to trigger pool recycling on pool-degraded errors.
	 *
	 * @param {object} pConnectionProvider - MeadowConnectionMSSQL instance
	 * @returns {MeadowSchemaMSSQL} this (for chaining)
	 */
	setConnectionProvider(pConnectionProvider)
	{
		this._ConnectionProvider = pConnectionProvider;
		return this;
	}

	/**
	 * Build the retry options block used by DDL operations.  Honors
	 * per-provider overrides via options.MSSQL.DDLRetryOptions.
	 *
	 * @param {string} pOperationName - name to use in log output
	 * @returns {Object}
	 */
	/**
	 * The per-statement ceiling for DDL, in ms.
	 *
	 * Deliberately NOT the pool's requestTimeout: that value is sized for
	 * queries, and the pool hands it to every request it creates. Bounding a
	 * schema change by a query budget is what made a legitimate index build
	 * abort at two minutes and re-abort on every retry.
	 *
	 * @return {number} Milliseconds; 0 means no client-side ceiling.
	 */
	_ddlRequestTimeoutMs()
	{
		let tmpMSSQLSettings = this.options.MSSQL || this.fable.settings.MSSQL || {};
		let tmpRetry = tmpMSSQLSettings.DDLRetryOptions || {};
		if (typeof(tmpRetry.RequestTimeoutMs) === 'number' && tmpRetry.RequestTimeoutMs >= 0)
		{
			return tmpRetry.RequestTimeoutMs;
		}
		return DEFAULT_DDL_REQUEST_TIMEOUT_MS;
	}

	/**
	 * Promise-shaped _executeDDLStatement, so the DDL call sites keep the
	 * .then/.catch shape they already had.
	 *
	 * @param {string} pStatement - The DDL SQL to execute.
	 * @param {string} pOperationName - Label for the progress lines.
	 * @return {Promise<Object>} Resolves with the driver result.
	 */
	_executeDDLStatementPromise(pStatement, pOperationName)
	{
		return new Promise((fResolve, fReject) =>
			{
				this._executeDDLStatement(pStatement, pOperationName,
					(pError, pResult) =>
					{
						if (pError)
						{
							return fReject(pError);
						}
						return fResolve(pResult);
					});
			});
	}

	/**
	 * How often a long-running DDL statement reports that it is still alive.
	 *
	 * @return {number} Milliseconds; 0 disables the progress lines.
	 */
	_ddlProgressLogIntervalMs()
	{
		let tmpMSSQLSettings = this.options.MSSQL || this.fable.settings.MSSQL || {};
		let tmpRetry = tmpMSSQLSettings.DDLRetryOptions || {};
		if (typeof(tmpRetry.ProgressLogIntervalMs) === 'number' && tmpRetry.ProgressLogIntervalMs >= 0)
		{
			return tmpRetry.ProgressLogIntervalMs;
		}
		return DDL_PROGRESS_LOG_INTERVAL_MS;
	}

	/**
	 * Build the request a DDL statement runs on, carrying the DDL ceiling rather
	 * than the pool's per-query one. Its own method so tests can substitute a
	 * request without a live server.
	 *
	 * @return {Object} An mssql Request bound to the connection pool.
	 */
	_createDDLRequest()
	{
		return new libMSSQL.Request(this._ConnectionPool, { requestTimeout: this._ddlRequestTimeoutMs() });
	}

	/**
	 * Run one DDL statement on its own budget, reporting progress while it runs.
	 *
	 * @param {string} pStatement - The DDL SQL to execute.
	 * @param {string} pOperationName - Label for the progress lines.
	 * @param {(pError: Error|null, pResult?: Object) => void} fCallback
	 */
	_executeDDLStatement(pStatement, pOperationName, fCallback)
	{
		let tmpRequest = this._createDDLRequest();

		let tmpStartedMs = Date.now();
		let tmpSettled = false;
		let tmpProgressIntervalMs = this._ddlProgressLogIntervalMs();
		let tmpProgressTimer = (tmpProgressIntervalMs > 0) ? setInterval(() =>
			{
				this.log.info(`${pOperationName}: still running (${((Date.now() - tmpStartedMs) / 60000).toFixed(1)} min elapsed)...`);
			}, tmpProgressIntervalMs) : null;
		if (tmpProgressTimer && typeof(tmpProgressTimer.unref) === 'function')
		{
			tmpProgressTimer.unref();
		}

		let fSettle = (pError, pResult) =>
		{
			if (tmpSettled)
			{
				return;
			}
			tmpSettled = true;
			if (tmpProgressTimer)
			{
				clearInterval(tmpProgressTimer);
			}
			return fCallback(pError, pResult);
		};

		tmpRequest.query(pStatement)
			.then((pResult) => fSettle(null, pResult))
			.catch((pError) => fSettle(pError));
	}

	_ddlRetryOptions(pOperationName)
	{
		let tmpMSSQLSettings = this.options.MSSQL || this.fable.settings.MSSQL || {};
		let tmpRetry = tmpMSSQLSettings.DDLRetryOptions || {};

		let tmpOptions = (
			{
				OperationName: pOperationName,
				MaxAttempts: tmpRetry.MaxAttempts || DEFAULT_DDL_MAX_ATTEMPTS,
				InitialDelayMs: tmpRetry.InitialDelayMs || DEFAULT_DDL_INITIAL_DELAY,
				MaxDelayMs: tmpRetry.MaxDelayMs || DEFAULT_DDL_MAX_DELAY,
				BackoffFactor: tmpRetry.BackoffFactor || 2,
				// DDL (e.g. CREATE INDEX on a large table) can legitimately run
				// long, so the hard wall-clock guard is opt-in here — a too-tight
				// cap would abort a real index build and re-abort it every retry.
				// Set MSSQL.DDLRetryOptions.OperationTimeoutMs where the DDL is
				// known to be bounded.  0 = rely on retries + pool recycle only.
				//
				// The statement's own ceiling is _ddlRequestTimeoutMs(), NOT the
				// pool's requestTimeout — see _executeDDLStatement. Because that
				// ceiling is already generous, hitting it means the work does not
				// fit rather than that the server hiccuped, so it is terminal
				// here: five replays would spend five ceilings and discard the
				// partial build each time.
				NonRetryableModes: [libRetry.ERROR_MODES.RequestTimeout],
				OperationTimeoutMs: tmpRetry.OperationTimeoutMs || 0,
				// "AlreadyExists" is always treated as success for DDL — a
				// re-deploy will naturally hit tables that already exist.
				SuccessModes: [libRetry.ERROR_MODES.AlreadyExists]
			});

		// Connect the pool recycle hook if we know how to reach the
		// connection provider (we always do when set up via
		// Meadow-Connection-MSSQL, but test harnesses sometimes wire the
		// schema provider standalone).
		if (this._ConnectionProvider && typeof (this._ConnectionProvider.recyclePool) === 'function')
		{
			tmpOptions.OnRecyclePool = (fRecycleDone) =>
			{
				this._ConnectionProvider.recyclePool((pErr) =>
				{
					// Refresh our pool reference from the connection provider
					// after the recycle so the next attempt uses the fresh pool.
					if (this._ConnectionProvider.pool)
					{
						this._ConnectionPool = this._ConnectionProvider.pool;
					}
					return fRecycleDone(pErr);
				});
			};
		}

		return tmpOptions;
	}

	generateDropTableStatement(pTableName)
	{
		let tmpDropTableStatement = `IF OBJECT_ID('dbo.[${pTableName}]', 'U') IS NOT NULL\n`;
		tmpDropTableStatement += `    DROP TABLE dbo.[${pTableName}];\n`;
		tmpDropTableStatement += `GO`;
		return tmpDropTableStatement;
	}

	generateCreateTableStatement(pMeadowTableSchema)
	{
		this.log.info(`--> Building the table create string for ${pMeadowTableSchema && pMeadowTableSchema.TableName ? pMeadowTableSchema.TableName : '(unknown)'} ...`);

		let tmpPrimaryKey = false;
		let tmpCreateTableStatement = `--   [ ${pMeadowTableSchema.TableName} ]`;
		tmpCreateTableStatement += `\nCREATE TABLE [dbo].[${pMeadowTableSchema.TableName}]\n    (`;
		for (let j = 0; j < pMeadowTableSchema.Columns.length; j++)
		{
			let tmpColumn = pMeadowTableSchema.Columns[j];

			// If we aren't the first column, append a comma.
			if (j > 0)
			{
				tmpCreateTableStatement += `,`;
			}

			tmpCreateTableStatement += `\n`;
			// Dump out each column......
			switch (tmpColumn.DataType)
			{
				case 'ID':
					// There is debate on whether IDENTITY(1,1) is better or not.
					tmpCreateTableStatement += `        [${tmpColumn.Column}] INT NOT NULL IDENTITY PRIMARY KEY`;
					tmpPrimaryKey = tmpColumn.Column;
					break;
				case 'GUID':
					// Use NCHAR to match MigrationGenerator (GUID → NCHAR(size))
					// and the introspector's fixed-width GUID detection.  The
					// previous VARCHAR(254) was inconsistent with later ALTER
					// migrations and caused every introspection-then-diff cycle
					// to re-alter the column.
					//
					// Default size is 255 — UUIDs need 36 but composite GUIDs
					// from integration adapters often exceed that, and silent
					// truncation manifests as missing rows downstream.
					tmpCreateTableStatement += `        [${tmpColumn.Column}] NCHAR(${tmpColumn.Size || '255'}) NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'`;
					break;
				case 'ForeignKey':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] INT NOT NULL DEFAULT 0`;
					tmpPrimaryKey = tmpColumn.Column;
					break;
				case 'Numeric':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] INT NOT NULL DEFAULT 0`;
					break;
				case 'Decimal':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] DECIMAL(${tmpColumn.Size})`;
					break;
				case 'String':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] VARCHAR(${tmpColumn.Size}) DEFAULT ''`;
					break;
				case 'Text':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] TEXT`;
					break;
				case 'DateTime':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] DATETIME`;
					break;
				case 'Boolean':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] TINYINT DEFAULT 0`;
					break;
				case 'JSON':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] NVARCHAR(MAX)`;
					break;
				case 'JSONProxy':
					tmpCreateTableStatement += `        [${tmpColumn.StorageColumn}] NVARCHAR(MAX)`;
					break;
				default:
					this.log.error(`Meadow-MSSQL ${pMeadowTableSchema.TableName}.${tmpColumn.Column} has unsupported DataType [${tmpColumn.DataType}]; no column definition was emitted and the generated DDL will be malformed.`);
					break;
			}
		}
		if (tmpPrimaryKey)
		{
			//				tmpCreateTableStatement += `,\n\n        PRIMARY KEY (${tmpPrimaryKey$})`;
		}
		tmpCreateTableStatement += `\n    );`;

		return tmpCreateTableStatement;
	}

	createTables(pMeadowSchema, fCallback)
	{
		// Now create the Book databases if they don't exist.
		this.fable.Utility.eachLimit(pMeadowSchema.Tables, 1,
			(pTable, fCreateComplete) =>
			{
				return this.createTable(pTable, fCreateComplete)
			},
			(pCreateError) =>
			{
				if (pCreateError)
				{
					this.log.error(`Meadow-MSSQL Error creating tables from Schema: ${pCreateError}`,pCreateError);
				}
				this.log.info('Done creating tables!');
				return fCallback(pCreateError);
			});
	}

	createTable(pMeadowTableSchema, fCallback)
	{
		let tmpTableName = (pMeadowTableSchema && pMeadowTableSchema.TableName) || '(unknown)';
		let tmpCreateTableStatement = this.generateCreateTableStatement(pMeadowTableSchema);

		// Wrap the DDL in the retry helper.  The helper classifies errors
		// and handles:
		//   - AlreadyExists  → treat as success (benign re-deploy)
		//   - NetworkError   → exponential backoff, pool recycle
		//   - RequestTimeout → exponential backoff, pool recycle (covers
		//                      server-side schema-lock contention)
		//   - PoolDegraded   → exponential backoff, pool recycle
		//   - ServerError    → fail fast (syntax/permission errors)
		//   - Unknown        → exponential backoff, no pool recycle
		//
		// Every attempt, failure, and recycle decision is logged so the
		// operator can see at a glance which failure mode is occurring.
		libRetry.runWithRetry(this.log,
			this._ddlRetryOptions(`Meadow-MSSQL CREATE TABLE ${tmpTableName}`),
			(fAttemptDone) =>
			{
				this._executeDDLStatementPromise(tmpCreateTableStatement, `Meadow-MSSQL CREATE TABLE ${tmpTableName}`)
					.then((pResult) => fAttemptDone(null, pResult))
					.catch((pError) => fAttemptDone(pError));
			},
			(pError) =>
			{
				if (pError)
				{
					// runWithRetry already logged the final failure with its
					// classified mode.  Propagate the error without adding
					// another noisy error line.
					return fCallback(pError);
				}
				this.log.info(`Meadow-MSSQL CREATE TABLE ${tmpTableName} success`);
				return fCallback();
			});
	}

	// ========================================================================
	// Index Generation
	// ========================================================================

	/**
	 * Derive index definitions from a Meadow table schema.
	 *
	 * Automatically generates indices for:
	 *   - GUID columns      -> unique index  AK_M_{Column}
	 *   - ForeignKey columns -> regular index IX_M_{Column}
	 *
	 * Column-level Indexed property:
	 *   - Indexed: true     -> regular index IX_M_T_{Table}_C_{Column}
	 *   - Indexed: 'unique' -> unique index  AK_M_T_{Table}_C_{Column}
	 *   - IndexName overrides the auto-generated name (for round-trip fidelity)
	 *
	 * Also includes any explicit entries from pMeadowTableSchema.Indices[]
	 * (for multi-column composite indices).
	 *
	 * Each index definition is:
	 *   { Name, TableName, Columns[], Unique, Strategy }
	 *
	 * @param {object} pMeadowTableSchema - Meadow table schema object
	 * @returns {Array} Array of index definition objects
	 */
	getIndexDefinitionsFromSchema(pMeadowTableSchema)
	{
		let tmpIndices = [];
		let tmpTableName = pMeadowTableSchema.TableName;

		// Auto-detect from column types
		for (let j = 0; j < pMeadowTableSchema.Columns.length; j++)
		{
			let tmpColumn = pMeadowTableSchema.Columns[j];

			switch (tmpColumn.DataType)
			{
				case 'GUID':
					tmpIndices.push(
						{
							Name: `AK_M_${tmpColumn.Column}`,
							TableName: tmpTableName,
							Columns: [tmpColumn.Column],
							Unique: true,
							Strategy: ''
						});
					break;
				case 'ForeignKey':
					tmpIndices.push(
						{
							Name: `IX_M_${tmpColumn.Column}`,
							TableName: tmpTableName,
							Columns: [tmpColumn.Column],
							Unique: false,
							Strategy: ''
						});
					break;
				default:
					// Column-level Indexed property: generates a single-column index
					// with a consistent naming convention.
					//   Indexed: true     -> IX_M_T_{Table}_C_{Column}  (regular)
					//   Indexed: 'unique' -> AK_M_T_{Table}_C_{Column}  (unique)
					// Optional IndexName property overrides the auto-generated name.
					if (tmpColumn.Indexed)
					{
						let tmpIsUnique = (tmpColumn.Indexed === 'unique');
						let tmpPrefix = tmpIsUnique ? 'AK_M_T' : 'IX_M_T';
						let tmpAutoName = `${tmpPrefix}_${tmpTableName}_C_${tmpColumn.Column}`;
						tmpIndices.push(
							{
								Name: tmpColumn.IndexName || tmpAutoName,
								TableName: tmpTableName,
								Columns: [tmpColumn.Column],
								Unique: tmpIsUnique,
								Strategy: ''
							});
					}
					break;
			}
		}

		// Include any explicitly defined indices on the schema
		if (Array.isArray(pMeadowTableSchema.Indices))
		{
			for (let k = 0; k < pMeadowTableSchema.Indices.length; k++)
			{
				let tmpExplicitIndex = pMeadowTableSchema.Indices[k];
				tmpIndices.push(
					{
						Name: tmpExplicitIndex.Name || `IX_${tmpTableName}_${k}`,
						TableName: tmpTableName,
						Columns: Array.isArray(tmpExplicitIndex.Columns) ? tmpExplicitIndex.Columns : [tmpExplicitIndex.Columns],
						Unique: tmpExplicitIndex.Unique || false,
						Strategy: tmpExplicitIndex.Strategy || ''
					});
			}
		}

		return tmpIndices;
	}

	/**
	 * Build the column list for an index, bracket-quoted and comma-separated.
	 * @param {Array} pColumns - Array of column name strings
	 * @returns {string}
	 */
	_buildColumnList(pColumns)
	{
		return pColumns.map((pCol) => { return '[' + pCol + ']'; }).join(', ');
	}

	/**
	 * Generate a full idempotent SQL script for creating all indices on a table.
	 *
	 * MSSQL does not support CREATE INDEX IF NOT EXISTS, so we use
	 * sys.indexes to check for existing indices before creating them.
	 *
	 * @param {object} pMeadowTableSchema - Meadow table schema object
	 * @returns {string} Complete SQL script
	 */
	generateCreateIndexScript(pMeadowTableSchema)
	{
		let tmpIndices = this.getIndexDefinitionsFromSchema(pMeadowTableSchema);
		let tmpTableName = pMeadowTableSchema.TableName;

		if (tmpIndices.length === 0)
		{
			return `-- No indices to create for ${tmpTableName}\n`;
		}

		let tmpScript = `-- Index Definitions for ${tmpTableName} -- Generated ${new Date().toJSON()}\n\n`;

		for (let i = 0; i < tmpIndices.length; i++)
		{
			let tmpIndex = tmpIndices[i];
			let tmpColumnList = this._buildColumnList(tmpIndex.Columns);
			let tmpCreateKeyword = tmpIndex.Unique ? 'CREATE UNIQUE NONCLUSTERED INDEX' : 'CREATE NONCLUSTERED INDEX';

			tmpScript += `-- Index: ${tmpIndex.Name}\n`;
			tmpScript += `IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '${tmpIndex.Name}' AND object_id = OBJECT_ID('dbo.[${tmpIndex.TableName}]'))\n`;
			tmpScript += `    ${tmpCreateKeyword} [${tmpIndex.Name}] ON [dbo].[${tmpIndex.TableName}] (${tmpColumnList});\n`;
			tmpScript += `GO\n\n`;
		}

		return tmpScript;
	}

	/**
	 * Generate an array of individual CREATE INDEX SQL statements for a table.
	 *
	 * Each entry is an object with:
	 *   { Name, Statement, CheckStatement }
	 *
	 * - Statement: the raw CREATE [UNIQUE] NONCLUSTERED INDEX ... SQL
	 * - CheckStatement: a SELECT against sys.indexes that returns the count
	 *   of matching indices (0 = does not exist)
	 *
	 * @param {object} pMeadowTableSchema - Meadow table schema object
	 * @returns {Array} Array of { Name, Statement, CheckStatement } objects
	 */
	generateCreateIndexStatements(pMeadowTableSchema)
	{
		let tmpIndices = this.getIndexDefinitionsFromSchema(pMeadowTableSchema);
		let tmpStatements = [];

		for (let i = 0; i < tmpIndices.length; i++)
		{
			let tmpIndex = tmpIndices[i];
			let tmpColumnList = this._buildColumnList(tmpIndex.Columns);
			let tmpCreateKeyword = tmpIndex.Unique ? 'CREATE UNIQUE NONCLUSTERED INDEX' : 'CREATE NONCLUSTERED INDEX';

			tmpStatements.push(
				{
					Name: tmpIndex.Name,
					Statement: `${tmpCreateKeyword} [${tmpIndex.Name}] ON [dbo].[${tmpIndex.TableName}] (${tmpColumnList})`,
					CheckStatement: `SELECT COUNT(*) AS IndexExists FROM sys.indexes WHERE name = '${tmpIndex.Name}' AND object_id = OBJECT_ID('dbo.[${tmpIndex.TableName}]')`
				});
		}

		return tmpStatements;
	}

	/**
	 * Programmatically create a single index on the database.
	 *
	 * Checks sys.indexes first; only runs CREATE INDEX if the index
	 * does not yet exist.
	 *
	 * @param {object} pIndexStatement - Object from generateCreateIndexStatements()
	 * @param {Function} fCallback - callback(pError)
	 */
	createIndex(pIndexStatement, fCallback)
	{
		if (!this._ConnectionPool)
		{
			this.log.error(`Meadow-MSSQL CREATE INDEX ${pIndexStatement.Name} failed: not connected.`);
			return fCallback(new Error('Not connected to MSSQL'));
		}

		// Wrap the (check, then create) sequence in the retry helper — on
		// a flaky connection either query can time out, and the classifier
		// will surface whether it's network, lock contention, or a stale
		// pool.  The retry helper will recycle the pool between attempts
		// when the failure mode recommends it.
		libRetry.runWithRetry(this.log,
			this._ddlRetryOptions(`Meadow-MSSQL CREATE INDEX ${pIndexStatement.Name}`),
			(fAttemptDone) =>
			{
				this._ConnectionPool.query(pIndexStatement.CheckStatement)
					.then((pCheckResult) =>
					{
						let tmpExists = pCheckResult && pCheckResult.recordset && pCheckResult.recordset[0] && pCheckResult.recordset[0].IndexExists > 0;
						if (tmpExists)
						{
							// Signal success with a sentinel result so the
							// outer callback can log the "already exists" case.
							return fAttemptDone(null, { AlreadyExisted: true });
						}
						this._executeDDLStatementPromise(pIndexStatement.Statement, `Meadow-MSSQL CREATE INDEX ${pIndexStatement.Name}`)
							.then(() => fAttemptDone(null, { AlreadyExisted: false }))
							.catch((pCreateError) => fAttemptDone(pCreateError));
					})
					.catch((pCheckError) => fAttemptDone(pCheckError));
			},
			(pError, pResult) =>
			{
				if (pError)
				{
					return fCallback(pError);
				}
				if (pResult && pResult.AlreadyExisted)
				{
					this.log.info(`Meadow-MSSQL INDEX ${pIndexStatement.Name} already exists, skipping.`);
				}
				else
				{
					this.log.info(`Meadow-MSSQL CREATE INDEX ${pIndexStatement.Name} executed successfully.`);
				}
				return fCallback();
			});
	}

	/**
	 * Programmatically drop a single index if it exists (idempotent).
	 *
	 * @param {string} pTableName
	 * @param {string} pIndexName
	 * @param {Function} fCallback - callback(pError)
	 */
	dropIndex(pTableName, pIndexName, fCallback)
	{
		if (!this._ConnectionPool)
		{
			this.log.error(`Meadow-MSSQL DROP INDEX ${pIndexName} failed: not connected.`);
			return fCallback(new Error('Not connected to MSSQL'));
		}

		let tmpStatement = `DROP INDEX IF EXISTS [${pIndexName}] ON [dbo].[${pTableName}]`;

		libRetry.runWithRetry(this.log,
			this._ddlRetryOptions(`Meadow-MSSQL DROP INDEX ${pIndexName}`),
			(fAttemptDone) =>
			{
				this._executeDDLStatementPromise(tmpStatement, `Meadow-MSSQL DROP INDEX ${pIndexName}`)
					.then(() => { return fAttemptDone(null); })
					.catch((pDropError) => { return fAttemptDone(pDropError); });
			},
			(pError) =>
			{
				if (pError)
				{
					return fCallback(pError);
				}
				this.log.info(`Meadow-MSSQL DROP INDEX ${pIndexName} on ${pTableName} executed.`);
				return fCallback();
			});
	}

	/**
	 * Programmatically create all indices for a single table.
	 *
	 * @param {object} pMeadowTableSchema - Meadow table schema object
	 * @param {Function} fCallback - callback(pError)
	 */
	createIndices(pMeadowTableSchema, fCallback)
	{
		let tmpStatements = this.generateCreateIndexStatements(pMeadowTableSchema);

		if (tmpStatements.length === 0)
		{
			this.log.info(`No indices to create for ${pMeadowTableSchema.TableName}.`);
			return fCallback();
		}

		this.fable.Utility.eachLimit(tmpStatements, 1,
			(pStatement, fCreateComplete) =>
			{
				return this.createIndex(pStatement, fCreateComplete);
			},
			(pCreateError) =>
			{
				if (pCreateError)
				{
					this.log.error(`Meadow-MSSQL Error creating indices for ${pMeadowTableSchema.TableName}: ${pCreateError}`, pCreateError);
				}
				else
				{
					this.log.info(`Done creating indices for ${pMeadowTableSchema.TableName}!`);
				}
				return fCallback(pCreateError);
			});
	}

	/**
	 * Programmatically create all indices for all tables in a schema.
	 *
	 * @param {object} pMeadowSchema - Meadow schema object with Tables array
	 * @param {Function} fCallback - callback(pError)
	 */
	createAllIndices(pMeadowSchema, fCallback)
	{
		this.fable.Utility.eachLimit(pMeadowSchema.Tables, 1,
			(pTable, fCreateComplete) =>
			{
				return this.createIndices(pTable, fCreateComplete);
			},
			(pCreateError) =>
			{
				if (pCreateError)
				{
					this.log.error(`Meadow-MSSQL Error creating indices from schema: ${pCreateError}`, pCreateError);
				}
				this.log.info('Done creating all indices!');
				return fCallback(pCreateError);
			});
	}

	// ========================================================================
	// Database Introspection
	// ========================================================================

	/**
	 * List all user tables in the connected MSSQL database.
	 *
	 * @param {Function} fCallback - callback(pError, pTableNames)
	 */
	listTables(fCallback)
	{
		if (!this._ConnectionPool)
		{
			return fCallback(new Error('Not connected to MSSQL'));
		}

		this._ConnectionPool.query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo' ORDER BY TABLE_NAME")
			.then((pResult) =>
			{
				let tmpNames = pResult.recordset.map((pRow) => { return pRow.TABLE_NAME; });
				return fCallback(null, tmpNames);
			})
			.catch((pError) =>
			{
				this.log.error('Meadow-MSSQL listTables failed!', pError);
				return fCallback(pError);
			});
	}

	/**
	 * Map a MSSQL native type to a Meadow DataType.
	 *
	 * @param {object} pColumnInfo - INFORMATION_SCHEMA.COLUMNS row with IS_IDENTITY
	 * @param {Set} pForeignKeyColumns - Set of column names that have FK constraints
	 * @returns {object} { DataType, Size }
	 */
	_mapMSSQLTypeToMeadow(pColumnInfo, pForeignKeyColumns)
	{
		let tmpName = pColumnInfo.COLUMN_NAME;
		let tmpType = (pColumnInfo.DATA_TYPE || '').toUpperCase().trim();

		// Priority 1: IDENTITY column → ID
		if (pColumnInfo.IS_IDENTITY === 1)
		{
			return { DataType: 'ID', Size: '' };
		}

		// Priority 2: Column name contains "GUID" and type is a fixed-width
		// character type (CHAR/NCHAR) → GUID.  Variable-width types
		// (VARCHAR/NVARCHAR) are intentionally excluded: meadow schemas
		// materialize GUID columns as CHAR/NCHAR (see MigrationGenerator
		// and Meadow-Schema-* providers), and a variable-width column
		// whose name happens to contain "GUID" (e.g. ExternalSyncGUID
		// defined as String(255)) is a regular string column.  Including
		// it here would cause an infinite ALTER loop: the diff would see
		// DataType=GUID (introspection) vs DataType=String (target) on
		// every run and issue an ALTER that doesn't actually change the
		// native type.
		if (tmpName.toUpperCase().indexOf('GUID') >= 0 && (tmpType === 'CHAR' || tmpType === 'NCHAR'))
		{
			return { DataType: 'GUID', Size: pColumnInfo.CHARACTER_MAXIMUM_LENGTH ? String(pColumnInfo.CHARACTER_MAXIMUM_LENGTH) : '' };
		}

		// Priority 3: Has FK constraint → ForeignKey
		if (pForeignKeyColumns && pForeignKeyColumns.has(tmpName))
		{
			return { DataType: 'ForeignKey', Size: '' };
		}

		// Priority 4: Native type mapping
		if (tmpType === 'DECIMAL' || tmpType === 'NUMERIC')
		{
			let tmpSize = '';
			if (pColumnInfo.NUMERIC_PRECISION)
			{
				tmpSize = String(pColumnInfo.NUMERIC_PRECISION);
				if (pColumnInfo.NUMERIC_SCALE && pColumnInfo.NUMERIC_SCALE > 0)
				{
					tmpSize += ',' + String(pColumnInfo.NUMERIC_SCALE);
				}
			}
			return { DataType: 'Decimal', Size: tmpSize };
		}

		if (tmpType === 'FLOAT' || tmpType === 'REAL')
		{
			return { DataType: 'Decimal', Size: '' };
		}

		if (tmpType === 'DATETIME' || tmpType === 'DATETIME2' || tmpType === 'SMALLDATETIME' || tmpType === 'DATETIMEOFFSET')
		{
			return { DataType: 'DateTime', Size: '' };
		}

		if (tmpType === 'TINYINT')
		{
			let tmpLowerName = tmpName.toLowerCase();
			if (tmpLowerName.indexOf('is') === 0 || tmpLowerName.indexOf('has') === 0 ||
				tmpLowerName.indexOf('in') === 0 || tmpLowerName === 'deleted' ||
				tmpLowerName === 'active' || tmpLowerName === 'enabled')
			{
				return { DataType: 'Boolean', Size: '' };
			}
			return { DataType: 'Numeric', Size: '' };
		}

		if (tmpType === 'BIT')
		{
			return { DataType: 'Boolean', Size: '' };
		}

		if (tmpType === 'TEXT' || tmpType === 'NTEXT')
		{
			return { DataType: 'Text', Size: '' };
		}

		if (tmpType === 'VARCHAR' || tmpType === 'NVARCHAR' || tmpType === 'CHAR' || tmpType === 'NCHAR')
		{
			let tmpSize = pColumnInfo.CHARACTER_MAXIMUM_LENGTH ? String(pColumnInfo.CHARACTER_MAXIMUM_LENGTH) : '';
			// -1 means MAX in MSSQL
			if (tmpSize === '-1')
			{
				return { DataType: 'Text', Size: '' };
			}
			return { DataType: 'String', Size: tmpSize };
		}

		if (tmpType === 'INT' || tmpType === 'INTEGER' || tmpType === 'BIGINT' || tmpType === 'SMALLINT')
		{
			return { DataType: 'Numeric', Size: '' };
		}

		// Default fallback
		return { DataType: 'Text', Size: '' };
	}

	/**
	 * Get column definitions for a single table.
	 *
	 * @param {string} pTableName - Name of the table
	 * @param {Function} fCallback - callback(pError, pColumns)
	 */
	introspectTableColumns(pTableName, fCallback)
	{
		if (!this._ConnectionPool)
		{
			return fCallback(new Error('Not connected to MSSQL'));
		}

		let tmpColumnQuery = `SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE, c.COLUMN_DEFAULT, CASE WHEN ic.object_id IS NOT NULL THEN 1 ELSE 0 END AS IS_IDENTITY FROM INFORMATION_SCHEMA.COLUMNS c LEFT JOIN sys.identity_columns ic ON ic.object_id = OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) AND ic.name = c.COLUMN_NAME WHERE c.TABLE_NAME = '${pTableName}' AND c.TABLE_SCHEMA = 'dbo' ORDER BY c.ORDINAL_POSITION`;

		let tmpFKQuery = `SELECT COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName FROM sys.foreign_key_columns fc WHERE fc.parent_object_id = OBJECT_ID('dbo.${pTableName}')`;

		this._ConnectionPool.query(tmpColumnQuery)
			.then((pColumnResult) =>
			{
				this._ConnectionPool.query(tmpFKQuery)
					.then((pFKResult) =>
					{
						let tmpFKColumnSet = new Set(pFKResult.recordset.map((pRow) => { return pRow.ColumnName; }));

						let tmpResult = [];
						for (let i = 0; i < pColumnResult.recordset.length; i++)
						{
							let tmpCol = pColumnResult.recordset[i];
							let tmpTypeInfo = this._mapMSSQLTypeToMeadow(tmpCol, tmpFKColumnSet);

							let tmpColumnDef = {
								Column: tmpCol.COLUMN_NAME,
								DataType: tmpTypeInfo.DataType
							};

							if (tmpTypeInfo.Size)
							{
								tmpColumnDef.Size = tmpTypeInfo.Size;
							}

							tmpResult.push(tmpColumnDef);
						}

						return fCallback(null, tmpResult);
					})
					.catch((pFKError) =>
					{
						this.log.error(`Meadow-MSSQL introspectTableColumns FK query for ${pTableName} failed!`, pFKError);
						return fCallback(pFKError);
					});
			})
			.catch((pError) =>
			{
				this.log.error(`Meadow-MSSQL introspectTableColumns for ${pTableName} failed!`, pError);
				return fCallback(pError);
			});
	}

	/**
	 * Get raw index definitions for a single table from the database.
	 *
	 * @param {string} pTableName - Name of the table
	 * @param {Function} fCallback - callback(pError, pIndices)
	 */
	introspectTableIndices(pTableName, fCallback)
	{
		if (!this._ConnectionPool)
		{
			return fCallback(new Error('Not connected to MSSQL'));
		}

		let tmpQuery = `SELECT i.name AS IndexName, c.name AS ColumnName, i.is_unique, ic.key_ordinal, i.is_primary_key FROM sys.indexes i JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id WHERE i.object_id = OBJECT_ID('dbo.${pTableName}') AND i.type > 0 ORDER BY i.name, ic.key_ordinal`;

		this._ConnectionPool.query(tmpQuery)
			.then((pResult) =>
			{
				// Group by index name, skip primary key indices
				let tmpIndexMap = {};
				for (let i = 0; i < pResult.recordset.length; i++)
				{
					let tmpRow = pResult.recordset[i];
					if (tmpRow.is_primary_key)
					{
						continue;
					}

					if (!tmpIndexMap[tmpRow.IndexName])
					{
						tmpIndexMap[tmpRow.IndexName] = {
							Name: tmpRow.IndexName,
							Columns: [],
							Unique: tmpRow.is_unique
						};
					}
					tmpIndexMap[tmpRow.IndexName].Columns.push(tmpRow.ColumnName);
				}

				let tmpIndices = Object.values(tmpIndexMap);
				return fCallback(null, tmpIndices);
			})
			.catch((pError) =>
			{
				this.log.error(`Meadow-MSSQL introspectTableIndices for ${pTableName} failed!`, pError);
				return fCallback(pError);
			});
	}

	/**
	 * Get foreign key relationships for a single table.
	 *
	 * @param {string} pTableName - Name of the table
	 * @param {Function} fCallback - callback(pError, pForeignKeys)
	 */
	introspectTableForeignKeys(pTableName, fCallback)
	{
		if (!this._ConnectionPool)
		{
			return fCallback(new Error('Not connected to MSSQL'));
		}

		let tmpQuery = `SELECT COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName, OBJECT_NAME(fc.referenced_object_id) AS ReferencedTable, COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferencedColumn FROM sys.foreign_key_columns fc WHERE fc.parent_object_id = OBJECT_ID('dbo.${pTableName}')`;

		this._ConnectionPool.query(tmpQuery)
			.then((pResult) =>
			{
				let tmpResult = [];
				for (let i = 0; i < pResult.recordset.length; i++)
				{
					let tmpRow = pResult.recordset[i];
					tmpResult.push(
						{
							Column: tmpRow.ColumnName,
							ReferencedTable: tmpRow.ReferencedTable,
							ReferencedColumn: tmpRow.ReferencedColumn
						});
				}

				return fCallback(null, tmpResult);
			})
			.catch((pError) =>
			{
				this.log.error(`Meadow-MSSQL introspectTableForeignKeys for ${pTableName} failed!`, pError);
				return fCallback(pError);
			});
	}

	/**
	 * Classify an index for round-trip fidelity.
	 *
	 * @param {object} pIndex - { Name, Columns[], Unique }
	 * @param {string} pTableName - Table name for pattern matching
	 * @returns {object} { type, column, indexed, indexName }
	 */
	_classifyIndex(pIndex, pTableName)
	{
		if (pIndex.Columns.length !== 1)
		{
			return { type: 'explicit' };
		}

		let tmpColumn = pIndex.Columns[0];
		let tmpName = pIndex.Name;

		if (tmpName === `AK_M_${tmpColumn}`)
		{
			return { type: 'guid-auto', column: tmpColumn };
		}

		if (tmpName === `IX_M_${tmpColumn}`)
		{
			return { type: 'fk-auto', column: tmpColumn };
		}

		let tmpRegularAutoName = `IX_M_T_${pTableName}_C_${tmpColumn}`;
		if (tmpName === tmpRegularAutoName && !pIndex.Unique)
		{
			return { type: 'column-auto', column: tmpColumn, indexed: true };
		}

		let tmpUniqueAutoName = `AK_M_T_${pTableName}_C_${tmpColumn}`;
		if (tmpName === tmpUniqueAutoName && pIndex.Unique)
		{
			return { type: 'column-auto', column: tmpColumn, indexed: 'unique' };
		}

		return {
			type: 'column-named',
			column: tmpColumn,
			indexed: pIndex.Unique ? 'unique' : true,
			indexName: tmpName
		};
	}

	/**
	 * Generate a complete DDL-level schema for a single table.
	 *
	 * @param {string} pTableName - Name of the table
	 * @param {Function} fCallback - callback(pError, pTableSchema)
	 */
	introspectTableSchema(pTableName, fCallback)
	{
		this.introspectTableColumns(pTableName,
			(pColumnError, pColumns) =>
			{
				if (pColumnError)
				{
					return fCallback(pColumnError);
				}

				this.introspectTableIndices(pTableName,
					(pIndexError, pIndices) =>
					{
						if (pIndexError)
						{
							return fCallback(pIndexError);
						}

						this.introspectTableForeignKeys(pTableName,
							(pFKError, pForeignKeys) =>
							{
								if (pFKError)
								{
									return fCallback(pFKError);
								}

								let tmpColumnMap = {};
								for (let i = 0; i < pColumns.length; i++)
								{
									tmpColumnMap[pColumns[i].Column] = pColumns[i];
								}

								let tmpExplicitIndices = [];

								for (let i = 0; i < pIndices.length; i++)
								{
									let tmpClassification = this._classifyIndex(pIndices[i], pTableName);

									switch (tmpClassification.type)
									{
										case 'column-auto':
											if (tmpColumnMap[tmpClassification.column])
											{
												tmpColumnMap[tmpClassification.column].Indexed = tmpClassification.indexed;
											}
											break;
										case 'column-named':
											if (tmpColumnMap[tmpClassification.column])
											{
												tmpColumnMap[tmpClassification.column].Indexed = tmpClassification.indexed;
												tmpColumnMap[tmpClassification.column].IndexName = tmpClassification.indexName;
											}
											break;
										case 'guid-auto':
											if (tmpColumnMap[tmpClassification.column] &&
												tmpColumnMap[tmpClassification.column].DataType !== 'GUID')
											{
												tmpColumnMap[tmpClassification.column].DataType = 'GUID';
											}
											break;
										case 'fk-auto':
											if (tmpColumnMap[tmpClassification.column] &&
												tmpColumnMap[tmpClassification.column].DataType !== 'ForeignKey')
											{
												tmpColumnMap[tmpClassification.column].DataType = 'ForeignKey';
											}
											break;
										case 'explicit':
											tmpExplicitIndices.push(
												{
													Name: pIndices[i].Name,
													Columns: pIndices[i].Columns,
													Unique: pIndices[i].Unique
												});
											break;
									}
								}

								let tmpSchema = {
									TableName: pTableName,
									Columns: pColumns
								};

								if (tmpExplicitIndices.length > 0)
								{
									tmpSchema.Indices = tmpExplicitIndices;
								}

								if (pForeignKeys.length > 0)
								{
									tmpSchema.ForeignKeys = pForeignKeys;
								}

								return fCallback(null, tmpSchema);
							});
					});
			});
	}

	/**
	 * Generate DDL schemas for ALL tables in the database.
	 *
	 * @param {Function} fCallback - callback(pError, { Tables: [...] })
	 */
	introspectDatabaseSchema(fCallback)
	{
		this.listTables(
			(pError, pTableNames) =>
			{
				if (pError)
				{
					return fCallback(pError);
				}

				let tmpTables = [];

				this.fable.Utility.eachLimit(pTableNames, 1,
					(pTableName, fEachComplete) =>
					{
						this.introspectTableSchema(pTableName,
							(pSchemaError, pSchema) =>
							{
								if (pSchemaError)
								{
									return fEachComplete(pSchemaError);
								}
								tmpTables.push(pSchema);
								return fEachComplete();
							});
					},
					(pEachError) =>
					{
						if (pEachError)
						{
							this.log.error('Meadow-MSSQL introspectDatabaseSchema failed!', pEachError);
							return fCallback(pEachError);
						}
						return fCallback(null, { Tables: tmpTables });
					});
			});
	}

	/**
	 * Map a DDL DataType to a Meadow Package schema Type.
	 *
	 * @param {string} pDataType - The DDL-level DataType
	 * @param {string} pColumnName - The column name (for magic column detection)
	 * @returns {string} The Meadow Package Type
	 */
	_mapDataTypeToMeadowType(pDataType, pColumnName)
	{
		let tmpLowerName = pColumnName.toLowerCase();

		if (tmpLowerName === 'createdate') return 'CreateDate';
		if (tmpLowerName === 'creatingiduser') return 'CreateIDUser';
		if (tmpLowerName === 'updatedate') return 'UpdateDate';
		if (tmpLowerName === 'updatingiduser') return 'UpdateIDUser';
		if (tmpLowerName === 'deleted') return 'Deleted';
		if (tmpLowerName === 'deletingiduser') return 'DeleteIDUser';
		if (tmpLowerName === 'deletedate') return 'DeleteDate';

		switch (pDataType)
		{
			case 'ID': return 'AutoIdentity';
			case 'GUID': return 'AutoGUID';
			case 'ForeignKey': return 'Numeric';
			case 'Numeric': return 'Numeric';
			case 'Decimal': return 'Numeric';
			case 'String': return 'String';
			case 'Text': return 'String';
			case 'DateTime': return 'DateTime';
			case 'Boolean': return 'Boolean';
			case 'JSON': return 'JSON';
			case 'JSONProxy': return 'JSONProxy';
			default: return 'String';
		}
	}

	/**
	 * Get a default value for a given DataType.
	 *
	 * @param {string} pDataType - The DDL-level DataType
	 * @returns {*} The default value
	 */
	_getDefaultValue(pDataType)
	{
		switch (pDataType)
		{
			case 'ID': return 0;
			case 'GUID': return '';
			case 'ForeignKey': return 0;
			case 'Numeric': return 0;
			case 'Decimal': return 0.0;
			case 'String': return '';
			case 'Text': return '';
			case 'DateTime': return '';
			case 'Boolean': return false;
			case 'JSON': return {};
			case 'JSONProxy': return {};
			default: return '';
		}
	}

	/**
	 * Generate a Meadow package JSON for a single table.
	 *
	 * @param {string} pTableName - Name of the table
	 * @param {Function} fCallback - callback(pError, pPackage)
	 */
	generateMeadowPackageFromTable(pTableName, fCallback)
	{
		this.introspectTableSchema(pTableName,
			(pError, pSchema) =>
			{
				if (pError)
				{
					return fCallback(pError);
				}

				let tmpDefaultIdentifier = '';
				let tmpSchemaEntries = [];
				let tmpDefaultObject = {};

				for (let i = 0; i < pSchema.Columns.length; i++)
				{
					let tmpCol = pSchema.Columns[i];
					let tmpMeadowType = this._mapDataTypeToMeadowType(tmpCol.DataType, tmpCol.Column);

					if (tmpCol.DataType === 'ID')
					{
						tmpDefaultIdentifier = tmpCol.Column;
					}

					let tmpEntry = {
						Column: tmpCol.Column,
						Type: tmpMeadowType
					};

					if (tmpCol.Size)
					{
						tmpEntry.Size = tmpCol.Size;
					}

					tmpSchemaEntries.push(tmpEntry);
					tmpDefaultObject[tmpCol.Column] = this._getDefaultValue(tmpCol.DataType);
				}

				let tmpPackage = {
					Scope: pTableName,
					DefaultIdentifier: tmpDefaultIdentifier,
					Schema: tmpSchemaEntries,
					DefaultObject: tmpDefaultObject
				};

				return fCallback(null, tmpPackage);
			});
	}
}

module.exports = MeadowSchemaMSSQL;
