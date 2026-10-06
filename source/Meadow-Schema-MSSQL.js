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

// MSSQL's integer types, narrowest first.  TINYINT is the only unsigned one;
// there is no unsigned SMALLINT/INT/BIGINT, which is why a schema's unsigned
// 32-bit ID has to be stored as BIGINT here.
const MSSQL_INTEGER_TYPES = (
	[
		{ Type: 'TINYINT', Signed: false, Precision: 8, Radix: 2 },
		{ Type: 'SMALLINT', Signed: true, Precision: 16, Radix: 2 },
		{ Type: 'INT', Signed: true, Precision: 32, Radix: 2 },
		{ Type: 'BIGINT', Signed: true, Precision: 64, Radix: 2 }
	]);
const MSSQL_INTEGER_RANK = { TINYINT: 0, SMALLINT: 1, INT: 2, BIGINT: 3 };

// The logical integer type Stricture gives each DataType.  Used only when a
// schema was compiled before Stricture recorded Signed/Precision/Radix on columns.
const LOGICAL_INTEGER_DEFAULTS = (
	{
		ID: { Signed: false, Precision: 32, Radix: 2 },
		ForeignKey: { Signed: false, Precision: 32, Radix: 2 },
		Numeric: { Signed: true, Precision: 32, Radix: 2 }
	});

/**
 * Inclusive value range of a logical integer type: Precision digits in base
 * Radix (2 = bits, 10 = decimal digits), signed or not.
 *
 * @param {{ Signed: boolean, Precision: number, Radix: number }} pType
 * @return {{ Min: bigint, Max: bigint }}
 */
function integerRange(pType)
{
	let tmpPrecision = BigInt(pType.Precision);
	if (pType.Radix === 10)
	{
		let tmpMax = (10n ** tmpPrecision) - 1n;
		return { Min: pType.Signed ? -tmpMax : 0n, Max: tmpMax };
	}
	if (!pType.Signed)
	{
		return { Min: 0n, Max: (2n ** tmpPrecision) - 1n };
	}
	return { Min: -(2n ** (tmpPrecision - 1n)), Max: (2n ** (tmpPrecision - 1n)) - 1n };
}

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

	/**
	 * The logical integer type a schema column asks for: its Stricture
	 * `Signed` / `Precision` / `Radix`, or the DataType default for older
	 * schemas.  A Precision without a Radix is taken to be in bits.
	 *
	 * @param {Object} pColumn - Meadow table schema column
	 * @return {{ Signed: boolean, Precision: number, Radix: number }|null} Null for non-integer columns.
	 */
	getLogicalIntegerType(pColumn)
	{
		if (!pColumn || !LOGICAL_INTEGER_DEFAULTS.hasOwnProperty(pColumn.DataType))
		{
			return null;
		}
		let tmpDefault = LOGICAL_INTEGER_DEFAULTS[pColumn.DataType];
		return (
			{
				Signed: (typeof (pColumn.Signed) === 'boolean') ? pColumn.Signed : tmpDefault.Signed,
				Precision: (typeof (pColumn.Precision) === 'number') ? pColumn.Precision : tmpDefault.Precision,
				Radix: (typeof (pColumn.Precision) === 'number') ? ((typeof (pColumn.Radix) === 'number') ? pColumn.Radix : 2) : tmpDefault.Radix
			});
	}

	/**
	 * The narrowest MSSQL integer type that can hold every value of a logical
	 * integer type.  This is where MSSQL's lack of unsigned types is absorbed:
	 * unsigned 32-bit becomes BIGINT, the only type covering 0..4,294,967,295.
	 *
	 * @param {{ Signed: boolean, Precision: number, Radix: number }} pIntegerType
	 * @return {string} TINYINT, SMALLINT, INT or BIGINT
	 */
	getNativeIntegerType(pIntegerType)
	{
		let tmpWanted = integerRange(pIntegerType);
		for (let i = 0; i < MSSQL_INTEGER_TYPES.length; i++)
		{
			let tmpRange = integerRange(MSSQL_INTEGER_TYPES[i]);
			if ((tmpRange.Min <= tmpWanted.Min) && (tmpRange.Max >= tmpWanted.Max))
			{
				return MSSQL_INTEGER_TYPES[i].Type;
			}
		}
		// Unsigned 64-bit: nothing covers it.  BIGINT holds the lower half.
		this.log.warn(`Meadow-MSSQL has no integer type for ${pIntegerType.Signed ? 'signed' : 'unsigned'} values of precision ${pIntegerType.Precision} (radix ${pIntegerType.Radix}); using BIGINT, which rejects values above 9,223,372,036,854,775,807.`);
		return 'BIGINT';
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
					// Integer widths come from the column's logical type, so an
					// unsigned 32-bit ID is BIGINT here (see getNativeIntegerType).
					// Tables created narrower are widened by migrateColumns().
					tmpCreateTableStatement += `        [${tmpColumn.Column}] ${this.getNativeIntegerType(this.getLogicalIntegerType(tmpColumn))} NOT NULL IDENTITY PRIMARY KEY`;
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
					tmpCreateTableStatement += `        [${tmpColumn.Column}] ${this.getNativeIntegerType(this.getLogicalIntegerType(tmpColumn))} NOT NULL DEFAULT 0`;
					tmpPrimaryKey = tmpColumn.Column;
					break;
				case 'Numeric':
					tmpCreateTableStatement += `        [${tmpColumn.Column}] ${this.getNativeIntegerType(this.getLogicalIntegerType(tmpColumn))} NOT NULL DEFAULT 0`;
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
	// Integer Widening (logical integer types MSSQL can only hold wider)
	// ========================================================================

	/**
	 * The physical work behind widenIntegerColumns, split out so tests can
	 * substitute the database round trips.
	 *
	 * @param {string} pStatement - T-SQL to run on the DDL budget.
	 * @param {string} pOperationName - Label for progress lines.
	 * @return {Promise<Object>} The driver result.
	 */
	_widenQuery(pStatement, pOperationName)
	{
		return this._executeDDLStatementPromise(pStatement, pOperationName);
	}

	/**
	 * Read everything widenIntegerColumns needs to decide what to do with a
	 * table: its identity column, whether a shadow / retired copy is lying
	 * around from an earlier run, and anything that would not survive a
	 * rename swap.
	 *
	 * @param {string} pTableName
	 * @param {string} pShadowName
	 * @param {string} pRetiredName
	 * @return {Promise<Object>}
	 */
	async _inspectTableForWiden(pTableName, pShadowName, pRetiredName)
	{
		let tmpObj = (pName) => `OBJECT_ID(N'dbo.${this._escapeSQLString(this._quoteName(pName))}')`;
		let tmpResult = await this._widenQuery(`
SELECT
	${tmpObj(pTableName)} AS TableID,
	${tmpObj(pShadowName)} AS ShadowID,
	${tmpObj(pRetiredName)} AS RetiredID;
SELECT c.name AS ColumnName, t.name AS TypeName, c.is_identity AS IsIdentity, c.is_computed AS IsComputed, c.is_nullable AS IsNullable
	FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
	WHERE c.object_id = ${tmpObj(pTableName)} ORDER BY c.column_id;
SELECT c.name AS ColumnName, t.name AS TypeName, c.is_identity AS IsIdentity
	FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
	WHERE c.object_id = ${tmpObj(pShadowName)} ORDER BY c.column_id;
SELECT CAST('Foreign key ' + fk.name + ' on ' + OBJECT_NAME(fk.parent_object_id) AS NVARCHAR(600)) COLLATE DATABASE_DEFAULT AS Blocker
	FROM sys.foreign_keys fk WHERE fk.referenced_object_id = ${tmpObj(pTableName)} AND fk.parent_object_id <> fk.referenced_object_id
UNION ALL SELECT CAST('Schema-bound ' + o.type_desc + ' ' + o.name AS NVARCHAR(600)) COLLATE DATABASE_DEFAULT
	FROM sys.sql_expression_dependencies d JOIN sys.objects o ON o.object_id = d.referencing_id
	WHERE d.referenced_id = ${tmpObj(pTableName)} AND d.is_schema_bound_reference = 1
UNION ALL SELECT CAST('Trigger ' + tr.name AS NVARCHAR(600)) COLLATE DATABASE_DEFAULT FROM sys.triggers tr WHERE tr.parent_id = ${tmpObj(pTableName)}
UNION ALL SELECT CAST('Index ' + i.name + ' (' + i.type_desc + ')' AS NVARCHAR(600)) COLLATE DATABASE_DEFAULT FROM sys.indexes i
	WHERE i.object_id = ${tmpObj(pTableName)} AND i.type NOT IN (0, 1, 2)
UNION ALL SELECT CAST('Clustered index ' + i.name + ' is not the primary key' AS NVARCHAR(600)) COLLATE DATABASE_DEFAULT FROM sys.indexes i
	WHERE i.object_id = ${tmpObj(pTableName)} AND i.type = 1 AND i.is_primary_key = 0;`, `Meadow-MSSQL widen ${pTableName}: inspect`);

		let tmpIDs = tmpResult.recordsets[0][0];
		let tmpColumns = tmpResult.recordsets[1];
		return (
			{
				TableExists: tmpIDs.TableID !== null,
				ShadowExists: tmpIDs.ShadowID !== null,
				RetiredExists: tmpIDs.RetiredID !== null,
				Columns: tmpColumns,
				Identity: tmpColumns.find((pColumn) => pColumn.IsIdentity) || null,
				ShadowColumns: tmpResult.recordsets[2],
				Blockers: tmpResult.recordsets[3].map((pRow) => pRow.Blocker).concat(
					tmpColumns.filter((pColumn) => pColumn.IsComputed).map((pColumn) => `Computed column ${pColumn.ColumnName}`))
			});
	}

	/**
	 * Bracket-quote an identifier for T-SQL.
	 *
	 * @param {string} pName
	 * @return {string}
	 */
	_quoteName(pName)
	{
		return `[${String(pName).replace(/\]/g, ']]')}]`;
	}

	/**
	 * Escape a value for use inside an N'...' literal.
	 *
	 * @param {string} pValue
	 * @return {string}
	 */
	_escapeSQLString(pValue)
	{
		return String(pValue).replace(/'/g, "''");
	}

	/**
	 * Apply the column modifications from a schema diff that this connector
	 * has to carry out itself, rather than with a plain ALTER COLUMN.
	 *
	 * Today that is integer columns too narrow for their logical type
	 * (`Changes.IntegerRange`, e.g. an INT identity holding unsigned 32-bit
	 * IDs): MSSQL cannot retype an identity key in place, so these are widened
	 * together in one table rebuild (widenIntegerColumns).  Everything else is
	 * left for the caller's generic migration path.
	 *
	 * Columns it claims are reported in `Handled` even when the rebuild fails,
	 * so the caller does not fall back to an ALTER that cannot work.
	 *
	 * @param {string} pTableName
	 * @param {Array<Object>} pColumnModifications - SchemaDiff ColumnsModified entries for the table.
	 * @param {(pError: Error|null, pResult: { Handled: Array<string>, Result?: Object }) => void} fCallback
	 */
	migrateColumns(pTableName, pColumnModifications, fCallback)
	{
		let tmpColumnTypes = {};
		let tmpHandled = [];
		let tmpModifications = Array.isArray(pColumnModifications) ? pColumnModifications : [];
		for (let i = 0; i < tmpModifications.length; i++)
		{
			let tmpChanges = tmpModifications[i].Changes || {};
			// Only claim a pure range change; anything else on the column is
			// not ours to interpret.
			if (tmpChanges.IntegerRange && (Object.keys(tmpChanges).length === 1))
			{
				tmpColumnTypes[tmpModifications[i].Column] = this.getNativeIntegerType(tmpChanges.IntegerRange.To);
				tmpHandled.push(tmpModifications[i].Column);
			}
		}
		if (tmpHandled.length < 1)
		{
			return fCallback(null, { Handled: [] });
		}
		this.widenIntegerColumns(pTableName, tmpColumnTypes, {},
			(pError, pResult) =>
			{
				return fCallback(pError || null, { Handled: tmpHandled, Result: pResult });
			});
	}

	/**
	 * Widen integer columns of an existing table, e.g. an INT identity that
	 * must become BIGINT because the schema says it holds unsigned 32-bit IDs.
	 *
	 * MSSQL cannot ALTER an identity primary key in place without dropping the
	 * key first, and an in-place ALTER rewrites every row in one transaction
	 * (measured on SQL Server 2017: ~425 MB of log per million rows, all of it
	 * pinned until commit, with the table locked throughout).  Instead this
	 * builds a widened copy beside the table and swaps names, widening every
	 * requested column in the same single pass:
	 *
	 *   1. `<Table>__MeadowWiden` is created from the live table's own column
	 *      definitions (SELECT INTO), the requested columns altered while it is
	 *      empty, defaults and clustered primary key restored.
	 *   2. Rows are copied in identity-ordered batches (keyset, so sparse ID
	 *      ranges cost nothing), each its own short transaction.  The copy
	 *      resumes from the shadow's high-water mark if the process dies.
	 *   3. Row count and MAX(identity) are verified against the live table.
	 *   4. Secondary indexes and object grants are recreated on the shadow.
	 *   5. One transaction renames the live table to `<Table>__MeadowRetired`
	 *      and the shadow into its place, so readers see the old table or the
	 *      new one and never neither.
	 *   6. The retired table is dropped (or kept, with RetainRetiredTable).
	 *
	 * Refuses, without changing anything, when the table has something a rename
	 * swap would orphan: inbound foreign keys, schema-bound dependents,
	 * triggers, computed columns, or non-rowstore / non-PK clustered indexes.
	 * The table needs an identity column to order the copy by.
	 *
	 * Never narrows: a column already at least as wide as requested is left as
	 * it is.  When nothing needs widening the call only finishes an interrupted
	 * sunset, so it is safe to repeat.
	 *
	 * @param {string} pTableName - Table to widen (dbo schema).
	 * @param {Record<string, string>} pColumnTypes - Column name -> native integer type (TINYINT|SMALLINT|INT|BIGINT).
	 * @param {Object} [pOptions] - Overrides MSSQL.IntegerWidenOptions from the connection config.
	 * @param {number} [pOptions.BatchSize=50000] - Rows per copy transaction.
	 * @param {boolean} [pOptions.RetainRetiredTable=false] - Keep the old table as `<Table>__MeadowRetired`.
	 * @param {(pError: Error|null, pResult?: Object) => void} fCallback
	 */
	widenIntegerColumns(pTableName, pColumnTypes, pOptions, fCallback)
	{
		if (typeof(pOptions) === 'function')
		{
			fCallback = pOptions;
			pOptions = {};
		}
		// Defaults < connection config (MSSQL.IntegerWidenOptions) < caller.
		let tmpMSSQLSettings = this.options.MSSQL || this.fable.settings.MSSQL || {};
		let tmpOptions = Object.assign({ BatchSize: 50000, RetainRetiredTable: false }, tmpMSSQLSettings.IntegerWidenOptions || {}, pOptions || {});

		if (!this._ConnectionPool)
		{
			return fCallback(new Error('Not connected to MSSQL'));
		}

		this._widenIntegerColumns(pTableName, pColumnTypes || {}, tmpOptions)
			.then((pResult) => fCallback(null, pResult))
			.catch((pError) =>
			{
				this.log.error(`Meadow-MSSQL widen ${pTableName}: ${pError.message}`);
				return fCallback(pError);
			});
	}

	async _widenIntegerColumns(pTableName, pColumnTypes, pOptions)
	{
		let tmpStartMs = Date.now();
		let tmpLabel = `Meadow-MSSQL widen ${pTableName}`;
		let tmpShadowName = `${pTableName}__MeadowWiden`;
		let tmpRetiredName = `${pTableName}__MeadowRetired`;
		let tmpTable = `dbo.${this._quoteName(pTableName)}`;
		let tmpShadow = `dbo.${this._quoteName(tmpShadowName)}`;
		let tmpRetired = `dbo.${this._quoteName(tmpRetiredName)}`;

		let tmpState = await this._inspectTableForWiden(pTableName, tmpShadowName, tmpRetiredName);
		if (!tmpState.TableExists)
		{
			throw new Error(`table ${pTableName} does not exist`);
		}
		if (!tmpState.Identity)
		{
			throw new Error(`table ${pTableName} has no identity column to order the copy by`);
		}

		let tmpIDColumn = tmpState.Identity.ColumnName;
		let tmpID = this._quoteName(tmpIDColumn);

		// Which requested columns are actually narrower than asked for.
		let tmpPending = [];
		let tmpRequested = Object.keys(pColumnTypes);
		for (let i = 0; i < tmpRequested.length; i++)
		{
			let tmpTo = String(pColumnTypes[tmpRequested[i]]).toUpperCase();
			let tmpLive = tmpState.Columns.find((pColumn) => pColumn.ColumnName === tmpRequested[i]);
			if (!tmpLive)
			{
				throw new Error(`column ${tmpRequested[i]} does not exist`);
			}
			let tmpFrom = tmpLive.TypeName.toUpperCase();
			if (!MSSQL_INTEGER_RANK.hasOwnProperty(tmpTo) || !MSSQL_INTEGER_RANK.hasOwnProperty(tmpFrom))
			{
				throw new Error(`column ${tmpRequested[i]} is ${tmpFrom}; only TINYINT/SMALLINT/INT/BIGINT columns are widened, to one of those types`);
			}
			if (MSSQL_INTEGER_RANK[tmpFrom] < MSSQL_INTEGER_RANK[tmpTo])
			{
				tmpPending.push({ Column: tmpRequested[i], From: tmpFrom, To: tmpTo, Nullable: tmpLive.IsNullable === true });
			}
		}
		let tmpResult = { Table: pTableName, Columns: tmpPending.map((pColumn) => ({ Column: pColumn.Column, From: pColumn.From, To: pColumn.To })), Widened: false, RowsCopied: 0, Resumed: false };
		let tmpDescription = tmpPending.map((pColumn) => `${pColumn.Column} ${pColumn.From} -> ${pColumn.To}`).join(', ');

		// ---- Already wide: only tidy up what an interrupted run left behind.
		if (tmpPending.length < 1)
		{
			if (tmpState.ShadowExists)
			{
				this.log.warn(`${tmpLabel}: columns are already wide enough; dropping orphaned ${tmpShadowName}.`);
				await this._widenQuery(`DROP TABLE ${tmpShadow};`, `${tmpLabel}: drop orphaned shadow`);
			}
			if (tmpState.RetiredExists && !pOptions.RetainRetiredTable)
			{
				this.log.info(`${tmpLabel}: columns are already wide enough; dropping retired ${tmpRetiredName}.`);
				await this._widenQuery(`DROP TABLE ${tmpRetired};`, `${tmpLabel}: drop retired table`);
			}
			return tmpResult;
		}

		if (tmpState.Blockers.length > 0)
		{
			throw new Error(`cannot rebuild without orphaning dependent objects; widen manually or remove them first: ${tmpState.Blockers.join('; ')}`);
		}
		if (tmpState.RetiredExists)
		{
			throw new Error(`${tmpRetiredName} already exists while ${pTableName} still needs ${tmpDescription}; resolve by hand (drop or rename it) before widening`);
		}

		let tmpColumnList = tmpState.Columns.map((pColumn) => this._quoteName(pColumn.ColumnName)).join(', ');

		// ---- 1. Shadow table.  Reuse one left by an interrupted run only if it
		// is unmistakably ours and shaped like the live table.
		if (tmpState.ShadowExists)
		{
			let tmpShadowShape = tmpState.ShadowColumns.map((pColumn) => pColumn.ColumnName).join('|');
			let tmpLiveShape = tmpState.Columns.map((pColumn) => pColumn.ColumnName).join('|');
			let tmpShadowWidened = tmpPending.every((pColumn) =>
				{
					let tmpShadowColumn = tmpState.ShadowColumns.find((pShadowColumn) => pShadowColumn.ColumnName === pColumn.Column);
					return tmpShadowColumn && (tmpShadowColumn.TypeName.toUpperCase() === pColumn.To);
				});
			if (tmpShadowShape === tmpLiveShape && tmpShadowWidened)
			{
				tmpResult.Resumed = true;
				this.log.info(`${tmpLabel}: resuming copy into existing ${tmpShadowName}.`);
			}
			else
			{
				this.log.warn(`${tmpLabel}: existing ${tmpShadowName} does not match ${pTableName}; rebuilding it.`);
				await this._widenQuery(`DROP TABLE ${tmpShadow};`, `${tmpLabel}: drop stale shadow`);
			}
		}
		if (!tmpResult.Resumed)
		{
			// SELECT INTO carries column types, nullability, collation and the
			// IDENTITY property; defaults and keys are restored explicitly.
			let tmpDefaults = await this._widenQuery(`
SELECT c.name AS ColumnName, dc.definition AS Definition
	FROM sys.default_constraints dc JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
	WHERE dc.parent_object_id = OBJECT_ID(N'${this._escapeSQLString(tmpTable)}');`, `${tmpLabel}: read defaults`);
			let tmpCreate = `SELECT * INTO ${tmpShadow} FROM ${tmpTable} WHERE 1 = 0;\n`;
			for (let i = 0; i < tmpPending.length; i++)
			{
				// Keep nullability; the identity is always NOT NULL.
				let tmpNull = (tmpPending[i].Nullable && (tmpPending[i].Column !== tmpIDColumn)) ? 'NULL' : 'NOT NULL';
				tmpCreate += `ALTER TABLE ${tmpShadow} ALTER COLUMN ${this._quoteName(tmpPending[i].Column)} ${tmpPending[i].To} ${tmpNull};\n`;
			}
			for (let i = 0; i < tmpDefaults.recordset.length; i++)
			{
				tmpCreate += `ALTER TABLE ${tmpShadow} ADD DEFAULT ${tmpDefaults.recordset[i].Definition} FOR ${this._quoteName(tmpDefaults.recordset[i].ColumnName)};\n`;
			}
			tmpCreate += `ALTER TABLE ${tmpShadow} ADD PRIMARY KEY CLUSTERED (${tmpID});`;
			this.log.info(`${tmpLabel}: creating ${tmpShadowName} with ${tmpDescription}.`);
			await this._widenQuery(`SET XACT_ABORT ON;\nBEGIN TRAN;\n${tmpCreate}\nCOMMIT;`, `${tmpLabel}: create shadow`);
		}

		// ---- 2. Batched, resumable copy.
		let tmpCounts = await this._widenQuery(`SELECT COUNT_BIG(*) AS LiveRows, (SELECT COUNT_BIG(*) FROM ${tmpShadow}) AS ShadowRows, (SELECT MAX(${tmpID}) FROM ${tmpShadow}) AS HighWater FROM ${tmpTable};`, `${tmpLabel}: count`);
		let tmpLiveRows = Number(tmpCounts.recordset[0].LiveRows);
		let tmpCopied = Number(tmpCounts.recordset[0].ShadowRows);
		let tmpHighWater = tmpCounts.recordset[0].HighWater;
		let tmpBatchSize = Math.max(1, parseInt(pOptions.BatchSize, 10) || 50000);
		this.log.info(`${tmpLabel}: copying ${tmpLiveRows} rows in batches of ${tmpBatchSize}${tmpCopied > 0 ? ` (resuming after ${tmpCopied} already copied, ID > ${tmpHighWater})` : ''}...`);

		let tmpLowerBound = (tmpHighWater === null || tmpHighWater === undefined) ? null : String(tmpHighWater);
		let tmpLastLogMs = 0;
		while (true)
		{
			if (tmpLowerBound !== null && !/^-?\d+$/.test(tmpLowerBound))
			{
				throw new Error(`unexpected identity high-water value [${tmpLowerBound}]`);
			}
			let tmpAfter = (tmpLowerBound === null) ? '' : `WHERE ${tmpID} > ${tmpLowerBound} `;
			let tmpBatch = await this._widenQuery(`
DECLARE @High BIGINT = (SELECT MAX(${tmpID}) FROM (SELECT TOP (${tmpBatchSize}) ${tmpID} FROM ${tmpTable} ${tmpAfter}ORDER BY ${tmpID}) AS b);
DECLARE @Copied INT = 0;
IF @High IS NOT NULL
BEGIN
	-- IDENTITY_INSERT is per session and only one table may hold it, so never
	-- hand this pooled connection back with it still on.
	SET IDENTITY_INSERT ${tmpShadow} ON;
	BEGIN TRY
		INSERT INTO ${tmpShadow} WITH (TABLOCK) (${tmpColumnList})
			SELECT ${tmpColumnList} FROM ${tmpTable} ${tmpAfter ? tmpAfter + 'AND' : 'WHERE'} ${tmpID} <= @High ORDER BY ${tmpID};
		SET @Copied = @@ROWCOUNT;
	END TRY
	BEGIN CATCH
		SET IDENTITY_INSERT ${tmpShadow} OFF;
		THROW;
	END CATCH
	SET IDENTITY_INSERT ${tmpShadow} OFF;
END
SELECT CAST(@High AS VARCHAR(20)) AS High, @Copied AS Copied;`, `${tmpLabel}: copy batch`);

			let tmpRow = tmpBatch.recordset[0];
			if (tmpRow.High === null)
			{
				break;
			}
			tmpLowerBound = tmpRow.High;
			tmpCopied += tmpRow.Copied;
			tmpResult.RowsCopied += tmpRow.Copied;
			// Every batch is progress, but a line per batch on a 30M row table
			// is noise; report at most every 5 seconds.
			if (Date.now() - tmpLastLogMs > 5000)
			{
				tmpLastLogMs = Date.now();
				this.log.info(`${tmpLabel}: copied ${tmpCopied} / ${tmpLiveRows} rows (${tmpLiveRows > 0 ? ((tmpCopied / tmpLiveRows) * 100).toFixed(1) : '100.0'}%), through ID ${tmpLowerBound}`);
			}
		}

		// ---- 3. Verify before touching the live table's name.
		let tmpVerify = await this._widenQuery(`
SELECT
	(SELECT COUNT_BIG(*) FROM ${tmpTable}) AS LiveRows, (SELECT COUNT_BIG(*) FROM ${tmpShadow}) AS ShadowRows,
	(SELECT CAST(MAX(${tmpID}) AS VARCHAR(20)) FROM ${tmpTable}) AS LiveMax, (SELECT CAST(MAX(${tmpID}) AS VARCHAR(20)) FROM ${tmpShadow}) AS ShadowMax;`, `${tmpLabel}: verify`);
		let tmpCheck = tmpVerify.recordset[0];
		if (String(tmpCheck.LiveRows) !== String(tmpCheck.ShadowRows) || tmpCheck.LiveMax !== tmpCheck.ShadowMax)
		{
			// Rows below the high-water mark changed under us; a resume cannot
			// fix that, so start clean next time.
			await this._widenQuery(`DROP TABLE ${tmpShadow};`, `${tmpLabel}: drop unverified shadow`);
			throw new Error(`copy verification failed (live ${tmpCheck.LiveRows} rows / max ${tmpCheck.LiveMax}, copy ${tmpCheck.ShadowRows} rows / max ${tmpCheck.ShadowMax}); the table changed during the copy. Shadow dropped; the next run starts over.`);
		}
		this.log.info(`${tmpLabel}: verified ${tmpCheck.ShadowRows} rows, max ID ${tmpCheck.ShadowMax}.`);

		// ---- 4. Secondary indexes and grants.  Index names are per-table, so
		// they carry over unchanged.
		let tmpDependents = await this._widenQuery(`
SELECT i.name AS IndexName, i.is_unique AS IsUnique, i.has_filter AS HasFilter, i.filter_definition AS FilterDefinition,
	c.name AS ColumnName, ic.is_included_column AS IsIncluded, ic.is_descending_key AS IsDescending, ic.key_ordinal AS KeyOrdinal, ic.index_column_id AS ColumnOrder
	FROM sys.indexes i
	JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
	JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
	WHERE i.object_id = OBJECT_ID(N'${this._escapeSQLString(tmpTable)}') AND i.is_primary_key = 0 AND i.type = 2
	ORDER BY i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
SELECT p.state_desc AS StateDesc, p.permission_name AS PermissionName, USER_NAME(p.grantee_principal_id) AS Grantee, COL_NAME(p.major_id, p.minor_id) AS ColumnName
	FROM sys.database_permissions p
	WHERE p.class = 1 AND p.major_id = OBJECT_ID(N'${this._escapeSQLString(tmpTable)}');`, `${tmpLabel}: read indexes and grants`);

		let tmpIndexes = {};
		for (let i = 0; i < tmpDependents.recordsets[0].length; i++)
		{
			let tmpRow = tmpDependents.recordsets[0][i];
			if (!tmpIndexes[tmpRow.IndexName])
			{
				tmpIndexes[tmpRow.IndexName] = { Unique: tmpRow.IsUnique, Filter: tmpRow.HasFilter ? tmpRow.FilterDefinition : null, Keys: [], Includes: [] };
			}
			let tmpColumnRef = this._quoteName(tmpRow.ColumnName);
			if (tmpRow.IsIncluded)
			{
				tmpIndexes[tmpRow.IndexName].Includes.push(tmpColumnRef);
			}
			else
			{
				tmpIndexes[tmpRow.IndexName].Keys.push(tmpColumnRef + (tmpRow.IsDescending ? ' DESC' : ''));
			}
		}
		let tmpIndexNames = Object.keys(tmpIndexes);
		for (let i = 0; i < tmpIndexNames.length; i++)
		{
			let tmpIndex = tmpIndexes[tmpIndexNames[i]];
			let tmpStatement = `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'${this._escapeSQLString(tmpIndexNames[i])}' AND object_id = OBJECT_ID(N'${this._escapeSQLString(tmpShadow)}'))\n`;
			tmpStatement += `\tCREATE ${tmpIndex.Unique ? 'UNIQUE ' : ''}NONCLUSTERED INDEX ${this._quoteName(tmpIndexNames[i])} ON ${tmpShadow} (${tmpIndex.Keys.join(', ')})`;
			if (tmpIndex.Includes.length > 0)
			{
				tmpStatement += ` INCLUDE (${tmpIndex.Includes.join(', ')})`;
			}
			if (tmpIndex.Filter)
			{
				tmpStatement += ` WHERE ${tmpIndex.Filter}`;
			}
			this.log.info(`${tmpLabel}: recreating index ${tmpIndexNames[i]} on ${tmpShadowName}.`);
			await this._widenQuery(tmpStatement + ';', `${tmpLabel}: index ${tmpIndexNames[i]}`);
		}

		let tmpGrants = tmpDependents.recordsets[1];
		for (let i = 0; i < tmpGrants.length; i++)
		{
			let tmpGrant = tmpGrants[i];
			let tmpVerb = (tmpGrant.StateDesc === 'DENY') ? 'DENY' : (tmpGrant.StateDesc === 'REVOKE' ? null : 'GRANT');
			if (!tmpVerb)
			{
				continue;
			}
			let tmpStatement = `${tmpVerb} ${tmpGrant.PermissionName} ON ${tmpShadow}${tmpGrant.ColumnName ? ` (${this._quoteName(tmpGrant.ColumnName)})` : ''} TO ${this._quoteName(tmpGrant.Grantee)}${tmpGrant.StateDesc === 'GRANT_WITH_GRANT_OPTION' ? ' WITH GRANT OPTION' : ''};`;
			this.log.info(`${tmpLabel}: carrying over ${tmpStatement}`);
			await this._widenQuery(tmpStatement, `${tmpLabel}: grant`);
		}

		// ---- 5. Swap.  sp_rename needs a schema lock on the live table; wait a
		// bounded time for readers rather than queueing behind a long report
		// and blocking every query that arrives after us.
		let tmpSwap = `SET XACT_ABORT ON;\nSET LOCK_TIMEOUT 30000;\nBEGIN TRAN;\n`;
		tmpSwap += `EXEC sp_rename N'${this._escapeSQLString(tmpTable)}', N'${this._escapeSQLString(tmpRetiredName)}';\n`;
		tmpSwap += `EXEC sp_rename N'${this._escapeSQLString(tmpShadow)}', N'${this._escapeSQLString(pTableName)}';\n`;
		tmpSwap += `COMMIT;`;
		for (let tmpAttempt = 1; ; tmpAttempt++)
		{
			try
			{
				await this._widenQuery(tmpSwap, `${tmpLabel}: swap`);
				break;
			}
			catch (pSwapError)
			{
				// 1222 = lock request timeout.  Anything else is not ours to retry.
				let tmpNumber = pSwapError && (pSwapError.number || (pSwapError.originalError && pSwapError.originalError.info && pSwapError.originalError.info.number));
				if (tmpNumber !== 1222 || tmpAttempt >= 5)
				{
					throw pSwapError;
				}
				this.log.warn(`${tmpLabel}: swap attempt ${tmpAttempt} timed out waiting for readers of ${pTableName}; retrying.`);
			}
		}
		tmpResult.Widened = true;
		this.log.info(`${tmpLabel}: swapped in the widened table (${tmpDescription}).`);

		// Views that SELECT * cache their column metadata; refresh them so they
		// report BIGINT too.  Best effort: the data is already correct.
		try
		{
			let tmpViews = await this._widenQuery(`
SELECT DISTINCT QUOTENAME(OBJECT_SCHEMA_NAME(v.object_id)) + '.' + QUOTENAME(v.name) AS ViewName
	FROM sys.sql_expression_dependencies d JOIN sys.views v ON v.object_id = d.referencing_id
	WHERE d.referenced_entity_name = N'${this._escapeSQLString(pTableName)}';`, `${tmpLabel}: find views`);
			for (let i = 0; i < tmpViews.recordset.length; i++)
			{
				await this._widenQuery(`EXEC sp_refreshview N'${this._escapeSQLString(tmpViews.recordset[i].ViewName)}';`, `${tmpLabel}: refresh view`);
			}
		}
		catch (pViewError)
		{
			this.log.warn(`${tmpLabel}: could not refresh dependent views: ${pViewError.message}`);
		}

		// ---- 6. Sunset the INT table.
		if (pOptions.RetainRetiredTable)
		{
			tmpResult.RetiredTable = tmpRetiredName;
			this.log.info(`${tmpLabel}: retained the previous table as ${tmpRetiredName}.`);
		}
		else
		{
			await this._widenQuery(`DROP TABLE ${tmpRetired};`, `${tmpLabel}: drop retired table`);
		}

		tmpResult.DurationMs = Date.now() - tmpStartMs;
		this.log.info(`${tmpLabel}: done in ${(tmpResult.DurationMs / 1000).toFixed(1)}s (${tmpResult.RowsCopied} rows copied${tmpResult.Resumed ? ', resumed' : ''}).`);
		return tmpResult;
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

							// Report what the integer column can physically hold,
							// in the same logical terms the schema uses, so a diff
							// can tell an INT identity (signed 32-bit, too narrow
							// for an unsigned 32-bit ID) from a BIGINT one.
							let tmpNativeInteger = MSSQL_INTEGER_TYPES.find((pType) => pType.Type === (tmpCol.DATA_TYPE || '').toUpperCase().trim());
							if (tmpNativeInteger && LOGICAL_INTEGER_DEFAULTS.hasOwnProperty(tmpTypeInfo.DataType))
							{
								tmpColumnDef.Signed = tmpNativeInteger.Signed;
								tmpColumnDef.Precision = tmpNativeInteger.Precision;
								tmpColumnDef.Radix = tmpNativeInteger.Radix;
							}

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
