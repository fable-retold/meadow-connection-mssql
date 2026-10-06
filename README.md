# Meadow Connection MSSQL

> **[Read the Meadow-Connection-Mssql Documentation](https://fable-retold.github.io/meadow-connection-mssql/)** - interactive docs with the full API reference.

A Microsoft SQL Server connection provider for the Meadow ORM. Wraps [mssql](https://github.com/tediousjs/node-mssql) (Tedious) as a Fable service, providing connection pooling with configurable timeouts, prepared statements, and DDL generation from Meadow table schemas.

[meadow-connection-mssql on npm](https://www.npmjs.com/package/meadow-connection-mssql) | [MIT License](LICENSE)

---

## Features

- **MSSQL Connection Pooling** -- Managed connection pool via [mssql](https://github.com/tediousjs/node-mssql) (Tedious driver)
- **Fable Service Provider** -- Registers with a Fable instance for dependency injection, logging, and configuration
- **Async Connection** -- Truly asynchronous connection flow with promise-based pool creation and callback interface
- **Prepared Statements** -- Built-in `preparedStatement` getter for creating parameterized queries against the pool
- **Direct Driver Access** -- `MSSQL` getter exposes the raw `mssql` package for type constants (`Int`, `VarChar`, `Decimal`, etc.)
- **Schema-Driven DDL** -- Generates `CREATE TABLE` statements with `[dbo]` schema prefix, `IDENTITY` primary keys, and proper MSSQL column types
- **Connection Safety** -- Guards against duplicate connection pools with descriptive logging (passwords are never leaked)
- **Configurable Timeouts** -- Request timeout (80s) and connection timeout (80s) with pool idle timeout (30s) defaults

## Installation

```bash
npm install meadow-connection-mssql
```

## Quick Start

```javascript
const libFable = require('fable');
const MeadowConnectionMSSQL = require('meadow-connection-mssql');

let fable = new libFable(
{
	MSSQL:
	{
		server: 'localhost',
		port: 1433,
		user: 'sa',
		password: 'PASSWORD',
		database: 'my_app'
	}
});

let connection = fable.instantiateServiceProvider('MeadowConnectionMSSQL',
	{}, MeadowConnectionMSSQL);

connection.connectAsync((pError, pPool) =>
{
	if (pError)
	{
		console.error('Connection failed:', pError);
		return;
	}

	// Query using the connection pool
	connection.pool.query('SELECT TOP 10 * FROM Book')
		.then((pResult) =>
		{
			console.log(`Found ${pResult.recordset.length} books.`);
		});
});
```

## Configuration

The MSSQL connection settings are provided through the Fable settings `MSSQL` object:

```javascript
let fable = new libFable(
{
	MSSQL:
	{
		server: 'localhost',
		port: 1433,
		user: 'sa',
		password: 'PASSWORD',
		database: 'my_app'
	}
});
```

### Configuration Options

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `server` | `String` | -- | SQL Server hostname or IP address |
| `port` | `Number` | `1433` | TCP port |
| `user` | `String` | -- | Login user |
| `password` | `String` | -- | Login password |
| `database` | `String` | -- | Database name |
| `MeadowConnectionMSSQLAutoConnect` | `Boolean` | `false` | Auto-connect on instantiation (calls `connect()`) |

### Connection Pool Defaults

The provider configures sensible pool defaults:

- **Pool max**: 10 connections
- **Pool min**: 0 connections
- **Idle timeout**: 30,000 ms
- **Request timeout**: 80,000 ms
- **Connection timeout**: 80,000 ms
- **Trust server certificate**: `true` (for local dev and self-signed certs)
- **UTC mode**: disabled (`useUTC: false`)

## API

### `connectAsync(fCallback)`

Open the MSSQL connection pool asynchronously. This is the recommended connection method -- MSSQL connections are inherently asynchronous.

| Parameter | Type | Description |
|-----------|------|-------------|
| `fCallback` | `Function` | Callback receiving `(error, connectionPool)` |

### `connect()`

Synchronous convenience wrapper that calls `connectAsync` without a callback. Logs a warning because this can cause race conditions with the async MSSQL driver.

### `pool` (getter)

Returns the underlying `mssql` connection pool for direct query access.

### `MSSQL` (getter)

Returns the `mssql` library module for direct access to types, prepared statements, and other driver features.

### `preparedStatement` (getter)

Creates and returns a new `mssql.PreparedStatement` bound to the active connection pool. Throws an error if the pool is not connected.

### `connected` (property)

Boolean indicating whether the connection pool is open.

### `generateCreateTableStatement(pMeadowTableSchema)`

Generate a `CREATE TABLE` SQL statement from a Meadow table schema object. Tables are created in the `[dbo]` schema with bracketed column names.

| Parameter | Type | Description |
|-----------|------|-------------|
| `pMeadowTableSchema` | `Object` | Meadow table schema with `TableName` and `Columns` array |

### `createTable(pMeadowTableSchema, fCallback)`

Execute a `CREATE TABLE` statement against the connected database. Silently succeeds if the table already exists.

### `createTables(pMeadowSchema, fCallback)`

Create all tables defined in a Meadow schema object (iterates `pMeadowSchema.Tables` sequentially).

### `generateDropTableStatement(pTableName)`

Generate a safe `DROP TABLE` statement using `IF OBJECT_ID` to check existence before dropping.

## Column Type Mapping

| Meadow Type | MSSQL Column |
|-------------|--------------|
| `ID` | `BIGINT NOT NULL IDENTITY PRIMARY KEY` (see [Integer Width](#integer-width)) |
| `GUID` | `NCHAR(size, default 255) NOT NULL` with default GUID |
| `ForeignKey` | `BIGINT NOT NULL DEFAULT 0` (see [Integer Width](#integer-width)) |
| `Numeric` | `INT NOT NULL DEFAULT 0` |
| `Decimal` | `DECIMAL(size)` |
| `String` | `VARCHAR(size) DEFAULT ''` |
| `Text` | `TEXT` |
| `DateTime` | `DATETIME` |
| `Boolean` | `TINYINT DEFAULT 0` |

## Integer Width

Stricture records each integer column's logical type as `Signed` + `Precision` + `Radix`, using SQL `INFORMATION_SCHEMA` naming (see the stricture README). Schemas compiled before that are read with Stricture's defaults: `ID` and `ForeignKey` unsigned 32-bit, `Numeric` signed 32-bit.

MSSQL has no unsigned integer types beyond `TINYINT`, so this connector stores each column in the narrowest native type that holds its whole range (`getNativeIntegerType`):

| Logical type | MSSQL type |
|--------------|------------|
| unsigned 32-bit (`ID`, `ForeignKey`) | `BIGINT` |
| signed 32-bit (`Numeric`) | `INT` |
| signed 64-bit | `BIGINT` |
| unsigned 8-bit | `TINYINT` |

Connector versions before 1.0.27 created `ID` and `ForeignKey` columns as `INT`, which rejects every value above 2,147,483,647.

Related behavior:

- `introspectTableColumns` reports what each integer column can physically hold, in the same terms: an `INT` column is `{ Signed: true, Precision: 32, Radix: 2 }`. meadow-migrationmanager's `SchemaDiff` compares that against the schema by range. It flags an `INT` holding unsigned 32-bit IDs, and accepts `BIGINT` because its range covers them.
- `BIGINT` results are returned as JavaScript numbers while they are exact (up to 2^53), matching the MySQL connector. Larger values stay strings.

### `migrateColumns(pTableName, pColumnModifications, fCallback)`

Takes a table's `ColumnsModified` entries from a schema diff and carries out the ones this connector must do itself. Currently that is a column whose only change is `IntegerRange`, meaning it is too narrow for its logical type. All such columns are widened together in a single `widenIntegerColumns` rebuild.

The callback receives `{ Handled: [column names], Result }`. Claimed columns are listed even if the rebuild fails, so the caller doesn't fall back to an `ALTER COLUMN` that can't work. retold-data-service's data cloner calls this from its schema check.

### `widenIntegerColumns(pTableName, pColumnTypes, [pOptions], fCallback)`

Widens the named integer columns (`{ IDWidget: 'BIGINT', IDOwner: 'BIGINT' }`) by rebuilding the table, all in one pass. MSSQL can't retype an identity key in place, and an in-place `ALTER COLUMN` rewrites every row in a single transaction.

The live table stays readable throughout. Readers are only blocked for the final rename.

1. Creates `<Table>__MeadowWiden` from the live table's own definition (`SELECT INTO`), with the requested columns widened (nullability kept), the original defaults, and the clustered primary key.
2. Copies rows in identity order, in batches that each commit separately. This keeps transaction log use per batch small. If the process dies, the next call resumes from the shadow table's highest ID.
3. Checks that row count and `MAX(identity)` match the live table. On a mismatch it drops the shadow table and fails.
4. Recreates secondary indexes (with key order, `INCLUDE` columns and filters) and object-level `GRANT`/`DENY` permissions on the shadow table.
5. In one transaction, renames the live table to `<Table>__MeadowRetired` and the shadow table to `<Table>`. It waits at most 30s for a schema lock and retries up to 5 times.
6. Refreshes views that reference the table, then drops the retired table.

The table needs an identity column to order the copy. The rebuild refuses, without changing anything, if a rename swap would break other objects:

- foreign keys from other tables pointing at this one
- schema-bound views or functions
- triggers
- computed columns
- columnstore, XML or spatial indexes, or a clustered index that is not the primary key

It never narrows a column. When every requested column is already wide enough it does nothing, apart from removing leftover shadow or retired tables.

Measured on SQL Server 2017:

- 2M narrow rows: 5.5s, at most 23 MB of log per batch, and a 102 ms rename. An in-place `ALTER COLUMN` took 10.4s and held 849 MB of log in a single transaction.
- 1M `Observation`-shaped rows (2.2 GB): 37.5s, about 59 MB/s.

| Option | Default | Description |
|--------|---------|-------------|
| `BatchSize` | `50000` | Rows per copy transaction |
| `RetainRetiredTable` | `false` | Keep the old table as `<Table>__MeadowRetired` instead of dropping it |

Defaults can also be set in the connection config as `MSSQL.IntegerWidenOptions`.

Disk: the shadow table temporarily doubles the table's size. Under the FULL recovery model, log space is only freed by the regular log backups.

## Part of the Retold Framework

Meadow Connection MSSQL is a database connector for the Meadow data access layer:

- [meadow](https://github.com/fable-retold/meadow) -- ORM and data access framework
- [foxhound](https://github.com/fable-retold/foxhound) -- Query DSL used by Meadow
- [stricture](https://github.com/fable-retold/stricture) -- Schema definition tool
- [meadow-endpoints](https://github.com/fable-retold/meadow-endpoints) -- RESTful endpoint generation
- [meadow-connection-mysql](https://github.com/fable-retold/meadow-connection-mysql) -- MySQL connector
- [meadow-connection-sqlite](https://github.com/fable-retold/meadow-connection-sqlite) -- SQLite connector
- [fable](https://github.com/fable-retold/fable) -- Application services framework

## Testing

Run the test suite:

```bash
npm test
```

The live suites expect the test container on port 21433 (`npm run docker-mssql-start`, SQL Server 2022). To run them against another server, for example a SQL Server 2017 container, set `MSSQL_TEST_PORT`:

```bash
MSSQL_TEST_PORT=21434 npm test
```

Run with coverage:

```bash
npm run coverage
```

## Related Packages

- [meadow](https://github.com/fable-retold/meadow) -- Data access and ORM
- [meadow-connection-mysql](https://github.com/fable-retold/meadow-connection-mysql) -- MySQL connection provider
- [meadow-connection-rocksdb](https://github.com/fable-retold/meadow-connection-rocksdb) -- RocksDB connection provider
- [fable](https://github.com/fable-retold/fable) -- Application services framework

## License

MIT

## Contributing

Pull requests are welcome. For details on our code of conduct, contribution process, and testing requirements, see the [Retold Contributing Guide](https://github.com/fable-retold/retold/blob/main/docs/contributing.md).
