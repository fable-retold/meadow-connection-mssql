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
| `ID` | `INT NOT NULL IDENTITY PRIMARY KEY` |
| `GUID` | `VARCHAR(254)` with default GUID |
| `ForeignKey` | `INT UNSIGNED NOT NULL DEFAULT 0` |
| `Numeric` | `INT NOT NULL DEFAULT 0` |
| `Decimal` | `DECIMAL(size)` |
| `String` | `VARCHAR(size) DEFAULT ''` |
| `Text` | `TEXT` |
| `DateTime` | `DATETIME` |
| `Boolean` | `TINYINT DEFAULT 0` |

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
