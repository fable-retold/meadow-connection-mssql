/**
* Unit tests for the Linked List ADT
*
* @license     MIT
*
* @author      Steven Velozo <steven@velozo.com>
*/

const Chai = require('chai');
const Expect = Chai.expect;
const Assert = Chai.assert;

const libFable = require('fable');
const libMeadowConnectionMSSQL = require('../source/Meadow-Connection-MSSQL.js');
const libMeadowSchemaMSSQL = require('../source/Meadow-Schema-MSSQL.js');

const _FableConfig = (
	{
		"Product": "MeadowMSSQLTestBookstore",
		"ProductVersion": "1.0.0",

		"UUID":
			{
				"DataCenter": 0,
				"Worker": 0
			},
		"LogStreams":
			[
				{
					"streamtype": "console"
				}
			],

		"MSSQL":
			{
				"server": "localhost",
				"port": 21433,
				"user": "sa",
				"password": "Retold1234567890!",
				"database": "bookstore",
				"ConnectionPoolLimit": 20
			}
	});

const _FableTestTableSchema =
{
	TableName: 'FableTest',
	Columns:
	[
		{ Column: 'IDFableTest', DataType: 'ID' },
		{ Column: 'GUIDFableTest', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256' },
		{ Column: 'Description', DataType: 'Text' },
		{ Column: 'IDAuthor', DataType: 'ForeignKey' }
	]
};

const _FableTestTableSchemaWithColumnIndexed =
{
	TableName: 'FableTest',
	Columns:
	[
		{ Column: 'IDFableTest', DataType: 'ID' },
		{ Column: 'GUIDFableTest', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256', Indexed: true },
		{ Column: 'Email', DataType: 'String', Size: '512', Indexed: 'unique' },
		{ Column: 'IDAuthor', DataType: 'ForeignKey' }
	]
};

const _FableTestTableSchemaWithIndexName =
{
	TableName: 'FableTestCustomIdx',
	Columns:
	[
		{ Column: 'IDFableTestCustomIdx', DataType: 'ID' },
		{ Column: 'GUIDFableTestCustomIdx', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256', Indexed: true, IndexName: 'IX_Custom_Title' },
		{ Column: 'Email', DataType: 'String', Size: '512', Indexed: 'unique', IndexName: 'UQ_FableTest_Email' },
		{ Column: 'Rating', DataType: 'Numeric', Indexed: true },
		{ Column: 'IDAuthor', DataType: 'ForeignKey' }
	]
};

// Schemas specifically for introspection testing (unique table names to avoid conflicts)
const _IntrospectTestSchema =
{
	TableName: 'IntrospTest',
	Columns:
	[
		{ Column: 'IDIntrospTest', DataType: 'ID' },
		{ Column: 'GUIDIntrospTest', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256' },
		{ Column: 'Description', DataType: 'Text' },
		{ Column: 'Price', DataType: 'Decimal', Size: '10,2' },
		{ Column: 'PageCount', DataType: 'Numeric' },
		{ Column: 'PublishDate', DataType: 'DateTime' },
		{ Column: 'InPrint', DataType: 'Boolean' },
		{ Column: 'IDAuthor', DataType: 'ForeignKey' }
	]
};

const _IntrospectTestIndexedSchema =
{
	TableName: 'IntrospTestIdx',
	Columns:
	[
		{ Column: 'IDIntrospTestIdx', DataType: 'ID' },
		{ Column: 'GUIDIntrospTestIdx', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256', Indexed: true },
		{ Column: 'Description', DataType: 'Text' },
		{ Column: 'ISBN', DataType: 'String', Size: '64', Indexed: 'unique' },
		{ Column: 'IDPublisher', DataType: 'ForeignKey' }
	]
};

const _IntrospectTestCustomIdxSchema =
{
	TableName: 'IntrospTestCustIdx',
	Columns:
	[
		{ Column: 'IDIntrospTestCustIdx', DataType: 'ID' },
		{ Column: 'GUIDIntrospTestCustIdx', DataType: 'GUID' },
		{ Column: 'Title', DataType: 'String', Size: '256', Indexed: true, IndexName: 'IX_Custom_Title' },
		{ Column: 'ISBN', DataType: 'String', Size: '64', Indexed: 'unique', IndexName: 'UQ_IntrospTestCustIdx_ISBN' },
		{ Column: 'YearPublished', DataType: 'Numeric', Indexed: true },
		{ Column: 'IDEditor', DataType: 'ForeignKey' }
	]
};

suite
(
	'Connection',
	()=>
	{
		setup(()=>{});

		suite
		(
			'Connect to MSSQL',
			()=>
			{
				test
				(
					'use default settings from fable.settings',
					(fDone) =>
					{
						let _Fable = new libFable(_FableConfig);
						_Fable.serviceManager.addServiceType('MeadowMSSQLProvider', libMeadowConnectionMSSQL);

						_Fable.serviceManager.instantiateServiceProvider('MeadowMSSQLProvider');

						Expect(_Fable.MeadowMSSQLProvider).to.be.an('object');

						_Fable.MeadowMSSQLProvider.connectAsync(
							(pError) =>
							{
								if (pError)
								{
									return fDone(pError);
								}
								_Fable.MeadowMSSQLProvider.pool.query(`SELECT TOP 10 * FROM FableTest`,
									(pError, pRows, pFields) =>
									{
										Expect(pRows.recordset).to.be.an('array');
										Expect(pRows.recordset.length).to.equal(10);
										Expect(pRows.recordset[0].Title).to.equal(`The Hunger Games`);
										return fDone();
									});
							}
						);
					}
				);
			}
		);

		suite
		(
			'Index Generation',
			()=>
			{
				let libSchemaMSSQL = null;

				setup(
					() =>
					{
						let _Fable = new libFable(_FableConfig);
						libSchemaMSSQL = _Fable.serviceManager.addServiceType('MeadowSchemaMSSQL', libMeadowSchemaMSSQL);
						libSchemaMSSQL = _Fable.serviceManager.instantiateServiceProvider('MeadowSchemaMSSQL');
					});

				test
				(
					'auto-detect GUID and ForeignKey indices',
					() =>
					{
						let tmpIndices = libSchemaMSSQL.getIndexDefinitionsFromSchema(_FableTestTableSchema);
						Expect(tmpIndices).to.be.an('array');
						Expect(tmpIndices.length).to.equal(2);
						Expect(tmpIndices[0].Name).to.equal('AK_M_GUIDFableTest');
						Expect(tmpIndices[0].Unique).to.equal(true);
						Expect(tmpIndices[1].Name).to.equal('IX_M_IDAuthor');
						Expect(tmpIndices[1].Unique).to.equal(false);
					}
				);

				test
				(
					'generate idempotent index script with sys.indexes check',
					() =>
					{
						let tmpScript = libSchemaMSSQL.generateCreateIndexScript(_FableTestTableSchema);
						Expect(tmpScript).to.contain('CREATE UNIQUE NONCLUSTERED INDEX');
						Expect(tmpScript).to.contain('CREATE NONCLUSTERED INDEX');
						Expect(tmpScript).to.contain('[AK_M_GUIDFableTest]');
						Expect(tmpScript).to.contain('[IX_M_IDAuthor]');
						Expect(tmpScript).to.contain('sys.indexes');
						Expect(tmpScript).to.contain('GO');
					}
				);

				test
				(
					'generate individual index statements with sys.indexes check',
					() =>
					{
						let tmpStatements = libSchemaMSSQL.generateCreateIndexStatements(_FableTestTableSchema);
						Expect(tmpStatements).to.be.an('array');
						Expect(tmpStatements.length).to.equal(2);
						Expect(tmpStatements[0].Name).to.equal('AK_M_GUIDFableTest');
						Expect(tmpStatements[0].Statement).to.contain('CREATE UNIQUE NONCLUSTERED INDEX');
						Expect(tmpStatements[0].CheckStatement).to.contain('sys.indexes');
					}
				);

				test
				(
					'column-level Indexed property generates consistently named indices',
					() =>
					{
						let tmpIndices = libSchemaMSSQL.getIndexDefinitionsFromSchema(_FableTestTableSchemaWithColumnIndexed);
						Expect(tmpIndices).to.be.an('array');
						Expect(tmpIndices.length).to.equal(4);
						Expect(tmpIndices[0].Name).to.equal('AK_M_GUIDFableTest');
						Expect(tmpIndices[1].Name).to.equal('IX_M_T_FableTest_C_Title');
						Expect(tmpIndices[1].Unique).to.equal(false);
						Expect(tmpIndices[2].Name).to.equal('AK_M_T_FableTest_C_Email');
						Expect(tmpIndices[2].Unique).to.equal(true);
						Expect(tmpIndices[3].Name).to.equal('IX_M_IDAuthor');
					}
				);

				test
				(
					'generate script with column-level Indexed property',
					() =>
					{
						let tmpScript = libSchemaMSSQL.generateCreateIndexScript(_FableTestTableSchemaWithColumnIndexed);
						Expect(tmpScript).to.contain('IX_M_T_FableTest_C_Title');
						Expect(tmpScript).to.contain('AK_M_T_FableTest_C_Email');
					}
				);

				test
				(
					'IndexName property overrides auto-generated index name',
					() =>
					{
						let tmpIndices = libSchemaMSSQL.getIndexDefinitionsFromSchema(_FableTestTableSchemaWithIndexName);
						Expect(tmpIndices).to.be.an('array');
						Expect(tmpIndices.length).to.equal(5);
						Expect(tmpIndices[0].Name).to.equal('AK_M_GUIDFableTestCustomIdx');
						Expect(tmpIndices[1].Name).to.equal('IX_Custom_Title');
						Expect(tmpIndices[1].Unique).to.equal(false);
						Expect(tmpIndices[2].Name).to.equal('UQ_FableTest_Email');
						Expect(tmpIndices[2].Unique).to.equal(true);
						Expect(tmpIndices[3].Name).to.equal('IX_M_T_FableTestCustomIdx_C_Rating');
						Expect(tmpIndices[3].Unique).to.equal(false);
						Expect(tmpIndices[4].Name).to.equal('IX_M_IDAuthor');
					}
				);

				test
				(
					'generate script with IndexName uses custom names in MSSQL SQL',
					() =>
					{
						let tmpScript = libSchemaMSSQL.generateCreateIndexScript(_FableTestTableSchemaWithIndexName);
						Expect(tmpScript).to.contain('IX_Custom_Title');
						Expect(tmpScript).to.contain('UQ_FableTest_Email');
						Expect(tmpScript).to.contain('IX_M_T_FableTestCustomIdx_C_Rating');
						Expect(tmpScript).to.not.contain('IX_M_T_FableTestCustomIdx_C_Title');
						Expect(tmpScript).to.not.contain('AK_M_T_FableTestCustomIdx_C_Email');
					}
				);

				test
				(
					'schema provider is accessible from connection provider',
					() =>
					{
						let _Fable = new libFable(_FableConfig);
						_Fable.serviceManager.addServiceType('MeadowMSSQLProvider', libMeadowConnectionMSSQL);
						_Fable.serviceManager.instantiateServiceProvider('MeadowMSSQLProvider');
						Expect(_Fable.MeadowMSSQLProvider.schemaProvider).to.be.an('object');
					}
				);
			}
		);

		suite
		(
			'Database Introspection',
			()=>
			{
				let _Fable = null;
				let libSchemaMSSQL = null;

				setup(
					(fDone) =>
					{
						_Fable = new libFable(_FableConfig);
						_Fable.serviceManager.addServiceType('MeadowSchemaMSSQL', libMeadowSchemaMSSQL);
						libSchemaMSSQL = _Fable.serviceManager.instantiateServiceProvider('MeadowSchemaMSSQL');
						_Fable.serviceManager.addServiceType('MeadowMSSQLProvider', libMeadowConnectionMSSQL);
						_Fable.serviceManager.instantiateServiceProvider('MeadowMSSQLProvider');

						_Fable.MeadowMSSQLProvider.connectAsync(
							(pError) =>
							{
								if (pError) return fDone(pError);
								libSchemaMSSQL.setConnectionPool(_Fable.MeadowMSSQLProvider.pool);

								// Drop test tables first (clean slate) using MSSQL-safe syntax
								let tmpDropSQL = "IF OBJECT_ID('dbo.[IntrospTest]', 'U') IS NOT NULL DROP TABLE dbo.[IntrospTest]; IF OBJECT_ID('dbo.[IntrospTestIdx]', 'U') IS NOT NULL DROP TABLE dbo.[IntrospTestIdx]; IF OBJECT_ID('dbo.[IntrospTestCustIdx]', 'U') IS NOT NULL DROP TABLE dbo.[IntrospTestCustIdx];";

								_Fable.MeadowMSSQLProvider.pool.query(tmpDropSQL)
									.then(() =>
									{
										let tmpSchema = { Tables: [_IntrospectTestSchema, _IntrospectTestIndexedSchema, _IntrospectTestCustomIdxSchema] };
										libSchemaMSSQL.createTables(tmpSchema,
											(pCreateError) =>
											{
												if (pCreateError) return fDone(pCreateError);
												libSchemaMSSQL.createAllIndices(tmpSchema,
													(pIdxError) =>
													{
														return fDone(pIdxError);
													});
											});
									})
									.catch((pDropError) =>
									{
										return fDone(pDropError);
									});
							});
					});

				test
				(
					'listTables returns tables including introspection test tables',
					(fDone) =>
					{
						libSchemaMSSQL.listTables(
							(pError, pTables) =>
							{
								Expect(pError).to.not.exist;
								Expect(pTables).to.be.an('array');
								Expect(pTables).to.include('IntrospTest');
								Expect(pTables).to.include('IntrospTestIdx');
								Expect(pTables).to.include('IntrospTestCustIdx');
								return fDone();
							});
					}
				);

				test
				(
					'introspectTableColumns returns column definitions for IntrospTest',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableColumns('IntrospTest',
							(pError, pColumns) =>
							{
								Expect(pError).to.not.exist;
								Expect(pColumns).to.be.an('array');
								Expect(pColumns.length).to.equal(9);

								// ID column (IDENTITY)
								Expect(pColumns[0].Column).to.equal('IDIntrospTest');
								Expect(pColumns[0].DataType).to.equal('ID');

								// GUID column (VARCHAR with GUID in name)
								Expect(pColumns[1].Column).to.equal('GUIDIntrospTest');
								Expect(pColumns[1].DataType).to.equal('GUID');

								// String column (VARCHAR)
								Expect(pColumns[2].Column).to.equal('Title');
								Expect(pColumns[2].DataType).to.equal('String');

								// Text column
								Expect(pColumns[3].Column).to.equal('Description');
								Expect(pColumns[3].DataType).to.equal('Text');

								// Decimal column
								Expect(pColumns[4].Column).to.equal('Price');
								Expect(pColumns[4].DataType).to.equal('Decimal');

								// Numeric column (INT)
								Expect(pColumns[5].Column).to.equal('PageCount');
								Expect(pColumns[5].DataType).to.equal('Numeric');

								// DateTime column
								Expect(pColumns[6].Column).to.equal('PublishDate');
								Expect(pColumns[6].DataType).to.equal('DateTime');

								// Boolean column (TINYINT with In prefix hint)
								Expect(pColumns[7].Column).to.equal('InPrint');
								Expect(pColumns[7].DataType).to.equal('Boolean');

								// ForeignKey column (no actual FK constraint, detected as Numeric)
								Expect(pColumns[8].Column).to.equal('IDAuthor');
								Expect(pColumns[8].DataType).to.equal('Numeric');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableIndices returns index definitions for IntrospTest',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableIndices('IntrospTest',
							(pError, pIndices) =>
							{
								Expect(pError).to.not.exist;
								Expect(pIndices).to.be.an('array');
								Expect(pIndices.length).to.equal(2);

								let tmpNames = pIndices.map((pIdx) => { return pIdx.Name; });
								Expect(tmpNames).to.include('AK_M_GUIDIntrospTest');
								Expect(tmpNames).to.include('IX_M_IDAuthor');

								let tmpGUIDIndex = pIndices.find((pIdx) => { return pIdx.Name === 'AK_M_GUIDIntrospTest'; });
								Expect(tmpGUIDIndex.Unique).to.equal(true);
								Expect(tmpGUIDIndex.Columns).to.deep.equal(['GUIDIntrospTest']);

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableForeignKeys returns empty for table without FK constraints',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableForeignKeys('IntrospTest',
							(pError, pFKs) =>
							{
								Expect(pError).to.not.exist;
								Expect(pFKs).to.be.an('array');
								Expect(pFKs.length).to.equal(0);
								return fDone();
							});
					}
				);

				test
				(
					'introspectTableSchema combines columns and indices for IntrospTestIdx',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableSchema('IntrospTestIdx',
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;
								Expect(pSchema).to.be.an('object');
								Expect(pSchema.TableName).to.equal('IntrospTestIdx');
								Expect(pSchema.Columns).to.be.an('array');

								// Check that column-level Indexed properties are folded in
								let tmpTitleCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'Title'; });
								Expect(tmpTitleCol.Indexed).to.equal(true);
								Expect(tmpTitleCol).to.not.have.property('IndexName');

								let tmpISBNCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'ISBN'; });
								Expect(tmpISBNCol.Indexed).to.equal('unique');
								Expect(tmpISBNCol).to.not.have.property('IndexName');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableSchema preserves IndexName for custom-named indices',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableSchema('IntrospTestCustIdx',
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;
								Expect(pSchema.TableName).to.equal('IntrospTestCustIdx');

								// Title has custom IndexName IX_Custom_Title
								let tmpTitleCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'Title'; });
								Expect(tmpTitleCol.Indexed).to.equal(true);
								Expect(tmpTitleCol.IndexName).to.equal('IX_Custom_Title');

								// ISBN has custom IndexName UQ_IntrospTestCustIdx_ISBN
								let tmpISBNCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'ISBN'; });
								Expect(tmpISBNCol.Indexed).to.equal('unique');
								Expect(tmpISBNCol.IndexName).to.equal('UQ_IntrospTestCustIdx_ISBN');

								// YearPublished has auto-generated name - no IndexName
								let tmpYearCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'YearPublished'; });
								Expect(tmpYearCol.Indexed).to.equal(true);
								Expect(tmpYearCol).to.not.have.property('IndexName');

								return fDone();
							});
					}
				);

				test
				(
					'introspectDatabaseSchema returns schemas for all tables',
					(fDone) =>
					{
						libSchemaMSSQL.introspectDatabaseSchema(
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;
								Expect(pSchema).to.be.an('object');
								Expect(pSchema.Tables).to.be.an('array');
								Expect(pSchema.Tables.length).to.be.greaterThan(0);

								let tmpTableNames = pSchema.Tables.map((pT) => { return pT.TableName; });
								Expect(tmpTableNames).to.include('IntrospTest');
								Expect(tmpTableNames).to.include('IntrospTestIdx');
								Expect(tmpTableNames).to.include('IntrospTestCustIdx');

								return fDone();
							});
					}
				);

				test
				(
					'generateMeadowPackageFromTable produces Meadow package JSON',
					(fDone) =>
					{
						libSchemaMSSQL.generateMeadowPackageFromTable('IntrospTest',
							(pError, pPackage) =>
							{
								Expect(pError).to.not.exist;
								Expect(pPackage).to.be.an('object');
								Expect(pPackage.Scope).to.equal('IntrospTest');
								Expect(pPackage.DefaultIdentifier).to.equal('IDIntrospTest');
								Expect(pPackage.Schema).to.be.an('array');
								Expect(pPackage.DefaultObject).to.be.an('object');

								// Verify schema entries
								let tmpIDEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'IDIntrospTest'; });
								Expect(tmpIDEntry.Type).to.equal('AutoIdentity');

								let tmpGUIDEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'GUIDIntrospTest'; });
								Expect(tmpGUIDEntry.Type).to.equal('AutoGUID');

								let tmpTitleEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'Title'; });
								Expect(tmpTitleEntry.Type).to.equal('String');

								// Verify default object
								Expect(pPackage.DefaultObject.IDIntrospTest).to.equal(0);
								Expect(pPackage.DefaultObject.GUIDIntrospTest).to.equal('');
								Expect(pPackage.DefaultObject.Title).to.equal('');

								return fDone();
							});
					}
				);

				test
				(
					'round-trip: introspect IntrospTestIdx and regenerate matching indices',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableSchema('IntrospTestIdx',
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;

								// Use the introspected schema to generate index definitions
								let tmpIndices = libSchemaMSSQL.getIndexDefinitionsFromSchema(pSchema);

								// The original IntrospTestIdx had:
								//   AK_M_GUIDIntrospTestIdx (GUID auto)
								//   IX_M_T_IntrospTestIdx_C_Title (Indexed: true)
								//   AK_M_T_IntrospTestIdx_C_ISBN (Indexed: 'unique')
								//   IX_M_IDPublisher (FK auto)
								let tmpNames = tmpIndices.map((pIdx) => { return pIdx.Name; });
								Expect(tmpNames).to.include('AK_M_GUIDIntrospTestIdx');
								Expect(tmpNames).to.include('IX_M_T_IntrospTestIdx_C_Title');
								Expect(tmpNames).to.include('AK_M_T_IntrospTestIdx_C_ISBN');
								Expect(tmpNames).to.include('IX_M_IDPublisher');

								return fDone();
							});
					}
				);

				test
				(
					'round-trip: introspect IntrospTestCustIdx and regenerate matching index names',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableSchema('IntrospTestCustIdx',
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;

								// Use the introspected schema to generate index definitions
								let tmpIndices = libSchemaMSSQL.getIndexDefinitionsFromSchema(pSchema);

								// The original IntrospTestCustIdx had:
								//   AK_M_GUIDIntrospTestCustIdx (GUID auto)
								//   IX_Custom_Title (IndexName override)
								//   UQ_IntrospTestCustIdx_ISBN (IndexName override, unique)
								//   IX_M_T_IntrospTestCustIdx_C_YearPublished (auto)
								//   IX_M_IDEditor (FK auto)
								let tmpNames = tmpIndices.map((pIdx) => { return pIdx.Name; });
								Expect(tmpNames).to.include('AK_M_GUIDIntrospTestCustIdx');
								Expect(tmpNames).to.include('IX_Custom_Title');
								Expect(tmpNames).to.include('UQ_IntrospTestCustIdx_ISBN');
								Expect(tmpNames).to.include('IX_M_T_IntrospTestCustIdx_C_YearPublished');
								Expect(tmpNames).to.include('IX_M_IDEditor');

								return fDone();
							});
					}
				);
			}
		);

		suite
		(
			'Chinook Database Introspection',
			()=>
			{
				let _Fable = null;
				let libSchemaMSSQL = null;

				setup(
					(fDone) =>
					{
						_Fable = new libFable(_FableConfig);
						_Fable.serviceManager.addServiceType('MeadowSchemaMSSQL', libMeadowSchemaMSSQL);
						libSchemaMSSQL = _Fable.serviceManager.instantiateServiceProvider('MeadowSchemaMSSQL');
						_Fable.serviceManager.addServiceType('MeadowMSSQLProvider', libMeadowConnectionMSSQL);
						_Fable.serviceManager.instantiateServiceProvider('MeadowMSSQLProvider');
						_Fable.MeadowMSSQLProvider.connectAsync(
							(pError) =>
							{
								if (pError) return fDone(pError);
								libSchemaMSSQL.setConnectionPool(_Fable.MeadowMSSQLProvider.pool);
								return fDone();
							});
					});

				test
				(
					'listTables includes all 11 Chinook tables',
					(fDone) =>
					{
						libSchemaMSSQL.listTables(
							(pError, pTables) =>
							{
								Expect(pError).to.not.exist;
								Expect(pTables).to.be.an('array');

								let tmpChinookTables = ['Album', 'Artist', 'Customer', 'Employee',
									'Genre', 'Invoice', 'InvoiceLine', 'MediaType',
									'Playlist', 'PlaylistTrack', 'Track'];

								tmpChinookTables.forEach(
									(pTableName) =>
									{
										Expect(pTables).to.include(pTableName);
									});

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableColumns on Track detects all 9 columns with correct types',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableColumns('Track',
							(pError, pColumns) =>
							{
								Expect(pError).to.not.exist;
								Expect(pColumns).to.be.an('array');
								Expect(pColumns.length).to.equal(9);

								let tmpTrackId = pColumns.find((pCol) => { return pCol.Column === 'TrackId'; });
								Expect(tmpTrackId.DataType).to.equal('ID');

								let tmpName = pColumns.find((pCol) => { return pCol.Column === 'Name'; });
								Expect(tmpName.DataType).to.equal('String');

								let tmpUnitPrice = pColumns.find((pCol) => { return pCol.Column === 'UnitPrice'; });
								Expect(tmpUnitPrice.DataType).to.equal('Decimal');

								let tmpMilliseconds = pColumns.find((pCol) => { return pCol.Column === 'Milliseconds'; });
								Expect(tmpMilliseconds.DataType).to.equal('Numeric');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableColumns on Employee detects 15 columns',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableColumns('Employee',
							(pError, pColumns) =>
							{
								Expect(pError).to.not.exist;
								Expect(pColumns.length).to.equal(15);

								let tmpEmployeeId = pColumns.find((pCol) => { return pCol.Column === 'EmployeeId'; });
								Expect(tmpEmployeeId.DataType).to.equal('ID');

								let tmpBirthDate = pColumns.find((pCol) => { return pCol.Column === 'BirthDate'; });
								Expect(tmpBirthDate.DataType).to.equal('DateTime');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableForeignKeys on Track detects 3 FK relationships',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableForeignKeys('Track',
							(pError, pFKs) =>
							{
								Expect(pError).to.not.exist;
								Expect(pFKs).to.be.an('array');
								Expect(pFKs.length).to.equal(3);

								let tmpAlbumFK = pFKs.find((pFK) => { return pFK.Column === 'AlbumId'; });
								Expect(tmpAlbumFK).to.exist;
								Expect(tmpAlbumFK.ReferencedTable).to.equal('Album');
								Expect(tmpAlbumFK.ReferencedColumn).to.equal('AlbumId');

								let tmpMediaTypeFK = pFKs.find((pFK) => { return pFK.Column === 'MediaTypeId'; });
								Expect(tmpMediaTypeFK).to.exist;
								Expect(tmpMediaTypeFK.ReferencedTable).to.equal('MediaType');

								let tmpGenreFK = pFKs.find((pFK) => { return pFK.Column === 'GenreId'; });
								Expect(tmpGenreFK).to.exist;
								Expect(tmpGenreFK.ReferencedTable).to.equal('Genre');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableForeignKeys on Employee detects self-referential FK',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableForeignKeys('Employee',
							(pError, pFKs) =>
							{
								Expect(pError).to.not.exist;
								Expect(pFKs).to.be.an('array');
								Expect(pFKs.length).to.equal(1);

								Expect(pFKs[0].Column).to.equal('ReportsTo');
								Expect(pFKs[0].ReferencedTable).to.equal('Employee');
								Expect(pFKs[0].ReferencedColumn).to.equal('EmployeeId');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableForeignKeys on PlaylistTrack detects 2 FKs',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableForeignKeys('PlaylistTrack',
							(pError, pFKs) =>
							{
								Expect(pError).to.not.exist;
								Expect(pFKs).to.be.an('array');
								Expect(pFKs.length).to.equal(2);

								let tmpPlaylistFK = pFKs.find((pFK) => { return pFK.Column === 'PlaylistId'; });
								Expect(tmpPlaylistFK).to.exist;
								Expect(tmpPlaylistFK.ReferencedTable).to.equal('Playlist');

								let tmpTrackFK = pFKs.find((pFK) => { return pFK.Column === 'TrackId'; });
								Expect(tmpTrackFK).to.exist;
								Expect(tmpTrackFK.ReferencedTable).to.equal('Track');

								return fDone();
							});
					}
				);

				test
				(
					'introspectTableSchema on Track combines columns with FK detection',
					(fDone) =>
					{
						libSchemaMSSQL.introspectTableSchema('Track',
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;
								Expect(pSchema.TableName).to.equal('Track');
								Expect(pSchema.ForeignKeys.length).to.equal(3);

								let tmpAlbumIdCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'AlbumId'; });
								Expect(tmpAlbumIdCol.DataType).to.equal('ForeignKey');

								let tmpMediaTypeIdCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'MediaTypeId'; });
								Expect(tmpMediaTypeIdCol.DataType).to.equal('ForeignKey');

								let tmpGenreIdCol = pSchema.Columns.find((pCol) => { return pCol.Column === 'GenreId'; });
								Expect(tmpGenreIdCol.DataType).to.equal('ForeignKey');

								return fDone();
							});
					}
				);

				test
				(
					'introspectDatabaseSchema includes all Chinook tables',
					(fDone) =>
					{
						libSchemaMSSQL.introspectDatabaseSchema(
							(pError, pSchema) =>
							{
								Expect(pError).to.not.exist;
								Expect(pSchema.Tables).to.be.an('array');

								let tmpTableNames = pSchema.Tables.map((pT) => { return pT.TableName; });
								Expect(tmpTableNames).to.include('Track');
								Expect(tmpTableNames).to.include('Album');
								Expect(tmpTableNames).to.include('Artist');
								Expect(tmpTableNames).to.include('Employee');
								Expect(tmpTableNames).to.include('Customer');
								Expect(tmpTableNames).to.include('Invoice');
								Expect(tmpTableNames).to.include('InvoiceLine');
								Expect(tmpTableNames).to.include('PlaylistTrack');

								let tmpTrack = pSchema.Tables.find((pT) => { return pT.TableName === 'Track'; });
								Expect(tmpTrack.ForeignKeys.length).to.equal(3);

								return fDone();
							});
					}
				);

				test
				(
					'generateMeadowPackageFromTable on Album produces valid package',
					(fDone) =>
					{
						libSchemaMSSQL.generateMeadowPackageFromTable('Album',
							(pError, pPackage) =>
							{
								Expect(pError).to.not.exist;
								Expect(pPackage.Scope).to.equal('Album');
								Expect(pPackage.DefaultIdentifier).to.equal('AlbumId');
								Expect(pPackage.Schema).to.be.an('array');
								Expect(pPackage.DefaultObject).to.be.an('object');

								let tmpIDEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'AlbumId'; });
								Expect(tmpIDEntry.Type).to.equal('AutoIdentity');

								let tmpTitleEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'Title'; });
								Expect(tmpTitleEntry.Type).to.equal('String');

								return fDone();
							});
					}
				);

				test
				(
					'generateMeadowPackageFromTable on Track handles FKs and Decimal',
					(fDone) =>
					{
						libSchemaMSSQL.generateMeadowPackageFromTable('Track',
							(pError, pPackage) =>
							{
								Expect(pError).to.not.exist;
								Expect(pPackage.Scope).to.equal('Track');
								Expect(pPackage.DefaultIdentifier).to.equal('TrackId');

								let tmpUnitPriceEntry = pPackage.Schema.find((pEntry) => { return pEntry.Column === 'UnitPrice'; });
								Expect(tmpUnitPriceEntry).to.exist;

								return fDone();
							});
					}
				);
			}
		);

		suite
			(
				'DDL Statement Budget',
				() =>
				{
					// A schema change is not a query. Inheriting the pool's
					// per-query requestTimeout is what aborted a legitimate
					// CREATE INDEX at 2 minutes and re-aborted it every retry.
					let fMakeSchemaProvider = (pMSSQLSettings) =>
					{
						let tmpFable = new libFable({ Product: 'DDLBudgetTest', LogStreams: [ { streamtype: 'console', level: 'fatal' } ] });
						tmpFable.settings.MSSQL = pMSSQLSettings || {};
						tmpFable.serviceManager.addServiceType('MeadowSchemaMSSQL', libMeadowSchemaMSSQL);
						let tmpProvider = tmpFable.serviceManager.instantiateServiceProvider('MeadowSchemaMSSQL');
						tmpProvider._ConnectionPool = {};
						return tmpProvider;
					};

					test
						(
							'DDL defaults to its own generous ceiling, not the pool per-query one',
							() =>
							{
								let tmpProvider = fMakeSchemaProvider({ RequestTimeoutMs: 120000 });
								Expect(tmpProvider._ddlRequestTimeoutMs()).to.equal(1800000);
								// The pool value must not leak into DDL.
								Expect(tmpProvider._ddlRequestTimeoutMs()).to.not.equal(120000);
							}
						);

					test
						(
							'the DDL ceiling is configurable, and 0 means no client-side ceiling',
							() =>
							{
								Expect(fMakeSchemaProvider({ DDLRetryOptions: { RequestTimeoutMs: 5400000 } })._ddlRequestTimeoutMs()).to.equal(5400000);
								Expect(fMakeSchemaProvider({ DDLRetryOptions: { RequestTimeoutMs: 0 } })._ddlRequestTimeoutMs()).to.equal(0);
							}
						);

					test
						(
							'RequestTimeout is terminal for DDL so a doomed build is attempted once',
							() =>
							{
								let tmpOptions = fMakeSchemaProvider({})._ddlRetryOptions('CREATE INDEX Huge');
								Expect(tmpOptions.NonRetryableModes).to.be.an('array');
								Expect(tmpOptions.NonRetryableModes).to.contain('RequestTimeout');
							}
						);

					test
						(
							'a long DDL statement reports progress while it runs, and stops on completion',
							(fDone) =>
							{
								// Silence is indistinguishable from a wedge to anything
								// watching the log -- including the data cloner's
								// pipeline watchdog, which would kill a healthy build.
								let tmpProvider = fMakeSchemaProvider({ DDLRetryOptions: { ProgressLogIntervalMs: 20 } });
								let tmpProgressLines = [];
								tmpProvider.log.info = (pMessage) => { tmpProgressLines.push(String(pMessage)); };

								let tmpAppliedTimeout = null;
								tmpProvider._createDDLRequest = () =>
									{
										tmpAppliedTimeout = tmpProvider._ddlRequestTimeoutMs();
										return { query: () => new Promise((fResolve) => setTimeout(() => fResolve({ rowsAffected: [ 0 ] }), 120)) };
									};

								tmpProvider._executeDDLStatement('CREATE INDEX IX_Huge ON dbo.Huge (GUIDHuge)', 'Meadow-MSSQL CREATE INDEX IX_Huge',
									(pError) =>
									{
										Expect(pError).to.equal(null);
										Expect(tmpAppliedTimeout).to.equal(1800000);
										let tmpStillRunning = tmpProgressLines.filter((pLine) => pLine.indexOf('still running') >= 0);
										Expect(tmpStillRunning.length).to.be.greaterThan(0);
										let tmpCountAtSettle = tmpStillRunning.length;
										// Nothing may be logged after the callback fires.
										setTimeout(() =>
											{
												Expect(tmpProgressLines.filter((pLine) => pLine.indexOf('still running') >= 0).length).to.equal(tmpCountAtSettle);
												fDone();
											}, 80);
									});
							}
						);

					test
						(
							'a failing DDL statement still stops its progress timer',
							(fDone) =>
							{
								let tmpProvider = fMakeSchemaProvider({ DDLRetryOptions: { ProgressLogIntervalMs: 20 } });
								let tmpProgressLines = [];
								tmpProvider.log.info = (pMessage) => { tmpProgressLines.push(String(pMessage)); };
								tmpProvider._createDDLRequest = () =>
									{
										return { query: () => new Promise((fResolve, fReject) => setTimeout(() => fReject(new Error('Timeout: Request failed to complete in 1800000ms')), 60)) };
									};

								tmpProvider._executeDDLStatement('CREATE INDEX IX_Huge ON dbo.Huge (GUIDHuge)', 'Meadow-MSSQL CREATE INDEX IX_Huge',
									(pError) =>
									{
										Expect(pError).to.be.an.instanceof(Error);
										let tmpCountAtSettle = tmpProgressLines.length;
										setTimeout(() =>
											{
												Expect(tmpProgressLines.length).to.equal(tmpCountAtSettle);
												fDone();
											}, 80);
									});
							}
						);
				}
			);
	}
);
