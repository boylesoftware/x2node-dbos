'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const selectQueryBuilder = require('./select-query-builder.js');


/////////////////////////////////////////////////////////////////////////////////
// FILTER PARAMETERS HANDLER
/////////////////////////////////////////////////////////////////////////////////

/**
 * Filter parameters handler used by a DBO. Any DBO that acts on a filtered set
 * of records (all except the insert) use a filter parameters handler to replace
 * parameter references in the SQL statements with the values provided to the DBO
 * execution method.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class FilterParamsHandler {

	/**
	 * Create new parameters handler for a DBO.
	 */
	constructor() {

		this._nextParamRef = 0;

		this._params = new Map();
	}

	/**
	 * Add parameter mapping.
	 *
	 * @param {string} paramName Parameter name.
	 * @param {function} valueFunc Function that given the parameter value
	 * provided with DBO call returns the ES literal for the value to include
	 * in the database statement.
	 * @returns {string} Parameter placeholder to include in the SQL in the
	 * form of "?{ref}", where "ref" is the parameter reference assigned by the
	 * handler.
	 */
	addParam(paramName, valueFunc) {

		const paramRef = String(this._nextParamRef++);

		this._params.set(paramRef, {
			name: paramName,
			valueFunc: valueFunc
		});

		return '?{' + paramRef + '}';
	}

	/**
	 * Get parameter value SQL expression.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {*} paramValue Parameter value from the DBO specification.
	 * @param {function} valueFunc Parameter value function (see
	 * <code>addParam</code> method).
	 * @param {string} paramName Parameter name (for error reporting only).
	 * @returns {string} Parameter value SQL expression to include in the
	 * database statement.
	 */
	paramValueToSql(dbDriver, paramValue, valueFunc, paramName) {

		const invalid = () => new common.X2UsageError(
			'Invalid ' + (paramName ? '"' + paramName + '"' : '') +
				' filter test parameter value: ' + String(paramValue) + '.');

		let preSql;
		if (valueFunc) {
			if ((paramValue === undefined) || (paramValue === null) || (
				((typeof paramValue) === 'number') &&
					!Number.isFinite(paramValue)) ||
				Array.isArray(paramValue))
				throw invalid();
			preSql = valueFunc(paramValue);
		} else {
			preSql = paramValue;
		}

		const sql = dbDriver.sql(preSql);
		if ((sql === null) || (sql === 'NULL'))
			throw invalid();

		return sql;
	}

	/**
	 * Get parameter value SQL expression given the parameter reference.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {Object.<string,*>} filterParams Parameters provided with the DBO
	 * execution call. Will throw error if not provided.
	 * @param {string} paramRef Parameter reference from the SQL.
	 * @returns {string} Parameter value SQL expression to include in the
	 * database statement.
	 * @throws {module:x2node-common.X2UsageError} If no value for the specified
	 * parameter reference.
	 */
	paramSql(dbDriver, filterParams, paramRef) {

		const ref = this._params.get(paramRef);
		const filterParam = (ref && filterParams && filterParams[ref.name]);
		if (filterParam === undefined)
			throw new common.X2UsageError(
				`Missing filter parameter "${ref.name}".`);

		const process = v => this.paramValueToSql(
			dbDriver, v, ref.valueFunc, ref.name);
		return (
			Array.isArray(filterParam) ?
				filterParam.map(v => process(v)).join(', ') :
				process(filterParam)
		);
	}
}


/////////////////////////////////////////////////////////////////////////////////
// COMMON COMMANDS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Interface for DBO commands.
 *
 * @protected
 * @interface DBOCommand
 * @memberof module:x2node-dbos
 */
/**
 * Queue up the command execution.
 *
 * @function module:x2node-dbos.DBOCommand#queueUp
 * @param {Promise} promiseChain DBO execution result promise chain.
 * @param {module:x2node-dbos~DBOExecutionContext ctx DBO execution context.
 * @returns {Promise} New result promise chain with the command queued up.
 */

/**
 * Command that simply executes the configured SQL statement.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class ExecuteStatementCommand {

	constructor(stmt, stmtId) {

		this._stmt = stmt;
		this._stmtId = stmtId;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const stmt = this._stmt;
		const stmtId = this._stmtId;
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(stmt);
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeUpdate(
						ctx.connection, sql, {
							onSuccess(affectedRows) {
								ctx.affectedRows(affectedRows, stmtId);
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						}
					);
				} catch (err) {
					common.error(`error executing SQL [${sql || stmt}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
}

/**
 * Command used to load matching record ids into a temporary table for a
 * multi-branch fetch operation.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class LoadAnchorTableCommand {

	constructor(
		anchorTableName, topTableName, idColumnName, idExpr, statementStump) {

		this._anchorTableName = anchorTableName;
		this._topTableName = topTableName;
		this._idColumnName = idColumnName;
		this._idExpr = idExpr;
		this._statementStump = statementStump;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const cmd = this;
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				try {
					let lastSql;
					ctx.dbDriver.selectIntoAnchorTable(
						ctx.connection,
						cmd._anchorTableName, cmd._topTableName,
						cmd._idColumnName, cmd._idExpr,
						ctx.replaceParams(cmd._statementStump), {
							trace(sql) {
								lastSql = sql;
								ctx.log(`executing SQL: ${sql}`);
							},
							onSuccess(numRows) {
								ctx.log(`anchor table has ${numRows} rows`);
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${lastSql}]`, err);
								reject(err);
							}
						}
					);
				} catch (err) {
					common.error('error loading anchor table', err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
}

/**
 * Property value generator command. When executed, calls generator function
 * associated with the configured property and adds it to the DBO execution
 * context as a generared param using the property path as the param name.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class GeneratorCommand {

	/**
	 * Create new command.
	 *
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Generated
	 * property descriptor.
	 */
	constructor(propDesc) {

		this._propDesc = propDesc;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const propDesc = this._propDesc;
		const propPath = propDesc.container.nestedPath + propDesc.name;
		return promiseChain.then(
			() => {
				try {

					const val = propDesc.generator(ctx.connection);

					if (val instanceof Promise)
						return val.then(
							resVal => {
								ctx.addGeneratedParam(propPath, resVal);
							},
							err => {
								common.error(
									`error generating property ${propPath}` +
										` value`, err);
								return Promise.reject(err);
							}
						);

					ctx.addGeneratedParam(propPath, val);

				} catch (err) {
					common.error(
						`error generating property ${propPath} value`, err);
					return Promise.reject(err);
				}
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Assigned id command. When executed, takes the property value from the provided
 * record data and adds it to the DBO execution context as a generared param
 * using the property path as the param name.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class AssignedIdCommand {

	/**
	 * Create new command.
	 *
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Id property
	 * descriptor.
	 * @param {Object} data The object data.
	 */
	constructor(propDesc, data) {

		this._propDesc = propDesc;
		this._idVal = data[propDesc.name];

		if ((this._idVal === undefined) || (this._idVal === null))
			throw new common.X2UsageError(
				`No value provided for non-generated id property ` +
					`${propDesc.container.nestedPath}${propDesc.name}.`);
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		return promiseChain.then(
			() => {
				ctx.addGeneratedParam(
					this._propDesc.container.nestedPath + this._propDesc.name,
					this._idVal
				);
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * SQL <code>INSERT</code> statement command. When executed, builds a SQL
 * <code>INSERT</code> statement for the configured table using column/value
 * pairs added to the command and executes the statement.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class InsertCommand {

	/**
	 * Create new command.
	 *
	 * @param {string} table The table.
	 */
	constructor(table) {

		this._table = table;

		this._columns = new Array();
		this._values = new Array();
	}

	/**
	 * Add column/value pair to the statement's <code>SET</code> clause.
	 *
	 * @param {string} column Column name.
	 * @param {string} value Value SQL expression.
	 */
	add(column, value) {

		this._columns.push(column);
		this._values.push(value);
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const stmt = this._buildStatement();

		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(stmt);
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeInsert(
						ctx.connection, sql, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						});
				} catch (err) {
					common.error(`error executing SQL [${sql || stmt}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}

	/**
	 * Build SQL <code>INSERT</code> statement.
	 *
	 * @protected
	 * @returns {string} The SQL statement (may contain param placeholders).
	 */
	_buildStatement() {

		return 'INSERT INTO ' + this._table + ' (' +
			this._columns.join(', ') + ') VALUES (' +
			this._values.join(', ') + ')';
	}
}

/**
 * SQL <code>INSERT</code> statement with auto-generated id command. Similar to
 * {module:x2node-dbos~InsertCommand}, but upon execution sets the generated by
 * the database id value for the new record or nested object into the record
 * object and also adds it to the DBO execution context as a generared param
 * using the id property path as the param name.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~InsertCommand
 */
class InsertWithGeneratedIdCommand extends InsertCommand {

	/**
	 * Create new command.
	 *
	 * @param {string} table The table.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Auto-generated
	 * id property descriptor.
	 * @param {Object} obj Object, into which to set the generated id property
	 * after the command is executed.
	 */
	constructor(table, propDesc, obj) {
		super(table);

		this._propDesc = propDesc;
		this._obj = obj;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const stmt = this._buildStatement();

		const propDesc = this._propDesc;
		const propPath = propDesc.container.nestedPath + propDesc.name;
		const obj = this._obj;
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(stmt);
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeInsert(
						ctx.connection, sql, {
							onSuccess(rawId) {
								const id = rsparser.extractValue(
									propDesc.scalarValueType, rawId);
								obj[propDesc.name] = id;
								ctx.addGeneratedParam(propPath, id);
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						}, propDesc.column);
				} catch (err) {
					common.error(`error executing SQL [${sql || stmt}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
}

/**
 * Update entangled records command executed at the end of the DBO. When
 * executed, the command takes the updated entangled records information from the
 * DBO execution context, generates the appropriate SQL <code>UPDATE</code>
 * statements and executes them.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class UpdateEntangledRecordsCommand {

	constructor(updatedRecordTypeNames) {

		this._updatedRecordTypeNames = updatedRecordTypeNames;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		return promiseChain.then(
			() => {

				// pre-resolve the completion promise
				let resPromise = Promise.resolve();

				// go over entangled record types and generate the updates
				const sets = new Array();
				for (let recordTypeName in ctx.entangledUpdates) {

					// get the entangled record ids
					const ids = ctx.entangledUpdates[recordTypeName];
					if (ids.size === 0)
						continue;

					// get record type descriptor
					const recordTypeDesc = ctx.recordTypes.getRecordTypeDesc(
						recordTypeName);

					// check if has anything to update and build SET clause
					getModificationMetaInfoSets(sets, recordTypeDesc);
					if (sets.length === 0)
						continue;

					// register record type update
					if (this._updatedRecordTypeNames)
						this._updatedRecordTypeNames.add(recordTypeName);

					// build the UPDATE statement
					const idColumn = recordTypeDesc.getPropertyDesc(
						recordTypeDesc.idPropertyName).column;
					const stmt =
						'UPDATE ' + recordTypeDesc.table + ' SET ' + sets.map(
							s => `${s.columnName} = ${s.value}`).join(', ') +
						' WHERE ' + (
							ids.size === 1 ?
								idColumn + ' = ' + ctx.dbDriver.sql(
									ids.values().next().value) :
								idColumn + ' IN (' + Array.from(ids).map(
									v => ctx.dbDriver.sql(v)).join(', ') + ')'
						);

					// queue up UPDATE statement execution
					resPromise = resPromise.then(
						() => new Promise((resolve, reject) => {
							let sql;
							try {
								sql = ctx.replaceParams(stmt);
								ctx.log(`executing SQL: ${sql}`);
								ctx.dbDriver.executeUpdate(ctx.connection, sql, {
									onSuccess() {
										resolve();
									},
									onError(err) {
										common.error(
											`error executing SQL [${sql}]`, err);
										reject(err);
									}
								});
							} catch (err) {
								common.error(
									`error executing SQL [${sql || stmt}]`, err);
								reject(err);
							}
						}),
						err => Promise.reject(err)
					);
				}

				// return the completion promise
				return resPromise;
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Call record collections monitor and notify it about a record collections
 * update.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class NotifyRecordCollectionsMonitorCommand {

	constructor(monitor, recordTypeNames) {

		this._monitor = monitor;
		this._recordTypeNames = recordTypeNames;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		if (!this._monitor)
			return promiseChain;

		return promiseChain.then(
			() => (
				this._recordTypeNames.size > 0 ?
					this._monitor.collectionsUpdated(
						ctx, this._recordTypeNames) :
					undefined
			),
			err => Promise.reject(err)
		);
	}
}


/////////////////////////////////////////////////////////////////////////////////
// THE ABSTRACT DBO
/////////////////////////////////////////////////////////////////////////////////

/**
 * Get elements of an UPDATE statement's SET clause for updating record
 * modification meta-info.
 *
 * @function module:x2node-dbos~AbstractDBO.getModificationMetaInfoSets
 * @param {Array} [sets] Array to use for the SET clause elements. If provided,
 * it is cleared before adding the SET clause elements. If not provided, a new
 * array is created.
 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc Record type
 * descriptor.
 * @param {string} [tableAlias] Alias of the record type main table in the UPDATE
 * statement, or nothing is not used.
 * @returns {Array} SET clause elements, or empty array is no record modification
 * meta-info on the record type.
 */
function getModificationMetaInfoSets(sets, recordTypeDesc, tableAlias) {

	if (sets)
		sets.length = 0;
	else
		sets = new Array();

	let propName = recordTypeDesc.getRecordMetaInfoPropName('version');
	if (propName) {
		const column = recordTypeDesc.getPropertyDesc(propName).column;
		sets.push({
			columnName: column,
			value: (tableAlias ? tableAlias + '.' : '') + column + ' + 1'
		});
	}

	propName = recordTypeDesc.getRecordMetaInfoPropName('modificationTimestamp');
	if (propName)
		sets.push({
			columnName: recordTypeDesc.getPropertyDesc(propName).column,
			value: '?{ctx.executedOn}'
		});

	propName = recordTypeDesc.getRecordMetaInfoPropName('modificationActor');
	if (propName)
		sets.push({
			columnName: recordTypeDesc.getPropertyDesc(propName).column,
			value: '?{ctx.actor}'
		});

	return sets;
}

/**
 * Abstract database operation implementation.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 * @abstract
 */
class AbstractDBO {

	/**
	 * Create new DBO.
	 *
	 * @constructor
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-dbos.RecordCollectionsMonitor} [rcMonitor] The
	 * record collections monitor.
	 */
	constructor(dbDriver, recordTypes, rcMonitor) {

		/**
		 * Database driver.
		 *
		 * @protected
		 * @member {module:x2node-dbos.DBDriver} module:x2node-dbos~AbstractDBO#_dbDriver
		 */
		this._dbDriver = dbDriver;

		/**
		 * The record types library.
		 *
		 * @protected
		 * @member {module:x2node-records~RecordTypesLibrary} module:x2node-dbos~AbstractDBO#_recordTypes
		 */
		this._recordTypes = recordTypes;

		this._rcMonitor = rcMonitor;
		this._updatedRecordTypeNames = (rcMonitor ? new Set() : null);

		/**
		 * Debug logger.
		 *
		 * @protected
		 * @member {function} module:x2node-dbos~AbstractDBO#_log
		 */
		this._log = common.getDebugLogger('X2_DBO');

		/**
		 * Filter parameters handler.
		 *
		 * @protected
		 * @member {module:x2node-dbos~FilterParamsHandler} module:x2node-dbos~AbstractDBO#_paramsHandler
		 */
		this._paramsHandler = new FilterParamsHandler();

		/**
		 * Tells if actor is required for the DBO execution. Initially is set to
		 * <code>false</code> allowing anonymous DBO executions.
		 *
		 * @protected
		 * @member {boolean} module:x2node-dbos~AbstractDBO#_actorRequired
		 */
		this._actorRequired = false;
	}


	/////////////////////////////////////////////////////////////////////////////
	// DBO commands construction helper methods
	/////////////////////////////////////////////////////////////////////////////

	// make it a static class function so it gets exported
	static getModificationMetaInfoSets(sets, recordTypeDesc, tableAlias) {

		return getModificationMetaInfoSets(sets, recordTypeDesc, tableAlias);
	}

	/**
	 * Build a DBO command that simply executes the specified SQL statement. The
	 * command also calls context's
	 * [affectedRows()]{@link module:x2node-dbos~DBOExecutionContext#affectedRows}
	 * method upon completion.
	 *
	 * @param {string} stmt The SQL statement (may contain param placeholders).
	 * @param {*} [stmtId] DBO-specific statement id, if any.
	 * @returns {module:x2node-dbos.DBOCommand} The command.
	 */
	_createExecuteStatementCommand(stmt, stmtId) {

		return new ExecuteStatementCommand(stmt, stmtId);
	}

	/**
	 * Build a DBO command that loads record ids into a temporary anchor table
	 * used for multi-branch fetches. The anchor table has the name of the main
	 * record type table prefixed with "q_" and has two columns: "id" and "ord".
	 *
	 * @param {module:x2node-dbos~QueryTreeNode} idsQueryTree Query tree for the
	 * record ids.
	 * @param {?module:x2node-dbos~RecordsFilter} [filter] Optional filter for
	 * the ids <code>SELECT</code> query.
	 * @param {?module:x2node-dbos~RecordsOrder} [order] Optional order for the
	 * ids <code>SELECT</code> query.
	 * @param {?module:x2node-dbos~RecordsRange} [range] Optional range for the
	 * ids <code>SELECT</code> query.
	 * @param {?string} [lockType] Optional lock type: "exclusive" or "shared".
	 * @returns {module:x2node-dbos.DBOCommand} The command.
	 */
	_createLoadAnchorTableCommand(idsQueryTree, filter, order, range, lockType) {

		const idsQuery = this._assembleSelect(idsQueryTree, filter, order);

		let idsQuerySql = idsQuery.toSql(true);

		if (range)
			idsQuerySql = this._dbDriver.makeRangedSelect(
				idsQuerySql, range.offset, range.limit);

		if (lockType) {
			const exclusiveLockTables = new Array();
			const sharedLockTables = new Array();
			idsQuery.getTablesForLock(
				lockType, exclusiveLockTables, sharedLockTables);
			idsQuerySql = this._dbDriver.makeSelectWithLocks(
				idsQuerySql, exclusiveLockTables, sharedLockTables);
		}

		return new LoadAnchorTableCommand(
			`q_${idsQueryTree.table}`, idsQueryTree.table,
			idsQueryTree.keyColumn, idsQuery.getIdValueExpr(), idsQuerySql);
	}

	/**
	 * Assemble a <code>SELECT</code> query.
	 *
	 * @protected
	 * @param {module:x2node-dbos~QueryTreeNode} idsQueryTree Query tree.
	 * @param {?module:x2node-dbos~RecordsFilter} [filter] Optional filter.
	 * @param {?module:x2node-dbos~RecordsOrder} [order] Optional order.
	 * @returns {module:x2node-dbos~SelectQuery} The query builder.
	 */
	_assembleSelect(queryTree, filter, order) {

		return selectQueryBuilder.assembleSelect(
			queryTree, queryTree.getTopTranslationContext(this._paramsHandler),
			filter, order);
	}

	/**
	 * Build DBO commands used to insert records and values and add them to the
	 * provided commands sequence.
	 *
	 * @protected
	 * @param {Array.<module:x2node-dbos.DBOCommand>} commands The commands
	 * sequence, to which to add the insert commands.
	 * @param {string} table The table.
	 * @param {?string} parentIdColumn Parent id column, or <code>null</code> for
	 * the top record type table.
	 * @param {string} [parentIdPropPath] If <code>parentIdColumn</code> is
	 * provided, this is the path to the id property in the parent container.
	 * @param {module:x2node-records~PropertyDescriptor} [colPropDesc] If
	 * commands are created for inserting nested objects into a nested object
	 * array or map property, then this is the array or the map property
	 * descriptor.
	 * @param {(number|string)} [keyVal] Array element index or map key value if
	 * <code>colPropDesc</code> is provided.
	 * @param {module:x2node-records~PropertiesContainer} container Top container
	 * for the table.
	 * @param {Object} data Object matching the container.
	 */
	_createInsertCommands(
		commands, table, parentIdColumn, parentIdPropPath, colPropDesc, keyVal,
		container, data) {

		// create insert command
		let insertCmd, idPropPath;
		if (container.idPropertyName) {
			const idPropDesc = container.getPropertyDesc(
				container.idPropertyName);
			idPropPath = idPropDesc.container.nestedPath + idPropDesc.name;
			if (idPropDesc.isGenerated()) {
				if (idPropDesc.generator === 'auto') {
					insertCmd = new InsertWithGeneratedIdCommand(
						table, idPropDesc, data);
				} else { // generator function
					commands.push(new GeneratorCommand(idPropDesc));
					insertCmd = new InsertCommand(table);
					insertCmd.add(idPropDesc.column, '?{' + idPropPath + '}');
				}
			} else {
				commands.push(new AssignedIdCommand(idPropDesc, data));
				insertCmd = new InsertCommand(table);
				insertCmd.add(idPropDesc.column, '?{' + idPropPath + '}');
			}
		} else {
			insertCmd = new InsertCommand(table);
			idPropPath = parentIdPropPath;
		}

		// add parent id column
		if (parentIdColumn)
			insertCmd.add(parentIdColumn, '?{' + parentIdPropPath + '}');

		// add array element index column
		if (colPropDesc && colPropDesc.isArray() && colPropDesc.indexColumn)
			insertCmd.add(
				colPropDesc.indexColumn, this._dbDriver.sql(keyVal));

		// add map key column
		if (colPropDesc && colPropDesc.isMap() && colPropDesc.keyColumn)
			insertCmd.add(
				colPropDesc.keyColumn, this._makeMapKeySql(colPropDesc, keyVal));

		// add record meta-info properties
		if (container.isRecordType()) {
			let propName = container.getRecordMetaInfoPropName('version');
			if (propName)
				insertCmd.add(
					container.getPropertyDesc(propName).column,
					'1'
				);
			propName = container.getRecordMetaInfoPropName('creationTimestamp');
			if (propName)
				insertCmd.add(
					container.getPropertyDesc(propName).column,
					'?{ctx.executedOn}'
				);
			propName = container.getRecordMetaInfoPropName('creationActor');
			if (propName) {
				this._actorRequired = true;
				insertCmd.add(
					container.getPropertyDesc(propName).column,
					'?{ctx.actor}'
				);
			}
		}

		// process the container properties
		this._createInsertCommandsForContainer(
			commands, commands.push(insertCmd) - 1, idPropPath, container, data);
	}

	/**
	 * Process record, add columns to the insert command and recursively add
	 * dependent inserts to the sequence.
	 *
	 * @private
	 * @param {Array.<module:x2node-dbos.DBOCommand>} commands The commands
	 * sequence, to which to add the insert commands.
	 * @param {number} insertCmdInd Index of the current insert command in the
	 * commands sequence.
	 * @param {string} idPropPath Property path of the id in the current insert
	 * command.
	 * @param {module:x2node-records~PropertiesContainer} container Container
	 * descriptor with the properties to add.
	 * @param {Object} data Object matching the container.
	 * @returns {number} The current insert command index in the sequence, which
	 * may have been changed by the method (generators could have been inserted
	 * before the command shifting it down the sequence).
	 */
	_createInsertCommandsForContainer(
		commands, insertCmdInd, idPropPath, container, data) {

		// get the current insert command
		const insertCmd = commands[insertCmdInd];

		// go over the container properties
		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);

			// skip properties we don't need to insert
			if (propDesc.isCalculated() || propDesc.isId() ||
				propDesc.isRecordMetaInfo() || propDesc.isView() ||
				propDesc.reverseRefPropertyName)
				continue;

			// get the value
			const propVal = data[propName];

			// check if nested object
			if (propDesc.scalarValueType === 'object') {

				// check if there is a value
				if ((propVal === undefined) || (propVal === null)) {
					if (!propDesc.optional)
						throw new common.X2UsageError(
							'No value provided for required property ' +
								container.nestedPath + propName + '.');
					continue;
				}

				// process depending on the structural type
				if (propDesc.isArray()) {

					// check the type
					if (!Array.isArray(propVal))
						new common.X2UsageError(
							'Invalid value type for property ' +
								container.nestedPath + propName +
								', expected an array.');

					// check if empty
					if (propVal.length === 0) {
						if (!propDesc.optional)
							throw new common.X2UsageError(
								'No value provided for required property ' +
									container.nestedPath + propName + '.');
						continue;
					}

					// add the inserts
					propVal.forEach((v, i) => {
						if ((v === undefined) || (v === null) ||
							((typeof v) !== 'object') || Array.isArray(v))
							new common.X2UsageError(
								'Invalid array element for property ' +
									container.nestedPath + propName +
									', expected an object.');
						this._createInsertCommands(
							commands, propDesc.table, propDesc.parentIdColumn,
							idPropPath, propDesc, i, propDesc.nestedProperties,
							v);
					});

				} else if (propDesc.isMap()) {

					// check the type
					if (((typeof propVal) !== 'object') ||
						Array.isArray(propVal))
						new common.X2UsageError(
							'Invalid value type for property ' +
								container.nestedPath + propName +
								', expected an object.');

					// check if empty
					const keys = Object.keys(propVal);
					if (keys.length === 0) {
						if (!propDesc.optional)
							throw new common.X2UsageError(
								'No value provided for required property ' +
									container.nestedPath + propName + '.');
						continue;
					}

					// add the inserts
					keys.forEach(key => {
						const v = propVal[key];
						if ((v === undefined) || (v === null) ||
							((typeof v) !== 'object') || Array.isArray(v))
							new common.X2UsageError(
								'Invalid map element for property ' +
									container.nestedPath + propName +
									', expected an object.');
						this._createInsertCommands(
							commands, propDesc.table, propDesc.parentIdColumn,
							idPropPath, propDesc, key, propDesc.nestedProperties,
							v);
					});

				} else {

					// check the type
					if (((typeof propVal) !== 'object') ||
						Array.isArray(propVal))
						new common.X2UsageError(
							'Invalid value type for property ' +
								container.nestedPath + propName +
								', expected an object.');

					// check if in a separate table
					if (propDesc.table) {
						this._createInsertCommands(
							commands, propDesc.table, propDesc.parentIdColumn,
							idPropPath, null, null, propDesc.nestedProperties,
							propVal);
					} else {
						insertCmdInd = this._createInsertCommandsForContainer(
							commands, insertCmdInd, idPropPath,
							propDesc.nestedProperties, propVal);
					}
				}

			} else { // simple value or ref

				// value SQL to add to the insert
				let propValSql;

				// check if generated and no value
				if ((propVal === undefined) && propDesc.isGenerated()) {

					// skip if auto-generated
					if (propDesc.generator === 'auto')
						continue;

					// add generator command
					commands.splice(
						insertCmdInd++, 0, new GeneratorCommand(propDesc));
					propValSql = '?{' + propDesc.container.nestedPath +
						propDesc.name + '}';

				} else if ((propVal !== undefined) && (propVal !== null)) {
					if (propDesc.isArray()) {
						if (!Array.isArray(propVal))
							new common.X2UsageError(
								'Invalid value type for property ' +
									container.nestedPath + propName +
									', expected an array.');
						if (propVal.length > 0)
							propValSql = propVal.map(
								v => this._makePropValSql(propDesc, v));
					} else if (propDesc.isMap()) {
						if (((typeof propVal) !== 'object') ||
							Array.isArray(propVal))
							new common.X2UsageError(
								'Invalid value type for property ' +
									container.nestedPath + propName +
									', expected an object.');
						const keys = Object.keys(propVal);
						if (keys.length > 0) {
							propValSql = keys.reduce((res, k) => {
								res.set(k, this._makePropValSql(
									propDesc, propVal[k]));
								return res;
							}, new Map());
						}
					} else {
						propValSql = this._makePropValSql(propDesc, propVal);
					}
				}

				// check if no value
				if (propValSql === undefined) {
					if (!propDesc.optional)
						throw new common.X2UsageError(
							'No value provided for required property ' +
								container.nestedPath + propName + '.');
					continue;
				}

				// make the corresponding inserts
				if (propDesc.table) {
					const idSql = '?{' + idPropPath + '}';
					if (propDesc.isArray()) {
						propValSql.forEach((v, i) => {
							const valInsert = new InsertCommand(propDesc.table);
							valInsert.add(propDesc.parentIdColumn, idSql);
							if (propDesc.indexColumn)
								valInsert.add(
									propDesc.indexColumn, this._dbDriver.sql(i));
							valInsert.add(propDesc.column, v);
							commands.push(valInsert);
						});
					} else if (propDesc.isMap()) {
						propValSql.forEach((v, k) => {
							const valInsert = new InsertCommand(propDesc.table);
							valInsert.add(propDesc.parentIdColumn, idSql);
							if (propDesc.keyColumn)
								valInsert.add(
									propDesc.keyColumn,
									this._makeMapKeySql(propDesc, k)
								);
							valInsert.add(propDesc.column, v);
							commands.push(valInsert);
						});
					} else {
						const valInsert = new InsertCommand(propDesc.table);
						valInsert.add(propDesc.parentIdColumn, idSql);
						valInsert.add(propDesc.column, propValSql);
						commands.push(valInsert);
					}
				} else {
					insertCmd.add(propDesc.column, propValSql);
				}
			}
		}

		// go over subtype-specific properties if polymorph object
		if (container.isPolymorphObject()) {

			// get the subtype
			const subtypeName = data[container.typePropertyName];
			if ((typeof subtypeName) !== 'string')
				throw new common.X2UsageError(
					'Type property of polymorphic ' + (
							container.isRecordType() ?
								'record' :
								'property ' + container.nestedPath
						) + ' is missing or is not a string.');

			// get the subtype extension container
			if (!container.hasProperty(subtypeName))
				throw new common.X2UsageError(
					'Unknown type "' + subtypeName + '" of polymorphic ' + (
							container.isRecordType() ?
								'record' :
								'property ' + container.nestedPath
						) + '.');
			const subtypeDesc = container.getPropertyDesc(subtypeName);
			if (!subtypeDesc.isSubtype())
				throw new common.X2UsageError(
					'Unknown type "' + subtypeName + '" of polymorphic ' + (
							container.isRecordType() ?
								'record' :
								'property ' + container.nestedPath
						) + '.');

			// add type column to the insert if any
			const typePropDesc = container.getPropertyDesc(
				container.typePropertyName);
			if (typePropDesc.column)
				insertCmd.add(
					typePropDesc.column, this._dbDriver.sql(subtypeName));

			// add insert commands for the subtype-specific properties
			if (subtypeDesc.table) {
				this._createInsertCommands(
					commands, subtypeDesc.table, subtypeDesc.parentIdColumn,
					idPropPath, null, null, subtypeDesc.nestedProperties, data);
			} else {
				insertCmdInd = this._createInsertCommandsForContainer(
					commands, insertCmdInd, idPropPath,
					subtypeDesc.nestedProperties, data);
			}
		}

		// TODO: support polymorphic reference container

		// return the insert command index
		return insertCmdInd;
	}

	/**
	 * Validate property value and make SQL for it.
	 *
	 * @protected
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Property
	 * descriptor.
	 * @param {*} val Property value. If <code>null</code> or
	 * <code>undefined</code>, SQL <code>NULL</code> is returned.
	 * @returns {string} The value SQL.
	 * @throws {module:x2node-common.X2UsageError} If the value is not good.
	 */
	_makePropValSql(propDesc, val) {

		if ((val === null) || (val === undefined))
			return this._dbDriver.sql(null);

		const valSql = this._valueToSql(
			val, propDesc.scalarValueType, propDesc.refTarget,
			expectedType => new common.X2UsageError(
				'Invalid value type [' + (typeof val) + '] for property ' +
					propDesc.container.nestedPath + propDesc.name +
					', expected [' + expectedType + '].')
		);

		if (valSql === null)
			throw new common.X2UsageError(
				'Invalid value [' + String(val) + '] for property ' +
					propDesc.container.nestedPath +  propDesc.name + '.');

		return valSql;
	}

	/**
	 * Validate value for a map property key and make SQL for it. The method is
	 * also capable of converting the key value from string to the expected key
	 * value type (relevant for "number" and "boolean" types).
	 *
	 * @protected
	 * @param {module:x2node-records~PropertyDescriptor} mapPropDesc Map property
	 * descriptor.
	 * @param {*} key Key value. May not be <code>null</code> or
	 * <code>undefined</code>.
	 * @returns {string} The key value SQL.
	 * @throws {module:x2node-common.X2UsageError} If the key value is not good.
	 */
	_makeMapKeySql(mapPropDesc, key) {

		const invalidKeyVal = () => new common.X2UsageError(
			`Invalid key value [${String(key)}] for map property ` +
				`${mapPropDesc.container.nestedPath}${mapPropDesc.name}.`);

		if ((key === null) || (key === undefined))
			throw invalidKeyVal();

		let keyToUse;
		if ((typeof key) === 'string') {
			switch (mapPropDesc.keyValueType) {
			case 'number':
				keyToUse = Number(key);
				break;
			case 'boolean':
				keyToUse = (
					key === 'true' ? true : (key === 'false' ? false : null));
				break;
			default:
				keyToUse = key;
			}
		}

		const keySql = this._valueToSql(
			keyToUse, mapPropDesc.keyValueType, mapPropDesc.keyRefTarget,
			expectedType => new common.X2UsageError(
				`Invalid key value type [${typeof key}] for map property ` +
					`${mapPropDesc.container.nestedPath}${mapPropDesc.name},` +
					` expected [${expectedType}].`)
		);

		if (keySql === null)
			throw invalidKeyVal();

		return keySql;
	}

	/**
	 * Validate value against expected value type and convert it into SQL value
	 * expression.
	 *
	 * @private
	 * @param {*} val The value.
	 * @param {string} valueType Scalar value type.
	 * @param {string} [expectedRefTarget] If expected value type is "ref", then
	 * this is expected target record type name.
	 * @param {function} invalidValType Function to use to create throwable error
	 * when the specified value's ES type does not match the specified expected
	 * value type. The function takes the expected ES type as an argument.
	 * @returns {string} SQL value expression, or <code>null</code> if the value
	 * is invalid, including <code>null</code> and <code>undefined</code>.
	 */
	_valueToSql(val, valueType, expectedRefTarget, invalidValType) {

		let valSql = null;
		let hashInd;
		switch (valueType) {
		case 'string':
			if ((typeof val) !== 'string')
				throw invalidValType('string');
			valSql = this._dbDriver.stringLiteral(val);
			break;
		case 'number':
			if ((typeof val) !== 'number')
				throw invalidValType('number');
			valSql = this._dbDriver.sql(val);
			break;
		case 'boolean':
			if ((typeof val) !== 'boolean')
				throw invalidValType('boolean');
			valSql = this._dbDriver.booleanLiteral(val);
			break;
		case 'datetime':
			if ((typeof val) !== 'string')
				throw invalidValType('string');
			if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(val)) {
				const dateVal = Date.parse(val);
				if (!Number.isNaN(dateVal))
					valSql = this._dbDriver.sql(
						(new Date(dateVal)).toISOString());
			}
			break;
		case 'ref':
			if ((typeof val) !== 'string')
				throw invalidValType('string');
			hashInd = val.indexOf('#');
			if ((hashInd > 0) && (hashInd < val.length - 1)) {
				const refTarget = val.substring(0, hashInd);
				if (refTarget === expectedRefTarget) {
					const refTargetDesc = this._recordTypes.getRecordTypeDesc(
						refTarget);
					const refIdPropDesc = refTargetDesc.getPropertyDesc(
						refTargetDesc.idPropertyName);
					valSql = this._dbDriver.sql(
						refIdPropDesc.scalarValueType === 'number' ?
							Number(val.substring(hashInd + 1)) :
							val.substring(hashInd + 1)
					);
				}
			}
		}

		return valSql;
	}

	/**
	 * Build DBO command used to update entangled records modification meta-info
	 * at the end of the DBO. The updated entangled records information is taken
	 * from the DBO execution context's
	 * [entangledUpdates]{@link module:x2node-dbos~DBOExecutionContext#entangledUpdates}
	 * property.
	 *
	 * @protected
	 * @returns {module:x2node-dbos.DBOCommand} The command.
	 */
	_createUpdateEntangledRecordsCommand() {

		return new UpdateEntangledRecordsCommand(this._updatedRecordTypeNames);
	}

	/**
	 * Build DBO command that notifies registered record collections monitor
	 * about the updated record types. The record type names are registered by
	 * the DBO throughout its execution via the
	 * [_registerRecordTypeUpdate()]{@link module:x2node-dbos~AbstractDBO#_registerRecordTypeUpdate}
	 * method.
	 *
	 * @protected
	 * @returns {module:x2node-dbos.DBOCommand} The command.
	 */
	_createNotifyRecordCollectionsMonitorCommand() {

		return new NotifyRecordCollectionsMonitorCommand(
			this._rcMonitor, this._updatedRecordTypeNames);
	}


	/////////////////////////////////////////////////////////////////////////////
	// DBO execution helper methods
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Register an update of a record of the specified record type.
	 *
	 * @param {string} recordTypeName Updated record type name.
	 */
	_registerRecordTypeUpdate(recordTypeName) {

		if (this._updatedRecordTypeNames)
			this._updatedRecordTypeNames.add(recordTypeName);
	}

	/**
	 * Execute DBO commands.
	 *
	 * @protected
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} Operation result promise.
	 */
	_executeCommands(ctx) {

		// check if actor is required
		if (this._actorRequired && !ctx.actor)
			throw new common.X2UsageError('Operation may not be anonymous.');

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up the commands
		this._commands.forEach(cmd => {
			resPromise = cmd.queueUp(resPromise, ctx);
		});

		// finish transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// build the final result object
		resPromise = resPromise.then(
			() => ctx.getResult(),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}

	/**
	 * Add transaction start to the operation execution promise chain.
	 *
	 * @private
	 * @param {Promise} promiseChain The promise chain.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} The promise chain with the transaction operation added.
	 */
	_startTx(promiseChain, ctx) {

		return promiseChain.then(
			() => {
				try {
					return ctx.transaction.start();
				} catch (err) {
					common.error('error starting transaction', err);
					return Promise.reject(err);
				}
			}
		);
	}

	/**
	 * Add transaction end to the operation execution promise chain.
	 *
	 * @private
	 * @param {Promise} promiseChain The promise chain.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} The promise chain with the transaction operation added.
	 */
	_endTx(promiseChain, ctx) {

		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				const rollbackAfterFailedCommit = err => {
					this._log('rolling back transaction after failed commit');
					this._dbDriver.rollbackTransaction(
						ctx.connection, {
							onSuccess() {
								reject(err);
							},
							onError(rollbackErr) {
								common.error(
									'error rolling transaction back after' +
										' failed commit', rollbackErr);
								reject(err);
							}
						}
					);
				};
				try {
					ctx.transaction.commit().then(
						() => {
							resolve();
						},
						err => {
							rollbackAfterFailedCommit(err);
						}
					);
				} catch (err) {
					common.error('error committing transaction', err);
					rollbackAfterFailedCommit(err);
				}
			}),
			err => new Promise((resolve, reject) => {
				if (ctx.rollbackOnError) {
					try {
						ctx.transaction.rollback().then(
							() => {
								reject(err);
							},
							rollbackErr => {
								common.error(
									'error rolling transaction back',
									rollbackErr);
								reject(err);
							}
						);
					} catch (rollbackErr) {
						common.error(
							'error rolling transaction back', rollbackErr);
						reject(err);
					}
				} else {
					reject(err);
				}
			})
		);
	}

	/**
	 * Replace parameter placeholders in the specified SQL statement with the
	 * corresponding values.
	 *
	 * @protected
	 * @param {string} stmt SQL statement text with parameter placeholders. Each
	 * placeholder has format "?{ref}" where "ref" is the parameter reference in
	 * the operation's records filter parameters handler.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context. The method uses context's
	 * [getParamSql()]{@link module:x2node-dbos~DBOExecutionContext#getParamSql}
	 * method to get values for the parameter placeholders.
	 * @returns {string} Ready to execute SQL statement with parameter
	 * placeholders replaced.
	 */
	_replaceParams(stmt, ctx) {

		let res = '';

		const re = new RegExp('(\'(?!\'))|(\')|\\?\\{([^}]+)\\}', 'g');
		let m, inLiteral = false, lastMatchIndex = 0;
		while ((m = re.exec(stmt)) !== null) {
			res += stmt.substring(lastMatchIndex, m.index);
			lastMatchIndex = re.lastIndex;
			const s = m[0];
			if (inLiteral) {
				res += s;
				if (m[1]) {
					inLiteral = false;
				} else if (m[2]) {
					re.lastIndex++;
				}
			} else {
				if (s === '\'') {
					res += s;
					inLiteral = true;
				} else {
					res += ctx.getParamSql(m[3]);
				}
			}
		}
		res += stmt.substring(lastMatchIndex);

		return res;
	}
}

// export the class
module.exports = AbstractDBO;
