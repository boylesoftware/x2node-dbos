'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');


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

		function invalid() {
			new common.X2UsageError(
				'Invalid ' + (paramName ? '"' + paramName + '"' : '') +
					' filter test parameter value ' + String(paramValue) +
					'.');
		}

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
	 * execution call.
	 * @param {string} paramRef Parameter reference from the SQL.
	 * @returns {string} Parameter value SQL expression to include in the
	 * database statement.
	 */
	paramSql(dbDriver, filterParams, paramRef) {

		const ref = this._params.get(paramRef);
		const filterParam = filterParams[ref.name];
		if (filterParam === undefined)
			throw new common.X2UsageError(
				'Missing filter parameter "' + ref.name + '".');

		const process = v => this.paramValueToSql(
			dbDriver, v, ref.valueFunc, ref.name);
		return (
			Array.isArray(filterParam) ?
				filterParam.map(v => process(v)).join(', ') :
				process(filterParam)
		);
	}
}


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
 * @function module:x2node-dbos.DBOCommand#execute
 * @param {Promise} promiseChain DBO execution result promise chain.
 * @param {module:x2node-dbos~DBOExecutionContext ctx DBO execution context.
 * @returns {Promise} New result promise chain with the command queued up.
 */

/**
 * Property value generator command.
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
	execute(promiseChain, ctx) {

		const propDesc = this._propDesc;
		const propPath = propDesc.container.nestedPath + propDesc.name;
		return promiseChain.then(
			() => {
				try {

					const val = propDesc.generator(ctx.connection);

					if (val instanceof Promise)
						return val.then(
							resVal => {
								ctx.addGeneratedParam(
									propPath, rsparser.extractValue(
										propDesc.scalarValueType, resVal));
							},
							err => {
								common.error(
									'error generating property ' + propPath +
										' value', err);
								return Promise.reject(err);
							}
						);

					ctx.addGeneratedParam(propPath, rsparser.extractValue(
						propDesc.scalarValueType, val));

				} catch (err) {
					common.error(
						'error generating property ' + propPath + ' value', err);
					return Promise.reject(err);
				}
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Assigned id command.
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
				'No value provided for non-generated id property ' +
					propDesc.container.nestedPath + propDesc.name + '.');
	}

	// add command execution to the chain
	execute(promiseChain, ctx) {

		return promiseChain.then(
			() => {
				ctx.addGeneratedParam(
					this._propDesc.container.nestedPath + this._propDesc.name,
					rsparser.extractValue(
						this._propDesc.scalarValueType, this._idVal));
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Record meta-info properties command.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class RecordMetaInfoPropsCommand {

	/**
	 * Create new command.
	 *
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc Record
	 * type descriptor.
	 */
	constructor(recordTypeDesc) {

		this._actorRequired = false;
		this._actions = new Array();
		if (recordTypeDesc.getRecordMetaInfoPropName('version'))
			this._actions.push(ctx => {
				ctx.addGeneratedParam(
					recordTypeDesc.getRecordMetaInfoPropName('version'),
					rsparser.extractValue('number', 1));
			});
		if (recordTypeDesc.getRecordMetaInfoPropName('creationTimestamp'))
			this._actions.push(ctx => {
				ctx.addGeneratedParam(
					recordTypeDesc.getRecordMetaInfoPropName(
						'creationTimestamp'),
					rsparser.extractValue('datetime', ctx._executedOn));
			});
		if (recordTypeDesc.getRecordMetaInfoPropName('creationActor')) {
			this._actorRequired = true;
			this._actions.push(ctx => {
				const propName = recordTypeDesc.getRecordMetaInfoPropName(
					'creationActor');
				ctx.addGeneratedParam(
					propName, rsparser.extractValue(
						recordTypeDesc.getPropertyDesc(propName).scalarValueType,
						ctx._actor.stamp));
			});
		}
	}

	/**
	 * Tells if actor is required to execute the DBO.
	 *
	 * @member {boolean}
	 * @readonly
	 */
	get actorRequired() { return this._actorRequired; }

	// add command execution to the chain
	execute(promiseChain, ctx) {

		return promiseChain.then(
			() => {
				this._actions.forEach(action => { action(ctx); });
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * SQL <code>INSERT</code> statement command.
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
	 * Add column to the statement.
	 *
	 * @param {string} column Column name.
	 * @param {string} value Value SQL expression.
	 */
	add(column, value) {

		this._columns.push(column);
		this._values.push(value);
	}

	// add command execution to the chain
	execute(promiseChain, ctx) {

		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(this._sql);
					ctx.log('executing SQL: ' + sql);
					ctx.dbDriver.executeInsert(
						ctx.connection, sql, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									'error executing SQL [' + sql + ']', err);
								reject(err);
							}
						});
				} catch (err) {
					common.error(
						'error executing SQL [' + (sql || this._sql) + ']', err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}

	/**
	 * Statement SQL.
	 *
	 * @private
	 * @member {string}
	 * @readonly
	 */
	get _sql() {

		if (!this._sqlText) {
			this._sqlText = 'INSERT INTO ' + this._table + ' (' +
				this._columns.join(', ') + ') VALUES (' +
				this._values.join(', ') + ')';
			delete this._table;
			delete this._columns;
			delete this._values;
		}

		return this._sqlText;
	}
}

/**
 * SQL <code>INSERT</code> statement with auto-generated id command.
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
	 */
	constructor(table, propDesc) {
		super(table);

		this._propDesc = propDesc;
	}

	// add command execution to the chain
	execute(promiseChain, ctx) {

		const propDesc = this._propDesc;
		const propPath = propDesc.container.nestedPath + propDesc.name;
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(this._sql);
					ctx.log('executing SQL: ' + sql);
					ctx.dbDriver.executeInsert(
						ctx.connection, sql, {
							onSuccess(id) {
								ctx.addGeneratedParam(
									propPath, rsparser.extractValue(
										propDesc.scalarValueType, id));
								resolve();
							},
							onError(err) {
								common.error(
									'error executing SQL [' + sql + ']', err);
								reject(err);
							}
						}, propDesc.column);
				} catch (err) {
					common.error(
						'error executing SQL [' + (sql || this._sql) + ']', err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
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
	 */
	constructor(dbDriver, recordTypes) {

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

		/**
		 * SQL statements to execute before the main operation sequence.
		 *
		 * @protected
		 * @member {string} module:x2node-dbos~AbstractDBO#_preStatements
		 */
		this._preStatements = new Array();

		/**
		 * SQL statements to execute after the main operation sequence. The
		 * post-statements are executed even if the main sequence is failing if
		 * at least one pre-statement was successfully executed.
		 *
		 * @protected
		 * @member {string} module:x2node-dbos~AbstractDBO#_postStatements
		 */
		this._postStatements = new Array();
	}


	/////////////////////////////////////////////////////////////////////////////
	// DBO commands construction helper methods
	/////////////////////////////////////////////////////////////////////////////

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
	 * @param {module:x2node-records~PropertyDescriptor} [mapPropDesc] If
	 * commands are created for inserting nested objects into a nested object map
	 * property, then this is the map property descriptor.
	 * @param {string} [keyVal] Map key value if <code>mapPropDesc</code> is
	 * provided.
	 * @param {module:x2node-records~PropertiesContainer} container Top container
	 * for the table.
	 * @param {Object} data Object matching the container.
	 */
	_createInsertCommands(
		commands, table, parentIdColumn, parentIdPropPath, mapPropDesc, keyVal,
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
						table, idPropDesc);
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

		// add map key column
		if (mapPropDesc && mapPropDesc.keyColumn)
			insertCmd.add(
				mapPropDesc.keyColumn, this._makeMapKeySql(mapPropDesc, keyVal));

		// add record meta-info properties
		if (container.isRecordType()) {
			const cmd = new RecordMetaInfoPropsCommand(container);
			this._actorRequired = cmd.actorRequired;
			commands.push(cmd);
			[ 'version', 'creationTimestamp', 'creationActor' ].forEach(r => {
				let propName = container.getRecordMetaInfoPropName(r);
				if (propName)
					insertCmd.add(
						container.getPropertyDesc(propName).column,
						'?{' + propName + '}'
					);
			});
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
					propVal.forEach(v => {
						if ((v === undefined) || (v === null) ||
							((typeof v) !== 'object') || Array.isArray(v))
							new common.X2UsageError(
								'Invalid array element for property ' +
									container.nestedPath + propName +
									', expected an object.');
						this._createInsertCommands(
							commands, propDesc.table, propDesc.parentIdColumn,
							idPropPath, null, null, propDesc.nestedProperties,
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
							idPropPath, propDesc, key,
							propDesc.nestedProperties, v);
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
						propValSql.forEach(v => {
							const valInsert = new InsertCommand(propDesc.table);
							valInsert.add(propDesc.parentIdColumn, idSql);
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
	 * Validate value for a map property key and make SQL for it.
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
			'Invalid key value [' + String(key) + '] for map property ' +
				mapPropDesc.container.nestedPath + mapPropDesc.name + '.');

		if ((key === null) || (key === undefined))
			throw invalidKeyVal();

		const keySql = this._valueToSql(
			key, mapPropDesc.keyValueType, mapPropDesc.keyRefTarget,
			expectedType => new common.X2UsageError(
				'Invalid key value type [' + (typeof key) +
					'] for map property ' + mapPropDesc.container.nestedPath +
					mapPropDesc.name + ', expected [' + expectedType + '].')
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
	 * is invalid.
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
					valSql = this._dbDriver.stringLiteral(
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


	/////////////////////////////////////////////////////////////////////////////
	// DBO execution helper methods
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Add transaction start to the operation execution promise chain.
	 *
	 * @protected
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
	 * @protected
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
	 * Add execution of the pre-statements to the operation execution promise
	 * chain.
	 *
	 * @protected
	 * @param {Promise} promiseChain The promise chain.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} The promise chain with the pre-statements added.
	 */
	_executePreStatements(promiseChain, ctx) {

		let resPromise = promiseChain;

		this._preStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					let sql;
					try {
						sql = this._replaceParams(stmt, ctx);
						this._log('executing SQL: ' + sql);
						this._dbDriver.executeQuery(
							ctx.connection, sql, {
								onSuccess() {
									ctx.executePostStatements = true;
									resolve();
								},
								onError(err) {
									common.error(
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || stmt) + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		return resPromise;
	}

	/**
	 * Add execution of the post-statements to the operation execution promise
	 * chain.
	 *
	 * @protected
	 * @param {Promise} promiseChain The promise chain.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} The promise chain with the post-statements added.
	 */
	_executePostStatements(promiseChain, ctx) {

		let resPromise = promiseChain;

		this._postStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => this._executePostStatement(stmt, ctx),
				err => this._executePostStatement(stmt, ctx)
					.then(
						() => Promise.reject(err),
						() => Promise.reject(err)
					)
			);
		});

		return resPromise;
	}

	/**
	 * Execute a single post-statement.
	 *
	 * @private
	 * @param {string} stmt The statement.
	 * @param {module:x2node-dbos~DBOExecutionContext} ctx The operation
	 * execution context.
	 * @returns {Promise} The promise of the statement execution result.
	 */
	_executePostStatement(stmt, ctx) {

		return new Promise((resolve, reject) => {
			if (!ctx.executePostStatements)
				return resolve();
			let sql;
			try {
				sql = this._replaceParams(stmt, ctx);
				this._log('executing SQL: ' + sql);
				this._dbDriver.executeQuery(
					ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(
								'error executing SQL [' + sql + ']', err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error('error executing SQL [' + (sql || stmt) + ']', err);
				reject(err);
			}
		});
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
