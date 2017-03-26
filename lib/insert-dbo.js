'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');


// TODO: support polymorphs

/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class InsertDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon) {
		super(dbo, txOrCon);

		this._generatedProps = new Map();
	}

	getGeneratedPropValue(propPath) {

		const p = this._generatedProps.get(propPath);

		return rsparser.extractValue(p.type, p.value);
	}

	setGeneratedProp(propPath, type, value) {

		this._generatedProps.set(propPath, {
			type: type,
			value: value
		});
	}

	replaceParams(stmtText) {

		return this._dbo._replaceParams(
			stmtText, paramRef => this._dbo._dbDriver.sql(
				this.getGeneratedPropValue(paramRef)));
	}
}


/**
 * Abstract operation sequence command.
 *
 * @private
 * @abstract
 */
class Command {

	constructor() {}
}

/**
 * Property value generator command.
 *
 * @private
 */
class GeneratorCommand extends Command {

	/**
	 * Create new command.
	 *
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Generated
	 * property descriptor.
	 */
	constructor(propDesc) {
		super();

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
								ctx.setGeneratedProp(
									propPath, propDesc.scalarValueType, resVal);
							},
							err => {
								common.error(
									'error generating property ' + propPath +
										' value', err);
								return Promise.reject(err);
							}
						);

					ctx.setGeneratedProp(
						propPath, propDesc.scalarValueType, val);

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
 */
class AssignedIdCommand extends Command {

	/**
	 * Create new command.
	 *
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Id property
	 * descriptor.
	 * @param {Object} data The object data.
	 */
	constructor(propDesc, data) {
		super();

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
				ctx.setGeneratedProp(
					this._propDesc.container.nestedPath + this._propDesc.name,
					this._propDesc.scalarValueType,
					this._idVal
				);
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * SQL <code>INSERT</code> statement command.
 *
 * @private
 */
class InsertCommand extends Command {

	/**
	 * Create new command.
	 *
	 * @param {string} table The table.
	 */
	constructor(table) {
		super();

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
								ctx.setGeneratedProp(
									propPath, propDesc.scalarValueType, id);
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
 * Insert database operation implementation (SQL <code>INSERT</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class InsertDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {Object} record The record data.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, recordTypeDesc, record) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;
		this._recordTypeDesc = recordTypeDesc;

		// the operation commands sequence
		this._commands = new Array();

		// add top insert command
		this._addInsert(
			recordTypeDesc.table,
			null, null, null, null,
			recordTypeDesc, record
		);
	}

	/**
	 * Add insert command to the commands sequence.
	 *
	 * @private
	 * @param {string} table The table.
	 * @param {?string} parentIdColumn Parent id column, or <code>null</code> for
	 * the top record type table.
	 * @param {string} [parentIdPropPath] If <code>parentIdColumn</code> is
	 * provided, this is the path to the id property in the parent container.
	 * @param {string} [keyColumn] Map key column, if any.
	 * @param {string} [keyVal] Map key value if <code>keyColumn</code> is
	 * provided.
	 * @param {module:x2node-records~PropertiesContainer} container Top container
	 * for the table.
	 * @param {Object} data Record matching the container.
	 */
	_addInsert(
		table, parentIdColumn, parentIdPropPath, keyColumn, keyVal,
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
					this._commands.push(new GeneratorCommand(idPropDesc));
					insertCmd = new InsertCommand(table);
					insertCmd.add(idPropDesc.column, '?{' + idPropPath + '}');
				}
			} else {
				this._commands.push(new AssignedIdCommand(idPropDesc, data));
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
		if (keyColumn)
			insertCmd.add(keyColumn, this._dbDriver.stringLiteral(keyVal));

		// process the container properties
		this._processContainer(
			this._commands.push(insertCmd) - 1, idPropPath, container, data);
	}

	/**
	 * Process record, add columns to the insert command and recursively add
	 * dependent inserts to the sequence.
	 *
	 * @private
	 * @param {number} insertCmdInd Index of the current insert command in the
	 * commands sequence.
	 * @param {string} idPropPath Property path of the id in the current insert
	 * command.
	 * @param {module:x2node-records~PropertiesContainer} container Container
	 * descriptor with the properties to add.
	 * @param {Object} data Record matching the container.
	 * @returns {number} The current insert command index in the sequence, which
	 * may have been changed by the method (generators could have been inserted
	 * before the command shifting it down the sequence).
	 */
	_processContainer(insertCmdInd, idPropPath, container, data) {

		// get the current insert command
		const insertCmd = this._commands[insertCmdInd];

		// go over the container properties
		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);

			// skip properties we don't need to insert
			if (propDesc.isCalculated() || propDesc.isId() ||
				propDesc.isView() || propDesc.reverseRefPropertyName)
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
						this._addInsert(
							propDesc.table, propDesc.parentIdColumn, idPropPath,
							null, null, propDesc.nestedProperties, v);
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
						this._addInsert(
							propDesc.table, propDesc.parentIdColumn, idPropPath,
							propDesc.keyColumn, key,
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
						this._addInsert(
							propDesc.table, propDesc.parentIdColumn, idPropPath,
							null, null, propDesc.nestedProperties, propVal);
					} else {
						insertCmdInd = this._processContainer(
							insertCmdInd, idPropPath, propDesc.nestedProperties,
							propVal);
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
					this._commands.splice(
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
							this._commands.push(valInsert);
						});
					} else if (propDesc.isMap()) {
						propValSql.forEach((v, k) => {
							const valInsert = new InsertCommand(propDesc.table);
							valInsert.add(propDesc.parentIdColumn, idSql);
							if (propDesc.keyColumn)
								valInsert.add(
									propDesc.keyColumn,
									this._dbDriver.stringLiteral(k)
								);
							valInsert.add(propDesc.column, v);
							this._commands.push(valInsert);
						});
					} else {
						const valInsert = new InsertCommand(propDesc.table);
						valInsert.add(propDesc.parentIdColumn, idSql);
						valInsert.add(propDesc.column, propValSql);
						this._commands.push(valInsert);
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
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Property
	 * descriptor.
	 * @param {*} val Property value.
	 * @returns {string} The value SQL.
	 */
	_makePropValSql(propDesc, val) {

		const invalidValType = expectedType => new common.X2UsageError(
			'Invalid value type [' + (typeof val) + '] for property ' +
				propDesc.container.nestedPath +  propDesc.name +
				', expected [' + expectedType + '].');
		const invalidVal = () => new common.X2UsageError(
			'Invalid value [' + String(val) + '] for property ' +
				propDesc.container.nestedPath +  propDesc.name + '.');

		if ((val === null) || (val === undefined))
			return this._dbDriver.sql(null);

		let valSql;
		let dateVal, hashInd, refTarget, refTargetDesc, refIdPropDesc;
		switch (propDesc.scalarValueType) {
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
			if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(val))
				throw invalidVal();
			dateVal = Date.parse(val);
			if (Number.isNaN(dateVal))
				throw invalidVal();
			valSql = this._dbDriver.stringLiteral(
				(new Date(dateVal)).toISOString());
			break;
		case 'ref':
			if ((typeof val) !== 'string')
				throw invalidValType('string');
			hashInd = val.indexOf('#');
			if ((hashInd <= 0) || (hashInd === val.length - 1))
				throw invalidVal();
			refTarget = val.substring(0, hashInd);
			if (refTarget !== propDesc.refTarget)
				throw invalidVal();
			refTargetDesc = this._recordTypes.getRecordTypeDesc(refTarget);
			refIdPropDesc = refTargetDesc.getPropertyDesc(
				refTargetDesc.idPropertyName);
			valSql = this._dbDriver.sql(
				refIdPropDesc.scalarValueType === 'number' ?
					Number(val.substring(hashInd + 1)) :
					val.substring(hashInd + 1)
			);
		}

		if (valSql === null)
			throw invalidVal();

		return valSql;
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {?module:x2node-common.Actor} actor Actor executing the DBO.
	 * @returns {Promise.<(string|number)>} Promise, which resolves to the new
	 * record id or is rejected with the error object of an error happens during
	 * the operation execution.
	 */
	execute(txOrCon, actor) {

		// create operation execution context
		const ctx = new InsertDBOExecutionContext(this, txOrCon);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up the commands
		this._commands.forEach(cmd => {
			resPromise = cmd.execute(resPromise, ctx);
		});

		// finish transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// resolve the execution promise chain
		resPromise = resPromise.then(
			() => ctx.getGeneratedPropValue(this._recordTypeDesc.idPropertyName),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}
}

// export the class
module.exports = InsertDBO;
