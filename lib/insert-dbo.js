'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const Transaction = require('./transaction.js');
const AbstractDBO = require('./abstract-dbo.js');


/**
 * SQL <code>INSERT</code> statement builder.
 *
 * @private
 */
class InsertStatement {

	/**
	 * Create new statement builder.
	 *
	 * @param {string} table The table.
	 * @param {string} [generatedIdColumn] Auto-generated value id column, if any.
	 */
	constructor(table, generatedIdColumn) {

		this._table = table;
		this._generatedIdColumn = generatedIdColumn;
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

	/**
	 * Name of the auto-generated id column, if any.
	 *
	 * @member {string=}
	 * @readonly
	 */
	get generatedIdColumn() { return this._generatedIdColumn; }

	/**
	 * Statement SQL.
	 *
	 * @member {string}
	 * @readonly
	 */
	get text() {

		if (!this._text) {
			this._text = 'INSERT INTO ' + this._table + ' (' +
				this._columns.join(', ') + ') VALUES (' +
				this._values.join(', ') + ')';
			delete this._table;
			delete this._columns;
			delete this._values;
		}

		return this._text;
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

		// the statements sequence
		this._statements = new Array();

		// build top statement
		const idPropDesc = this._recordTypeDesc.getPropertyDesc(
			this._recordTypeDesc.idPropertyName);
		const stmt = new InsertStatement(
			this._recordTypeDesc.table, (
				idPropDesc.idStrategy === 'generated' ?
					idPropDesc.column : undefined
			));
		stmt.after = (ctx, id) => {
			ctx.lastId = {
				type: idPropDesc.scalarValueType,
				value: id
			};
		};
		this._statements.push(stmt);
		this._processContainer(stmt, this._recordTypeDesc, record);
	}

	_processContainer(stmt, container, data) {

		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.isCalculated())
				continue;
			if (propDesc.isId() && (propDesc.idStrategy === 'generated'))
				continue;
			const propVal = data[propName];
			if ((propVal === undefined) || (propVal === null)) {
				if (!propDesc.optional)
					throw new common.X2UsageError(
						'No value provided for required property ' +
							container.nestedPath + propName + '.');
				continue;
			}
			if (propDesc.table) {
				// TODO:...
			} else {
				stmt.add(propDesc.column, this._dbDriver.sql(propVal));
			}
		}
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} tx The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @returns {Promise.<(string|number)>} Promise, which resolves to the new
	 * record id or is rejected with the error object of an error happens during
	 * the operation execution.
	 */
	execute(tx) {

		// part of a transaction?
		const hasTx = (tx instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !tx.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// determine if needs to be wrapped in a transaction
		const wrapInTx = !hasTx;

		// create operation execution context
		const ctx = {
			connection: (hasTx ? tx.connection: tx),
			idStack: new Array(),
			lastId: null,
			rollbackOnError: wrapInTx
		};

		// create parameters resolver function
		const paramsResolver = paramRef => (
			paramRef === 'lastId' ?
				this._dbDriver.sql(
					rsparser.extractValue(ctx.lastId.type, ctx.lastId.value)) :
				undefined // should never happen
		);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up the inserts
		this._statements.forEach(stmt => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					let sql;
					try {
						if (stmt.before)
							stmt.before(ctx);
						sql = this._replaceParams(stmt.text, paramsResolver);
						this._log('executing SQL: ' + sql);
						this._dbDriver.executeInsert(
							ctx.connection, sql, {
								onSuccess(id) {
									if (stmt.after)
										stmt.after(ctx, id);
									resolve();
								},
								onError(err) {
									common.error(
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}, stmt.generatedIdColumn);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || stmt.text) + ']',
							err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// finish transaction if necessary
		if (wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// resolve the execution promise chain
		resPromise = resPromise.then(
			() => rsparser.extractValue(ctx.lastId.type, ctx.lastId.value),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}
}

// export the class
module.exports = InsertDBO;
