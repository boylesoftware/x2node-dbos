'use strict';

const common = require('x2node-common');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');


// TODO: support polymorphs
// TODO: inserting into linked references table may update ref target record meta

/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class InsertDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor) {
		super(dbo, txOrCon, actor);
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
		super(dbDriver, recordTypes);

		// save the basics
		this._recordTypeDesc = recordTypeDesc;

		// the operation commands sequence
		this._commands = new Array();

		// create insert commands starting from the top record
		this._createInsertCommands(
			this._commands,
			recordTypeDesc.table,
			null, null, null, null,
			recordTypeDesc, record
		);
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

		// check if actor is required
		if (this._actorRequired && !actor)
			throw new common.X2UsageError('Operation may not be anonymous.');

		// create operation execution context
		const ctx = new InsertDBOExecutionContext(this, txOrCon, actor);

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
