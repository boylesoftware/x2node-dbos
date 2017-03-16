'use strict';

const common = require('x2node-common');

const Transaction = require('./transaction.js');


/**
 * Base class for various DBO execution contexts.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class DBOExecutionContext {

	/**
	 * Create new context.
	 *
	 * @param {module:x2node-dbos~AbstractDBO} dbo The DBO.
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon Active transaction or
	 * database connection.
	 * @throws {module:x2node-common.X2UsageError} If transaction was provided
	 * but it was not active.
	 */
	constructor(dbo, txOrCon) {

		// part of a transaction?
		const hasTx = (txOrCon instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !txOrCon.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// wrap in tx if no tx
		this._wrapInTx = !hasTx;

		// initial rollback on error flag
		this._rollbackOnError = !hasTx;

		// members for subclasses:

		/**
		 * The DBO.
		 *
		 * @protected
		 * @member {module:x2node-dbos~AbstractDBO} module:x2node-dbos~DBOExecutionContext#_dbo
		 */
		this._dbo = dbo;

		/**
		 * The database connection.
		 *
		 * @protected
		 * @member {*} module:x2node-dbos~DBOExecutionContext#_connection
		 */
		this._connection = (hasTx ? txOrCon.connection: txOrCon);
	}

	/**
	 * The database driver.
	 *
	 * @member {module:x2node-dbos.DBDriver}
	 * @readonly
	 */
	get dbDriver() { return this._dbo._dbDriver; }

	/**
	 * Log debug message on behalf of the DBO.
	 *
	 * @param {string} msg The message.
	 */
	log(msg) { this._dbo._log(msg); }

	/**
	 * The database connection.
	 *
	 * @member {*}
	 * @readonly
	 */
	get connection() { return this._connection; }

	/**
	 * Tells if the operation needs to be wrapped in a transaction by the DBO.
	 *
	 * @member {boolean}
	 * @readonly
	 */
	get wrapInTx() { return this._wrapInTx; }

	/**
	 * Tells if the transaction needs to be rolled back by the DBO upon error.
	 *
	 * @member {boolean}
	 */
	get rollbackOnError() { return this._rollbackOnError; }
	set rollbackOnError(v) { this._rollbackOnError = v; }
}

// export the class
module.exports = DBOExecutionContext;
