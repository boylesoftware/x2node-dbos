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
	 * @param {?module:x2node-common.Actor} actor Actor executing the command.
	 * @param {Object.<string,*>} [filterParams] Optional filter parameters
	 * passed to the DBO execution.
	 * @throws {module:x2node-common.X2UsageError} If transaction was provided
	 * but it was not active.
	 */
	constructor(dbo, txOrCon, actor, filterParams) {

		// part of a transaction?
		const hasTx = (txOrCon instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !txOrCon.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// wrap in tx if no tx
		this._wrapInTx = !hasTx;

		// initial rollback on error flag
		this._rollbackOnError = !hasTx;

		// initial execute post-statements flag
		this._executePostStatements = false;

		// DBO parameters
		this._filterParams = filterParams;
		this._generatedParams = new Map();

		// members for subclasses:

		/**
		 * The DBO.
		 *
		 * @protected
		 * @member {module:x2node-dbos~AbstractDBO} module:x2node-dbos~DBOExecutionContext#_dbo
		 */
		this._dbo = dbo;

		/**
		 * The database driver.
		 *
		 * @protected
		 * @member {module:x2node-dbos.DBDriver}
		 */
		this._dbDriver = dbo._dbDriver;

		/**
		 * Record types library.
		 *
		 * @protected
		 * @member {module:x2node-records~RecordTypesLibrary}
		 */
		this._recordTypes = dbo._recordTypes;

		/**
		 * The transaction.
		 *
		 * @protected
		 * @member {module:x2node-dbos~Transaction} module:x2node-dbos~DBOExecutionContext#_transaction
		 */
		this._transaction = (
			hasTx ? txOrCon : new Transaction(dbo._dbDriver, txOrCon));

		/**
		 * The database connection.
		 *
		 * @protected
		 * @member {*} module:x2node-dbos~DBOExecutionContext#_connection
		 */
		this._connection = (hasTx ? txOrCon.connection: txOrCon);

		/**
		 * Actor executing the command, if any.
		 *
		 * @protected
		 * @member {module:x2node-common.Actor} module:x2node-dbos~DBOExecutionContext#_actor
		 */
		this._actor = actor;

		/**
		 * Date and time of the command execution (context creation to be
		 * precise).
		 *
		 * @protected
		 * @member {Date} module:x2node-dbos~DBOExecutionContext#_executedOn
		 */
		this._executedOn = new Date();
	}

	/**
	 * The database driver.
	 *
	 * @member {module:x2node-dbos.DBDriver}
	 * @readonly
	 */
	get dbDriver() { return this._dbDriver; }

	/**
	 * Log debug message on behalf of the DBO.
	 *
	 * @param {string} msg The message.
	 */
	log(msg) { this._dbo._log(msg); }

	/**
	 * The transaction.
	 *
	 * @member {module:x2node-dbos~Transaction}
	 * @readonly
	 */
	get transaction() { return this._transaction; }

	/**
	 * The database connection.
	 *
	 * @member {*}
	 * @readonly
	 */
	get connection() { return this._connection; }

	/**
	 * Actor executing the command, if any.
	 *
	 * @member {module:x2node-common.Actor}
	 * @readonly
	 */
	get actor() { return this._actor; }

	/**
	 * Date and time of the command execution.
	 *
	 * @member {Date}
	 * @readonly
	 */
	get executedOn() { return this._executedOn; }

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

	/**
	 * For DBOs where applicable, tells if post-statments must be executed even
	 * if the whole operation execution sequence is failing.
	 *
	 * @member {boolean}
	 */
	get executePostStatements() { return this._executePostStatements; }
	set executePostStatements(v) { this._executePostStatements = v; }

	/**
	 * Add generated parameter.
	 *
	 * @param {string} paramRef Parameter reference.
	 * @param {*} value Parameter ES value.
	 */
	addGeneratedParam(paramRef, value) {

		this._generatedParams.set(paramRef, value);
	}

	/**
	 * Get generated parameter value.
	 *
	 * @param {string} paramRef Parameter reference.
	 * @returns {*} Parameter ES value.
	 */
	getGeneratedParam(paramRef) {

		return this._generatedParams.get(paramRef);
	}

	/**
	 * Clear all generated parameters.
	 */
	clearGeneratedParams() {

		this._generatedParams.clear();
	}

	/**
	 * Get SQL value expression for the specified DBO parameter.
	 *
	 * @param {string} paramRef Parameter reference as used in the placeholder in
	 * the SQL statement.
	 * @return {string} Parameter SQL value expression.
	 * @throws {module:x2node-common.X2UsageError} If provided parameter is
	 * missing or its value is invalid (e.g. <code>NaN</code> value or value of
	 * unsupported type).
	 */
	getParamSql(paramRef) {

		if (paramRef === 'ctx.executedOn')
			return this._dbDriver.sql(this._executedOn.toISOString());
		if (paramRef === 'ctx.actor')
			return this._dbDriver.sql(this._actor ? this._actor.stamp : null);

		const generatedParam = this._generatedParams.get(paramRef);
		if (generatedParam !== undefined) {
			const generatedParamSql = this._dbDriver.sql(generatedParam);
			if ((generatedParamSql === null) || (generatedParamSql === 'NULL'))
				throw new common.X2UsageError(
					'Generated parameter "' + paramRef +
						'" has invalid value: ' + String(generatedParam) + '.');
			return generatedParamSql;
		}

		return this._dbo._paramsHandler.paramSql(
			this._dbDriver, this._filterParams, paramRef);
	}

	/**
	 * Convenience shortcut method to call the DBO's
	 * [_replaceParams()]{@link module:x2node-dbos~AbstractDBO#_replaceParams}
	 * method.
	 *
	 * @param {string} stmt SQL statement text with parameter placeholders.
	 * @returns {string} SQL statement with parameter placeholders replaced.
	 */
	replaceParams(stmt) {

		return this._dbo._replaceParams(stmt, this);
	}
}

// export the class
module.exports = DBOExecutionContext;
