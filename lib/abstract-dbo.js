'use strict';

const common = require('x2node-common');


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
	 */
	constructor(dbDriver) {

		/**
		 * Database driver.
		 *
		 * @protected
		 * @member {module:x2node-dbos.DBDriver} module:x2node-dbos~AbstractDBO#_dbDriver
		 */
		this._dbDriver = dbDriver;

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
	 * @param {function} paramsResolver Parameter values resolver function for
	 * the {@link module:x2node-dbos~AbstractDBO#_replaceParams} calls.
	 * @returns {Promise} The promise chain with the pre-statements added.
	 */
	_executePreStatements(promiseChain, ctx, paramsResolver) {

		let resPromise = promiseChain;

		this._preStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					let sql;
					try {
						sql = this._replaceParams(stmt, paramsResolver);
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
	 * @param {function} paramsResolver Parameter values resolver function for
	 * the {@link module:x2node-dbos~AbstractDBO#_replaceParams} calls.
	 * @returns {Promise} The promise chain with the post-statements added.
	 */
	_executePostStatements(promiseChain, ctx, paramsResolver) {

		let resPromise = promiseChain;

		this._postStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => this._executePostStatement(stmt, ctx, paramsResolver),
				err => this._executePostStatement(stmt, ctx, paramsResolver)
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
	 * @param {function} paramsResolver Parameter values resolver function.
	 * @returns {Promise} The promise of the statement execution result.
	 */
	_executePostStatement(stmt, ctx, paramsResolver) {

		return new Promise((resolve, reject) => {
			if (!ctx.executePostStatements)
				return resolve();
			let sql;
			try {
				sql = this._replaceParams(stmt, paramsResolver);
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
	 * @param {string} stmtText SQL statement text with parameter placeholders.
	 * Each placeholder has format "?{ref}" where "ref" is the parameter
	 * reference in the operation's records filter parameters handler.
	 * @param {function} paramsResolver Parameter values resolver function. The
	 * function receives parameter reference as its only argument and returns the
	 * parameter value SQL expression.
	 * @returns {string} Ready to execute SQL statement with parameter
	 * placeholders replaced.
	 */
	_replaceParams(stmtText, paramsResolver) {

		let res = '';

		const re = new RegExp('(\'(?!\'))|(\')|\\?\\{([^}]+)\\}', 'g');
		let m, inLiteral = false, lastMatchIndex = 0;
		while ((m = re.exec(stmtText)) !== null) {
			res += stmtText.substring(lastMatchIndex, m.index);
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
					res += paramsResolver(m[3]);
				}
			}
		}
		res += stmtText.substring(lastMatchIndex);

		return res;
	}
}

// export the class
module.exports = AbstractDBO;
