'use strict';

const common = require('x2node-common');


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
	}

	/**
	 * Add transaction start to the operation execution promise chain.
	 *
	 * @protected
	 * @param {Promise} promiseChain The promise chain.
	 * @param {Object} ctx The operation execution context.
	 * @returns {Promise} The promise chain with the transaction operation added.
	 */
	_startTx(promiseChain, ctx) {

		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				try {
					this._log('starting transaction');
					this._dbDriver.startTransaction(
						ctx.connection, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								ctx.rollbackOnError = false;
								common.error('error starting transaction', err);
								reject(err);
							}
						}
					);
				} catch (err) {
					common.error('error starting transaction', err);
					reject(err);
				}
			})
		);
	}

	/**
	 * Add transaction end to the operation execution promise chain.
	 *
	 * @protected
	 * @param {Promise} promiseChain The promise chain.
	 * @param {Object} ctx The operation execution context.
	 * @returns {Promise} The promise chain with the transaction operation added.
	 */
	_endTx(promiseChain, ctx) {

		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				try {
					this._log('committing transaction');
					this._dbDriver.commitTransaction(
						ctx.connection, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									'error committing transaction', err);
								this._log(
									'rolling back transaction after' +
										' failed commit');
								this._dbDriver.rollbackTransaction(
									ctx.connection, {
										onSuccess() {
											reject(err);
										},
										onError(rollbackErr) {
											common.error(
												'error rolling transaction' +
													' back after failed commit',
												rollbackErr);
											reject(err);
										}
									}
								);
							}
						}
					);
				} catch (err) {
					common.error('error committing transaction', err);
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
				}
			}),
			err => new Promise((resolve, reject) => {
				if (ctx.rollbackOnError) {
					try {
						this._log('rolling back transaction');
						this._dbDriver.rollbackTransaction(
							ctx.connection, {
								onSuccess() {
									reject(err);
								},
								onError(rollbackErr) {
									common.error(
										'error rolling transaction back',
										rollbackErr);
									reject(err);
								}
							}
						);
					} catch (rollbackErr) {
						common.error(
							'error rolling transaction back', rollbackErr);
						reject(err);
					}
				}
			})
		);
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
