'use strict';

const common = require('x2node-common');


/**
 * Transaction.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class Transaction {

	/**
	 * <b>The constructor is not available to the client code. Transaction
	 * instances are created by the DBO factory's
	 * [newTransaction]{@link module:x2node-queries~DBOFactory#newTransaction}
	 * method.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver DB driver.
	 * @param {*} connection DB driver specific connection object.
	 */
	constructor(dbDriver, connection) {

		this._dbDriver = dbDriver;
		this._connection = connection;

		this._log = common.getDebugLogger('X2_DBO');

		this._active = false;
		this._finished = false;
	}

	/**
	 * Start the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return on
	 * success.
	 * @returns {Promise} Promise that is resolved with the
	 * <code>passThrough</code> value if the transaction started successfully and
	 * is rejected with an error object in the case of failure.
	 * @throws {module:x2node-common.X2UsageError} If the transaction is already
	 * in progress or if it has already finished.
	 */
	start(passThrough) {

		if (this._finished)
			throw new common.X2UsageError(
				'The transaction is already complete.');

		if (this._active)
			throw new common.X2UsageError(
				'The transaction is already active.');

		this._active = true;

		return new Promise((resolve, reject) => {
			this._log('starting transaction');
			this._dbDriver.startTransaction(this._connection, {
				onSuccess() {
					resolve(passThrough);
				},
				onError(err) {
					common.error('error starting transaction', err);
					this._finished = true;
					this._active = false;
					reject(err);
				}
			});
		});
	}

	/**
	 * Rollback the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return on
	 * success.
	 * @returns {Promise} Promise that is resolved with the
	 * <code>passThrough</code> value if the transaction rolled back successfully
	 * and is rejected with an error object in the case of failure.
	 * @throws {module:x2node-common.X2UsageError} If the transaction has not
	 * been started or if it has already finished.
	 */
	rollback(passThrough) {

		if (this._finished)
			throw new common.X2UsageError(
				'The transaction is already complete.');

		if (!this._active)
			throw new common.X2UsageError(
				'The transaction has not been started.');

		this._finished = true;
		this._active = false;

		return new Promise((resolve, reject) => {
			this._log('rolling back transaction');
			this._dbDriver.rollbackTransaction(this._connection, {
				onSuccess() {
					resolve(passThrough);
				},
				onError(err) {
					common.error('error rolling transaction back', err);
					reject(err);
				}
			});
		});
	}

	/**
	 * Commit the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return on
	 * success.
	 * @returns {Promise} Promise that is resolved with the
	 * <code>passThrough</code> value if the transaction committed successfully
	 * and is rejected with an error object in the case of failure.
	 * @throws {module:x2node-common.X2UsageError} If the transaction has not
	 * been started or if it has already finished.
	 */
	commit(passThrough) {

		if (this._finished)
			throw new common.X2UsageError(
				'The transaction is already complete.');

		if (!this._active)
			throw new common.X2UsageError(
				'The transaction has not been started.');

		this._finished = true;
		this._active = false;

		return new Promise((resolve, reject) => {
			this._log('committing transaction');
			this._dbDriver.commitTransaction(this._connection, {
				onSuccess() {
					resolve(passThrough);
				},
				onError(err) {
					common.error('error committing transaction', err);
					reject(err);
				}
			});
		});
	}

	/**
	 * Tell if the transaction is started (and not finished).
	 *
	 * @returns {boolean} <code>true</code> if active transaction.
	 */
	isActive() { return this._active; }

	/**
	 * Underlying DB driver specific connection object associated with the
	 * transaction.
	 *
	 * @type {*}
	 * @readonly
	 */
	get connection() { return this._connection; }
}

// export the class
module.exports = Transaction;
