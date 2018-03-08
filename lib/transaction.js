'use strict';

const common = require('x2node-common');


let gNextTxId = 1;

/**
 * Transaction.
 *
 * <p>Note, that errors thrown by the transaction event listeners are logged, but
 * otherwise are ignored. Also note that the listeners are invoked asynchronously
 * in a <code>process.nextTick()</code>. All that makes the event fired by the
 * transaction good for notifications but not for implementing the main
 * transaction logic.
 *
 * @memberof module:x2node-dbos
 * @inner
 * @fires module:x2node-dbos~Transaction#begin
 * @fires module:x2node-dbos~Transaction#commit
 * @fires module:x2node-dbos~Transaction#rollback
 */
class Transaction {

	/**
	 * Transaction start event. Fired upon successful transaction start.
	 *
	 * @event module:x2node-dbos~Transaction#begin
	 * @type {string}
	 */
	/**
	 * Transaction commit event. Fired upon successful transaction commit.
	 *
	 * @event module:x2node-dbos~Transaction#commit
	 * @type {string}
	 */
	/**
	 * Transaction rollback event. Fired upon transaction rollback, whether the
	 * rollback was successful or not. If the rollback was unsuccessful, the
	 * event listener receives the rollback error.
	 *
	 * @event module:x2node-dbos~Transaction#rollback
	 * @type {string}
	 */

	/**
	 * <strong>Note:</strong> The constructor is not available to the client
	 * code. Transaction instances are provided by the
	 * [TxFactory]{@link module:x2node-dbos~TxFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {*} connection DB driver specific connection object.
	 */
	constructor(dbDriver, connection) {

		if (!connection)
			throw new common.X2UsageError(
				'Database connection was not provided for the new transaction.');

		this._id = String(gNextTxId++);
		this._startedOn = null;

		this._dbDriver = dbDriver;
		this._connection = connection;

		this._log = common.getDebugLogger('X2_DBO');

		this._listeners = {};

		this._active = false;
		this._finished = false;
	}

	/**
	 * Transaction id unique for the process.
	 *
	 * @member {string}
	 * @readonly
	 */
	get id() { return this._id; }

	/**
	 * Timestamp when the transaction was started.
	 *
	 * @member {Date}
	 * @readonly
	 */
	get startedOn() { return this._startedOn; }

	/**
	 * Add listener for the specified transaction event.
	 *
	 * @param {string} eventName Event name. Can be "begin", "commit" or
	 * "rollback".
	 * @param {function} listener The listener function. The listener return
	 * values are ignored. If any listener throws an error, the error is logged
	 * but otherwise the process is not affected. The listeners are called
	 * asynchronously in a <code>process.nextTick()</code>.
	 * @returns {module:x2node-dbos~Transaction} This transaction for chaining.
	 */
	on(eventName, listener) {

		let listeners = this._listeners[eventName];
		if (!listeners)
			listeners = this._listeners[eventName] = new Array();

		if ((typeof listener) !== 'function')
			throw new common.X2UsageError('Listener is not a function.');

		listeners.push(listener);

		return this;
	}

	/**
	 * Call the event listeners.
	 *
	 * @protected
	 * @param {string} eventName The event name.
	 * @param {*} [arg] Optional argument to pass to the listeners.
	 */
	emit(eventName, arg) {

		const listeners = this._listeners[eventName];
		if (listeners)
			process.nextTick(() => {
				for (let listener of listeners) {
					try {
						listener(arg);
					} catch (err) {
						common.error(
							`error in transaction #${this._id} ${eventName}` +
								' event listener (ignoring it)', err);
					}
				}
			});
	}

	/**
	 * Start the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return on
	 * success. May not be a promise itself (use promise chaining for that).
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

		if (passThrough instanceof Promise)
			throw new common.X2UsageError(
				'Passthrough may not be a Promise.');

		this._active = true;

		const tx = this;
		return new Promise((resolve, reject) => {
			this._log(`(tx #${this._id}) starting transaction`);
			this._dbDriver.startTransaction(this._connection, {
				onSuccess() {
					this._startedOn = new Date();
					this.emit('begin');
					resolve(passThrough);
				},
				onError(err) {
					common.error('error starting transaction', err);
					tx._finished = true;
					tx._active = false;
					reject(err);
				}
			});
		});
	}

	/**
	 * Rollback the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return
	 * <em>on both success and failure.</em> May not be a promise itself (use
	 * promise chaining for that).
	 * @returns {Promise} Promise that is resolved with the
	 * <code>passThrough</code> value if the transaction rolled back successfully
	 * and is rejected <em>with the same <code>passThrough</code> value</em>
	 * in the case of failure. The rollback failure error is only logged.
	 * @throws {module:x2node-common.X2UsageError} If the transaction has not
	 * been started or if it has already finished.
	 */
	rollback(passThrough) {

		return this.rollbackInternal(passThrough, false);
	}

	/**
	 * Rollback the transaction and return promise that always rejects.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return
	 * <em>on both success and failure.</em> May not be a promise itself (use
	 * promise chaining for that).
	 * @returns {Promise} Promise that is rejected with the
	 * <code>passThrough</code> value if the transaction rolled back successfully
	 * and is rejected <em>with the same <code>passThrough</code> value</em>
	 * in the case of failure. The rollback failure error is only logged.
	 * @throws {module:x2node-common.X2UsageError} If the transaction has not
	 * been started or if it has already finished.
	 */
	rollbackAndReject(passThrough) {

		return this.rollbackInternal(passThrough, true);
	}

	/**
	 * Internal implementation of the rollback that can either resolve or reject
	 * the resulting promise upon rollback success.
	 *
	 * @private
	 */
	rollbackInternal(passThrough, rejectOnly) {

		if (this._finished)
			throw new common.X2UsageError(
				'The transaction is already complete.');

		if (!this._active)
			throw new common.X2UsageError(
				'The transaction has not been started.');

		if (passThrough instanceof Promise)
			throw new common.X2UsageError(
				'Passthrough may not be a Promise.');

		this._finished = true;
		this._active = false;

		return new Promise((resolve, reject) => {
			this._log(`(tx #${this._id}) rolling back transaction`);
			this._dbDriver.rollbackTransaction(this._connection, {
				onSuccess() {
					this.emit('rollback');
					if (rejectOnly)
						reject(passThrough);
					else
						resolve(passThrough);
				},
				onError(err) {
					common.error('error rolling transaction back', err);
					this.emit('rollback', err);
					reject(passThrough);
				}
			});
		});
	}

	/**
	 * Commit the transaction.
	 *
	 * @param {*} [passThrough] Arbitrary value to have the promise to return on
	 * success. May not be a promise itself (use promise chaining for that).
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

		if (passThrough instanceof Promise)
			throw new common.X2UsageError(
				'Passthrough may not be a Promise.');

		this._finished = true;
		this._active = false;

		return new Promise((resolve, reject) => {
			this._log(`(tx #${this._id}) committing transaction`);
			this._dbDriver.commitTransaction(this._connection, {
				onSuccess() {
					this.emit('commit');
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
	 * The database driver.
	 *
	 * @member {module:x2node-dbos.DBDriver}
	 * @readonly
	 */
	get dbDriver() { return this._dbDriver; }

	/**
	 * Underlying DB driver specific connection object associated with the
	 * transaction.
	 *
	 * @member {*}
	 * @readonly
	 */
	get connection() { return this._connection; }
}

// export the class
module.exports = Transaction;
