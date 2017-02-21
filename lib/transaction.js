'use strict';

const common = require('x2node-common');


/**
 * Transaction.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class Transaction {

	constructor(dbDriver, connection) {

		this._dbDriver = dbDriver;
		this._connection = connection;

		this._log = common.getDebugLogger('X2_QUERY');

		this._active = false;
		this._finished = false;
	}

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

	isActive() { return this._active; }

	get connection() { return this._connection; }
}

module.exports = Transaction;
