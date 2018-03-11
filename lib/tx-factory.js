'use strict';

const common = require('x2node-common');

const Transaction = require('./transaction.js');


/**
 * Database transactions factory.
 *
 * @memberof module:x2node-dbos
 * @inner
 */
class TxFactory {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using DBO factory's
	 * [createTxFactory()]{@link module:x2node-dbos~DBOFactory#createTxFactory}
	 * method.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {module:x2node-dbos.DataSource} ds Connections data source.
	 */
	constructor(dbDriver, ds) {

		this._dbDriver = dbDriver;
		this._ds = ds;
	}

	/**
	 * Create and execute a database transaction.
	 *
	 * @param {function} cb Callback function with the transaction logic. The
	 * function is passed the [transaction]{@link module:x2node-dbos~Transaction}
	 * as its only argument. The transaction is started by the time the callback
	 * is called. The callback may return a <code>Promise</code>. If the promise
	 * is rejected or the callback throws an error, the transaction is
	 * automatically rolled back. Otherwise, the transaction is committed.
	 * @returns {Promise} The promise of the callback result. If the promise is
	 * successfully fulfilled, the transaction is committed. If an error happens
	 * and the transaction is rolled back, the promise is rejected with the
	 * error.
	 */
	executeTransaction(cb) {

		let dbCon, tx;
		return this._ds.getConnection(
		).then(
			con => (
				tx = new Transaction(this._dbDriver, dbCon = con)
			).start()
		).then(() => cb(
			tx
		)).then(result => tx.commit(
			result
		)).then(result => {
			this._ds.releaseConnection(dbCon);
			return result;
		}).catch(err => {
			if (tx && tx.isActive())
				return tx.rollbackAndReject(err).catch(() => {
					try {
						this._ds.releaseConnection(
							dbCon, (err instanceof Error ? err : undefined));
					} catch (releaseErr) {
						common.error(
							'error releasing database connection after' +
								` transaction #${tx.id} rollback`, releaseErr);
					}
					return Promise.reject(err);
				});
			try {
				this._ds.releaseConnection(
					dbCon, (err instanceof Error ? err : undefined));
			} catch (releaseErr) {
				common.error(
					'error releasing database connection', releaseErr);
			}
			return Promise.reject(err);
		});
	}
}

// export the class
module.exports = TxFactory;
