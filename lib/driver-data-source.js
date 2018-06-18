'use strict';

const common = require('x2node-common');


/**
 * Data source implementation that uses the database driver.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DataSource
 */
class DBDriverDataSource {

	/**
	 * Create new data source.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver Database driver to use.
	 * @param {*} source Driver-specific database connections source.
	 */
	constructor(dbDriver, source) {

		this._dbDriver = dbDriver;
		this._source = source;

		this._log = common.getDebugLogger('X2_DBO');
	}

	// get connection
	getConnection() {

		const log = this._log;

		return new Promise((resolve, reject) => {
			this._dbDriver.connect(this._source, {
				onSuccess(connection) {
					log('acquired database connection');
					resolve(connection);
				},
				onError(err) {
					reject(err);
				}
			});
		});
	}

	// release connection
	releaseConnection(connection, err) {

		this._log((err ? 'destroying' : 'releasing') + ' database connection');

		this._dbDriver.releaseConnection(this._source, connection, err);
	}
}

// export the class
module.exports = DBDriverDataSource;
