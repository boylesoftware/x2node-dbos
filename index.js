/**
 * Database query builder module.
 *
 * @module x2node-queries
 */
'use strict';

const common = require('x2node-common');

const QueryFactory = require('./lib/query-factory.js');


/**
 * The drivers registry.
 *
 * @private
 */
const DRIVERS = {
	'mysql': new (require('./lib/driver/mysql-driver.js'))(),
	'pg': new (require('./lib/driver/pg-driver.js'))()
};

/**
 * Create new factory using the specified database driver. Once created, the
 * factory instance can be used by the application throughout its life cycle.
 *
 * @param {string} dbDriverName Database driver name. Out of the box, two drivers
 * are available: "mysql" (for
 * {@link https://www.npmjs.com/package/mysql} and compatible others) and
 * "pg" (for {@link https://www.npmjs.com/package/pg}). Additional drivers can be
 * registered using {@link registerDriver} function before creating the factory.
 * @returns {module:x2node-queries~QueryFactory} Query factory instance.
 * @throws {module:x2node-common.X2UsageError} If the provided driver name is
 * invalid.
 */
exports.createQueryFactory = function(dbDriverName) {

	// lookup the driver
	const dbDriver = DRIVERS[dbDriverName];
	if (dbDriver === undefined)
		throw new common.X2UsageError(
			'Invalid database driver "' + dbDriverName + '".');

	// create and return the factory
	return new QueryFactory(dbDriver);
};

/**
 * Register custom database driver. After the driver is registered, a query
 * factory instance can be created using the driver.
 *
 * @param {string} dbDriverName Database driver name.
 * @param {module:x2node-queries.DBDriver} dbDriver Driver implementation.
 */
exports.registerDriver = function(dbDriverName, dbDriver) {

	DRIVERS[dbDriverName] = dbDriver;
};

// export basic DB driver to allow extending
exports.BasicDBDriver = require('./lib/driver/basic-driver.js');
