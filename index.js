/**
 * Database query builder module.
 *
 * @module x2node-queries
 */
'use strict';

const common = require('x2node-common');

const QueryFactory = require('./lib/query-factory.js');
const placeholders = require('./lib/placeholders.js');


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

// export placeholders functions
exports.param = placeholders.param;
exports.isParam = placeholders.isParam;
exports.expr = placeholders.expr;
exports.isExpr = placeholders.isExpr;

// record types library extension:

const SUPERTYPE_NAME = Symbol('superRecordTypeName');

exports.extendRecordTypesLibrary = function(ctx, recordTypes) {

	// tag the library
	placeholders.tag(recordTypes);

	// return it
	return recordTypes;
};

exports.extendPropertiesContainer = function(ctx, container) {

	// create supertype for record type descriptor
	if ((container.nestedPath.length === 0) &&
		!container.definition[SUPERTYPE_NAME]) {

		// set the super type name symbol on the descriptor
		const recordTypeName = container.recordTypeName;
		const superTypeName = Symbol('$' + recordTypeName);
		container[SUPERTYPE_NAME] = superTypeName;
		Object.defineProperty(container, 'superRecordTypeName', {
			get() { return this[SUPERTYPE_NAME]; }
		});

		// create the super type after the library is complete
		ctx.onLibraryComplete(recordTypes => {

			// base supertype definition
			const superTypeDef = {
				[SUPERTYPE_NAME]: superTypeName,
				properties: {
					'recordTypeName': {
						valueType: 'string',
						role: 'id',
						valueExpr: '\'' + recordTypeName + '\''
					},
					'records': {
						valueType: '[ref(' + recordTypeName + ')]',
						optional: false
					},
					'count': {
						valueType: 'number',
						aggregate: {
							collection: 'records',
							valueExpr: 'id => count'
						}
					}
				}
			};

			// complete the supertype definition with super-properties
			const recordTypeDesc = recordTypes.getRecordTypeDesc(recordTypeName);
			const recordTypeDef = recordTypeDesc.definition;
			for (let superPropName in recordTypeDef.superProperties) {
				const superPropDef = recordTypeDef.superProperties[
					superPropName];
				if (superTypeDef.properties[superPropName])
					throw new common.X2UsageError(
						'Invalid record type ' + recordTypeName +
							' definition: super property name "' +
							superPropName + '" is reserved.');
				const superTypePropDef = Object.create(superPropDef);
				superTypeDef.properties[superPropName] = superTypePropDef;
			}

			// add the type
			ctx.addRecordType(superTypeName, superTypeDef);
		});
	}

	// return the container
	return container;
};

exports.extendPropertyDescriptor = function(ctx, propDesc) {

	//...

	// return the descriptor
	return propDesc;
};
