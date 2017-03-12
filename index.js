/**
 * Database query builder module.
 *
 * @module x2node-queries
 */
'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const QueryFactory = require('./lib/query-factory.js');
const placeholders = require('./lib/placeholders.js');
const ValueExpressionContext = require('./lib/value-expression-context.js');
const ValueExpression = require('./lib/value-expression.js');
const filterBuilder = require('./lib/filter-builder.js');
const orderBuilder = require('./lib/order-builder.js');


/////////////////////////////////////////////////////////////////////////////////
// Module
/////////////////////////////////////////////////////////////////////////////////

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

/**
 * Tell if the provided object is supported by the module. Currently, only a
 * record types library instance can be tested using this function and it tells
 * if the library was constructed with the queries extension.
 *
 * @param {*} obj Object to test.
 * @returns {boolean} <code>true</code> if supported by the queries module.
 */
exports.isSupported = function(obj) {

	return placeholders.isTagged(obj);
};


/////////////////////////////////////////////////////////////////////////////////
// Record Types Library Extension
/////////////////////////////////////////////////////////////////////////////////

// extend record types library
exports.extendRecordTypesLibrary = function(ctx, recordTypes) {

	// check if extended by the result set parser
	if (!rsparser.isSupported(recordTypes))
		throw new common.X2UsageError(
			'The library must be extended by the rsparser module first.');

	// tag the library
	if (placeholders.isTagged(recordTypes))
		throw new common.X2UsageError(
			'The library is already extended by the queries module.');
	placeholders.tag(recordTypes);

	// return it
	return recordTypes;
};

/**
 * Queries module specific
 * [RecordTypeDescriptor]{@link module:x2node-records~RecordTypeDescriptor}
 * extension.
 *
 * @mixin QueriesRecordTypeDescriptorExtension
 * @static
 */

// extend record type descriptors and property containers
exports.extendPropertiesContainer = function(ctx, container) {

	// process record type descriptor
	if ((container.nestedPath.length === 0) &&
		!container.definition.superRecordType) {

		// set the super type name symbol on the descriptor
		const recordTypeName = container.recordTypeName;
		const superTypeName = Symbol('$' + recordTypeName);
		container._superRecordTypeName = superTypeName;

		// create the super type after the library is complete
		ctx.onLibraryComplete(recordTypes => {

			// base supertype definition
			const superTypeDef = {
				superRecordType: true,
				properties: {
					'recordTypeName': {
						valueType: 'string',
						role: 'id'
					},
					'records': {
						valueType: 'ref(' + recordTypeName + ')[]',
						optional: false,
						implicitDependentRef: true
					},
					'count': {
						valueType: 'number',
						aggregate: {
							collection: 'records',
							valueExpr: container.idPropertyName + ' => count'
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
				// TODO: validate: must be aggregate of records or nested, or
				// view of records
			}

			// add the type
			ctx.addRecordType(superTypeName, superTypeDef);
		});

		// get the record type storage table
		container._table = (
			container.definition.table || container.recordTypeName);

		// add properties and methods to the descriptor:

		/**
		 * Super record type name.
		 *
		 * @member {Symbol} module:x2node-queries.QueriesRecordTypeDescriptorExtension#superRecordTypeName
		 * @readonly
		 */
		Object.defineProperty(container, 'superRecordTypeName', {
			get() { return this._superRecordTypeName; }
		});

		/**
		 * Top table used to store the record type's records.
		 *
		 * @member {Symbol} module:x2node-queries.QueriesRecordTypeDescriptorExtension#table
		 * @readonly
		 */
		Object.defineProperty(container, 'table', {
			get() { return this._table; }
		});
	}

	// return the container
	return container;
};

/**
 * Queries module specific
 * [PropertyDescriptor]{@link module:x2node-records~PropertyDescriptor}
 * extension.
 *
 * @mixin QueriesPropertyDescriptorExtension
 * @static
 */

/**
 * Get invalid property definition error.
 *
 * @private
 * @param {module:x2node-records~PropertyDescriptor} propDesc Property
 * descriptor.
 * @param {string} msg Error message.
 * @returns {module:x2node-common.X2UsageError} Error to throw.
 */
function invalidPropDef(propDesc, msg) {
	return new common.X2UsageError(
		'Property ' + propDesc.container.nestedPath + propDesc.name +
			' of record type ' + String(propDesc.container.recordTypeName) +
			' has invalid definition: ' + msg);
}

// extend property descriptors
exports.extendPropertyDescriptor = function(ctx, propDesc) {

	// add value expression context
	ctx.onLibraryComplete(() => {
		propDesc._valueExprContext = new ValueExpressionContext(
			propDesc.container.nestedPath + propDesc.name,
			propDesc.containerChain.concat(propDesc.nestedProperties));
	});

	// get property definition
	const propDef = propDesc.definition;

	// implicit dependent reference flag
	if (propDesc.isRef())
		propDesc._implicitDependentRef = propDef.implicitDependentRef;

	// check if stored property
	if (!propDef.valueExpr && !propDef.aggregate &&
		!propDef.reverseRefProperty && !propDef.implicitDependentRef) {

		// check if nested object
		if (propDesc.scalarValueType === 'object') {

			// validate the definition
			if (propDesc.column)
				throw invalidPropDef(
					propDesc, 'nested object property may not have a column' +
						' attribute.');

		} else { // not a nested object

			// get the storage column
			propDesc._column = (propDef.column || propDesc.name);
		}

		// get table and parent id column
		propDesc._table = propDef.table;
		propDesc._parentIdColumn = propDef.parentIdColumn;
		ctx.onLibraryValidation(() => {

			// must have a table if collection
			if (!propDesc.isScalar() && !propDesc.table)
				throw invalidPropDef(
					propDesc, 'must be stored in a separate table.');

			// must have parent id column if has table
			if (propDesc.table && !propDesc.parentIdColumn)
				throw invalidPropDef(
					propDesc, 'missing parentIdColumn attribute.');
		});
	}

	// validate id property
	if (propDesc.isId()) {
		ctx.onLibraryValidation(() => {
			if (propDesc.isCalculated())
				throw invalidPropDef(
					propDesc, 'id property may not be calculated.');
		});
	}

	// check if has reverse reference
	if (propDef.reverseRefProperty) {

		// validate the definition
		if (!propDesc.isRef() || propDef.table || propDef.column)
			throw invalidPropDef(
				propDesc, 'non-reference or stored property may not have' +
					' reverseRefProperty attribute.');
		if (!propDesc.container.isRecordType())
			throw invalidPropDef(
				propDesc, 'only top record type properties can have' +
					' reverseRefProperty attribute.');

		// save it on the descriptor and validate
		propDesc._reverseRefPropertyName = propDef.reverseRefProperty;
		ctx.onLibraryValidation(recordTypes => {
			const refTarget = recordTypes.getRecordTypeDesc(propDesc.refTarget);
			if (!refTarget.hasProperty(propDesc.reverseRefPropertyName))
				throw invalidPropDef(
					propDesc, 'reverse reference property ' +
						propDesc.reverseRefPropertyName +
						' does not exist in the reference target record type ' +
						propDesc.refTarget + '.');
			const revRefPropDesc = refTarget.getPropertyDesc(
				propDesc.reverseRefPropertyName);
			if (!revRefPropDesc.isRef() || !revRefPropDesc.isScalar() ||
				revRefPropDesc.isCalculated() ||
				revRefPropDesc.reverseRefPropertyName || /*TODO revRefPropDesc.table ||*/
				(revRefPropDesc.refTarget !== propDesc.container.recordTypeName))
				throw invalidPropDef(
					propDesc, 'reverse reference property does not match the' +
						' property definition.');
		});
	}

	// check if calculated value property
	if (propDef.valueExpr) {

		// validate property definition
		if (propDef.aggregate || propDef.table || propDef.column ||
			propDef.presenceTest || propDef.order || propDef.filter ||
			propDef.reverseRefProperty || !propDesc.isScalar() ||
			(propDesc.scalarValueType === 'object'))
			throw invalidPropDef(
				propDesc, 'conflicting calculated value property definition' +
					' attributes or invalid property value type or role.');

		// compile the property value expression
		propDesc._valueExpr = true; // mark as calculated right away
		ctx.onLibraryComplete(() => {
			try {
				propDesc._valueExpr = new ValueExpression(
					new ValueExpressionContext(
						propDesc.container.nestedPath, propDesc.containerChain),
					propDef.valueExpr
				);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid property value expression: ' +
							err.message);
				throw err;
			}
		});
	}

	// check if aggregate property
	if (propDef.aggregate) {

		// validate property definition
		if (propDef.table || propDef.column ||
			propDef.presenceTest || propDef.order || propDef.filter ||
			propDef.reverseRefProperty ||
			(!propDesc.isScalar() && !propDesc.isMap()) ||
			(propDesc.scalarValueType === 'object'))
			throw invalidPropDef(
				propDesc, 'conflicting aggregate property definition' +
					' attributes or invalid property value type or role.');

		// check if has needed attributes
		const aggColPath = propDef.aggregate.collection;
		const valueExprSpec = propDef.aggregate.valueExpr;
		if (!aggColPath || !valueExprSpec)
			throw invalidPropDef(
				propDesc, 'aggregate definition attribute must have collection' +
					' and valueExpr properties.');
		if (propDesc.isMap() && !propDesc.keyPropertyName)
			throw invalidPropDef(
				propDesc, 'missing keyPropertyName attribute.');

		// parse value expression
		const valueExprSpecParts = valueExprSpec.match(
				/^\s*([^=\s].*?)\s*=>\s*(count|sum|min|max|avg)\s*$/i);
		if (valueExprSpecParts === null)
			throw invalidPropDef(
				propDesc, 'invalid aggregated value expression syntax.');
		propDesc._valueExpr = true; // mark as calculated right away
		propDesc._aggregateFunc = valueExprSpecParts[2].toUpperCase();
		ctx.onLibraryComplete(recordTypes => {

			// build value expression context based on the aggregated collection
			const valueExprCtx = (
				new ValueExpressionContext(
					propDesc.container.nestedPath, propDesc.containerChain)
			).getRelativeContext(aggColPath);

			// save the aggregated property path
			propDesc._aggregatedPropPath = valueExprCtx.basePath;

			// compile value expression and add it to the descriptor
			try {
				propDesc._valueExpr = new ValueExpression(
					valueExprCtx, valueExprSpecParts[1]);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid aggregation expression: ' +
							err.message);
				throw err;
			}

			// add aggregated collection filter if any
			const aggColFilterSpec = propDef.aggregate.filter;
			if (aggColFilterSpec) {
				try {
					propDesc._filter = filterBuilder.buildFilter(
						recordTypes, valueExprCtx, [ ':and', aggColFilterSpec ]);
				} catch (err) {
					if (err instanceof common.X2UsageError)
						throw invalidPropDef(
							propDesc, 'invalid aggregated collection filter: ' +
								err.message);
					throw err;
				}
			}

			// set up aggregate map key
			if (propDesc.isMap()) {
				if (!propDesc.keyPropertyName)
					throw invalidPropDef(
						propDesc, 'missing keyPropertyName attribute.');
				const container = valueExprCtx.baseContainer;
				if (!container.hasProperty(propDesc.keyPropertyName))
					throw invalidPropDef(
						propDesc, 'invalid keyPropertyName attribute:' +
							' no such property.');
				const keyPropDesc = container.getPropertyDesc(
					propDesc.keyPropertyName);
				if (!keyPropDesc.isScalar() || keyPropDesc.isCalculated() ||
					keyPropDesc.table || keyPropDesc.reverseRefPropertyName ||
					(keyPropDesc.scalarValueType === 'object'))
					throw invalidPropDef(
						propDesc, 'key property ' + propDesc.keyPropertyName +
							' is not suitable to be a map key.');
				propDesc._keyValueType = keyPropDesc.scalarValueType;
				if (keyPropDesc.isRef())
					propDesc._keyRefTarget = keyPropDesc.refTarget;
			}
		});
	}

	// check if non-aggregate map
	if (propDesc.isMap() && !propDef.aggregate) {

		// get the key column
		if (propDef.keyColumn) {
			if (propDesc.keyPropertyName)
				throw invalidPropDef(
					propDesc, 'may not have both keyPropertyName and' +
						' keyColumn attriutes.');
			propDesc._keyColumn = propDef.keyColumn;
		}

		// validate the map key
		ctx.onLibraryValidation(() => {

			// must have key column or property
			if (!propDesc.keyPropertyName && !propDesc.keyColumn)
				throw invalidPropDef(propDesc, 'missing keyColumn attribute.');

			// validate key property
			if (propDesc.keyPropertyName) {
				const keyPropDesc = propDesc.nestedProperties.getPropertyDesc(
					propDesc.keyPropertyName);
				if (keyPropDesc.isCalculated() || keyPropDesc.table ||
					keyPropDesc.reverseRefPropertyName)
					throw invalidPropDef(
						propDesc, 'key property may not be calculated, stored' +
							' in its own table or have a reverse reference.');
			}
		});
	}

	// check if has a presense test
	if (propDef.presenceTest) {

		// validate property definition
		if (!propDesc.isScalar() || (propDesc.scalarValueType !== 'object') ||
			!propDesc.optional || propDef.table)
			throw invalidPropDef(
				propDesc, 'presence test may only be specified on an optional' +
					' scalar object property stored in the parent record\'s' +
					' table.');

		// parse the test specification
		ctx.onLibraryComplete(recordTypes => {
			try {
				propDesc._presenceTest = filterBuilder.buildFilter(
					recordTypes, propDesc.valueExprContext,
					[ ':and', propDef.presenceTest ]);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid presence test: ' + err.message);
				throw err;
			}
		});

	} else if ( // check if the presence test is required
		propDesc.isScalar() && (propDesc.scalarValueType === 'object') &&
			propDesc.optional && !propDef.table) {
		ctx.onLibraryValidation(() => {
			if (!propDesc.presenceTest)
				throw invalidPropDef(
					propDesc, 'optional scalar object property stored in the' +
						' parent record\'s table must have a presence test.');
		});
	}

	// check if has scoped filter
	if (propDef.filter) {

		// validate property definition
		if (propDesc.isScalar() || !propDesc.isView() || !propDesc.optional)
			throw invalidPropDef(
				propDesc, 'scoped filters are only allowed on non-scalar,' +
					' optional view properties.');

		// parse the filter
		ctx.onLibraryComplete(recordTypes => {
			try {
				propDesc._filter = filterBuilder.buildFilter(
					recordTypes, propDesc.valueExprContext,
					[ ':and', propDef.filter ]);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid scoped filter: ' + err.message);
				throw err;
			}
		});
	}

	// check if has scoped order
	if (propDef.order) {

		// validate property definition
		if (propDesc.isScalar())
			throw invalidPropDef(
				propDesc, 'scoped orders are only allowed on non-scalar' +
					' properties.');

		// parse the order
		ctx.onLibraryComplete(() => {
			try {
				propDesc._order = orderBuilder.buildOrder(
					propDesc.valueExprContext, propDef.order);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid scoped order: ' + err.message);
				throw err;
			}
		});
	}

	// determine fetch by default flag
	ctx.onLibraryComplete(() => {
		propDesc._fetchByDefault = ((
			(propDef.fetchByDefault === undefined) &&
				!propDesc.isView() &&
				!propDesc.isCalculated()
		) || propDef.fetchByDefault);
	});

	// add properties and methods to the descriptor:

	Object.defineProperty(propDesc, 'implicitDependentRef', {
		get() { return this._implicitDependentRef; }
	});
	Object.defineProperty(propDesc, 'fetchByDefault', {
		get() { return this._fetchByDefault; }
	});
	Object.defineProperty(propDesc, 'column', {
		get() { return this._column; }
	});
	Object.defineProperty(propDesc, 'table', {
		get() { return this._table; }
	});
	Object.defineProperty(propDesc, 'parentIdColumn', {
		get() { return this._parentIdColumn; }
	});
	Object.defineProperty(propDesc, 'keyColumn', {
		get() { return this._keyColumn; }
	});

	/**
	 * For a dependent record reference property, name of the property in the
	 * target record type that refers back to this record type. A dependent
	 * record reference property can only appear among the top record type
	 * properties (not in a nested object).
	 *
	 * @member {string} module:x2node-queries.QueriesPropertyDescriptorExtension#reverseRefPropertyName
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'reverseRefPropertyName', {
		get() { return this._reverseRefPropertyName; }
	});

	/**
	 * For a calculated value or aggregate property, the value expression. The
	 *  expression is based at the record type.
	 *
	 * @member {module:x2node-queries~ValueExpression} module:x2node-queries.QueriesPropertyDescriptorExtension#valueExpr
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'valueExpr', {
		get() { return this._valueExpr; }
	});

	/**
	 * Tell if this is a calculated value property, which includes properties
	 * with a value expression and aggregate properties. If a property is
	 * calculated, <code>valueExpr</code> descriptor property is also available.
	 *
	 * @function module:x2node-queries.QueriesPropertyDescriptorExtension#isAggregate
	 * @returns {boolean} <code>true</code> if aggregate property.
	 */
	propDesc.isCalculated = function() { return this._valueExpr; };

	/**
	 * For an aggregate property, the aggregation function, which may be "COUNT",
	 * "MAX", "MIN", "SUM" or "AVG".
	 *
	 * @member {string} module:x2node-queries.QueriesPropertyDescriptorExtension#aggregateFunc
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'aggregateFunc', {
		get() { return this._aggregateFunc; }
	});

	/**
	 * Tell if this is an aggregate property. If a property is an aggregate,
	 * <code>valueExpr</code>, <code>aggregatedPropPath</code> and
	 * <code>aggregateFunc</code> descriptor properties are also available.
	 *
	 * @function module:x2node-queries.QueriesPropertyDescriptorExtension#isAggregate
	 * @returns {boolean} <code>true</code> if aggregate property.
	 */
	propDesc.isAggregate = function() { return this._aggregateFunc; };

	/**
	 * For an aggregate property, path of the aggregated collection property
	 * starting from the record type.
	 *
	 * @member {string} module:x2node-queries.QueriesPropertyDescriptorExtension#aggregatedPropPath
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'aggregatedPropPath', {
		get() { return this._aggregatedPropPath; }
	});
	Object.defineProperty(propDesc, 'filter', {
		get() { return this._filter; }
	});
	Object.defineProperty(propDesc, 'presenceTest', {
		get() { return this._presenceTest; }
	});
	Object.defineProperty(propDesc, 'order', {
		get() { return this._order; }
	});
	Object.defineProperty(propDesc, 'valueExprContext', {
		get() { return this._valueExprContext; }
	});

	// return the descriptor
	return propDesc;
};
