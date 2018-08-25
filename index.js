/**
 * Database operations module.
 *
 * @module x2node-dbos
 * @requires module:x2node-common
 * @requires module:x2node-records
 * @requires module:x2node-pointers
 * @requires module:x2node-patches
 * @requires module:x2node-validators
 * @requires module:x2node-rsparser
 * @implements {module:x2node-records.Extension}
 */
'use strict';

const common = require('x2node-common');
const validators = require('x2node-validators');
const rsparser = require('x2node-rsparser');

const DBOFactory = require('./lib/dbo-factory.js');
const placeholders = require('./lib/placeholders.js');
const ValueExpressionContext = require('./lib/value-expression-context.js');
const ValueExpression = require('./lib/value-expression.js');
const filterBuilder = require('./lib/filter-builder.js');
const orderBuilder = require('./lib/order-builder.js');


/////////////////////////////////////////////////////////////////////////////////
// Module
/////////////////////////////////////////////////////////////////////////////////

/**
 * Built-in database drivers.
 *
 * @private
 * @constant {Object.<string,function>}
 */
const DRIVERS = {
	'mysql': require('./lib/driver/mysql-driver.js'),
	'pg': require('./lib/driver/pg-driver.js')
};

/**
 * Create new database operations (DBO) factory using the specified database
 * driver and the record types library. Once created, the factory instance can be
 * used by the application throughout its life cycle.
 *
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library. The factory builds operations against this library. The library must
 * be extended with the <code>x2node-dbos</code> module.
 * @param {(string|module:x2node-dbos.DBDriver)} dbDriver Either a built-in
 * database driver name, or a custom database driver implementation. Available
 * built-in drivers include: "mysql" (for
 * {@link https://www.npmjs.com/package/mysql} and compatible others) and "pg"
 * (for {@link https://www.npmjs.com/package/pg}).
 * @param {Object.<string,*>} [options] Options. If provided, the options are
 * passed to the DB driver constructor.
 * @returns {module:x2node-dbos~DBOFactory} DBO factory instance.
 * @throws {module:x2node-common.X2UsageError} If the provided driver name is
 * invalid.
 */
exports.createDBOFactory = function(recordTypes, dbDriver, options) {

	// make sure that the record types library is compatible
	if (!placeholders.isTagged(recordTypes))
		throw new common.X2UsageError(
			'Record types library does not have the DBOs extension.');

	// get driver instance
	let dbDriverInst;
	if ((typeof dbDriver) === 'string') {
		if (!DRIVERS[dbDriver])
			throw new common.X2UsageError(
				`Invalid built-in database driver "${dbDriver}".`);
		dbDriverInst = new DRIVERS[dbDriver](options);
	} else { // custom driver
		dbDriverInst = dbDriver;
	}

	// create and return the factory
	return new DBOFactory(dbDriverInst, recordTypes);
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
 * if the library was constructed with the DBOs extension.
 *
 * @param {*} obj Object to test.
 * @returns {boolean} <code>true</code> if supported by the DBOs module.
 */
exports.isSupported = function(obj) {

	return placeholders.isTagged(obj);
};


/////////////////////////////////////////////////////////////////////////////////
// Record Types Library Extension
/////////////////////////////////////////////////////////////////////////////////

/**
 * Default id generator property on the library construction context.
 *
 * @private
 * @constant {Symbol}
 */
const DEFAULT_IDGEN = Symbol('DEFAULT_IDGEN');

// requires rsparser extension
exports.requiredExtensions = [ validators, rsparser ];

// extend record types library
exports.extendRecordTypesLibrary = function(ctx, recordTypes) {

	// check if extended by the result set parser
	if (!rsparser.isSupported(recordTypes))
		throw new common.X2UsageError(
			'The library must be extended by the RSParser module first.');

	// tag the library
	if (placeholders.isTagged(recordTypes))
		throw new common.X2UsageError(
			'The library is already extended by the DBOs module.');
	placeholders.tag(recordTypes);

	// save default id generator on the context
	ctx[DEFAULT_IDGEN] = (recordTypes.definition.defaultIdGenerator || 'auto');

	// return it
	return recordTypes;
};

/**
 * DBOs module specific
 * [RecordTypeDescriptor]{@link module:x2node-records~RecordTypeDescriptor}
 * extension.
 *
 * @mixin RecordTypeDescriptorWithDBOs
 * @static
 */

/**
 * DBOs module specific
 * [RecordTypeDescriptor]{@link module:x2node-records~PropertiesContainer}
 * extension.
 *
 * @mixin PropertiesContainerWithDBOs
 * @static
 */

/**
 * Implicit dependent reference marker on a property definition.
 *
 * @private
 * @constant {Symbol}
 */
const IMPLICIT_DEP_REF = Symbol('IMPLICIT_DEP_REF');

/**
 * Super record type definition marker.
 *
 * @private
 * @constant {Symbol}
 */
const SUPER_RECORD_TYPE = Symbol('SUPER_RECORD_TYPE');

// extend record type descriptors and property containers
exports.extendPropertiesContainer = function(ctx, container) {

	// process record type descriptor
	if ((container.nestedPath.length === 0) &&
		!container.definition[SUPER_RECORD_TYPE]) {

		// find record meta-info properties
		container._recordMetaInfoPropNames = {};
		ctx.onContainerComplete(container => {
			container.allPropertyNames.forEach(propName => {
				const propDesc = container.getPropertyDesc(propName);
				if (propDesc.isRecordMetaInfo()) {
					const role = propDesc.recordMetaInfoRole;
					if (container._recordMetaInfoPropNames[role] !== undefined)
						throw new common.X2UsageError(
							'Record type ' + String(container.recordTypeName) +
								' has more than one ' + role +
								' record meta-info property.');
					container._recordMetaInfoPropNames[role] = propName;
				}
			});
		});

		// set the super type name symbol on the descriptor
		const recordTypeName = container.recordTypeName;
		const superTypeName = Symbol('$' + recordTypeName);
		container._superRecordTypeName = superTypeName;

		// create the super type after the library is complete
		ctx.onLibraryComplete(recordTypes => {

			// base supertype definition
			const superTypeDef = {
				[SUPER_RECORD_TYPE]: true,
				properties: {
					'recordTypeName': {
						valueType: 'string',
						role: 'id',
						generator: null
					},
					'records': {
						valueType: 'ref(' + recordTypeName + ')[]',
						optional: false,
						[IMPLICIT_DEP_REF]: true
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
		 * @member {Symbol} module:x2node-dbos.RecordTypeDescriptorWithDBOs#superRecordTypeName
		 * @readonly
		 */
		Object.defineProperty(container, 'superRecordTypeName', {
			get() { return this._superRecordTypeName; }
		});

		/**
		 * Top table used to store the record type's records.
		 *
		 * @member {string} module:x2node-dbos.RecordTypeDescriptorWithDBOs#table
		 * @readonly
		 */
		Object.defineProperty(container, 'table', {
			get() { return this._table; }
		});

		/**
		 * Get name of the property for the specified record meta-info role.
		 *
		 * @function module:x2node-dbos.RecordTypeDescriptorWithDBOs#getRecordMetaInfoPropName
		 * @param {string} role Record meta-info role: "version",
		 * "creationTimestamp", "creationActor", "modificationTimestamp" or
		 * "modificationActor".
		 * @returns {string} The property name, or <code>undefined</code> if
		 * none.
		 */
		container.getRecordMetaInfoPropName = function(role) {
			return this._recordMetaInfoPropNames[role];
		};
	}

	// complete polymorphic object container descriptor
	ctx.onContainerComplete(container => {
		if (container.isPolymorphObject() &&
			container.hasProperty(container.typePropertyName) &&
			((typeof container.definition.typeColumn) === 'string')) {
			container.getPropertyDesc(container.typePropertyName)._column =
				container.definition.typeColumn;
		}
	});

	// return the container
	return container;
};

/**
 * DBOs module specific
 * [PropertyDescriptor]{@link module:x2node-records~PropertyDescriptor}
 * extension.
 *
 * @mixin PropertyDescriptorWithDBOs
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

/* JUST AN IDEA FOR THE FUTURE:
const PROPDESC_VALIDATORS = [
	{
		description: 'implicit dependent reference property',
		selector: {
			implicitDependentRef: true
		},
		requirements: [
			{
				description: 'must be a reference',
				test: {
					isRef: true
				}
			},
			{
				description: 'may not be stored in table/column',
				test: {
					column: undefined,
					table: undefined
				}
			}
		]
	},
	{
		selector: {
			isCalculated: false,
			reverseRefPropertyName: undefined,
			implicitDependentRef: false
		},
		requirements: [
			{
				descriptior: 'must be stored in a column',
				test: {
					column: not(undefined)
				}
			}
		]
	}
];*/

/**
 * Record meta-info property roles.
 *
 * @private
 * @type {Set.<string>}
 */
const RECORD_METAINFO_ROLES = new Set([
	'version',
	'creationTimestamp', 'creationActor',
	'modificationTimestamp', 'modificationActor'
]);

// extend property descriptors
exports.extendPropertyDescriptor = function(ctx, propDesc) {

	// restrict property name
	if (!(/^[a-z_$][a-z_$0-9]*$/i).test(propDesc.name))
		throw invalidPropDef(
			propDesc, 'illegal characters in the property name.');

	// add value expression context
	ctx.onLibraryComplete(() => {
		propDesc._valueExprContext = new ValueExpressionContext(
			propDesc.container.nestedPath + propDesc.name,
			propDesc.containerChain.concat(propDesc.nestedProperties));
	});

	// process property definition:

	// get property definition
	const propDef = propDesc.definition;

	// implicit dependent reference flag
	propDesc._implicitDependentRef = false;
	if (propDef[IMPLICIT_DEP_REF]) {

		// validate the definition
		if (!propDesc.isRef() || propDef.reverseRefProperty)
			throw invalidPropDef(
				propDesc, 'only a reference may be marked as'+
					' implicit dependent ref and it may not combine with' +
					' reverseRefProperty attribute.');

		// store the flag on the descriptor
		propDesc._implicitDependentRef = true;
	}

	// get generator
	if (propDef.generator !== undefined) {

		// store the generator on the descriptor
		propDesc._generator = propDef.generator;

	} else if (propDesc.isId()) { // default generator for id property
		propDesc._generator = ctx[DEFAULT_IDGEN];
	}

	// set generated flag
	propDesc._isGenerated = (
		(propDesc._generator !== undefined) && (propDesc._generator !== null));

	// set id table-wide uniqueness
	if (propDesc.isId())
		propDesc._tableUnique = (
			(propDef.tableUnique === undefined) ||
				(propDef.tableUnique ? true : false));

	// check if record meta-info property
	if (RECORD_METAINFO_ROLES.has(propDef.role)) {
		propDesc._isRecordMetaInfo = true;
		propDesc._recordMetaInfoRole = propDef.role;
	}

	// get stored property parameters
	if (!propDef.valueExpr && !propDef.aggregate &&
		!propDef.reverseRefProperty && !propDef[IMPLICIT_DEP_REF] &&
		!propDesc.isPolymorphObjectType()) {

		// check if nested object
		if (propDesc.scalarValueType === 'object') {

			// may not have column attribute
			if (propDef.column)
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

		// update default optionality for some meta-info properties
		if (propDesc._isRecordMetaInfo &&
			propDesc._recordMetaInfoRole.startsWith('modification') &&
			(propDef.optional === undefined))
			propDesc._optional = true;

		// update default modifiability for meta-info properties
		if (propDesc._isRecordMetaInfo && (propDef.modifiable === undefined))
			propDesc._modifiable = false;

	} else {

		// may not have explicit modifiability
		if (propDef.modifiable)
			throw invalidPropDef(propDesc, 'may not be modifiable.');

		// set default modifiability
		propDesc._modifiable = false;
	}

	// check if dependent reference
	if ((typeof propDef.reverseRefProperty) === 'string') {

		// validate the definition
		if (!propDesc.isRef())
			throw invalidPropDef(
				propDesc, 'non-reference property may not have' +
					' reverseRefProperty attribute.');

		// save reverse reference info on the descriptor
		propDesc._reverseRefPropertyName = propDef.reverseRefProperty;
		propDesc._weakDependency = (propDef.weakDependency ? true : false);
		if (!propDesc._weakDependency) {
			if (!propDesc.container._dependentRecordTypes)
				propDesc.container._dependentRecordTypes = new Set();
			propDesc.container._dependentRecordTypes.add(propDesc.refTarget);
		}
	}

	// check if calculated value property
	if ((typeof propDef.valueExpr) === 'string') {

		// validate property definition
		if (propDef.aggregate || !propDesc.isScalar())
			throw invalidPropDef(
				propDesc, 'calculated property may not be aggregate or' +
					' non-scalar.');

		// compile the property value expression
		propDesc._isCalculated = true; // mark as calculated right away
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
		if (propDesc.isArray())
			throw invalidPropDef(
				propDesc, 'aggregate property may not be an array.');

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
		propDesc._isCalculated = true; // mark as calculated right away
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
				const keyPropNameParts = propDesc.keyPropertyName.split('.');
				let container = valueExprCtx.baseContainer;
				let keyPropDesc;
				for (let propName of keyPropNameParts) {
					if (!container)
						throw invalidPropDef(
							propDesc, 'key property path ' +
								propDesc.keyPropertyName +
								' includes elements without nested properties.');
					if (!container.hasProperty(propName))
						throw invalidPropDef(
							propDesc, 'invalid keyPropertyName attribute:' +
								' no such property.');
					keyPropDesc = container.getPropertyDesc(propName);
					if (!keyPropDesc.isScalar() || keyPropDesc.isCalculated() ||
						keyPropDesc.reverseRefPropertyName)
						throw invalidPropDef(
							propDesc, 'key property ' +
								propDesc.keyPropertyName +
								' is not scalar, calculated or dependent.');
					container = keyPropDesc.nestedProperties;
				}
				if (keyPropDesc.table ||
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

	// check if array
	if (propDesc.isArray()) {

		// get index column
		if ((typeof propDef.indexColumn) === 'string')
			propDesc._indexColumn = propDef.indexColumn;
	}

	// check if non-aggregate map
	if (propDesc.isMap() && !propDef.aggregate) {

		// get the key column
		if ((typeof propDef.keyColumn) === 'string') {
			if (propDesc.keyPropertyName)
				throw invalidPropDef(
					propDesc, 'may not have both keyPropertyName and' +
						' keyColumn attriutes.');
			propDesc._keyColumn = propDef.keyColumn;
		}
	}

	// add default presence test for polymorphic object subtype
	let presenceTest = propDef.presenceTest;
	if (propDesc.isSubtype() && (propDesc.scalarValueType === 'object') &&
		!propDef.table && !presenceTest) {
		if (!propDesc.container.definition.typeColumn)
			throw invalidPropDef(
				propDesc, 'no presence test and no typeColumn attribute on the' +
					' polymorphic container.');
		presenceTest = [
			[
				'^.' + propDesc.container.typePropertyName + ' => is',
				propDesc.name
			]
		];
	}

	// check if has a presense test
	if (presenceTest) {

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
					[ ':and', presenceTest ]);
			} catch (err) {
				if (err instanceof common.X2UsageError)
					throw invalidPropDef(
						propDesc, 'invalid presence test: ' + err.message);
				throw err;
			}
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

	// final touches
	ctx.onLibraryComplete(() => {

		// determine fetch by default flag
		propDesc._fetchByDefault = ((
			(propDef.fetchByDefault === undefined) &&
				!propDesc.isView() &&
				!propDesc.isCalculated() &&
				!propDesc.reverseRefPropertyName
		) || propDef.fetchByDefault);

		// determine if entangled reference property
		if (propDesc.isRef() && propDesc.table) {
			propDesc._entangled = false;
			const targetRecordTypeDesc = propDesc.nestedProperties;
			for (let targetPropName of targetRecordTypeDesc.allPropertyNames) {
				const targetPropDesc = targetRecordTypeDesc.getPropertyDesc(
					targetPropName);
				if (targetPropDesc.isRef() &&
					(targetPropDesc.table === propDesc.table) &&
					(targetPropDesc.column === propDesc.parentIdColumn) &&
					(targetPropDesc.parentIdColumn === propDesc.column)) {
					propDesc._entangled = true;
					break;
				}
			}
		}
	});

	// setup validation for non-stored and special role properties
	if (!propDesc.isView()) {
		if (propDesc._isRecordMetaInfo || propDesc._isGenerated ||
			propDesc._isCalculated || propDesc._reverseRefPropertyName)
			validators.replaceDefaultValidators(propDesc, {
				'onCreate': [ 'empty' ]
			});
	}

	// overall property descriptor validation:

	// final property descriptor validation
	ctx.onLibraryValidation(recordTypes => {

		// validate stored property
		if (!propDesc.isCalculated() && !propDesc.reverseRefPropertyName &&
			!propDesc.implicitDependentRef &&
			(propDesc.name !== propDesc.container.typePropertyName)) {

			// must have a table if collection
			if (!propDesc.isScalar() && !propDesc.table)
				throw invalidPropDef(
					propDesc, 'must be stored in a separate table.');

			// must have parent id column if has table
			if (propDesc.table && !propDesc.parentIdColumn)
				throw invalidPropDef(
					propDesc, 'missing parentIdColumn attribute.');

			// validate column
			if (propDesc.scalarValueType === 'object') {
				if (propDesc.column)
					throw invalidPropDef(
						propDesc, 'nested object property may not be stored in' +
							' a column.');
			} else {
				if (!propDesc.column)
					throw invalidPropDef(
						propDesc, 'stored property must have a column.');
			}
		}

		// validate generated property
		if (propDesc.isGenerated()) {

			// validate property type
			if (!propDesc.isScalar() || propDesc.isCalculated() ||
				(propDesc.scalarValueType === 'object') || propDesc.isRef())
				throw invalidPropDef(
					propDesc, 'generated property may not be calculated,' +
						' non-scalar, nested object or reference.');

			// validate generator
			const generator = propDesc.generator;
			if ((generator !== 'auto') && ((typeof generator) !== 'function'))
				throw invalidPropDef(
					propDesc, 'generator may only be "auto" or a function.');
		}

		// validate id property
		if (propDesc.isId()) {

			// validate property type
			if (propDesc.isCalculated() || propDesc.table)
				throw invalidPropDef(
					propDesc, 'id property may not be calculated or stored in' +
						' its own table.');

			// validate top record id uniqueness
			if (propDesc.container.isRecordType() && !propDesc.tableUnique)
				throw invalidPropDef(
					propDesc, 'top record id must be table-wide unique.');
		}

		// validate meta-info property
		if (propDesc.isRecordMetaInfo()) {

			// validate property type
			if (propDesc.isCalculated() || propDesc.table ||
				!propDesc.container.isRecordType() || !propDesc.isScalar() ||
				propDesc.isGenerated() || propDesc.modifiable)
				throw invalidPropDef(
					propDesc, 'record meta-info property may not be' +
						' calculated, generated, non-scalar, modifiable,' +
						' stored in its own table or belong to a nested' +
						' object.');

			// validate property value type
			/* eslint-disable no-fallthrough */
			switch (propDesc.recordMetaInfoRole) {
			case 'version':
				if (propDesc.scalarValueType !== 'number')
					throw invalidPropDef(
						propDesc, 'record version may only be a number.');
				if (!propDesc.fetchByDefault)
					throw invalidPropDef(
						propDesc, 'record version must be fetched by default.');
				break;
			case 'modificationTimestamp':
				if (!propDesc.fetchByDefault)
					throw invalidPropDef(
						propDesc, 'record modification timestamp must be' +
							' fetched by default.');
			case 'creationTimestamp':
				if (propDesc.scalarValueType !== 'datetime')
					throw invalidPropDef(
						propDesc, 'record creation/modification timestamp may' +
							' only be a datetime.');
				break;
			case 'modificationActor':
			case 'creationActor':
				if ((propDesc.scalarValueType !== 'string') &&
					(propDesc.scalarValueType !== 'number'))
					throw invalidPropDef(
						propDesc, 'record creation/modification actor may only' +
							' be a string or a number.');
			}
			/* eslint-enable no-fallthrough */
		}

		// validate calculated property
		if (propDesc.isCalculated()) {

			// validate property type
			if (propDesc.table || propDesc.column || propDesc.modifiable ||
				(propDesc.scalarValueType === 'object') ||
				propDesc.presenceTest ||
				(!propDesc.isAggregate() && propDesc.isFiltered()) ||
				propDesc.isOrdered())
				throw invalidPropDef(
					propDesc, 'calculated property may not be modifiable,' +
						' stored in a table/column, be a nested object, have a' +
						' presence test or be filtered or ordered.');
		}

		// validate dependent reference property
		if (propDesc.reverseRefPropertyName) {

			// validate property type
			if (propDesc.table || propDesc.column || propDesc.modifiable ||
				propDesc.isCalculated() || !propDesc.container.isRecordType())
				throw invalidPropDef(
					propDesc, 'dependent reference property may not be' +
						' modifiable, calculated, stored in a table/column' +
						' or belong to a nested object.');

			// index column not allowed on dependent record references
			if (propDesc.isArray() && propDesc.indexColumn)
				throw invalidPropDef(
					propDesc, 'dependent reference array property may not have' +
						' index column.');

			// validate reverse reference property in the referred record type
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
				revRefPropDesc.reverseRefPropertyName ||
				(revRefPropDesc.refTarget !== propDesc.container.recordTypeName))
				throw invalidPropDef(
					propDesc, 'reverse reference property does not match the' +
						' property definition.');

			// detect any cyclical strong dependencies
			const seenRecordTypes = new Set();
			const checkForCycles = (dependentRecordType) => {
				seenRecordTypes.add(dependentRecordType.name);
				const drt = dependentRecordType._dependentRecordTypes;
				if (drt) for (let recordTypeName of drt) {
					if (seenRecordTypes.has(recordTypeName))
						throw invalidPropDef(
							propDesc, 'cyclical strong dependency between' +
								' record types ' +
								dependentRecordType.name + ' and ' +
								recordTypeName + '.');
					checkForCycles(
						recordTypes.getRecordTypeDesc(recordTypeName));
				}
			};
			checkForCycles(propDesc.container);
		}

		// validate non-aggregate map property key
		if (propDesc.isMap() && !propDesc.isAggregate()) {

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
		}

		// check if presence test is required
		if (propDesc.isScalar() && (propDesc.scalarValueType === 'object') &&
			propDesc.optional && !propDef.table) {

			// validate presence test
			if (!propDesc.presenceTest)
				throw invalidPropDef(
					propDesc, 'optional scalar object property stored in the' +
						' parent record\'s table must have a presence test.');
		}
	});

	// add properties and methods to the descriptor:

	/**
	 * Indicates if the reference property's target can be linked to the parent
	 * without a join. This is a special case used, for example, for the
	 * <code>records</code> property in the super-type.
	 *
	 * @protected
	 * @member {boolean} module:x2node-dbos.PropertyDescriptorWithDBOs#implicitDependentRef
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'implicitDependentRef', {
		get() { return this._implicitDependentRef; }
	});

	/**
	 * <code>true</code> if the property is fetched by default (included in the
	 * fetch operation result when addressed with a wildcard pattern).
	 *
	 * @member {boolean} module:x2node-dbos.PropertyDescriptorWithDBOs#fetchByDefault
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'fetchByDefault', {
		get() { return this._fetchByDefault; }
	});

	/**
	 * Name of the database column used to store the property value, or
	 * <code>undefined</code> if the property is calculated or a dependent
	 * rereference.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#column
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'column', {
		get() { return this._column; }
	});

	/**
	 * Name of the database table used to store the property value, or
	 * <code>undefined</code> if the property is calculated, a dependent
	 * rereference or stored in the main record table.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#table
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'table', {
		get() { return this._table; }
	});

	/**
	 * If <code>table</code> property is present, this is the name of the column
	 * in that table that points back to the main record table.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#parentIdColumn
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'parentIdColumn', {
		get() { return this._parentIdColumn; }
	});

	/**
	 * For an id property (<code>isId()</code> returns <code>true</code>), tells
	 * if the id value is unique table-wide.
	 *
	 * @member {boolean=} module:x2node-dbos.PropertyDescriptorWithDBOs#tableUnique
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'tableUnique', {
		get() { return this._tableUnique; }
	});

	/**
	 * For a record meta-info property this is the property role, which can be
	 * one of the following: "version", "creationTimestamp", "creationActor",
	 * "modificationTimestamp" or "modificationActor".
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#recordMetaInfoRole
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'recordMetaInfoRole', {
		get() { return this._recordMetaInfoRole; }
	});

	/**
	 * For a property, whose value is generated for new records, this is the
	 * generator, which can be a string "auto" (for automatically generated by
	 * the database), or a function that takes the database connection as its
	 * only argument, and returns either the id value or a promise of it. The
	 * property descriptor is made available to the generator function as
	 * <code>this</code>.
	 *
	 * @member {(string|function)=} module:x2node-dbos.PropertyDescriptorWithDBOs#generator
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'generator', {
		get() { return this._generator; }
	});

	/**
	 * For an array property, the name of the database column that contains the
	 * element indexes, or nothing if the array does not utilize any.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#indexColumn
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'indexColumn', {
		get() { return this._indexColumn; }
	});

	/**
	 * For a map property that does not utilize <code>keyPropertyName</code>, the
	 * name of the database column that contains the map entry keys.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#keyColumn
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'keyColumn', {
		get() { return this._keyColumn; }
	});

	/**
	 * For a dependent record reference property, name of the property in the
	 * target record type that refers back to this record type. A dependent
	 * record reference property can only appear among the top record type
	 * properties (not in a nested object).
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#reverseRefPropertyName
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'reverseRefPropertyName', {
		get() { return this._reverseRefPropertyName; }
	});

	/**
	 * For a dependent record reference property (one that has
	 * <code>reverseRefPropertyName</code> descriptor property), tell if the
	 * dependency is <em>weak</em>. A weak dependency means that when the
	 * referring record is being deleted, no attempt is made to cascade the
	 * deletion to the referred record(s). Strongly dependent records, on the
	 * other hand, are automatically deleted when the referring record is deleted
	 * (the deletion operation is cascaded over the strong dependencies).
	 *
	 * @function module:x2node-dbos.PropertyDescriptorWithDBOs#isWeakDependency
	 * @returns {boolean} <code>true</code> if weak dependency.
	 */
	propDesc.isWeakDependency = function() {
		return this._weakDependency;
	};

	/**
	 * Tell if the stored reference property is entangled with a stored reference
	 * property of its target record type (they share the same link table).
	 *
	 * @function module:x2node-dbos.PropertyDescriptorWithDBOs#isEntangled
	 * @returns {boolean} <code>true</code> if entangled reference property.
	 */
	propDesc.isEntangled = function() {
		return this._entangled;
	};

	/**
	 * For a calculated value or aggregate property, the value expression. The
	 *  expression is based at the record type.
	 *
	 * @protected
	 * @member {module:x2node-dbos~ValueExpression=} module:x2node-dbos.PropertyDescriptorWithDBOs#valueExpr
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'valueExpr', {
		get() { return this._valueExpr; }
	});

	/**
	 * For an aggregate property, the aggregation function, which may be "COUNT",
	 * "MAX", "MIN", "SUM" or "AVG".
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#aggregateFunc
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'aggregateFunc', {
		get() { return this._aggregateFunc; }
	});

	/**
	 * Tell if this is an aggregate property. If a property is an aggregate,
	 * <code>isCalculated()</code> also returns <code>true</code> and
	 * <code>aggregatedPropPath</code> and <code>aggregateFunc</code> descriptor
	 * properties are made available as well.
	 *
	 * @function module:x2node-dbos.PropertyDescriptorWithDBOs#isAggregate
	 * @returns {boolean} <code>true</code> if aggregate property.
	 */
	propDesc.isAggregate = function() {
		return (this._aggregateFunc !== undefined);
	};

	/**
	 * For an aggregate property, path of the aggregated collection property
	 * starting from the record type.
	 *
	 * @member {string=} module:x2node-dbos.PropertyDescriptorWithDBOs#aggregatedPropPath
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'aggregatedPropPath', {
		get() { return this._aggregatedPropPath; }
	});

	/**
	 * Tell if filtered collection view property.
	 *
	 * @function module:x2node-dbos.PropertyDescriptorWithDBOs#isFiltered
	 * @returns {boolean} <code>true</code> if filtered property.
	 */
	propDesc.isFiltered = function() {
		return (this._filter !== undefined);
	};

	/**
	 * For a filtered collection view property, this is the filter.
	 *
	 * @protected
	 * @member {module:x2node-dbos~RecordsFilter=} module:x2node-dbos.PropertyDescriptorWithDBOs#filter
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'filter', {
		get() { return this._filter; }
	});

	/**
	 * For a optional scalar nested object property, this is the presence test.
	 *
	 * @protected
	 * @member {module:x2node-dbos~RecordsFilter=} module:x2node-dbos.PropertyDescriptorWithDBOs#presenceTest
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'presenceTest', {
		get() { return this._presenceTest; }
	});

	/**
	 * Tell if ordered collection property.
	 *
	 * @function module:x2node-dbos.PropertyDescriptorWithDBOs#isOrdered
	 * @returns {boolean} <code>true</code> if ordered property.
	 */
	propDesc.isOrdered = function() {
		return (this._order !== undefined);
	};

	/**
	 * For an ordered collection property, this is the order specification.
	 *
	 * @protected
	 * @member {module:x2node-dbos~RecordsOrder=} module:x2node-dbos.PropertyDescriptorWithDBOs#order
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'order', {
		get() { return this._order; }
	});

	/**
	 * Value expression context for value expressions based at this property.
	 *
	 * @protected
	 * @member {module:x2node-dbos~ValueExpressionContext} module:x2node-dbos.PropertyDescriptorWithDBOs#valueExprContext
	 * @readonly
	 */
	Object.defineProperty(propDesc, 'valueExprContext', {
		get() { return this._valueExprContext; }
	});

	// return the descriptor
	return propDesc;
};
