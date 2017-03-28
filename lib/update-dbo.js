'use strict';

const common = require('x2node-common');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const FetchDBO = require('./fetch-dbo.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');



/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class UpdateDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor) {
		super(dbo, txOrCon, actor);

		this._recordsUpdated = 0;
		this._testFailed = false;
	}

	flushRecord(promiseChain) {

		// TODO: implement
		return promiseChain;
	}

	getResult() {

		return {
			recordsUpdated: this._recordsUpdated,
			testFailed: this._testFailed
		};
	}
}


/**
 * Abstract operation sequence command.
 *
 * @private
 * @abstract
 */
class Command {

	constructor() {}
}

/**
 * Command for updating column in a table.
 *
 * @private
 */
class UpdateColumnCommand extends Command {

	constructor(propDesc, value) {
		super();

		this._propDesc = propDesc;
		this._value = value;
	}

	prepare(ctx, record) {
	}
}


/**
 * Property pointer.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 */
class PropertyPointer {

	/**
	 * Create new pointer. The constructor is not used directly, but via the
	 * <code>parse</code> static method.
	 *
	 * @private
	 * @param {?PropertyPointer} parent Parent pointer, or <code>null</code> for
	 * the root pointer.
	 * @param {string} pointerToken Pointer token.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Descriptor of
	 * the property, at which the pointer points.
	 * @param {string} propPath Path to the property, at which the pointer
	 * points.
	 * @param {boolean} collectionElement <code>true</code> if the pointer is for
	 * an array or map element.
	 */
	constructor(parent, pointerToken, propDesc, propPath, collectionElement) {

		this._parent = parent;
		this._pointerToken = pointerToken;
		this._propDesc = propDesc;
		this._propPath = propPath;
		this._collectionElement = collectionElement;
	}

	/**
	 * Parse the specified pointer.
	 *
	 * @param {module:x2node-records~PropertyDescriptor} recordsPropDesc
	 * Descriptor of the "records" property in the record super-type.
	 * @param {string} propPointer Property pointer string.
	 * @param {boolean} allowDash <code>true</code> if a dash at the end of the
	 * pointer to an array element is allowed.
	 * @returns {module:x2node-dbos~PropertyPointer} Parsed property pointer.
	 * @throws {module:x2node-common.X2UsageError} If the pointer is invalid.
	 */
	static parse(recordsPropDesc, propPointer, allowDash) {

		// basic validation of the pointer
		if (((typeof propPointer) !== 'string') ||
			((propPointer.length > 0) && !propPointer.startsWith('/')))
			throw new common.X2UsageError(
				'Invalid property pointer "' + propPointer +
					'" in the patch operation.');

		// parse the pointer
		const propPointerTokens = propPointer.split('/');
		let lastPointer = new PropertyPointer(
			null, null, recordsPropDesc, '', true);
		for (let i = 1, len = propPointerTokens.length; i < len; i++) {
			lastPointer = lastPointer._createChildPointer(
				propPointerTokens[i].replace(
						/~[01]/g, m => (m === '~0' ? '~' : '/')),
				propPointer, allowDash);
		}

		// return the pointer chain
		return lastPointer;
	}

	/**
	 * Create child pointer.
	 *
	 * @private
	 * @param {string} pointerToken Child pointer token.
	 * @param {string} fullPointer Full pointer for error reporting.
	 * @param {boolean} allowDash <code>true</code> to allow dash pointer.
	 * @returns {module:x2node-dbos~PropertyPointer} Child property pointer.
	 * @throws {module:x2node-common.X2UsageError} If the resulting pointer would
	 * be invalid.
	 */
	_createChildPointer(pointerToken, fullPointer, allowDash) {

		// check if beyond dash
		if (this._collectionElement && this._propDesc.isArray() &&
			(this._pointerToken === '-'))
			throw new common.X2UsageError(
				'Invalid property pointer "' + fullPointer +
					'" in patch operation: unexpected dash for an array index.');

		// check if array element
		if (!this._collectionElement && this._propDesc.isArray()) {
			const dash = (pointerToken === '-');
			if (dash && !allowDash)
				throw new common.X2UsageError(
					'Invalid property pointer "' + fullPointer +
						'" in patch operation: dash not allowed for an array' +
						' index in this pointer.');
			if (!dash && !/^(?:0|[1-9][0-9]*)$/.test(pointerToken))
				throw new common.X2UsageError(
					'Invalid property pointer "' + fullPointer +
						'" in patch operation: invalid array index.');
			return new PropertyPointer(
				this, (dash ? pointerToken : Number(pointerToken)),
				this._propDesc, this._propPath, true);
		}

		// check if map element
		if (!this._collectionElement && this._propDesc.isMap())
			return new PropertyPointer(
				this, pointerToken, this._propDesc, this._propPath, true);

		// object property:

		if ((this._parent !== null) &&
			(this._propDesc.scalarValueType !== 'object'))
			throw new common.X2UsageError(
				'Invalid property pointer "' + fullPointer +
					'" in patch operation: ' +
					this._propDesc.container.nestedPath +
					this._propDesc.name + ' does not have nested elements.');
		const container = this._propDesc.nestedProperties;
		if (!container.hasProperty(pointerToken))
			throw new common.X2UsageError(
				'Invalid property pointer "' + fullPointer +
					'" in patch operation: no such property.');

		return new PropertyPointer(
			this, pointerToken, container.getPropertyDesc(pointerToken),
			container.nestedPath + pointerToken, false);
	}

	/**
	 * Get value of the property, at which the pointer points.
	 *
	 * @param {Object} record The record, from which to get the value.
	 * @param {boolean} forUpdate <code>true</code> if the property is intended
	 * to be updated by a patch operation. If so, leaf arrays and maps are
	 * created if none found in the record to allow addition of elements.
	 * @returns {*} The property value, or <code>null</code> if no value. For
	 * absent collection elements returns <code>undefined</code>.
	 * @throws {module:x2node-common.X2DataError} If the property cannot be
	 * reached.
	 */
	getValue(record, forUpdate) {

		const pointerChain = new Array();
		for (let p = this; p !== null; p = p._parent)
			pointerChain.push(p);

		return pointerChain.reduceRight(
			(obj, p, i) => p._getImmediate(obj, i, forUpdate), record);
	}

	/**
	 * Get value of the property, at which the pointer points provided with the
	 * value of the parent property.
	 *
	 * @private
	 * @param {(Object|Array)} obj The parent object that is supposed to have the
	 * value.
	 * @param {number} i Index of the token in the pointer chain. Zero is for the
	 * last token.
	 * @param {boolean} forUpdate <code>true</code> if for update.
	 * @returns {*} The value.
	 */
	_getImmediateValue(obj, i, forUpdate) {

		const noValue = () => new common.X2UsageError(
			'Requested property value at ' + this._propPath +
				' does not exist.');

		// check if top record
		if (this._parent === null)
			return obj;

		// check if array index, map key or object property
		let val;
		if (this._propDesc.isArray() && !this._collectionElement) {
			if (this._pointerToken !== '-') {
				if (this._pointerToken >= obj.length)
					throw noValue();
				val = obj[this._pointerToken];
				if (((val === undefined) || (val === null)) && (i > 0))
					throw noValue();
			}
		} else if (this._propDesc.isMap() && !this._collectionElement) {
			val = obj[this._pointerToken];
			if (((val === undefined) || (val === null)) && (i > 0))
				throw noValue();
		} else {
			val = obj[this._pointerToken];
			if (val === undefined)
				val = null;
			if ((val === null) && (i > 0)) {
				if (i > 1)
					throw noValue();
				if (this._propDesc.isArray() && forUpdate)
					val = obj[this._pointerToken] = new Array();
				else if (this._propDesc.isMap() && forUpdate)
					val = obj[this._pointerToken] = new Object();
				else
					throw noValue();
			}
		}

		// return the value
		return val;
	}

	/**
	 * Tell if the pointer is the root pointer.
	 *
	 * @returns {boolean} <code>true</code> If root pointer.
	 */
	isRoot() { return (this._parent === null); }

	/**
	 * Descriptor of the property, at which the pointer points.
	 *
	 * @member {module:x2node-records~PropertyDescriptor}
	 * @readonly
	 */
	get propDesc() { return this._propDesc; }

	/**
	 * Path of the property, at which the pointer points.
	 *
	 * @member {string}
	 * @readonly
	 */
	get propPath() { return this._propPath; }

	/**
	 * <code>true</code> if the pointer is for an array or map element.
	 *
	 * @member {boolean}
	 * @readonly
	 */
	get collectionElement() { return this._collectionElement; }
}


/**
 * Update database operation implementation (potentially a combination of SQL
 * <code>UPDATE</code>, <code>INSERT</code> and <code>DELETE</code> queries).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class UpdateDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {Array.<Object>} patch The JSON patch specification.
	 * @param {Array.<Array>} [filterSpec] Optional filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, recordTypeDesc, patch, filterSpec) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;

		// make sure the patch spec is an array
		if (!Array.isArray(patch))
			throw new common.X2UsageError(
				'Patch specification is not an array.');

		// the operation commands sequence
		this._commands = new Array();

		// process patch operations
		const involvedPropPaths = new Set();
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		patch.forEach((patchOp, opInd) => {
			this._parsePatchOperation(
				recordsPropDesc, patchOp, opInd, involvedPropPaths);
		});

		// build the initial fetch DBO
		this._fetchDBO = new FetchDBO(
			dbDriver, recordTypes, recordTypeDesc.name, involvedPropPaths, [],
			filterSpec);

		// build update properties tree
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const updatePropsTree = propsTreeBuilder.buildSimplePropsTree(
			recordTypes, recordsPropDesc, 'update', baseValueExprCtx,
			involvedPropPaths);

		// build update query tree
		this._updateQueryTree = queryTreeBuilder.forDirectQuery(
			dbDriver, recordTypes, 'update', false, updatePropsTree);
	}

	/**
	 * Parse patch operation.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} recordsPropDesc
	 * Descriptor of the "records" property from the record super-type.
	 * @param {Object} patchOp Patch operation.
	 * @param {number} opInd Index of the patch operation in the patch operations
	 * list.
	 * @param {Set.<string>} involvedPropPaths Involved property paths
	 * collection.
	 * @throws {module:x2node-common.X2UsageError} If the operation is invalid.
	 */
	_parsePatchOperation(recordsPropDesc, patchOp, opInd, involvedPropPaths) {

		const invalidOp = msg => new common.X2UsageError(
			'Invalid patch operation #' + (opInd + 1) + ': ' + msg);

		// validate the op
		if ((typeof patchOp.op) !== 'string')
			throw invalidOp('op is missing or is not a string.');

		// process the operation
		let pathInfo, fromInfo;
		switch (patchOp.op) {
		case 'add':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, true, true);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathInfo, patchOp.value, true);
			//...
			break;
		case 'remove':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, false, true);
			if ((pathInfo.propDesc.isScalar() || !pathInfo.collectionElement) &&
				!pathInfo.propDesc.optional)
				throw invalidOp('may not remove a required property.');
			//...
			break;
		case 'replace':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, false, true);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathInfo, patchOp.value, true);
			//...
			break;
		case 'move':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, true, true);
			fromInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.from, false, true);
			if ((fromInfo.propDesc.isScalar() || !fromInfo.collectionElement) &&
				!fromInfo.propDesc.optional)
				throw invalidOp('may not move a required property.');
			this._validatePatchOperationFrom(
				patchOp.op, opInd, pathInfo, fromInfo);
			//...
			break;
		case 'copy':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, true, true);
			fromInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.from, false, false);
			this._validatePatchOperationFrom(
				patchOp.op, opInd, pathInfo, fromInfo);
			//...
			break;
		case 'test':
			pathInfo = this._resolvePropPointer(
				recordsPropDesc, patchOp.path, false, false);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathInfo, patchOp.value, false);
			//...
			break;
		default:
			throw new common.X2UsageError(
				'Invalid patch operation: unknown operation "' + patchOp.op +
					'".');
		}

		// save involved property paths
		this._addInvolvedProperty(pathInfo, involvedPropPaths);
		if (fromInfo)
			this._addInvolvedProperty(fromInfo, involvedPropPaths);
	}

	/**
	 * Add property to the involved property paths collection.
	 *
	 * @private
	 * @param {Object} pathInfo Resolved property pointer.
	 * @param {Set.<string>} involvedPropPaths Involved property paths
	 * collection.
	 */
	_addInvolvedProperty(pathInfo, involvedPropPaths) {

		const propDesc = pathInfo.propDesc;
		if (propDesc.scalarValueType === 'object')
			this._addInvolvedObjectProperty(propDesc, involvedPropPaths);
		else
			involvedPropPaths.add(pathInfo.propPath);
	}

	/**
	 * Recursively add nested object property to the involved property paths
	 * collection.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} objectPropDesc Nested
	 * object property descriptor.
	 * @param {Set.<string>} involvedPropPaths Involved property paths
	 * collection.
	 */
	_addInvolvedObjectProperty(objectPropDesc, involvedPropPaths) {

		const container = objectPropDesc.nestedProperties;
		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.isCalculated() || propDesc.isView())
				continue;
			if (propDesc.scalarValueType === 'object')
				this._addInvolvedObjectProperty(
					objectPropDesc, involvedPropPaths);
			else
				involvedPropPaths.add(container.nestedPath + propName);
		}
	}

	/**
	 * Resolve property pointer.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} recordsPropDesc
	 * Descriptor of the "records" property in the record super-type.
	 * @param {string} propPointer Property pointer.
	 * @param {boolean} allowDash <code>true</code> if dash at the end of the
	 * array property pointer is allowed (in the context of the patch operation).
	 * @param {boolean} forUpdate <code>true</code> if the property is intended
	 * to be updated by the patch operation.
	 * @returns {Object} Information object for the resolved property.
	 * @throws {module:x2node-common.X2UsageError} If the pointer is invalid.
	 */
	_resolvePropPointer(recordsPropDesc, propPointer, allowDash, forUpdate) {

		// parse the pointer
		const resolvedPointer = PropertyPointer.parse(
			recordsPropDesc, propPointer, allowDash);

		// check if top pointer
		if (resolvedPointer.isRoot())
			throw new common.X2UsageError(
				'Patch operations involving top records as a whole are not' +
					' allowed.');

		// check if modifiable
		if (forUpdate && !resolvedPointer.propDesc.isModifiable())
			throw new common.X2UsageError(
				'May not update non-modifiable property ' +
					resolvedPointer.propPath + '.');

		// return the resolved pointer
		return resolvedPointer;
	}

	/**
	 * Validate value provided with a patch operation.
	 *
	 * @private
	 * @param {string} opType Patch operation type.
	 * @param {number} opInd Index of the patch operation in the list of
	 * operations.
	 * @param {Object} pathInfo Information object for the property path where
	 * the value is supposed to belong.
	 * @param {*} val The value to test. Can to be <code>null</code>.
	 * @param {boolean} forUpdate <code>true</code> if the value is intended as
	 * a new value for the property, or <code>false</code> if only used to test
	 * the current property value.
	 * @throws {module:x2node-common.X2UsageError} If the value is invalid.
	 */
	_validatePatchOperationValue(opType, opInd, pathInfo, val, forUpdate) {

		const validate = errMsg => {
			if (errMsg)
				throw new common.X2UsageError(
					'Invalid value in patch operation #' + (opInd + 1) +
						' (' + opType + '): ' + errMsg);
		};

		const propDesc = pathInfo.propDesc;

		if (val === null) {
			if ((propDesc.isScalar() || !pathInfo.collectionElement) &&
				!propDesc.optional)
				validate('null for required property.');
			if (pathInfo.collectionElement &&
				(propDesc.scalarValueType === 'object'))
				validate('null for nested object collection element.');
			return; // valid
		}

		if (propDesc.isArray() && !pathInfo.collectionElement) {
			if (!Array.isArray(val))
				validate('expected an array.');
			if (!propDesc.optional && (val.length === 0))
				validate('empty array for required property.');
			val.forEach(v => {
				validate(this._isInvalidScalarValueType(v, propDesc, forUpdate));
			});
		} else if (propDesc.isMap() && !pathInfo.collectionElement) {
			if ((typeof val) !== 'object')
				validate('expected an object.');
			const keys = Object.keys(val);
			if (!propDesc.optional && (keys.length === 0))
				validate('empty object for required property.');
			keys.forEach(k => {
				validate(this._isInvalidScalarValueType(
					val[k], propDesc, forUpdate));
			});
		} else {
			validate(this._isInvalidScalarValueType(val, propDesc, forUpdate));
		}
	}

	/**
	 * Tell if the specified value is not good as a value for the specified
	 * property as a scalar (so if the property is not scalar, tests if the value
	 * is not good to be a collection element).
	 *
	 * @private
	 * @param {*} val Value to test. Valid to be <code>null</code> unless the
	 * property is a nested object.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Property
	 * descriptor.
	 * @param {boolean} forUpdate <code>true</code> if the value is intended as
	 * a new value for the property, or <code>false</code> if only used to test
	 * the current property value.
	 * @returns {string} Error message if invalid, <code>false</code> if valid.
	 */
	_isInvalidScalarValueType(val, propDesc, forUpdate) {

		switch (propDesc.scalarValueType) {
		case 'string':
			if ((val !== null) && ((typeof val) !== 'string'))
				return 'expected string.';
			break;
		case 'number':
			if ((val !== null) && (
				(typeof val) !== 'number') || !Number.isFinite(val))
				return 'expected number.';
			break;
		case 'boolean':
			if ((val !== null) && ((typeof val) !== 'boolean'))
				return 'expected boolean.';
			break;
		case 'datetime':
			if ((val !== null) && (
				((typeof val) !== 'string') ||
					!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(val) ||
					Number.isNaN(Date.parse(val))))
				return 'expected ISO 8601 string.';
			break;
		case 'ref':
			if ((val !== null) && !this._isValidRefValue(val, propDesc))
				return ('expected ' + propDesc.refTarget + ' reference.');
			break;
		case 'object':
			if (val === null)
				return 'unexpected null instead of an object.';
			if (!this._isValidObjectValue(val, propDesc, forUpdate))
				return 'expected matching object properties.';
		}

		return false;
	}

	/**
	 * Tell if the specified value is suitable to be the specified reference
	 * property's value.
	 *
	 * @private
	 * @param {*} val The value to test.
	 * @param {module:x2node-records~PropertyDescriptor} propDef Reference
	 * property descriptor.
	 * @returns {boolean} <code>true</code> if the value is a string (so not
	 * <code>null</code> or <code>undefined</code> either) and matches the
	 * reference property.
	 */
	_isValidRefValue(val, propDesc) {

		if ((typeof val) !== 'string')
			return false;

		const hashInd = val.indexOf('#');
		if ((hashInd <= 0) || (hashInd === val.length - 1))
			return false;

		const refTarget = val.substring(0, hashInd);
		if (refTarget !== propDesc.refTarget)
			return false;

		const refTargetDesc = this._recordTypes.getRecordTypeDesc(refTarget);
		const refIdPropDesc = refTargetDesc.getPropertyDesc(
			refTargetDesc.idPropertyName);

		if ((refIdPropDesc.scalarValueType === 'number') &&
			!Number.isFinite(Number(val.substring(hashInd + 1))))
			return false;

		return true;
	}

	/**
	 * Tell if the specified object is suitable to be a value for the specified
	 * nested object property.
	 *
	 * @private
	 * @param {Object} val The object to test. May not be <code>null</code> or
	 * <code>undefined</code>.
	 * @param {module:x2node-records~PropertyDescriptor} objectPropDesc Nested
	 * object property descriptor (either scalar or not).
	 * @param {boolean} forUpdate <code>true</code> if the object is intended as
	 * a new value for the property, or <code>false</code> if only used to test
	 * the current property value.
	 * @returns {boolean} <code>true</code> if valid.
	 */
	_isValidObjectValue(val, objectPropDesc, forUpdate) {

		const container = objectPropDesc.nestedProperties;
		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.isView() || propDesc.isCalculated())
				continue;
			const propVal = val[propName];
			if ((propVal === undefined) || (propVal === null)) {
				if (!propDesc.optional &&
					(!forUpdate || !propDesc.isGenerated()))
					return false;
			} else if (propDesc.isArray()) {
				if (!Array.isArray(propVal))
					return false;
				if (!propDesc.optional && (propVal.length === 0))
					return false;
				if (propVal.some(
					v => this._isInvalidScalarValueType(v, propDesc, forUpdate)))
					return false;
			} else if (propDesc.isMap()) {
				if ((typeof propVal) !== 'object')
					return false;
				const keys = Object.keys(propVal);
				if (!propDesc.optional && (keys.length === 0))
					return false;
				if (keys.some(
					k => this._isInvalidScalarValueType(
						propVal[k], propDesc, forUpdate)))
					return false;
			} else {
				if (this._isInvalidScalarValueType(propVal, propDesc, forUpdate))
					return false;
			}
		}

		return true;
	}

	/**
	 * Validate "from" property provided with a patch operation.
	 *
	 * @private
	 * @param {string} opType Patch operation type.
	 * @param {number} opInd Index of the patch operation in the list of
	 * operations.
	 * @param {Object} pathInfo Information object for the property path where
	 * the "from" property is supposed to be placed.
	 * @param {Object} fromInfo Information object for the "from" property.
	 * @throws {module:x2node-common.X2UsageError} If the "from" is incompatible.
	 */
	_validatePatchOperationFrom(opType, opInd, pathInfo, fromInfo) {

		const invalidFrom = msg => new common.X2UsageError(
			'Invalid "from" pointer in patch operation #' + (opInd + 1) +
				' (' + opType + '): ' + msg);

		const fromPropDesc = fromInfo.propDesc;
		const toPropDesc = pathInfo.propDesc;

		if (fromPropDesc.scalarValueType !== toPropDesc.scalarValueType)
			throw invalidFrom('incompatible property value types.');
		if (toPropDesc.isRef() &&
			(fromPropDesc.refTarget !== toPropDesc.refTarget))
			throw invalidFrom('incompatible reference property targets.');
		if ((toPropDesc.scalarValueType === 'object') &&
			!this._isCompatibleObjects(fromPropDesc, toPropDesc))
			throw invalidFrom('incompatible nested objects.');

		if (toPropDesc.isArray() && !pathInfo.collectionElement) {
			if (!fromPropDesc.isArray() || fromPropDesc.collectionElement)
				throw invalidFrom('not an array.');
		} else if (toPropDesc.isMap() && !pathInfo.collectionElement) {
			if (!fromPropDesc.isMap() || fromPropDesc.collectionElement)
				throw invalidFrom('not a map.');
		} else {
			if (!fromPropDesc.isScalar() && !fromInfo.collectionElement)
				throw invalidFrom('not a scalar.');
		}
	}

	/**
	 * Tell if a value of the nested object property 2 can be used as a value of
	 * the nested object property 1.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} objectPropDesc1 Nested
	 * object property 1.
	 * @param {module:x2node-records~PropertyDescriptor} objectPropDesc2 Nested
	 * object property 2.
	 * @returns {boolean} <code>true</code> if compatible nested object
	 * properties.
	 */
	_isCompatibleObjects(objectPropDesc1, objectPropDesc2) {

		const propNames2 = new Set(objectPropDesc2.allPropertyNames);
		const container1 = objectPropDesc1.nestedProperties;
		const container2 = objectPropDesc2.nestedProperties;
		for (let propName of objectPropDesc1.allPropertyNames) {
			const propDesc1 = container1.getPropertyDesc(propName);
			if (propDesc1.isView() || propDesc1.isCalculated())
				continue;
			if (!propNames2.has(propName)) {
				if (!propDesc1.optional)
					return false;
				continue;
			}
			propNames2.delete(propName);
			const propDesc2 = container2.getPropertyDesc(propName);
			if (!propDesc1.optional && propDesc2.optional)
				return false;
			if ((propDesc1.isArray() && !propDesc2.isArray()) ||
				(propDesc1.isMap() && !propDesc2.isMap()) ||
				(propDesc1.isScalar() && !propDesc2.isScalar()))
				return false;
			if (propDesc1.scalarValueType !== propDesc2.scalarValueType)
				return false;
			if (propDesc1.isRef() &&
				(propDesc1.refTarget !== propDesc2.refTarget))
				return false;
			if ((propDesc1.scalarValueType === 'object') &&
				!this._isCompatibleObjects(propDesc1, propDesc2))
				return false;
		}

		for (let propName of propNames2) {
			const propDesc2 = container2.getPropertyDesc(propName);
			if (!propDesc2.isView() && !propDesc2.isCalculated())
				return false;
		}

		return true;
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {?module:x2node-common.Actor} actor Actor executing the DBO.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<Object>} The result promise, which resolves to the
	 * result object. The result object includes property
	 * <code>recordsUpdated</code>, which provides the number of records affected
	 * by the operation, including zero. It also includes Boolean property
	 * <code>testFailed</code>, which is <code>true</code> if the whole operation
	 * was rejected because one of the "test" patch operations failed. The
	 * promise is rejected with the error object of an error happens during the
	 * operation execution.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, actor, filterParams) {

		// create operation execution context
		const ctx = new UpdateDBOExecutionContext(this, txOrCon, actor);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up initial fetch
		resPromise = resPromise.then(
			() => this._fetchDBO.execute(ctx.transaction, actor, filterParams),
			err => Promise.reject(err)
		);

		// queue up updates
		resPromise = resPromise.then(
			fetchResult => {
				let recordsChain = Promise.resolve();
				fetchResult.records.forEach(record => {
					this._commands.forEach(cmd => {
						cmd.prepare(ctx, record);
					});
					recordsChain = ctx.flushRecord(recordsChain);
				});
				return recordsChain;
			},
			err => Promise.reject(err)
		);

		// finish transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// build the final result object
		resPromise = resPromise.then(
			() => ctx.getResult(),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}
}

// export the class
module.exports = UpdateDBO;
