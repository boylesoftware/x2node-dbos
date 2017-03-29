'use strict';

const common = require('x2node-common');
const records = require('x2node-records');

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

		this._updateQueryTree = dbo._updateQueryTree;
		this._translationCtx = this._updateQueryTree.getTopTranslationContext(
			dbo._paramsHandler);
		this._recordTypeDesc = dbo._recordTypeDesc;
		this._recordIdPropName = dbo._recordTypeDesc.idPropertyName;

		this._recordsUpdated = 0;
		this._testFailed = false;

		this._recordId = null;
		this._updateBlocks = new Array();
		this._curUpdateBlock = null;
	}

	_getUpdateBlock() {

		if (this._curUpdateBlock === null) {
			this._curUpdateBlock = {
				updates: new Map(),
				deletes: new Set()
			};
			this._updateBlocks.push(this._curUpdateBlock);
		}

		return this._curUpdateBlock;
	}

	/**
	 * Add update of a non-object scalar property to the current record.
	 *
	 * @param {string} propPath Property path.
	 * @param {string} valueExpr SQL value expression to set.
	 */
	addSimpleScalarUpdate(propPath, valueExpr) {

		const columnInfo = this._translationCtx.getPropValueColumn(propPath);

		const updateBlock = this._getUpdateBlock();
		let updatesList = updateBlock.updates.get(columnInfo.tableAlias);
		if (!updatesList)
			updateBlock.updates.set(
				columnInfo.tableAlias, (updatesList = new Array()));

		updatesList.push({
			columnName: columnInfo.columnName,
			value: valueExpr
		});
	}

	/**
	 * Add clearing a simple value array or map property to the current record.
	 *
	 * @param {string} propPath Array or map property path.
	 */
	addSimpleCollectionClear(propPath) {

		//...
	}

	/**
	 * Start processing of a record update.
	 *
	 * @param {Object} record The record data read from the database for update.
	 */
	startRecord(record) {

		this._recordId = record[this._recordIdPropName];
	}

	/**
	 * Flush current record updates.
	 *
	 * @param {Promise} promiseChain The promise chain.
	 * @returns {Promise} The promise chain with record update operations added.
	 */
	flushRecord(promiseChain) {

		// check if any updates have been accumulated for the current record
		if (this._updateBlocks.length === 0)
			return promiseChain;

		// add meta-info property updates
		let metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'version');
		if (metaPropName)
			this.addSimpleScalarUpdate(
				metaPropName,
				this._translationCtx.translatePropPath(metaPropName) + ' + 1');
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationTimestamp');
		if (metaPropName)
			this.addSimpleScalarUpdate(
				metaPropName,
				this._dbDriver.sql(this._executedOn.toISOString()));
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationActor');
		if (metaPropName)
			this.addSimpleScalarUpdate(
				metaPropName,
				this._dbDriver.sql(this._actor.stamp));

		// initial result promise
		let resPromise = promiseChain;

		// build record id filter expression
		const recordIdFilterExpr = this._translationCtx.translatePropPath(
			this._recordIdPropName) + ' = ' + this._dbDriver.sql(this._recordId);

		// process update blocks
		for (let updateBlock of this._updateBlocks) {

			// build and queue up statements
			this._updateQueryTree.walkReverse(
				this._translationCtx, (propNode, tableDesc, tableChain) => {

					// flip the table chain
					if (tableChain.length > 0)
						tableChain[0].joinCondition = tableDesc.joinCondition;

					// DELETE statements
					if (updateBlock.deletes.has(tableDesc.tableAlias)) {
						const sql = this._dbDriver.buildDeleteWithJoins(
							tableDesc.tableName, tableDesc.tableAlias,
							tableChain, recordIdFilterExpr, false);
						resPromise = resPromise.then(
							() => {
								console.log('=== DELETE: [' + sql + ']');
								//...
							},
							err => Promise.reject(err)
						);
					}

					// INSERT statements
					//...

					// UPDATE statements
					const sets = updateBlock.updates.get(tableDesc.tableAlias);
					if (sets && (sets.length > 0)) {
						const sql = this._dbDriver.buildUpdateWithJoins(
							tableDesc.tableName, tableDesc.tableAlias, sets,
							tableChain, recordIdFilterExpr, false);
						resPromise = resPromise.then(
							() => {
								console.log('=== UPDATE: [' + sql + ']');
								//...
							},
							err => Promise.reject(err)
						);
					}
				});
		}

		// update the updated records count
		resPromise = resPromise.then(
			() => {
				this._recordsUpdated++;
			},
			err => Promise.reject(err)
		);

		// reset update blocks
		this._updateBlocks.length = 0;
		this._curUpdateBlock = null;

		// return the result promise
		return resPromise;
	}

	/**
	 * Get update DBO execution result object.
	 *
	 * @returns {Object} The DBO result object.
	 */
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

	constructor(pathPtr, value) {
		super();

		this._pathPtr = pathPtr;
		this._value = value;
	}

	prepare(ctx, record) {

		const curVal = this._pathPtr.getValue(record);
		if (curVal !== this._value) {
			this._pathPtr.replaceValue(record, this._value);
			ctx.addSimpleScalarUpdate(
				this._pathPtr.propPath, ctx.dbDriver.sql(this._value));
		}
	}
}

/**
 * Command for clearing a simple value array.
 *
 * @private
 */
class ClearSimpleArrayCommand extends Command {

	constructor(pathPtr) {
		super();

		this._pathPtr = pathPtr;
	}

	prepare(ctx, record) {

		const curArr = this._pathPtr.getValue(record);
		if (curArr && (curArr.length > 0)) {
			this._pathPtr.deleteValue(record);
			ctx.addSimpleCollectionClear(this._pathPtr.propPath);
		}
	}
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
		this._recordTypeDesc = recordTypeDesc;

		// make sure the patch spec is an array
		if (!Array.isArray(patch))
			throw new common.X2UsageError(
				'Patch specification is not an array.');

		// the operation commands sequence
		this._commands = new Array();

		// process patch operations
		const involvedPropPaths = new Set();
		patch.forEach((patchOp, opInd) => {
			this._parsePatchOperation(patchOp, opInd, involvedPropPaths);
		});

		// assume actor not required until appropriate meta-info prop detected
		this._actorRequired = false;

		// add record meta-info props to the query
		[ 'version', 'modificationTimestamp', 'modificationActor' ]
			.forEach(r => {
				const propName = recordTypeDesc.getRecordMetaInfoPropName(r);
				if (propName) {
					involvedPropPaths.add(propName);
					if (r === 'modificationActor')
						this._actorRequired = true;
				}
			});

		// build the initial fetch DBO
		this._fetchDBO = new FetchDBO(
			dbDriver, recordTypes, recordTypeDesc.name, involvedPropPaths, [],
			filterSpec);

		// build update properties tree
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
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
	 * @param {Object} patchOp Patch operation.
	 * @param {number} opInd Index of the patch operation in the patch operations
	 * list.
	 * @param {Set.<string>} involvedPropPaths Involved property paths
	 * collection.
	 * @throws {module:x2node-common.X2UsageError} If the operation is invalid.
	 */
	_parsePatchOperation(patchOp, opInd, involvedPropPaths) {

		const invalidOp = msg => new common.X2UsageError(
			'Invalid patch operation #' + (opInd + 1) + ': ' + msg);

		const pathTypeSig = pathPtr => (
			(
				pathPtr.propDesc.isScalar() ?
					'scalar' : (pathPtr.propDesc.isArray() ? 'array' : 'map')
			) + ':' + (
				pathPtr.collectionElement ? 'element' : 'whole'
			) + ':' + (
				pathPtr.propDesc.scalarValueType === 'object' ?
					'object' : 'simple'
			)
		);

		// validate the op
		if ((typeof patchOp.op) !== 'string')
			throw invalidOp('op is missing or is not a string.');

		// process the operation
		let pathPtr, fromPtr;
		switch (patchOp.op) {
		case 'add':
			pathPtr = this._resolvePropPointer(patchOp.path, false, true);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathPtr, patchOp.value, true);
			switch (pathTypeSig(pathPtr)) {
			case 'scalar:whole:simple':
				this._commands.push(new UpdateColumnCommand(
					pathPtr, patchOp.value));
				break;
			case 'array:whole:simple':
				//...
				break;
			case 'map:whole:simple':
				//...
				break;
			case 'scalar:whole:object':
				//...
				break;
			case 'array:whole:object':
				//...
				break;
			case 'map:whole:object':
				//...
				break;
			case 'array:element:simple':
				//...
				break;
			case 'map:element:simple':
				//...
				break;
			case 'array:element:object':
				//...
				break;
			case 'map:element:object':
				//...
			}
			break;
		case 'remove':
			pathPtr = this._resolvePropPointer(patchOp.path, true, true);
			if ((pathPtr.propDesc.isScalar() || !pathPtr.collectionElement) &&
				!pathPtr.propDesc.optional)
				throw invalidOp('may not remove a required property.');
			//...
			break;
		case 'replace':
			pathPtr = this._resolvePropPointer(patchOp.path, true, true);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathPtr, patchOp.value, true);
			//...
			break;
		case 'move':
			pathPtr = this._resolvePropPointer(patchOp.path, false, true);
			fromPtr = this._resolvePropPointer(patchOp.from, true, true);
			if ((fromPtr.propDesc.isScalar() || !fromPtr.collectionElement) &&
				!fromPtr.propDesc.optional)
				throw invalidOp('may not move a required property.');
			this._validatePatchOperationFrom(
				patchOp.op, opInd, pathPtr, fromPtr);
			//...
			break;
		case 'copy':
			pathPtr = this._resolvePropPointer(patchOp.path, false, true);
			fromPtr = this._resolvePropPointer(patchOp.from, true, false);
			this._validatePatchOperationFrom(
				patchOp.op, opInd, pathPtr, fromPtr);
			//...
			break;
		case 'test':
			pathPtr = this._resolvePropPointer(patchOp.path, true, false);
			this._validatePatchOperationValue(
				patchOp.op, opInd, pathPtr, patchOp.value, false);
			//...
			break;
		default:
			throw new common.X2UsageError(
				'Invalid patch operation: unknown operation "' + patchOp.op +
					'".');
		}

		// save involved property paths
		this._addInvolvedProperty(pathPtr, involvedPropPaths);
		if (fromPtr)
			this._addInvolvedProperty(fromPtr, involvedPropPaths);
	}

	/**
	 * Add property to the involved property paths collection.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyPointer} pathPtr Resolved property
	 * pointer.
	 * @param {Set.<string>} involvedPropPaths Involved property paths
	 * collection.
	 */
	_addInvolvedProperty(pathPtr, involvedPropPaths) {

		const propDesc = pathPtr.propDesc;
		if (propDesc.scalarValueType === 'object')
			this._addInvolvedObjectProperty(propDesc, involvedPropPaths);
		else
			involvedPropPaths.add(pathPtr.propPath);
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
	 * @param {string} propPointer Property pointer.
	 * @param {boolean} noDash <code>true</code> if dash at the end of the
	 * array property pointer is disallowed (in the context of the patch
	 * operation).
	 * @param {boolean} forUpdate <code>true</code> if the property is intended
	 * to be updated by the patch operation.
	 * @returns {Object} Information object for the resolved property.
	 * @throws {module:x2node-common.X2UsageError} If the pointer is invalid.
	 */
	_resolvePropPointer(propPointer, noDash, forUpdate) {

		// parse the pointer
		const resolvedPointer = records.parseJSONPointer(
			this._recordTypeDesc, propPointer, noDash);

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
	 * @param {module:x2node-records~PropertyPointer} pathPtr Information object
	 * for the property path where the value is supposed to belong.
	 * @param {*} val The value to test. Can to be <code>null</code>.
	 * @param {boolean} forUpdate <code>true</code> if the value is intended as
	 * a new value for the property, or <code>false</code> if only used to test
	 * the current property value.
	 * @throws {module:x2node-common.X2UsageError} If the value is invalid.
	 */
	_validatePatchOperationValue(opType, opInd, pathPtr, val, forUpdate) {

		const validate = errMsg => {
			if (errMsg)
				throw new common.X2UsageError(
					'Invalid value in patch operation #' + (opInd + 1) +
						' (' + opType + '): ' + errMsg);
		};

		const propDesc = pathPtr.propDesc;

		if (val === null) {
			if ((propDesc.isScalar() || !pathPtr.collectionElement) &&
				!propDesc.optional)
				validate('null for required property.');
			if (pathPtr.collectionElement &&
				(propDesc.scalarValueType === 'object'))
				validate('null for nested object collection element.');
			return; // valid
		}

		if (propDesc.isArray() && !pathPtr.collectionElement) {
			if (!Array.isArray(val))
				validate('expected an array.');
			if (!propDesc.optional && (val.length === 0))
				validate('empty array for required property.');
			val.forEach(v => {
				validate(this._isInvalidScalarValueType(v, propDesc, forUpdate));
			});
		} else if (propDesc.isMap() && !pathPtr.collectionElement) {
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
	 * @param {module:x2node-records~PropertyPointer} pathPtr Information object
	 * for the property path where the "from" property is supposed to be placed.
	 * @param {module:x2node-records~PropertyPointer} fromPtr Information object
	 * for the "from" property.
	 * @throws {module:x2node-common.X2UsageError} If the "from" is incompatible.
	 */
	_validatePatchOperationFrom(opType, opInd, pathPtr, fromPtr) {

		const invalidFrom = msg => new common.X2UsageError(
			'Invalid "from" pointer in patch operation #' + (opInd + 1) +
				' (' + opType + '): ' + msg);

		const fromPropDesc = fromPtr.propDesc;
		const toPropDesc = pathPtr.propDesc;

		if (fromPropDesc.scalarValueType !== toPropDesc.scalarValueType)
			throw invalidFrom('incompatible property value types.');
		if (toPropDesc.isRef() &&
			(fromPropDesc.refTarget !== toPropDesc.refTarget))
			throw invalidFrom('incompatible reference property targets.');
		if ((toPropDesc.scalarValueType === 'object') &&
			!this._isCompatibleObjects(fromPropDesc, toPropDesc))
			throw invalidFrom('incompatible nested objects.');

		if (toPropDesc.isArray() && !pathPtr.collectionElement) {
			if (!fromPropDesc.isArray() || fromPropDesc.collectionElement)
				throw invalidFrom('not an array.');
		} else if (toPropDesc.isMap() && !pathPtr.collectionElement) {
			if (!fromPropDesc.isMap() || fromPropDesc.collectionElement)
				throw invalidFrom('not a map.');
		} else {
			if (!fromPropDesc.isScalar() && !fromPtr.collectionElement)
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

		// check if actor is required
		if (this._actorRequired && !actor)
			throw new common.X2UsageError('Operation may not be anonymous.');

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
					ctx.startRecord(record);
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
