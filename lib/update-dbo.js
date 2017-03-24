'use strict';

const common = require('x2node-common');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');


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

		// make sure the patch spec is an array
		if (!Array.isArray(patch))
			throw new common.X2UsageError(
				'Patch specification is not an array.');

		// extract all involved property paths
		const propPaths = new Set();
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		patch.forEach(patchOp => {

			// get the pointer
			const propPointer = patchOp.path;

			// basic validation of the patch operation spec
			if (((typeof patchOp.op) !== 'string') ||
				((typeof propPointer) !== 'string') ||
				((propPointer.length > 0) && !propPointer.startsWith('/')))
				throw new common.X2UsageError(
					'An operastion in the patch specification is missing or' +
						' has invalid "op" or "path" property.');

			// parse the property pointer
			const propPointerParts = propPointer.split('/');
			let propPath = '', lastPropDesc = recordsPropDesc, inCol = true;
			for (let i = 1, len = propPointerParts.length; i < len; i++) {
				//...
				const propName = propPointerParts[i];
				if (!container.hasProperty(propName))
					throw new common.X2UsageError(
						'Invalid property path "' + propPointer +
							'" in patch operation: no such property.');
				const propDesc = container.getPropertyDesc(propName);
				//...
			}

			//...
		});

		//...
	}

	_resolvePropPointer(recordsPropDesc, propPointer, allowDash) {

		// basic validation of the pointer
		if (((typeof propPointer) !== 'string') ||
			((propPointer.length > 0) && !propPointer.startsWith('/')))
			throw new common.X2UsageError(
				'Invalid property pointer "' + propPointer +
					'" in the patch operation.');

		// parse the pointer
		const propPointerTokens = propPointer.split('/');
		let propPath = '', lastPropDesc = recordsPropDesc, inCol = true;
		for (let i = 1, len = propPointerTokens.length; i < len; i++) {
			const propPointerToken = propPointerTokens[i]
				.replace(/~0/g, '~')
				.replace(/~1/g, '/');
			if (!inCol && lastPropDesc.isArray()) {
				//...
			} else if (!inCol && lastPropDesc.isMap()) {
				//...
			} else {
				if ((propPath.length > 0) &&
					(lastPropDesc.scalarValueType !== 'object'))
					throw new common.X2UsageError(
						'Invalid property pointer "' + propPointer +
							'" in patch operation: ' + propPath +
							' does not have nested elements.');
				const container = lastPropDesc.nestedProperties;
				if (!container.hasProperty(propPointerToken))
					throw new common.X2UsageError(
						'Invalid property pointer "' + propPointer +
							'" in patch operation: no such property.');
				const propDesc = container.getPropertyDesc(propPointerToken);
				//...
			}

			//...
		}

		// return the property path
		return propPath;
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<Object>} The result promise, which resolves to the
	 * result object. The result object includes property
	 * <code>recordsUpdated</code>, which provides the number of records affected
	 * by the operation, including zero. The promise is rejected with the error
	 * object of an error happens during the operation execution.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, filterParams) {

		//...
	}
}

// export the class
module.exports = UpdateDBO;
