'use strict';

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');


/////////////////////////////////////////////////////////////////////////////////
// EXECUTION CONTEXT
/////////////////////////////////////////////////////////////////////////////////

/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class InsertDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor, entangledUpdates, idPropName) {
		super(dbo, txOrCon, actor, null, entangledUpdates);

		this._idPropName = idPropName;
	}

	getResult() {

		return this.getGeneratedParam(this._idPropName);
	}
}


/////////////////////////////////////////////////////////////////////////////////
// THE DBO
/////////////////////////////////////////////////////////////////////////////////

/**
 * Insert database operation implementation (SQL <code>INSERT</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class InsertDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-dbos.RecordCollectionsMonitor} rcMonitor The record
	 * collections monitor.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {Object} record The record data.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, rcMonitor, recordTypeDesc, record) {
		super(dbDriver, recordTypes, rcMonitor);

		// save the basics
		this._recordTypeDesc = recordTypeDesc;

		// register record type update
		this._registerRecordTypeUpdate(recordTypeDesc.name);

		// the operation commands sequence
		this._commands = new Array();

		// create insert commands starting from the top record
		this._createInsertCommands(
			this._commands,
			recordTypeDesc.table,
			null, null, null, null,
			recordTypeDesc, record
		);

		// add entangled records update commands
		this._commands.push(this._createUpdateEntangledRecordsCommand());

		// add record collections monitor notification command
		this._commands.push(this._createNotifyRecordCollectionsMonitorCommand());

		// find entanglements
		this._entangledUpdates = new Object();
		for (let propName of recordTypeDesc.allPropertyNames) {
			const propDesc = recordTypeDesc.getPropertyDesc(propName);
			if (!propDesc.isEntangled() || propDesc.isView())
				continue;
			let ids = this._entangledUpdates[propDesc.refTarget];
			if (!ids)
				this._entangledUpdates[propDesc.refTarget] = ids = new Set();
			const propVal = record[propName];
			if (propDesc.isArray() && Array.isArray(propVal)) {
				for (let ref of propVal)
					if ((typeof ref) === 'string')
						ids.add(recordTypes.refToId(propDesc.refTarget, ref));
			} else if (propDesc.isMap() && ((typeof propVal) === 'object') &&
				(propVal !== null)) {
				for (let key in propVal) {
					const ref = propVal[key];
					if ((typeof ref) === 'string')
						ids.add(recordTypes.refToId(propDesc.refTarget, ref));
				}
			} else if ((typeof propVal) === 'string') {
				ids.add(recordTypes.refToId(propDesc.refTarget, propVal));
			}
		}
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {?module:x2node-common.Actor} actor Actor executing the DBO.
	 * @returns {Promise.<(string|number)>} Promise, which resolves to the new
	 * record id or is rejected with the error object of an error happens during
	 * the operation execution.
	 */
	execute(txOrCon, actor) {

		return this._executeCommands(new InsertDBOExecutionContext(
			this, txOrCon, actor, this._entangledUpdates,
			this._recordTypeDesc.idPropertyName));
	}
}

// export the class
module.exports = InsertDBO;
