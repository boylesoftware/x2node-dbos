'use strict';

const common = require('x2node-common');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const filterBuilder = require('./filter-builder.js');


/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class DeleteDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon) {
		super(dbo, txOrCon);
	}
}


/**
 * Delete database operation implementation (SQL <code>DELETE</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class DeleteDBO extends AbstractDBO {

	constructor(dbDriver, recordTypes, recordTypeDesc, filterSpec) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;
		this._recordTypeDesc = recordTypeDesc;

		// build statements
		this._preStatements = new Array();
		this._deletes = new Array();
		this._postStatements = new Array();

		//...
	}

	_addDelete(table, tableChain, joinConditionsChain, container) {

	}

	execute(txOrCon, filterParams) {

		// create operation execution context
		const ctx = new DeleteDBOExecutionContext(this, txOrCon);

		//...
	}
}

// export the class
module.exports = DeleteDBO;
