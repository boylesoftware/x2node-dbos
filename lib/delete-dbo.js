'use strict';

const common = require('x2node-common');

const filterBuilder = require('./filter-builder.js');
const Transaction = require('./transaction.js');
const AbstractDBO = require('./abstract-dbo.js');


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

	execute(tx, filterParams) {

		// part of a transaction?
		const hasTx = (tx instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !tx.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// determine if needs to be wrapped in a transaction
		const wrapInTx = !hasTx;

		// create operation execution context
		const ctx = {
			connection: (hasTx ? tx.connection: tx),
			rollbackOnError: wrapInTx,
			executePostStatements: false
		};

		//...
	}
}

// export the class
module.exports = DeleteDBO;
