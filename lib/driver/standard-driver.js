'use strict';


/**
 * Standard SQL database driver base class.
 *
 * @private
 * @memberof module:x2node-query
 * @inner
 * @abstract
 * @implements {module:x2node-query.DBDriver}
 */
class StandardDBDriver {

	booleanLiteral(val) { return (val ? 'TRUE' : 'FALSE'); }

	safeLabel(label) { return '"' + label + '"'; }

	execute(connection, statement, handler) {
		throw new Error('Not implemented.');
	}
}

module.exports = StandardDBDriver;
