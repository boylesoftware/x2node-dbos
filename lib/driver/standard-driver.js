'use strict';


/**
 * Standard SQL database driver base class.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @abstract
 * @implements {module:x2node-queries.DBDriver}
 */
class StandardDBDriver {

	booleanLiteral(val) { return (val ? 'TRUE' : 'FALSE'); }

	stringLiteral(val) { return '\'' + val.replace('\'', '\'\'') + '\''; }

	safeLabel(label) { return '"' + label + '"'; }

	nullableConcat(str, expr) {
		throw new Error('Not implemented.');
	}

	castToString(expr) {
		throw new Error('Not implemented.');
	}

	/*datetimeToString(expr) {
		throw new Error('Not implemented.');
	}*/

	makeRangedSelect(selectStmt, offset, limit) {
		throw new Error('Not implemented.');
	}

	execute(connection, statement, handler) {
		throw new Error('Not implemented.');
	}
}

module.exports = StandardDBDriver;
