'use strict';


/**
 * Interface for database drivers.
 *
 * @interface DBDriver
 * @memberof module:x2node-queries
 */
/**
 * Get SQL for the specified ES value.
 *
 * @function module:x2node-queries.DBDriver#sql
 * @param {*} val The ES value. If object, <code>toString()</code> is called on
 * it and the result is returned as is. If <code>null</code>, string "NULL" is
 * returned.
 * @returns {string} String representing the value in SQL, or <code>null</code>
 * if the value cannot be represented in SQL (includes <code>undefined</code>,
 * <code>NaN</code>, <code>Infinity</code> and arrays).
 */
/**
 * Get Boolean SQL literal.
 *
 * @function module:x2node-queries.DBDriver#booleanLiteral
 * @param {*} val The ES value.
 * @returns {string} String representing Boolean true or false in SQL.
 */
/**
 * Get string SQL literal.
 *
 * @function module:x2node-queries.DBDriver#stringLiteral
 * @param {string} val The ES string.
 * @returns {string} String representing the string in SQL.
 */
// TODO: more interface methods documentation


/**
 * SQL database driver base class.
 *
 * @memberof module:x2node-queries
 * @abstract
 * @implements {module:x2node-queries.DBDriver}
 */
class BasicDBDriver {

	sql(val) {

		switch (typeof val) {
		case 'undefined':
			return null;
		case 'string':
			return this.stringLiteral(val);
		case 'number':
			return (Number.isFinite(val) ? String(val) : null);
		case 'boolean':
			return this.booleanLiteral(val);
		case 'object':
			return (val === null ? 'NULL' : (
				Array.isArray(val) ? null : val.toString()));
		default:
			return null;
		}
	}

	booleanLiteral(val) {

		return (val ? 'TRUE' : 'FALSE');
	}

	stringLiteral(val) {

		return '\'' + val.replace('\'', '\'\'') + '\'';
	}

	safeLabel(label) {

		return '"' + label + '"';
	}

	safeLikePatternFromString(str) {

		return str.replace(/([%_\\])/g, '\\$1');
	}

	nullableConcat() {

		throw new Error('Not implemented.');
	}

	castToString(expr) {

		throw new Error('Not implemented.');
	}

	stringLength(expr) {

		return 'LENGTH(' + expr + ')';
	}

	stringLowercase(expr) {

		return 'LOWER(' + expr + ')';
	}

	stringUppercase(expr) {

		return 'UPPER(' + expr + ')';
	}

	stringSubstring(expr, from, len) {

		throw new Error('Not implemented.');
	}

	stringLeftPad(expr, width, pad) {

		return 'LPAD(' + expr + ', ' + width + ', \'' +
			(pad === '\'' ? '\'\'' : pad) + '\')';
	}

	booleanToNull(expr) {

		return 'CASE WHEN ' + expr + ' THEN ' + this.booleanLiteral(true) +
			' ELSE NULL END';
	}

	coalesce() {

		return 'COALESCE(' + Array.from(arguments).join(', ') + ')';
	}

	makeRangedSelect(selectStmt, offset, limit) {

		throw new Error('Not implemented.');
	}

	startTransaction(connection, handler) {

		throw new Error('Not implemented.');
	}

	rollbackTransaction(connection, handler) {

		throw new Error('Not implemented.');
	}

	commitTransaction(connection, handler) {

		throw new Error('Not implemented.');
	}

	execute(connection, statement, handler) {

		throw new Error('Not implemented.');
	}
}

// export the class
module.exports = BasicDBDriver;
