'use strict';


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

module.exports = BasicDBDriver;
