'use strict';


/**
 * SQL database driver base class.
 *
 * @memberof module:x2node-dbos
 * @abstract
 * @implements {module:x2node-dbos.DBDriver}
 */
class BasicDBDriver {

	constructor(options) {

		/**
		 * Options.
		 *
		 * @protected
		 * @member {Object.<string,*>}
		 */
		this._options = options;
	}

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

	stringLength(expr) {

		return `LENGTH(${expr})`;
	}

	stringLowercase(expr) {

		return `LOWER(${expr})`;
	}

	stringUppercase(expr) {

		return `UPPER(${expr})`;
	}

	stringLeftPad(expr, widthExpr, padExpr) {

		return `LPAD(${expr}, ${widthExpr}, ${padExpr})`;
	}

	booleanToNull(expr) {

		return 'CASE WHEN ' + expr + ' THEN ' + this.booleanLiteral(true) +
			' ELSE NULL END';
	}

	coalesce() {

		return 'COALESCE(' + Array.from(arguments).join(', ') + ')';
	}
}

// export the class
module.exports = BasicDBDriver;
