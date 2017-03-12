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
/**
 * Get SQL fragment that can be used as a column label in a <code>SELECT</code>
 * clause from the specified string.
 *
 * @function module:x2node-queries.DBDriver#safeLabel
 * @param {string} label The ES string to use as a column label.
 * @returns {string} SQL fragment for the label.
 */
/**
 * Make necessary escapes in the specified string to make it safe to use as a
 * SQL <code>LIKE</code> condition pattern.
 *
 * @function module:x2node-queries.DBDriver#safeLikePatternFromString
 * @param {string} str The ES string to make safe to use as a pattern.
 * @returns {string} The string with all the necessary escapes.
 */
/**
 * Make SQL expression that gets the string length for the specified string SQL
 * expression.
 *
 * @function module:x2node-queries.DBDriver#stringLength
 * @param {string} expr The string SQL expression.
 * @returns {string} SQL expression that gets the string length.
 */
/**
 * Make SQL expression that converts the specified string SQL expression to lower
 * case.
 *
 * @function module:x2node-queries.DBDriver#stringLowercase
 * @param {string} expr The string SQL expression.
 * @returns {string} SQL expression that converts the string to lower case.
 */
/**
 * Make SQL expression that converts the specified string SQL expression to upper
 * case.
 *
 * @function module:x2node-queries.DBDriver#stringUppercase
 * @param {string} expr The string SQL expression.
 * @returns {string} SQL expression that converts the string to upper case.
 */
/**
 * Make SQL expression that pads the specified string SQL expression with the
 * specified character on the left until the specified length is achieved.
 *
 * @function module:x2node-queries.DBDriver#stringLeftPad
 * @param {string} expr The string SQL expression.
 * @param {number} width Minimum resulting string length to achieve with padding.
 * @param {string} pad The character to use as the padding.
 * @returns {string} SQL expression that pads the string on the left.
 */
/**
 * Make SQL expression that concatenates the specified string SQL expressions and
 * evaluates to <code>NULL</code> if any of them is <code>NULL</code>.
 *
 * @function module:x2node-queries.DBDriver#nullableConcat
 * @param {...string} exprs String SQL expressions to concatenate.
 * @returns {string} SQL expression that concatenates the specified string SQL
 * expressions.
 */
/**
 * Make SQL expression for getting a substring of the specified string SQL
 * expression.
 *
 * @function module:x2node-queries.DBDriver#stringSubstring
 * @param {string} expr SQL string expression.
 * @param {number} from Zero-based first character to include in the substring.
 * @param {number} [len] Optional maximum length of the substring. If not
 * specified, the end of the string is assumed.
 * @returns {string} SQL expression for the substring.
 */
/**
 * Make SQL expression that converts the specified SQL expression to string.
 *
 * @function module:x2node-queries.DBDriver#castToString
 * @param {string} expr The SQL expression to convert to string.
 * @returns {string} SQL expression that evaluates to a string value.
 */
/**
 * Make SQL expression that evaluates the specified SQL expression and resolves
 * into SQL <code>TRUE</code> or SQL </code>NULL</code> if the result is true or
 * false respectively.
 *
 * @function module:x2node-queries.DBDriver#booleanToNull
 * @param {string} expr The Boolean SQL expression to evaluate.
 * @returns {string} SQL expression that evaluates to <code>TRUE</code> or
 * <code>NULL</code>.
 */
/**
 * Make specified <code>SELECT</code> statement ranged.
 *
 * @function module:x2node-queries.DBDriver#makeRangedSelect
 * @param {string} selectStmt The <code>SELECT</code> statement to make ranged.
 * @param {number} offset Zero-based number of the first row in the range.
 * @param {number} limit Maximum number of rows in the range.
 * @returns {string} Ranged <code>SELECT</code> statement.
 */
/**
 * Make SQL expression that evaluates to the result of the first of the specified
 * SQL expressions that is not <code>NULL</code>, or <code>NULL</code> if all
 * evaluate to <code>NULL</code>.
 *
 * @function module:x2node-queries.DBDriver#coalesce
 * @param {...string} exprs SQL expressions to evaluate.
 * @returns {string} SQL expression that evaluates to the first
 * non-<code>NULL</code>.
 */
/**
 * Start transaction on the specified database connection.
 *
 * @function module:x2node-queries.DBDriver#startTransaction
 * @param {*} connection Driver-specific database connection object.
 * @param {Object} handler The operation result handler.
 * @param {Function} handler.onSuccess Fucntion that gets called upon operation
 * success.
 * @param {Function} handler.onError Fucntion that gets called upon operation
 * failure. The function receives a single argument with the error object.
 */
/**
 * Roll back transaction on the specified database connection.
 *
 * @function module:x2node-queries.DBDriver#rollbackTransaction
 * @param {*} connection Driver-specific database connection object.
 * @param {Object} handler The operation result handler.
 * @param {Function} handler.onSuccess Fucntion that gets called upon operation
 * success.
 * @param {Function} handler.onError Fucntion that gets called upon operation
 * failure. The function receives a single argument with the error object.
 */
/**
 * Commit transaction on the specified database connection.
 *
 * @function module:x2node-queries.DBDriver#commitTransaction
 * @param {*} connection Driver-specific database connection object.
 * @param {Object} handler The operation result handler.
 * @param {Function} handler.onSuccess Fucntion that gets called upon operation
 * success.
 * @param {Function} handler.onError Fucntion that gets called upon operation
 * failure. The function receives a single argument with the error object.
 */
/**
 * Execute specified statement on the specified database connection.
 *
 * @function module:x2node-queries.DBDriver#execute
 * @param {*} connection Driver-specific database connection object.
 * @param {string} statement The statement to execute.
 * @param {Object} handler The operation result handler.
 * @param {Function} handler.onSuccess Fucntion that gets called upon operation
 * success.
 * @param {Function} handler.onError Fucntion that gets called upon operation
 * failure. The function receives a single argument with the error object.
 * @param {Function} [handler.onHeader] Function that gets called when the
 * <code>SELECT</code> query result set descriptor is received from the database.
 * The function receives a single argument, which is an array of strings
 * corresponding to the column names in the result set.
 * @param {Function} [handler.onRow] Function that gets called when the
 * <code>SELECT</code> query result set row is received from the database. The
 * function gets a single argument, which, depending on the driver, can be an
 * array of values, one for each column, or an object with column names as the
 * keys and the corresponding values as the values.
 */


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

	stringLength(expr) {

		return 'LENGTH(' + expr + ')';
	}

	stringLowercase(expr) {

		return 'LOWER(' + expr + ')';
	}

	stringUppercase(expr) {

		return 'UPPER(' + expr + ')';
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
}

// export the class
module.exports = BasicDBDriver;
