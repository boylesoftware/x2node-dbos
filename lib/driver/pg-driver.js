'use strict';

const BasicDBDriver = require('./basic-driver.js');


/**
 * Symbol used to indicate that there is an error on the query object.
 *
 * @private
 */
const HAS_ERROR = Symbol();

/**
 * Symbol used to indicate that header needs to be passed to the callback.
 *
 * @private
 */
const SET_HEADER = Symbol();

/**
 * PostgreSQL database driver.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends {module:x2node-queries~BasicDBDriver}
 * @implements {module:x2node-queries.DBDriver}
 */
class PostgreSQLDBDriver extends BasicDBDriver {

	safeLikePatternFromExpr(expr) {

		return 'REGEXP_REPLACE(' + expr +
			', \'([%_\\\\])\', \'\\\\\\1\', \'g\')';
	}

	nullableConcat() {

		return Array.from(arguments).join(' || ');
	}

	patternMatch(expr, pattern, invert, caseSensitive) {

		return expr + (invert ? ' NOT' : '') +
			(caseSensitive ? ' LIKE ' : ' ILIKE ') + pattern;
	}

	regexpMatch(expr, regexp, invert, caseSensitive) {

		return expr + (invert ? ' !' : ' ') +
			(caseSensitive ? '~ ' : '~* ') + regexp;
	}

	castToString(expr) {

		return 'CAST(' + expr + ' AS VARCHAR)';
	}

	/*datetimeToString(expr) {

		return 'TO_CHAR(' + expr +
			//' AT TIME ZONE \'UTC\', \'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"\')';
			', \'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"\')';
	}*/

	stringSubstring(expr, from, len) {

		return 'SUBSTRING(' + expr +
			' FROM ' + (
				(typeof from) === 'number' ?
					String(from + 1) : '(' + String(from) + ') + 1'
			) +
			(len !== undefined ? ' FOR ' + String(len) : '') + ')';
	}

	makeRangedSelect(selectStmt, offset, limit) {

		return selectStmt + ' LIMIT ' + limit +
			(offset > 0 ? ' OFFSET ' + offset : '');
	}

	makeSelectIntoTempTable(
		selectStmt, tempTableName, preStatements, postStatements) {

		preStatements.push(
			'CREATE TEMPORARY TABLE ' + tempTableName +
				' ON COMMIT DROP AS ' + selectStmt);

		postStatements.push(
			'DROP TABLE IF EXISTS ' + tempTableName);
	}

	startTransaction(connection, handler) {

		connection.query('BEGIN', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	rollbackTransaction(connection, handler) {

		connection.query('ROLLBACK', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	commitTransaction(connection, handler) {

		connection.query('COMMIT', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	execute(connection, statement, handler) {

		const querySpec = {
			text: statement
		};
		if (!handler.noRowsAsArrays)
			querySpec.rowMode = 'array';

		const query = connection.query(querySpec);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess();
		});

		if (handler.onHeader || handler.onRow) {
			query[SET_HEADER] = handler.onHeader;
			query.on('row', (row, result) => {
				if (query[HAS_ERROR])
					return;
				try {
					if (query[SET_HEADER]) {
						handler.onHeader(result.fields.map(field => field.name));
						query[SET_HEADER] = false;
					}
					handler.onRow(row);
				} catch (err) {
					query[HAS_ERROR] = true;
					handler.onError(err);
				}
			});
		}
	}
}

module.exports = PostgreSQLDBDriver;
