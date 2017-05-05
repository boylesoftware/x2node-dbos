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
 * Symbol used to store the "done()" callback on the pooled database connection.
 *
 * @private
 */
const DONE = Symbol();

/**
 * PostgreSQL database driver.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends {module:x2node-dbos~BasicDBDriver}
 * @implements {module:x2node-dbos.DBDriver}
 */
class PostgreSQLDBDriver extends BasicDBDriver {

	constructor(options) {
		super(options);
	}

	supportsRowLocksWithAggregates() { return false; }

	safeLikePatternFromExpr(expr) {

		return `REGEXP_REPLACE(${expr}, '([%_\\\\])', '\\\\\\1', 'g')`;
	}

	stringSubstring(expr, from, len) {

		return 'SUBSTRING(' + expr +
			' FROM ' + (
				(typeof from) === 'number' ?
					String(from + 1) : '(' + String(from) + ') + 1'
			) +
			(len !== undefined ? ' FOR ' + String(len) : '') + ')';
	}

	nullableConcat() {

		return Array.from(arguments).join(' || ');
	}

	castToString(expr) {

		return `CAST(${expr} AS VARCHAR)`;
	}

	patternMatch(expr, pattern, invert, caseSensitive) {

		return expr + (invert ? ' NOT' : '') +
			(caseSensitive ? ' LIKE ' : ' ILIKE ') + pattern;
	}

	regexpMatch(expr, regexp, invert, caseSensitive) {

		return expr + (invert ? ' !' : ' ') +
			(caseSensitive ? '~ ' : '~* ') + regexp;
	}

	/*datetimeToString(expr) {

		return 'TO_CHAR(' + expr +
			//' AT TIME ZONE \'UTC\', \'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"\')';
			', \'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"\')';
	}*/

	makeRangedSelect(selectStmt, offset, limit) {

		return selectStmt + ' LIMIT ' + limit +
			(offset > 0 ? ' OFFSET ' + offset : '');
	}

	makeSelectIntoTempTable(
		selectStmt, tempTableName, preStatements, postStatements) {

		preStatements.push(
			`CREATE TEMPORARY TABLE ${tempTableName}` +
				` ON COMMIT DROP AS ${selectStmt}`);

		postStatements.push(
			`DROP TABLE IF EXISTS ${tempTableName}`);
	}

	makeSelectWithLocks(selectStmt, exclusiveLockTables, sharedLockTables) {

		return selectStmt + (
			exclusiveLockTables && (exclusiveLockTables.length > 0) ?
				' FOR UPDATE OF ' + exclusiveLockTables.join(', ') : ''
		) + (
			sharedLockTables && (sharedLockTables.length > 0) ?
				' FOR SHARE OF ' + sharedLockTables.join(', ') : ''
		);
	}

	buildLockTables(exclusiveLockTables, sharedLockTables) {

		const exclusiveLockStmt = (
			exclusiveLockTables && (exclusiveLockTables.length > 0) ?
				'LOCK TABLE ' + exclusiveLockTables.join(', ') +
				' IN EXCLUSIVE MODE' : null
		);
		const sharedLockStmt = (
			sharedLockTables && (sharedLockTables.length > 0) ?
				'LOCK TABLE ' + sharedLockTables.join(', ') +
				' IN SHARE MODE' : null
		);

		if (exclusiveLockStmt) {
			if (sharedLockStmt)
				return exclusiveLockStmt + '; ' + sharedLockStmt;
			return exclusiveLockStmt;
		}
		return sharedLockStmt;
	}

	buildDeleteWithJoins(
		fromTableName, fromTableAlias, refTables, filterExpr, filterExprParen) {

		const hasRefTables = (refTables && (refTables.length > 0));
		const hasFilter = filterExpr;

		return 'DELETE FROM ' + fromTableName + ' AS ' + fromTableAlias +
			(
				hasRefTables ?
					' USING ' + refTables.map(
						t => t.tableName + ' AS ' + t.tableAlias).join(', ') :
					''
			) +
			(
				hasRefTables || hasFilter ?
					' WHERE ' + (
						(
							hasRefTables ?
								refTables.map(
									t => t.joinCondition).join(' AND ') :
								''
						) + (
							hasFilter && hasRefTables ?
								' AND ' + (
									filterExprParen ?
										'(' + filterExpr + ')' : filterExpr
								) :
								''
						) + (
							hasFilter && !hasRefTables ? filterExpr : ''
						)
					) :
					''
			);
	}

	buildUpdateWithJoins(
		updateTableName, updateTableAlias, sets, refTables, filterExpr,
		filterExprParen) {

		const hasRefTables = (refTables && (refTables.length > 0));
		const hasFilter = filterExpr;

		return 'UPDATE ' + updateTableName + ' AS ' + updateTableAlias +
			' SET ' + sets.map(s => s.columnName + ' = ' + s.value).join(', ') +
			(
				hasRefTables ?
					' FROM ' + refTables.map(
						t => t.tableName + ' AS ' + t.tableAlias).join(', ') :
					''
			) +
			(
				hasRefTables || hasFilter ?
					' WHERE ' + (
						(
							hasRefTables ?
								refTables.map(
									t => t.joinCondition).join(' AND ') :
								''
						) + (
							hasFilter && hasRefTables ?
								' AND ' + (
									filterExprParen ?
										'(' + filterExpr + ')' : filterExpr
								) :
							''
						) + (
							hasFilter && !hasRefTables ? filterExpr : ''
						)
					) :
					''
			);
	}

	connect(source, handler) {

		if (source.pool) {
			source.connect((err, client, done) => {
				if (err) {
					handler.onError(err);
				} else {
					client[DONE] = done;
					handler.onSuccess(client);
				}
			});
		} else {
			source.connect(err => {
				if (err)
					handler.onError(err);
				else
					handler.onSuccess(source);
			});
		}
	}

	releaseConnection(source, connection, err) {

		if (source.pool) {
			connection[DONE](err);
		} else {
			connection.end();
		}
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

	setSessionVariable(connection, varName, valueExpr, handler) {

		connection.query(`SET SESSION ${varName} TO ${valueExpr}`, err => {

			if (err)
				return handler.onError(err);

			handler.onSuccess();
		});
	}

	getSessionVariable(connection, varName, type, handler) {

		connection.query(`SHOW ${varName}`, (err, result) => {

			if (err) {
				if (err.code === '42704')
					return handler.onSuccess();
				return handler.onError(err);
			}

			const valRaw = result.rows[0][varName];
			switch (type) {
			case 'number':
				handler.onSuccess(Number(valRaw));
				break;
			case 'boolean':
				handler.onSuccess(valRaw === 'true');
				break;
			default:
				handler.onSuccess(valRaw);
			}
		});
	}

	executeQuery(connection, statement, handler) {

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

	executeUpdate(connection, statement, handler) {

		const query = connection.query(statement);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		query.on('end', result => {
			if (!query[HAS_ERROR])
				handler.onSuccess(result.rowCount);
		});
	}

	executeInsert(connection, statement, handler, idColumn) {

		const query = connection.query({
			text: statement + (idColumn ? ' RETURNING ' + idColumn : ''),
			rowMode: 'array'
		});

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		let id;
		query.on('row', row => {
			id = row[0];
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess(id);
		});
	}
}

module.exports = PostgreSQLDBDriver;
