'use strict';

const BasicDBDriver = require('./basic-driver.js');


/**
 * Symbol used to store the "done()" callback on the pooled database connection.
 *
 * @private
 * @constant {Symbol}
 */
const DONE = Symbol();

/**
 * Symbol used to mark a connection that must be destroyed upon release no matter
 * what.
 *
 * @private
 * @constant {Symbol}
 */
const DESTROY = Symbol();


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

	makeSelectWithLocks(selectStmt, exclusiveLockTables, sharedLockTables) {

		return selectStmt + (
			exclusiveLockTables && (exclusiveLockTables.length > 0) ?
				' FOR UPDATE OF ' + exclusiveLockTables.map(
					t => t.tableAlias).join(', ') : ''
		) + (
			sharedLockTables && (sharedLockTables.length > 0) ?
				' FOR SHARE OF ' + sharedLockTables.map(
					t => t.tableAlias).join(', ') : ''
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

	buildUpsert(tableName, insertColumns, insertValues, uniqueColumn, sets) {

		return `INSERT INTO ${tableName} (${insertColumns})` +
			` VALUES (${insertValues}) ON CONFLICT ${uniqueColumn} DO UPDATE` +
			` SET ${sets}`;
	}

	connect(source, handler) {

		if (/.*Pool$/.test(source.constructor.name)) {
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
			connection[DONE](err || connection[DESTROY]);
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

		this._finishTransaction(connection, 'ROLLBACK', handler);
	}

	commitTransaction(connection, handler) {

		this._finishTransaction(connection, 'COMMIT', handler);
	}

	_finishTransaction(connection, command, handler) {

		const trace = (handler.trace || function() {});

		trace(command);
		connection.query(command, err => {
			if (err) {
				connection[DESTROY] = true;
				handler.onError(err);
			} else {
				handler.onSuccess();
			}
		});
	}

	setSessionVariable(connection, varName, valueExpr, handler) {

		connection.query(`SET SESSION ${varName} TO ${valueExpr}`, err => {

			if (err)
				handler.onError(err);
			else
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

	selectIntoAnchorTable(
		connection, anchorTableName, topTableName, idColumnName, idExpr,
		statementStump, handler) {

		const trace = (handler.trace || function() {});

		let sql;
		trace(
			sql = `CREATE TEMPORARY TABLE ${anchorTableName} (id, ord)` +
				` ON COMMIT DROP AS WITH ids AS (` +
				statementStump.replace(
					/\bSELECT\s+\{\*\}\s+FROM\b/i,
					`SELECT ${idExpr} AS id FROM`
				) + ') SELECT id, ROW_NUMBER() OVER () AS ord FROM ids'
		);
		connection.query(sql, (err, result) => {

			if (err)
				return handler.onError(err);

			const rowCount = result.rowCount;

			if (rowCount > 1) {
				trace(sql = `CREATE UNIQUE INDEX ON ${anchorTableName} (id)`);
				connection.query(sql, err => {

					if (err)
						return handler.onError(err);

					trace(
						sql = `CREATE UNIQUE INDEX ON ${anchorTableName} (ord)`);
					connection.query(sql, err => {

						if (err)
							return handler.onError(err);

						handler.onSuccess(rowCount);
					});
				});
			} else {
				handler.onSuccess(rowCount);
			}
		});
	}

	executeQuery(connection, statement, handler) {

		const querySpec = {
			text: statement
		};
		if (!handler.noRowsAsArrays)
			querySpec.rowMode = 'array';

		connection.query(querySpec, (err, result) => {

			if (err)
				return handler.onError(err);

			let success = false;
			if (result && result.rows) {

				try {

					if (handler.onHeader)
						handler.onHeader(result.fields.map(field => field.name));

					const onRow = handler.onRow;
					if (onRow)
						for (let row of result.rows)
							onRow(row);

					success = true;

				} catch (handlerErr) {
					handler.onError(handlerErr);
				}

			} else {
				success = true;
			}

			if (success)
				handler.onSuccess();
		});
	}

	executeUpdate(connection, statement, handler) {

		connection.query(statement, (err, result) => {

			if (err)
				handler.onError(err);
			else
				handler.onSuccess(result.rowCount);
		});
	}

	executeInsert(connection, statement, handler, idColumn) {

		connection.query({
			text: statement + (idColumn ? ' RETURNING ' + idColumn : ''),
			rowMode: 'array'
		}, (err, result) => {

			if (err)
				handler.onError(err);
			else
				handler.onSuccess(result.rows[0][0]);
		});
	}

	createVersionTableIfNotExists(connection, tableName, itemNames, handler) {

		const trace = (handler.trace || function() {});
		let sql;
		trace(
			sql = `CREATE TABLE IF NOT EXISTS ${tableName} (` +
				'name VARCHAR(64) PRIMARY KEY, ' +
				'modified_on TIMESTAMP NOT NULL, ' +
				'version INTEGER NOT NULL)'
		);

		connection.query(sql, err => {

			if (err)
				return handler.onError(err);

			trace(sql = 'BEGIN');
			connection.query(sql, err => {

				if (err)
					return handler.onError(err);

				trace(sql = `LOCK TABLE ${tableName} IN ACCESS EXCLUSIVE MODE`);
				connection.query(sql, err => {

					if (err) {
						trace(sql = 'ROLLBACK');
						return connection.query(
							sql, () => handler.onError(err));
					}

					trace(sql = `SELECT name FROM ${tableName}`);
					connection.query(sql, (err, result) => {

						if (err) {
							trace(sql = 'ROLLBACK');
							return connection.query(
								sql, () => handler.onError(err));
						}

						const namesToInsert = new Set(itemNames);
						for (let i = 0, len = result.rows.length; i < len; i++)
							namesToInsert.delete(result.rows[i].name);
						if (namesToInsert.size > 0) {
							trace(
								sql = `INSERT INTO ${tableName}` +
									' (name, modified_on, version) VALUES ' +
									Array.from(namesToInsert).map(name => (
										'(' + this.stringLiteral(name) +
											', CURRENT_TIMESTAMP, 0)'
									)).join(', ')
							);
							connection.query(sql, err => {
								if (err) {
									trace(sql = 'ROLLBACK');
									return connection.query(
										sql, () => handler.onError(err));
								}
								trace(sql = 'COMMIT');
								connection.query(sql, err => {
									if (err)
										return handler.onError(err);
									handler.onSuccess();
								});
							});
						} else {
							trace(sql = 'COMMIT');
							connection.query(sql, err => {
								if (err)
									return handler.onError(err);
								handler.onSuccess();
							});
						}
					});
				});
			});
		});
	}

	updateVersionTable(
		connection, tableName, itemNames, modificationTimestamp, handler) {

		const filterExpr = 'name' + (
			itemNames.length === 1 ?
				' = ' + this.stringLiteral(itemNames[0]) :
				' IN (' + itemNames.map(v => this.stringLiteral(v)).join(', ') +
					')'
		);

		const trace = (handler.trace || function() {});
		let sql;
		trace(
			sql = `UPDATE ${tableName} SET ` +
				`modified_on = ${this.datetimeLiteral(modificationTimestamp)}, ` +
				`version = version + 1 WHERE ${filterExpr}`
		);
		connection.query(sql, (err, result) => {

			if (err)
				return handler.onError(err);

			if (result.rowCount !== itemNames.length)
				return handler.onError(new Error(
					'Version rows are missing for some of the following' +
						' record types: ' + itemNames.join(', ')));

			return handler.onSuccess();
		});
	}
}

module.exports = PostgreSQLDBDriver;
