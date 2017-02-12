'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const queryTree = require('./query-tree.js');


/**
 * The query.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class Query {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using
	 * [QueryFactory]{@link module:x2node-queries~QueryFactory}.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
	 * @param {string} topRecordTypeName Name of the top record type to fetch.
	 * @param {module:x2node-queries~QuerySpec} querySpec Query specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the underlying record types library is not
	 * suitable for it.
	 */
	constructor(dbDriver, topRecordTypeName, querySpec) {

		this._log = common.getDebugLogger('X2_QUERY');

		this._dbDriver = dbDriver;
		this._topRecordTypeName = topRecordTypeName;
		this._querySpec = querySpec;

		//...

		this._withSuperaggregates = false;

		const queryBuilders = new Array();
		this._assembleQueryBuilders(
			queryBuilders, true, queryTree.build(dbDriver, querySpec), false);
		this._queries = queryBuilders.map(queryBuilder => queryBuilder.toSQL());
	}

	/**
	 * Recursively process the query sub-tree and create necessary query
	 * builders.
	 *
	 * @private
	 * @param {Object[]} queryBuilders List of query builders representing the
	 * query branches. The method adds query builders to this list.
	 * @param {boolean} noExpansion <code>true</code> if no expanding subquery
	 * can be added to the current (the last) query builder in the query builders
	 * list. If specified query tree node if not scalar, new branch is created
	 * and added to the query builders list.
	 * @param {module:x2node-queries~QueryTreeNode} queryTreeNode Root node of
	 * the query tree subtree to process.
	 * @param {boolean} forceOuter <code>true<code> to force any tables
	 * associated with the query subtree to be joined using outer joins.
	 * @returns {boolean} <code>true</code> if after the method call the current
	 * query builder in the query builders list is expanding.
	 */
	_assembleQueryBuilders(
		queryBuilders, noExpansion, queryTreeNode, forceOuter) {

		// create branch if nessecary
		if (queryTreeNode.many && noExpansion)
			queryBuilders.push(this._createQueryBuilder(queryTreeNode));

		// get the current query builder
		let queryBuilder = queryBuilders[queryBuilders.length - 1];

		// add query tree node to the query builder
		this._addQueryTreeNodeToQueryBuilder(
			queryBuilder, queryTreeNode, forceOuter, false);

		// add query tree node's scalar children
		let noMoreChildExpansion = false;
		queryTreeNode.children.forEach(childQueryTreeNode => {
			if (!childQueryTreeNode.many)
				noMoreChildExpansion |= this._assembleQueryBuilders(
					queryBuilders, noMoreChildExpansion, childQueryTreeNode,
					(forceOuter || childQueryTreeNode.virtual));
		});

		// add query tree node's non-scalar children
		queryTreeNode.children.forEach(childQueryTreeNode => {
			if (childQueryTreeNode.many)
				noMoreChildExpansion |= this._assembleQueryBuilders(
					queryBuilders, noMoreChildExpansion, childQueryTreeNode,
					(forceOuter || childQueryTreeNode.virtual));
		});

		// tell the caller if adding the node made the query expanding
		return (queryTreeNode.many || noMoreChildExpansion);
	}

	/**
	 * Create new branch query builder. The new query builder represents a select
	 * of the keys from the specified query tree node's parent chain. The node
	 * itself is not added.
	 *
	 * @private
	 * @param {module:x2node-queries~QueryTreeNode} queryTreeNode Query tree
	 * node.
	 * @returns {Object} Query tree builder for the parent chain keys.
	 */
	_createQueryBuilder(queryTreeNode) {

		// create query builder object
		const queryBuilder = {
			select: [],
			from: null,
			orderBy: [],
			groupBy: [],
			where: null,

			toSQL() {
				return 'SELECT ' +
					this.select.join(', ') +
					' FROM ' + this.from +
					(this.where ? ' WHERE ' + this.where : '') +
					(this.orderBy.length > 0 ? ' ORDER BY ' + this.orderBy.join(
						', ') : '') +
					(this.groupBy.length > 0 ? ' GROUP BY ' + this.groupBy.join(
						', ') : '') +
					' LIMIT 10'; // TODO: remove limit
			}
		};

		// add parent chain to the query
		const parentTreeNodeChain = new Array();
		for (let n = queryTreeNode.parent; n; n = n.parent)
			parentTreeNodeChain.push(n);
		let forceOuter = false;
		for (let i = parentTreeNodeChain.length - 1; i >= 0; i--) {
			const n = parentTreeNodeChain[i];
			this._addQueryTreeNodeToQueryBuilder(
				queryBuilder, n, forceOuter, true);
			forceOuter |= n.virtual;
		}

		// return the new query builder
		return queryBuilder;
	}

	/**
	 * Add query tree node's information to the query (without descending to the
	 * node children).
	 *
	 * @private
	 * @param {Object} queryBuilder The query builder, to which to append the
	 * query tree node.
	 * @param {module:x2node-queries~QueryTreeNode} queryTreeNode Query tree node
	 * to add.
	 * @param {boolean} forceOuter Force joining the query tree node's table to
	 * the query builder using an outer join regardless of whether the node is
	 * virtual or not.
	 * @param {boolean} keyOnly Add only the keys from the query tree node to the
	 * query builder's SELECT clause.
	 */
	_addQueryTreeNodeToQueryBuilder(
		queryBuilder, queryTreeNode, forceOuter, keysOnly) {

		// add node properties to the SELECT list
		queryBuilder.select.push.apply(
			queryBuilder.select,
			(keysOnly ? queryTreeNode.keys : queryTreeNode.props).map(
				p => p.expr + ' AS ' + this._dbDriver.safeLabel(p.markup))
		);

		// add node table to the FROM clause
		if (queryTreeNode.parent) {
			queryBuilder.from += (
				(forceOuter || queryTreeNode.virtual) ?
					' LEFT OUTER' : ' INNER') + ' JOIN ' +
				queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias +
				' ON ' + queryTreeNode.tableAlias + '.' +
				queryTreeNode.joinByColumn + ' = ' +
				queryTreeNode.parent.tableAlias + '.' +
				queryTreeNode.joinToColumn;
		} else {
			queryBuilder.from =
				queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias;
		}

		// add parent key ORDER BY if collection
		if (queryTreeNode.many && queryTreeNode.parent)
			queryBuilder.orderBy.push(
				queryTreeNode.parent.tableAlias + '.' +
					queryTreeNode.parent.keyColumn);

		// add query ORDER BY if any
		if (queryTreeNode.order)
			queryTreeNode.order.forEach(orderSpec => {
				queryBuilder.orderBy.push(
					orderSpec.valueExpr.translate(
						propPath => queryTreeNode.propsToCols.get(propPath),
						null // TODO: funcResolvers
					) + (orderSpec.reverse ? ' DESC' : ''));
			});

		// TODO: add node's group and where
		//...
	}

	/**
	 * Execute the query.
	 *
	 * @param {*} connection The database connection compatible with the database
	 * driver.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {external:Promise.<Object>} The result promise. The result object
	 * has <code>records</code> and optionally <code>referredRecords</code>
	 * properties plus any requested super-aggregates.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(connection, filterParams) {

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// queue up pre-statements
		if (this._preStatements)
			this._preStatements.forEach(stmt => {
				resPromise = resPromise.then(
					() => new Promise((resolve, reject) => {
						const sql = this._replaceParams(stmt, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							connection, sql, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									reject(err);
								}
							}
						);
					}),
					err => Promise.reject(err)
				);
			});

		// queue up main queries
		this._queries.forEach((query, index) => {
			resPromise = resPromise.then(
				rootParser => new Promise((resolve, reject) => {
					let parser = rsparser.createResultSetParser(
						this._querySpec.recordTypes,
						this._topRecordTypeName
					);
					const sql = this._replaceParams(query, filterParams);
					this._log('executing SQL: ' + sql);
					this._dbDriver.execute(
						connection, sql, {
							onHeader(fieldNames) {
								parser.init(fieldNames);
							},
							onRow(row) {
								parser.feedRow(row);
							},
							onSuccess() {
								resolve(
									rootParser ? rootParser.merge(parser) :
										parser);
							},
							onError(err) {
								reject(err);
							}
						}
					);
				}),
				err => Promise.reject(err)
			);
		});

		// transform the parser into the result object
		resPromise = resPromise.then(
			rootParser => {
				const res = new Object();
				if (this._withSuperaggregates) {
					const superRec = rootParser.records[0];
					Object.keys(superRec).forEach(propName => {
						res[propName] = superRec[propName];
					});
				} else {
					res.records = rootParser.records;
				}
				const refRecs = rootParser.referredRecords;
				if (Object.keys(refRecs).length > 0)
					res.referredRecords = refRecs;
				return res;
			},
			err => Promise.reject(err)
		);

		// queue up post-statements
		if (this._postStatements)
			this._postStatements.forEach(stmt => {
				resPromise = resPromise.then(
					res => new Promise((resolve, reject) => {
						const sql = this._replaceParams(stmt, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							connection, sql, {
								onSuccess() {
									resolve(res);
								},
								onError(err) {
									reject(err);
								}
							}
						);
					}),
					err => Promise.reject(err)
				);
			});

		// return the result promise chain
		return resPromise;
	}

	/**
	 * Replace parameter placeholders in the specified SQL statement with the
	 * corresponding values.
	 *
	 * @private
	 * @param {string} stmtText SQL statement text with parameter placeholders.
	 * Each placeholder has format "${name}" where "name" is the parameter name.
	 * @param {Object.<string,*>} params Parameter values by parameter name.
	 * Array values are expanded into comma-separated lists of element values.
	 * @returns {string} SQL statement with parameters inserted.
	 * @throws {module:x2node-common.X2UsageError} If provided parameters object
	 * is invalid (missing parameter, NaN value).
	 */
	_replaceParams(stmtText, params) {

		let res = '';

		const re = new RegExp('(\'(?!\'))|(\')|\\?\\{([^}]+)\\}', 'g');
		let m, inLiteral = false, lastMatchIndex = 0;
		while ((m = re.exec(stmtText)) !== null) {
			res += stmtText.substring(lastMatchIndex, m.index);
			lastMatchIndex = re.lastIndex;
			const s = m[0];
			if (inLiteral) {
				res += s;
				if (m[1]) {
					inLiteral = false;
				} else if (m[2]) {
					re.lastIndex++;
				}
			} else {
				if (s === '\'') {
					res += s;
					inLiteral = true;
				} else {
					const paramName = m[3];
					const val = params[paramName];
					if (Array.isArray(val)) {
						val.forEach((valEl, index) => {
							if (index > 0)
								res += ', ';
							res += this._paramToSQL(paramName, valEl);
						});
					} else {
						res += this._paramToSQL(paramName, val);
					}
				}
			}
		}
		res += stmtText.substring(lastMatchIndex);

		return res;
	}

	/**
	 * Convert specified parameter value to SQL.
	 *
	 * @private
	 * @param {string} paramName Parameter name.
	 * @param {*} val Parameter value. If function, the function is called with
	 * no arguments and the result is used as the value.
	 * @returns {string} The SQL for the parameter value.
	 * @throws {module:x2node-common.X2UsageError} If provided value is invalid,
	 * such as <code>undefined</code> or <code>NaN</code>.
	 */
	_paramToSQL(paramName, val) {

		if ((typeof val) === 'function')
			val = val.call(null);

		if (val === undefined)
			throw new common.X2UsageError(
				'Missing query parameter ' + paramName + '.');

		const res = this._dbDriver(val);
		if (res === null)
			throw new common.X2UsageError(
				'Query parameter ' + paramName + ' has invalid value.');

		return res;
	}
}

module.exports = Query;
