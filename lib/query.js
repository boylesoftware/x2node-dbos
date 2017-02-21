'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const queryTreeBuilder = require('./query-tree-builder.js');
const Transaction = require('./transaction.js');


const VALUE_EXPR_FUNCS = {
	//...
};

class FilterParamsHandler {

	constructor() {

		this._nextParamRef = 0;

		this._params = new Map();
	}

	addParam(paramName, valueFunc) {

		const paramRef = String(this._nextParamRef++);

		this._params.set(paramRef, {
			name: paramName,
			valueFunc: valueFunc
		});

		return '?{' + paramRef + '}';
	}

	paramValueToSql(dbDriver, paramValue, valueFunc, paramName) {

		function invalid() {
			new common.X2UsageError(
				'Invalid ' + (paramName ? '"' + paramName + '"' : '') +
					' filter test parameter value ' + String(paramValue) +
					'.');
		}

		let preSql;
		if (valueFunc) {
			if ((paramValue === undefined) || (paramValue === null) || (
				((typeof paramValue) === 'number') &&
					!Number.isFinite(paramValue)) ||
				Array.isArray(paramValue))
				throw invalid();
			preSql = valueFunc(paramValue);
		} else {
			preSql = paramValue;
		}

		const sql = dbDriver.sql(preSql);
		if ((sql === null) || (sql === 'NULL'))
			throw invalid();

		return sql;
	}

	paramSql(dbDriver, filterParams, paramRef) {

		const ref = this._params.get(paramRef);
		const filterParam = filterParams[ref.name];
		if (filterParam === undefined)
			throw new common.X2UsageError(
				'Missing filter parameter "' + ref.name + '".');

		function process(v) {
			return paramValueToSql(dbDriver, v, ref.valueFunc, param.name);
		}

		return (
			Array.isArray(filterParam) ?
				filterParam.map(v => process(v)).join(', ') :
				process(v)
		);
	}
}

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
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} recordTypeName Name of the top record type to fetch.
	 * @param {module:x2node-queries~QuerySpec} [?recordsQuerySpec] Query
	 * specification for fetching the records. If not specified or
	 * <code>null</code>, records are not fetched.
	 * @param {Symbol} superTypeName Name of the top record type's supertype.
	 * @param {module:x2node-queries~QuerySpec} [superPropsQuerySpec] Query
	 * specification for fetching super-properties. If not specified or
	 * <code>null</code>, no super-properties are fetched.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the underlying record types library is not
	 * suitable for it.
	 */
	constructor(
		dbDriver, recordTypes, recordTypeName, recordsQuerySpec, superTypeName,
		superPropsQuerySpec) {

		this._log = common.getDebugLogger('X2_QUERY');

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._recordTypeName = recordTypeName;
		this._recordsQuerySpec = recordsQuerySpec;
		this._superTypeName = superTypeName;
		this._superPropsQuerySpec = superPropsQuerySpec;

		// filter parameters handler
		this._paramsHandler = new FilterParamsHandler();

		// build super properties queries
		this._superPropsQueries = new Array();
		if (superPropsQuerySpec) {
			// TODO: ...
		}

		// records queries
		this._preStatements = new Array();
		this._queries = new Array();
		this._postStatements = new Array();

		// helper functions
		function combineWithFilter(propsTree, querySpec) {
			if (!querySpec.filter)
				return propsTree;
			const propTrees = propsTree.combine(
				querySpec.filterPropsTree).debranch();
			if (propTrees.length > 1)
				throw new common.X2UsageError(
					'The query filter and/or order uses properties' +
						' that lay on different collection axises.');
			return propTrees[0];
		}
		function combineWithOrder(propsTree, querySpec) {
			if (!querySpec.order)
				return propsTree;
			const propTrees = propsTree.combine(
				querySpec.orderPropsTree).debranch();
			if (propTrees.length > 1)
				throw new common.X2UsageError(
					'The query filter and/or order uses properties' +
						' that lay on different collection axises.');
			return propTrees[0];
		}

		// build the records queries
		if (recordsQuerySpec) {

			// check if multiple branches
			const selectedPropsTrees = recordsQuerySpec.selectedPropsTrees;
			if (selectedPropsTrees.length > 1) {

				// check if ranged
				if (recordsQuerySpec.range) {

					// add filter and order properties to the tree
					let selectedPropsTree = combineWithFilter(
						selectedPropsTrees[0], recordsQuerySpec);
					selectedPropsTree = combineWithOrder(
						selectedPropsTree, recordsQuerySpec);

					// build ids query tree
					const idsQueryTree = queryTreeBuilder.buildIdsOnly(
						dbDriver, recordTypes, selectedPropsTree);

					// assemble ranged ids query
					const idsQuery = dbDriver.makeRangedSelect(
						this._assembleQuery(
							idsQueryTree, recordsQuerySpec.filter,
							recordsQuerySpec.order),
						recordsQuerySpec.range.offset,
						recordsQuerySpec.range.limit);

					// create anchor table
					const anchorTable = 'q_' + idsQueryTree.table;
					dbDriver.makeSelectIntoTempTable(
						idsQuery, anchorTable,
						this._preStatements, this._postStatements);

					// build branch queries
					selectedPropsTrees.forEach(selectedPropsBranch => {

						// add order properties to the tree
						selectedPropsBranch = combineWithOrder(
							selectedPropsBranch, recordsQuerySpec);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectedPropsBranch,
							anchorTable);

						// assemble the query
						this._queries.push(this._assembleQuery(
							queryTree, null, recordsQuerySpec.order));
					});

				} else { // multi-branch, not ranged

					// add filter properties to the tree
					let selectedPropsTree = combineWithFilter(
						selectedPropsTrees[0], recordsQuerySpec);

					// build ids query tree
					const idsQueryTree = queryTreeBuilder.buildIdsOnly(
						dbDriver, recordTypes, selectedPropsTree);

					// assemble ids query
					const idsQuery = this._assembleQuery(
						idsQueryTree, recordsQuerySpec.filter, null);

					// create anchor table
					const anchorTable = 'q_' + idsQueryTree.table;
					dbDriver.makeSelectIntoTempTable(
						idsQuery, anchorTable,
						this._preStatements, this._postStatements);

					// build branch queries
					selectedPropsTrees.forEach(selectedPropsBranch => {

						// add order properties to the tree
						selectedPropsBranch = combineWithOrder(
							selectedPropsBranch, recordsQuerySpec);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectedPropsBranch,
							anchorTable);

						// assemble the query
						this._queries.push(this._assembleQuery(
							queryTree, null, recordsQuerySpec.order));
					});
				}

			} else { // single branch

				// get the single tree
				let selectedPropsTree = selectedPropsTrees[0];

				// add filter and order properties to the tree
				selectedPropsTree = combineWithFilter(
					selectedPropsTree, recordsQuerySpec);
				selectedPropsTree = combineWithOrder(
					selectedPropsTree, recordsQuerySpec);

				// check if ranged
				if (recordsQuerySpec.range) {

					// check if expanding children
					if (selectedPropsTree.hasExpandingChild()) {

						// build ids query tree
						const idsQueryTree = queryTreeBuilder.buildIdsOnly(
							dbDriver, recordTypes, selectedPropsTree);

						// assemble ranged ids query
						const idsQuery = dbDriver.makeRangedSelect(
							this._assembleQuery(
								idsQueryTree, recordsQuerySpec.filter,
								recordsQuerySpec.order),
							recordsQuerySpec.range.offset,
							recordsQuerySpec.range.limit);

						// create anchor table
						const anchorTable = 'q_' + idsQueryTree.table;
						dbDriver.makeSelectIntoTempTable(
							idsQuery, anchorTable,
							this._preStatements, this._postStatements);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectedPropsTree,
							anchorTable);

						// assemble the query
						this._queries.push(this._assembleQuery(
							queryTree, null, recordsQuerySpec.order));

					} else { // single branch, ranged, not expanding

						// build query tree
						const queryTree = queryTreeBuilder.buildDirect(
							dbDriver, recordTypes, selectedPropsTree);

						// assemble the query
						const query = this._assembleQuery(
							queryTree, recordsQuerySpec.filter,
							recordsQuerySpec.order);

						// make ranged
						this._queries.push(dbDriver.makeRangedSelect(
							query, recordsQuerySpec.range.offset,
							recordsQuerySpec.range.limit));
					}

				} else { // single branch, not ranged

					// build query tree
					const queryTree = queryTreeBuilder.buildDirect(
						dbDriver, recordTypes, selectedPropsTree);

					// assemble the query
					this._queries.push(this._assembleQuery(
						queryTree, recordsQuerySpec.filter,
						recordsQuerySpec.order));
				}
			}
		}
	}

	_assembleQuery(queryTree, topFilter, topOrder) {

		// create query builder
		const queryBuilder = {
			select: new Array(),
			from: null,
			where: null,
			orderBy: new Array(),
			groupBy: new Array()
		};

		// properties resolver function
		const propsSql = queryTree.propsSql;
		function propsResolver(propPath) {
			return propsSql.get(propPath);
		}

		// add top filter if any
		if (topFilter)
			queryBuilder.where = topFilter.translate(
				this._dbDriver, propsResolver, VALUE_EXPR_FUNCS,
				this._paramsHandler);

		// add top order if any
		if (topOrder)
			topOrder.forEach(orderSpec => {
				queryBuilder.push(
					orderSpec.valueExpr.translate(
						propsResolver, VALUE_EXPR_FUNCS
					) + (orderSpec.isReverse() ? ' DESC' : ''));
			});

		// process query tree nodes
		this._addQueryTreeNodeToQuery(
			queryBuilder, null, queryTree, propsResolver);

		// assemble the query and return it
		return 'SELECT ' +
			queryBuilder.select.join(', ') +
			' FROM ' + queryBuilder.from +
			(
				queryBuilder.where ?
					' WHERE ' + queryBuilder.where : ''
			) +
			(
				queryBuilder.orderBy.length > 0 ?
					' ORDER BY ' + queryBuilder.orderBy.join(', ') : ''
			) +
			(
				queryBuilder.groupBy.length > 0 ?
					' GROUP BY ' + queryBuilder.groupBy.join(', ') : ''
			);
	}

	_addQueryTreeNodeToQuery(
		queryBuilder, parentQueryTreeNode, queryTreeNode, propsResolver) {

		// add SELECT clause elements
		queryTreeNode.select.forEach(s => {
			queryBuilder.select.push((
				(typeof s.sql) === 'function' ?
					s.sql(
						this._dbDriver, propsResolver, VALUE_EXPR_FUNCS,
						this._paramsHandler) :
					s.sql
			) + ' AS ' + this._dbDriver.safeLabel(s.markup));
		});

		// add node to the FROM chain
		if (parentQueryTreeNode) {
			let joinCondition =
				queryTreeNode.tableAlias + '.' + queryTreeNode.joinByColumn +
				' = ' + parentQueryTreeNode.tableAlias + '.' +
				queryTreeNode.joinToColumn;
			if (queryTreeNode.joinCondition) {
				// TODO: add scoped join condition
			}
			queryBuilder.from +=
				(queryTreeNode.isVirtual() ? ' LEFT OUTER' : ' INNER') +
				' JOIN ' + queryTreeNode.table + ' AS ' +
				queryTreeNode.tableAlias + ' ON ' + joinCondition;
		} else { // top node
			queryBuilder.from =
				queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias;
		}

		// add order
		queryTreeNode.order.forEach(o => {
			queryBuilder.orderBy.push((
				(typeof o.sql) === 'function' ?
					o.sql(
						this._dbDriver, propsResolver, VALUE_EXPR_FUNCS,
						this._paramsHandler) :
					o.sql
			) + (o.reverse ? ' DESC' : ''));
		});

		// add children
		queryTreeNode.children.forEach(childNode => {
			this._addQueryTreeNodeToQuery(
				queryBuilder, queryTreeNode, childNode, propsResolver)
		});
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
	/*_assembleQueryBuilders(
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
	}*/

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
	/*_createQueryBuilder(queryTreeNode) {

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
	}*/

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
	/*_addQueryTreeNodeToQueryBuilder(
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
					) + (orderSpec.isReverse() ? ' DESC' : ''));
			});

		// TODO: add node's group and where
		//...
	}*/

	/**
	 * Execute the query.
	 *
	 * @param {(module:x2node-queries~Transaction|*)} tx The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the query execution.
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
	execute(tx, filterParams) {

		// part of a transaction?
		const hasTx = (tx instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !tx.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// create query execution context
		const ctx = {
			connection: (hasTx ? tx.connection: tx),
			superPropsParser: null,
			recordsParser: null,
			executePostStatements: false
		};

		// determine if needs to be wrapped in a transaction
		const wrapInTx = (
			!hasTx && (
				(
					this._superPropsQueries.length +
						this._preStatements.length +
						this._queries.length +
						this._postStatements.length
				) > 1)
		);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (wrapInTx)
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						this._log('starting transaction');
						this._dbDriver.startTransaction(
							ctx.connection, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error starting transaction', err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error('error starting transaction', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);

		// queue up super property queries
		this._superPropsQueries.forEach(query => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const parser = rsparser.createResultSetParser(
							this._recordTypes, this._superTypeName);
						const sql = this._replaceParams(query, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onHeader(fieldNames) {
									parser.init(fieldNames);
								},
								onRow(row) {
									parser.feedRow(row);
								},
								onSuccess() {
									if (ctx.superPropsParser)
										ctx.superPropsParser.merge(parser);
									else
										ctx.superPropsParser = parser;
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + query + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// always execute post-statement after this point
		resPromise = resPromise.then(
			() => {
				ctx.executePostStatements = true;
			},
			err => Promise.reject(err)
		);

		// queue up pre-statements
		this._preStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const sql = this._replaceParams(stmt, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + stmt + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// queue up main queries
		this._queries.forEach(query => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const parser = rsparser.createResultSetParser(
							this._recordTypes, this._recordTypeName);
						const sql = this._replaceParams(query, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onHeader(fieldNames) {
									parser.init(fieldNames);
								},
								onRow(row) {
									parser.feedRow(row);
								},
								onSuccess() {
									if (ctx.recordsParser)
										ctx.recordsParser.merge(parser);
									else
										ctx.recordsParser = parser;
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + query + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// queue up post-statements
		const executePostStatement = stmt => new Promise((resolve, reject) => {
			if (!ctx.executePostStatements)
				return resolve();
			try {
				const sql = this._replaceParams(stmt, filterParams);
				this._log('executing SQL: ' + sql);
				this._dbDriver.execute(
					ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(
								'error executing query [' + sql + ']', err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error('error executing query [' + stmt + ']', err);
				reject(err);
			}
		});
		this._postStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => executePostStatement(stmt),
				err => executePostStatement(stmt).then(
					() => Promise.reject(err),
					() => Promise.reject(err)
				)
			);
		});

		// finish transaction if necessary
		if (wrapInTx)
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						this._log('committing transaction');
						this._dbDriver.commitTransaction(
							ctx.connection, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error committing transaction', err);
									this._log(
										'rolling back transaction after' +
											' failed commit');
									this._dbDriver.rollbackTransaction(
										ctx.connection, {
											onSuccess() {
												reject(err);
											},
											onError(rollackError) {
												common.error(
													'error rolling transaction' +
														' back after failed' +
														' commit',
													rollbackError);
												reject(err);
											}
										}
									);
								}
							}
						);
					} catch (err) {
						common.error('error committing transaction', err);
						this._log(
							'rolling back transaction after failed commit');
						this._dbDriver.rollbackTransaction(
							ctx.connection, {
								onSuccess() {
									reject(err);
								},
								onError(rollackError) {
									common.error(
										'error rolling transaction back after' +
											' failed commit', rollbackErr);
									reject(err);
								}
							}
						);
					}
				}),
				err => new Promise((resolve, reject) => {
					try {
						this._log('rolling back transaction');
						this._dbDriver.rollbackTransaction(
							ctx.connection, {
								onSuccess() {
									reject(err);
								},
								onError(rollbackErr) {
									common.error(
										'error rolling transaction back',
										rollbackErr);
									reject(err);
								}
							}
						);
					} catch (rollnackErr) {
						common.error(
							'error rolling transaction back', rollbackErr);
						reject(err);
					}
				})
			);

		// build the final result object
		resPromise = resPromise.then(
			() => {
				const res = new Object();
				if (ctx.superPropsParser) {
					const superRec = ctx.superPropsParser.records[0];
					for (let superPropName in superRec)
						res[superPropName] = superRec[superPropName];
				} else {
					res.recordTypeName = this._recordTypeName;
				}
				if (ctx.recordsParser) {
					res.records = ctx.recordsParser.records;
					const refRecs = ctx.recordsParser.referredRecords;
					if (Object.keys(refRecs).length > 0)
						res.referredRecords = refRecs;
				}
				return res;
			},
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}

	/**
	 * Replace parameter placeholders in the specified SQL statement with the
	 * corresponding values.
	 *
	 * @private
	 * @param {string} stmtText SQL statement text with parameter placeholders.
	 * Each placeholder has format "?{ref}" where "ref" is the parameter
	 * reference in the query's filter parameters handler.
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
					res += this._paramsHandler.paramSql(
						this._dbDriver, filterParams, m[3]);
					/*const paramName = m[3];
					const val = params[paramName];
					if (Array.isArray(val)) {
						val.forEach((valEl, index) => {
							if (index > 0)
								res += ', ';
							res += this._paramToSQL(paramName, valEl);
						});
					} else {
						res += this._paramToSQL(paramName, val);
					}*/
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
	/*_paramToSQL(paramName, val) {

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
	}*/
}

module.exports = Query;
