'use strict';

const common = require('x2node-common');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const filterBuilder = require('./filter-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');


/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class DeleteDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon) {
		super(dbo, txOrCon);

		this._affectedRecordTypes = {};
	}

	deletedRows(stmtInd, numRows) {

		if (numRows > 0) {
			const recordTypeName = this._dbo._topRecordTypeDeletes[stmtInd];
			if (recordTypeName !== undefined) {
				if (this._affectedRecordTypes.hasOwnProperty(recordTypeName))
					this._affectedRecordTypes[recordTypeName] += numRows;
				else
					this._affectedRecordTypes[recordTypeName] = numRows;
			}
		}
	}

	getResult() {

		return this._affectedRecordTypes;
	}
}


/**
 * Delete database operation implementation (SQL <code>DELETE</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class DeleteDBO extends AbstractDBO {

	constructor(dbDriver, recordTypes, recordTypeDesc, filterSpec) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;
		this._recordTypeDesc = recordTypeDesc;

		// build the base properties tree
		const recordProps = new Set();
		this._collectRecordProperties('', recordTypeDesc, recordProps);
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const basePropsTree = propsTreeBuilder.buildSimplePropsTree(
			recordTypes, recordsPropDesc, 'delete', baseValueExprCtx,
			recordProps);

		// build filter and properties tree for it
		const filter = (filterSpec && filterBuilder.buildFilter(
			recordTypes, baseValueExprCtx, [ ':and', filterSpec ]));
		const filterPropsTree = (
			filter && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'where', baseValueExprCtx, '',
				filter.usedPropertyPaths, {
					noWildcards: true,
					noAggregates: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);

		// build combined properties tree
		const propsTree = (
			filter ? basePropsTree.combine(filterPropsTree) : basePropsTree);

		// build delete statements
		this._deletes = new Array();

		// decide what strategy to use and build initial table chain
		let queryTree, useFilter;
		if (!filter || (
			(filter.usedPropertyPaths.size === 1) &&
				filter.usedPropertyPaths.has(recordTypeDesc.idPropertyName)) ||
			this._isSingleDelete(recordTypeDesc)) {

			// use filter in the deletes
			useFilter = true;

			// build query tree
			queryTree = queryTreeBuilder.forDirectQuery(
				dbDriver, recordTypes, 'delete', false, propsTree);

		} else { // use anchor table

			// filter is used in the ids query
			useFilter = false;

			// build ids only query tree
			const idsQueryTree = queryTreeBuilder.forIdsOnlyQuery(
				dbDriver, recordTypes, propsTree);

			// assemble ids SELECT statement
			const idsQuery = idsQueryTree.assembleSelect(
				filter, null, this._paramsHandler);

			// create anchor table
			const anchorTable = 'q_' + idsQueryTree.table;
			dbDriver.makeSelectIntoTempTable(
				idsQuery, anchorTable,
				this._preStatements, this._postStatements);

			// build anchored query tree
			queryTree = queryTreeBuilder.forAnchoredQuery(
				dbDriver, recordTypes, 'delete', false, propsTree, anchorTable);
		}

		// translate the filter for the WHERE clauses
		const translationCtx = queryTree.getTopTranslationContext(
			this._paramsHandler);
		const filterExpr = (
			useFilter && filter && filter.translate(translationCtx));
		const filterExprParen = (
			useFilter && filter && filter.needsParen('AND'));

		// walk the tree and build the delete statements
		const refTables = new Array();
		const usedRefs = new Set();
		this._topRecordTypeDeletes = new Array();
		queryTree.walk(translationCtx, (propNode, tableDesc) => {
			if ((propNode.path !== '') && propNode.isUsedIn('where') &&
				!propNode.isUsedIn('delete')) {
				refTables.push(tableDesc);
				usedRefs.add(tableDesc.tableAlias);
			}
		}).walkReverse(translationCtx, (propNode, tableDesc, tableChain) => {
			if (propNode.isUsedIn('delete')) {
				if (tableChain.length > 0)
					tableChain[0].joinCondition = tableDesc.joinCondition;
				const childrenContainer = propNode.childrenContainer;
				if ((childrenContainer !== null) &&
					childrenContainer.isRecordType()) {
					this._topRecordTypeDeletes[this._deletes.length] =
						childrenContainer.recordTypeName;
				}
				this._deletes.push(dbDriver.buildDeleteWithJoins(
					tableDesc.tableName, tableDesc.tableAlias,
					refTables.concat(tableChain.filter(td => !usedRefs.has(
						td.tableAlias))),
					filterExpr, filterExprParen
				));
			}
		});
	}

	/**
	 * Recursively collect all property paths to include in the query tree to
	 * cover all tables involved in the records deletion.
	 *
	 * @private
	 * @param {string} basePathPrefix Path prefix, empty or ending with a dot.
	 * @param {module:x2node-records~PropertiesContainer} container Properties
	 * container to recursively process.
	 * @param {Set.<string>} recordProps Set, to which to add identified property
	 * paths.
	 */
	_collectRecordProperties(basePathPrefix, container, recordProps) {

		let hasThisLevel = false;

		if (container.idPropertyName) {
			recordProps.add(basePathPrefix + container.idPropertyName);
			hasThisLevel = true;
		}

		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.isView() || propDesc.isCalculated())
				continue;
			if ((propDesc.scalarValueType === 'object') || (
				propDesc.reverseRefPropertyName &&
					!propDesc.isWeakDependency())) {
				this._collectRecordProperties(
					basePathPrefix + propName + '.', propDesc.nestedProperties,
					recordProps);
			} else if (propDesc.table || !hasThisLevel) {
				recordProps.add(basePathPrefix + propName);
				hasThisLevel = true;
			}
		}
	}

	/**
	 * Tell if the operation can be completed in a single <code>DELETE</code>
	 * statement.
	 *
	 * @private
	 * @param {module:x2node-records~PropertiesContainer} container Properties
	 * container to recursively process.
	 * @returns {boolean} <code>true</code> if single statement delete operation.
	 */
	_isSingleDelete(container) {

		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.table || (
				propDesc.reverseRefPropertyName && propDesc.isWeakDependency()))
				return false;
			if ((propDesc.scalarValueType === 'object') &&
				!this._isSingleDelete(propDesc.nestedProperties))
				return false;
		}

		return true;
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<Object>} The result promise, which resolves to the
	 * result object. The result object's properties are names of record types,
	 * records of which were actually deleted, and the values are the numbers of
	 * records of the type deleted (zeros are not included). The promise is
	 * rejected with the error object of an error happens during the operation
	 * execution.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, filterParams) {

		// create operation execution context
		const ctx = new DeleteDBOExecutionContext(this, txOrCon);

		// create parameters resolver function
		const paramsResolver = paramRef => this._paramsHandler.paramSql(
			this._dbDriver, filterParams, paramRef);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up pre-statements
		resPromise = this._executePreStatements(
			resPromise, ctx, paramsResolver);

		// queue up the deletes
		this._deletes.forEach((stmt, stmtInd) => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					let sql;
					try {
						sql = this._replaceParams(stmt, paramsResolver);
						this._log('executing SQL: ' + sql);
						this._dbDriver.executeUpdate(
							ctx.connection, sql, {
								onSuccess(affectedRows) {
									ctx.deletedRows(stmtInd, affectedRows);
									resolve();
								},
								onError(err) {
									common.error(
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || stmt) + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// queue up post-statements
		resPromise = this._executePostStatements(
			resPromise, ctx, paramsResolver);

		// finish transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// build the final result object
		resPromise = resPromise.then(
			() => ctx.getResult(),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}
}

// export the class
module.exports = DeleteDBO;
