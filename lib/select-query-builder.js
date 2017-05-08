'use strict';


/**
 * A fully assembled SQL <code>SELECT</code> query.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class SelectQuery {

	/**
	 * Assemble new query.
	 *
	 * @private
	 * @param {module:x2node-dbos~QueryTreeNode} queryTree Query tree.
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {?module:x2node-dbos~RecordsFilter} [filter] Filter to generate
	 * the <code>WHERE</code> clause.
	 * @param {?module:x2node-dbos~RecordsOrder} [order] Order to generate the
	 * <code>ORDER BY</code> clause.
	 */
	constructor(queryTree, ctx, filter, order) {

		// set up initial query clauses
		this._select = new Array();
		this._from = null;
		this._where = (filter ? filter.translate(ctx) : null);
		this._groupBy = null;
		this._orderBy = new Array();
		if (order)
			for (let orderElement of order.elements)
				this._orderBy.push(orderElement.translate(ctx));

		// initial has aggregates flag
		this._hasAggregates = false;

		// proper and referred tables collections
		this._properTables = new Array();
		this._referredTables = new Array();

		// process the query tree
		queryTree.walk(ctx, (propNode, tableDesc, tableChain) => {

			// mark query as having aggregates if aggregated
			if (tableDesc.aggregated)
				this._hasAggregates = true;

			// add SELECT clause elements
			tableDesc.selectElements.forEach(s => {
				this._select.push({
					valueExpr: s.valueExpr,
					label: ctx.dbDriver.safeLabel(s.markup)
				});
			});

			// add node to the FROM chain
			const isTopNode = (tableChain.length === 0);
			if (isTopNode) {
				this._from = tableDesc.tableName + ' AS ' + tableDesc.tableAlias;
			} else {
				this._from +=
					(tableDesc.outerJoin ? ' LEFT OUTER' : ' INNER') + ' JOIN ' +
					tableDesc.tableName + ' AS ' + tableDesc.tableAlias +
					' ON ' + tableDesc.joinCondition;
			}

			// add node's table to the appropriate tables group for locking
			const tableInfo = {
				tableName: tableDesc.tableName,
				tableAlias: tableDesc.tableAlias,
			};
			if (tableDesc.referred)
				this._referredTables.push(tableInfo);
			else
				this._properTables.push(tableInfo);

			// add groupping
			let topAggregate = false;
			if (tableDesc.groupByElements) {
				topAggregate = true;
				this._groupBy = (
					this._orderBy[0] === 'q.ord' ?
						[ 'q.ord' ].concat(tableDesc.groupByElements) :
						tableDesc.groupByElements
				);
			}

			// add order
			if (!tableDesc.aggregated || topAggregate)
				tableDesc.orderByElements.forEach(o => {
					this._orderBy.push(o);
				});
		});

		// weed out repeats in the order
		const seen = new Set();
		this._orderBy = this._orderBy.filter(o => {
			const v = o.match(/^(.+?)(?:\s+(?:asc|desc))?$/i)[1];
			return (seen.has(v) ? false : (seen.add(v), true));
		});
	}

	/**
	 * Get tables used by the query in two groups appropriate for the specified
	 * locking.
	 *
	 * @param {?string} lockType Lock type: "exclusive" or "shared".
	 * @param {Array.<Object>} exclusiveLockTables Array, to which to add tables
	 * for exclusive locking.
	 * @param {Array.<Object>} sharedLockTables Array, to which to add tables for
	 * shared locking.
	 */
	getTablesForLock(lockType, exclusiveLockTables, sharedLockTables) {

		let merged;
		switch (lockType) {
		case 'exclusive':
			this._properTables.forEach(t => { exclusiveLockTables.push(t); });
			this._referredTables.forEach(t => { sharedLockTables.push(t); });
			break;
		case 'shared':
			merged = new Map();
			this._properTables.forEach(t => { merged.set(t.tableAlias, t); });
			this._referredTables.forEach(t => { merged.set(t.tableAlias, t); });
			for (let t of merged.values())
				sharedLockTables.push(t);
		}
	}

	/**
	 * Get SQL value expression for the top record id returned by the query.
	 *
	 * @returns {string} SQL value expression.
	 */
	getIdValueExpr() {

		return this._select[0].valueExpr;
	}

	/**
	 * Tell if the query has any aggregates in it.
	 *
	 * @returns {boolean} <code>true</code> if has aggregates.
	 */
	hasAggregates() {

		return this._hasAggregates;
	}

	/**
	 * Get the query SQL.
	 *
	 * @param {boolean} [stumpOnly] If <code>true</code>, the select list of the
	 * returned query is replaced with "{*}" (for further substitution).
	 * @returns {string} The query SQL.
	 */
	toSql(stumpOnly) {

		return 'SELECT ' +
			(
				stumpOnly ?
					'{*}' :
					this._select.map(
						s => `${s.valueExpr} AS ${s.label}`).join(', ')
			) +
			' FROM ' + this._from +
			(
				this._where ?
					' WHERE ' + this._where : ''
			) +
			(
				this._groupBy && (this._groupBy.length > 0) ?
					' GROUP BY ' + this._groupBy.join(', ') : ''
			) +
			(
				this._orderBy.length > 0 ?
					' ORDER BY ' + this._orderBy.join(', ') : ''
			);
	}
}


/**
 * Assemble <code>SELECT</code> query.
 *
 * @protected
 * @param {module:x2node-dbos~QueryTreeNode} queryTree Query tree.
 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
 * @param {?module:x2node-dbos~RecordsFilter} [filter] Filter to generate the
 * <code>WHERE</code> clause.
 * @param {?module:x2node-dbos~RecordsOrder} [order] Order to generate the
 * <code>ORDER BY</code> clause.
 * @returns {module:x2node-dbos~SelectQuery} The query.
 */
exports.assembleSelect = function(queryTree, ctx, filter, order) {

	return new SelectQuery(queryTree, ctx, filter, order);
};
