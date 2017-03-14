'use strict';

const propsTreeBuilder = require('./props-tree-builder.js');
const Translatable = require('./translatable.js');


/**
 * Get id column for the container.
 *
 * @private
 * @param {module:x2node-records~PropertiesContainer} container Properties
 * container.
 * @returns {string} Column name.
 */
function getIdColumn(container) {

	return container.getPropertyDesc(container.idPropertyName).column;
}

/**
 * Get map key column.
 *
 * @private
 * @param {module:x2node-records~PropertyDescriptor} propDesc Map property
 * descriptor.
 * @param {module:x2node-records~PropertiesContainer} keyPropContainer Container
 * where to look for the key property, if applicable.
 * @returns {string} Column name.
 */
function getKeyColumn(propDesc, keyPropContainer) {
	if (propDesc.keyColumn)
		return propDesc.keyColumn;
	const keyPropDesc = keyPropContainer.getPropertyDesc(
		propDesc.keyPropertyName);
	return keyPropDesc.column;
}

/**
 * Create and return an object for the select list element.
 *
 * @private
 * @param {(string|module:x2node-dbos~Translatable|function)} sql The
 * value, which can be a SQL expression, a value expression object or a SQL
 * translation function.
 * @param {string} markup Markup for the result set parser.
 * @returns {Object} The select list element descriptor.
 */
function makeSelector(sql, markup) {
	return {
		sql: (sql instanceof Translatable ? ctx => sql.translate(ctx) : sql),
		markup: markup
	};
}

/**
 * Create and return an object for the order list element.
 *
 * @private
 * @param {(string|module:x2node-dbos~Translatable|function)} sql The
 * value, which can be a SQL expression, a value expression object or a SQL
 * translation function.
 * @returns {Object} The order list element descriptor.
 */
function makeOrderElement(sql) {
	return {
		sql: (sql instanceof Translatable ? ctx => sql.translate(ctx) : sql)
	};
}


/**
 * Aggregate function SQL generators by aggregate function name.
 *
 * @private
 * @enum {function}
 */
const AGGREGATE_FUNCS = {
	'COUNT': function(valueExpr, ctx) {
		return 'COUNT(' + valueExpr.translate(ctx) + ')';
	},
	'SUM': function(valueExpr, ctx) {
		return ctx.dbDriver.coalesce(
			'SUM(' + valueExpr.translate(ctx) + ')', '0');
	},
	'MIN': function(valueExpr, ctx) {
		return 'MIN(' + valueExpr.translate(ctx) + ')';
	},
	'MAX': function(valueExpr, ctx) {
		return 'MAX(' + valueExpr.translate(ctx) + ')';
	},
	'AVG': function(valueExpr, ctx) {
		return 'AVG(' + valueExpr.translate(ctx) + ')';
	}
};


/**
 * SQL translation context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 */
class TranslationContext {

	/**
	 * Create new context.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} basePath Base path. When the context is asked to resolve a
	 * property path to the corresponding SQL, it prepends the base path to the
	 * property path before performing the lookup.
	 * @param {module:x2node-dbos~QueryTreeNode} queryTree The query tree
	 * being translated.
	 * @param {module:x2node-dbos~FilterParamsHandler} paramsHandler Filter
	 * parameters handler.
	 */
	constructor(recordTypes, basePath, queryTree, paramsHandler) {

		this._recordTypes = recordTypes;

		this._basePath = basePath;
		this._basePathPrefix = (basePath.length > 0 ? basePath + '.' : '');

		this._queryTree = queryTree;
		this._dbDriver = queryTree._dbDriver;
		this._propsSql = queryTree._propsSql;
		this._rootPropNode = queryTree.rootPropNode;

		this._paramsHandler = paramsHandler;
	}

	/**
	 * Create new context by cloning this one and replacing the base path with
	 * the one provided.
	 *
	 * @param {string} basePath The new base path.
	 * @returns {module:x2node-dbos~TranslationContext} The new context.
	 */
	rebase(basePath) {

		return new TranslationContext(
			this._recordTypes, basePath, this._queryTree, this._paramsHandler);
	}

	/**
	 * Query tree being translated.
	 *
	 * @member {module:x2node-dbos~QueryTreeNode}
	 * @readonly
	 */
	get queryTree() { return this._queryTree; }

	/**
	 * The DB driver being used for the translation.
	 *
	 * @member {module:x2node-dbos.DBDriver}
	 * @readonly
	 */
	get dbDriver() { return this._dbDriver; }

	/**
	 * Translate the specified property path into the corresponding value SQL
	 * expression. Context's base path, if any, is automatically added to the
	 * specified path before looking it up in the query tree's mappings.
	 *
	 * @param {string} propPath Property path.
	 * @returns {string} Property value SQL expression.
	 */
	translatePropPath(propPath) {

		const sql = this._propsSql.get(this._basePathPrefix + propPath);

		return ((typeof sql) === 'function' ? sql(this) : sql);
	}

	/**
	 * Filter parameters handler.
	 *
	 * @member {module:x2node-dbos~FilterParamsHandler}
	 * @readonly
	 */
	get paramsHandler() { return this._paramsHandler; }

	/**
	 * Rebase the specified property path by adding the context's base path to
	 * it.
	 *
	 * @param {string} propPath Property path to rebase.
	 * @returns {string} Rebased property path.
	 */
	rebasePropPath(propPath) {

		return this._basePathPrefix + propPath;
	}

	/**
	 * Rebase the specified <code>Translatable</code> to the context's base path.
	 *
	 * @param {module:x2node-dbos~Translatable} translatable The translatable.
	 * @returns {module:x2node-dbos~Translatable} Rebased translatable.
	 */
	rebaseTranslatable(translatable) {

		return translatable.rebase(this._basePath);
	}

	/**
	 * Build properties tree for a subquery.
	 *
	 * @param {string} colPropPath Path of the collection property being
	 * subqueried. The context automatically adds its base path to it.
	 * @param {Iterable.<string>} propPaths Paths of the properties to include in
	 * the tree. The context automatically adds its base path to all of these.
	 * @param {string} clause The subqiery clause.
	 * @returns {module:x2node-dbos~PropertyTreeNode} The properties tree.
	 */
	buildSubqueryPropsTree(colPropPath, propPaths, clause) {

		let basedPropPaths;
		if (this._basePathPrefix.length > 0) {
			basedPropPaths = new Set();
			propPaths.forEach(p => {
				basedPropPaths.add(this.rebasePropPath(p));
			});
		} else {
			basedPropPaths = propPaths;
		}

		return propsTreeBuilder.buildPropsTreeBranches(
			this._recordTypes,
			this._rootPropNode.desc,
			clause,
			this._rootPropNode.getValueExpressionContext(),
			this.rebasePropPath(colPropPath),
			basedPropPaths, {
				noWildcards: true,
				noAggregates: true,
				ignoreScopedOrders: true,
				noScopedFilters: true,
				includeScopeProp: true,
				ignoreFiltersOn: this.rebasePropPath(colPropPath)
			}
		)[0];
	}
}


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * The query tree node.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 */
class QueryTreeNode {

	/**
	 * Create new node. Used by the <code>createChildNode</code> method as well
	 * as once directly to create the top tree node.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {Map.<string,string>} propsSql Map being populated with mappings
	 * between property paths and corresponding SQL value expressions.
	 * @param {Map.<string,module:x2node-dbos~RecordsFilter>} delayedJoinConditions
	 * Map used to delay attaching join conditions until a matching descendant
	 * node is added to the tree.
	 * @param {module:x2node-dbos~PropertiesTreeNode} propNode Properties tree
	 * node, for which the node was created (the first node that needs to be
	 * included in order to get the property and any of its children).
	 * @param {boolean} collection <code>true</code> if the top table of a
	 * collection property.
	 * @param {string} table The table, for which the node is being created.
	 * @param {string} tableAlias Table alias.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 */
	constructor(
		dbDriver, recordTypes, propsSql, delayedJoinConditions, propNode,
		collection, table, tableAlias, keyColumn) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._propsSql = propsSql;
		this._delayedJoinConditions = delayedJoinConditions;

		this._propNode = propNode;
		this._collection = collection;
		this._table = table;
		this._tableAlias = tableAlias;
		this._keyColumn = keyColumn;

		this._childTableAliasPrefix = tableAlias;
		this._nextChildTableAliasDisc = 'a'.charCodeAt(0);

		this._select = new Array();
		this._order = new Array();

		this._singleRowChildren = new Array();
	}

	/**
	 * Clone the node without adding any of its children.
	 *
	 * @returns {module:x2node-dbos~QueryTreeNode} The node clone.
	 */
	cloneWithoutChildren() {

		return new QueryTreeNode(
			this._dbDriver, this._recordTypes, new Map(this._propsSql),
			new Map(), this._propNode, this._collection, this._table,
			this._tableAlias, this._keyColumn);
	}

	/**
	 * Remove the top node of the tree and return the new top, which used to be
	 * the first and only child of this node.
	 *
	 * @returns {module:x2node-dbos~QueryTreeNode} The new top node.
	 */
	behead() {

		const allChildren = this._allChildren;
		if (allChildren.length > 1)
			throw new Error('Internal X2 error: more than one neck.');

		const newHead = allChildren[0];
		delete newHead[PARENT_NODE];

		newHead.rootPropNode = this.rootPropNode;

		newHead._select = this._select.concat(newHead._select);

		newHead._aggregatedBelow = this._aggregatedBelow;
		newHead._aggregatedKeySql = this._aggregatedKeySql;

		newHead._virtual = false;
		delete newHead._joinByColumn;
		delete newHead._joinToColumn;

		return newHead;
	}

	/**
	 * Change the table alias and the child table alias prefix for the node.
	 *
	 * @param {string} tableAlias New table alias.
	 * @param {string} [childTableAliasPrefix] New child table alias prefix. If
	 * unspecified, the table alias is used. If empty string, the next child
	 * table will have alias "z" (used for the anchor table node).
	 */
	setTableAlias(tableAlias, childTableAliasPrefix) {

		this._tableAlias = tableAlias;
		this._childTableAliasPrefix = (
			childTableAliasPrefix !== undefined ?
				childTableAliasPrefix : tableAlias);
		if (this._childTableAliasPrefix.length === 0)
			this._nextChildTableAliasDisc = 'z'.charCodeAt(0);
	}

	/**
	 * Create child node.
	 *
	 * @param {string} propNode Corresponding properties tree node.
	 * @param {string} table The table, for which the node is being created.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 * @param {boolean} many <code>true</code> if node's table is on the "many"
	 * side of the relation.
	 * @param {boolean} virtual <code>true</code> if may select no rows.
	 * @param {string} joinByColumn Name of the column in the node's table used
	 * to join to the parent table.
	 * @param {string} joinToColumn Name of the column in the parent node's table
	 * used for the join.
	 * @param {module:x2node-dbos~RecordsFilter} [joinCondition] Optional
	 * additional condition for the join. If provided, the node is made virtual
	 * regardless of the <code>virtual</code> flag.
	 * @param {module:x2node-dbos~RecordsOrder} [order] Optional additional
	 * ordering specification for the join.
	 * @returns {module:x2node-dbos~QueryTreeNode} The new child node.
	 */
	createChildNode(
		propNode, table, keyColumn, many, virtual, joinByColumn, joinToColumn,
		joinCondition, order) {

		// create child node
		const childNode = new QueryTreeNode(
			this._dbDriver, this._recordTypes, this._propsSql,
			this._delayedJoinConditions, propNode, many, table,
			this._childTableAliasPrefix +
				String.fromCharCode(this._nextChildTableAliasDisc++),
			keyColumn
		);

		// add join parameters
		childNode._virtual = (virtual || (joinCondition !== undefined));
		childNode._joinByColumn = joinByColumn;
		childNode._joinToColumn = joinToColumn;

		// add join condition if any
		const propPath = propNode.path;
		const delayedJoinCondition = this._delayedJoinConditions.get(propPath);
		if (joinCondition && delayedJoinCondition) {
			this._delayedJoinConditions.delete(propPath);
			childNode._joinCondition = joinCondition.conjoin(
				delayedJoinCondition);
		} else if (delayedJoinCondition) {
			this._delayedJoinConditions.delete(propPath);
			childNode._joinCondition = delayedJoinCondition;
		} else if (joinCondition) {
			childNode._joinCondition = joinCondition;
		}

		// add collection anchor ordering
		if (this.anchor) {
			childNode._order.push(makeOrderElement(
				childNode._tableAlias + '.' + childNode._keyColumn));
		} else if (many) {
			let anchorNode = this;
			while (anchorNode && !anchorNode._collection)
				anchorNode = anchorNode[PARENT_NODE];
			if (anchorNode && !anchorNode.anchor)
				childNode._order.push(makeOrderElement(
					anchorNode._tableAlias + '.' + anchorNode._keyColumn));
		}

		// add scoped order if any
		if (order)
			order.elements.forEach(orderElement => {
				childNode._order.push(makeOrderElement(orderElement));
			});

		// set the child node parent
		childNode[PARENT_NODE] = this;

		// add the child to the parent children
		if (propNode.isExpanding()) {
			if (this._expandingChild) // should not happen
				throw new Error(
					'Internal X2 error: attempt to add more than one expanding' +
						' child to a query tree node.');
			this._expandingChild = childNode;
		} else {
			this._singleRowChildren.push(childNode);
		}

		// return the new child node
		return childNode;
	}

	/**
	 * Add element to the select list.
	 *
	 * @param {Object} selector Select list element descriptor.
	 * @returns {Object} Added select list element descriptor.
	 */
	addSelect(selector) {

		this._select.push(selector);

		return selector;
	}

	/**
	 * Mark the node as having its collection child node the first table in the
	 * chain of aggregated tables.
	 *
	 * @param {(string|function)} [keySql] Aggregated map key SQL.
	 * @param {string} colPropPath Aggregated collection property path.
	 * @param {module:x2node-dbos~RecordsFilter} [colFilter] Optional
	 * aggregated collection property filter.
	 */
	makeAggregatedBelow(keySql, colPropPath, colFilter) {

		this._aggregatedBelow = true;

		this._aggregatedKeySql = keySql;

		if (colFilter)
			this._delayedJoinConditions.set(colPropPath, colFilter);
	}

	/**
	 * Recursively add child property to this query tree node.
	 *
	 * @param {module:x2node-dbos~PropertyTreeNode} propNode Child property
	 * tree node to add to this query tree node.
	 * @param {string[]} clauses List of clauses to include. The property is not
	 * added if it is not used in one of the listed clauses.
	 * @param {Object} markupCtx Markup context for the property.
	 */
	addProperty(propNode, clauses, markupCtx) {

		// check the clause
		if (!clauses.some(clause => propNode.isUsedIn(clause)))
			return;

		// determine if the property needs to be selected
		const select = (
			propNode.isSelected() &&
				clauses.some(clause => clause === 'select'));

		// property basics
		const propDesc = propNode.desc;

		// create markup context for possible children
		const markupPrefix = markupCtx.prefix;
		const childrenMarkupCtx = (
			propNode.hasChildren() || (select && !propDesc.isScalar()) ? {
				prefix: markupPrefix.substring(0, markupPrefix.length - 1) +
					String.fromCharCode(markupCtx.nextChildMarkupDisc++) + '$',
				nextChildMarkupDisc: 'a'.charCodeAt(0)
			} :
			undefined
		);

		// get reference related data
		let refTargetDesc, refTargetIdColumn, fetch, reverseRefPropDesc;
		if (propDesc.isRef()) {
			refTargetDesc = this._recordTypes.getRecordTypeDesc(
				propDesc.refTarget);
			refTargetIdColumn = getIdColumn(refTargetDesc);
			fetch = (
				propNode.hasChildren() && Array.from(propNode.children).some(
					childPropNode => childPropNode.isSelected())
			);
			if (propDesc.reverseRefPropertyName)
				reverseRefPropDesc = refTargetDesc.getPropertyDesc(
					propDesc.reverseRefPropertyName);
		}

		// process property node depending on its type
		let queryTreeNode, anchorSelector, keyColumn, idIncluded;
		let valueSelectors = new Array();
		switch (
			(propDesc.isScalar() ? 'scalar' : (
				propDesc.isArray() ? 'array' : 'map')) +
				':' + propDesc.scalarValueType
		) {
		case 'scalar:string':
		case 'scalar:number':
		case 'scalar:boolean':
		case 'scalar:datetime':

			// add the property
			queryTreeNode = this._addScalarSimpleProperty(
				propNode, markupPrefix,
				propDesc.table, propDesc.parentIdColumn, this._keyColumn,
				null, propDesc.column, false,
				valueSelectors
			);

			// add value to the select list
			if (select)
				valueSelectors.forEach(s => { queryTreeNode.addSelect(s); });

			break;

		case 'scalar:object':

			// check if stored in a separate table
			if (propDesc.table) {

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propNode, propDesc.table, propDesc.parentIdColumn,
					false, propDesc.optional, propDesc.parentIdColumn,
					this._keyColumn);

				// create anchor selector
				anchorSelector = makeSelector(
					queryTreeNode.tableAlias + '.' + propDesc.parentIdColumn,
					markupPrefix + propDesc.name
				);

			} else { // stored in the same table

				// create anchor selector
				anchorSelector = makeSelector(
					(
						propDesc.presenceTest ?
							ctx => ctx.dbDriver.booleanToNull(
								propDesc.presenceTest
									.rebase(propNode.basePath)
									.translate(ctx)) :
							this._dbDriver.booleanLiteral(true)
					),
					markupPrefix + propDesc.name
				);

				// add child properties to the same node
				queryTreeNode = this;
			}

			// add anchor selector
			if (select)
				queryTreeNode.addSelect(anchorSelector);

			// add selected child properties
			for (let p of propNode.children)
				queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

			break;

		case 'scalar:ref':

			// check if implicit dependent record reference
			if (propDesc.implicitDependentRef) {

				// add referred record type table
				queryTreeNode = this.createChildNode(
					propNode, refTargetDesc.table, refTargetIdColumn,
					false, false,
					refTargetIdColumn, propDesc.column);

			} else if (propDesc.reverseRefPropertyName) { // dependent reference

				// add the property
				if (reverseRefPropDesc.table) {

					// add the reference property
					queryTreeNode = this._addScalarSimpleProperty(
						propNode, markupPrefix,
						reverseRefPropDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						reverseRefPropDesc.parentIdColumn,
						reverseRefPropDesc.parentIdColumn, fetch,
						valueSelectors
					);

					// add referred record table if used
					if (propNode.hasChildren())
						queryTreeNode = queryTreeNode.createChildNode(
							propNode, refTargetDesc.table, refTargetIdColumn,
							false, false,
							refTargetIdColumn,
							reverseRefPropDesc.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addScalarSimpleProperty(
						propNode, markupPrefix,
						refTargetDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						refTargetIdColumn, refTargetIdColumn, fetch,
						valueSelectors
					);
				}

			} else { // direct reference

				// add the reference property
				queryTreeNode = this._addScalarSimpleProperty(
					propNode, markupPrefix,
					propDesc.table, propDesc.parentIdColumn, this._keyColumn,
					propDesc.column, propDesc.column, fetch,
					valueSelectors
				);

				// add referred record table if used
				if (propNode.hasChildren())
					queryTreeNode = queryTreeNode.createChildNode(
						propNode, refTargetDesc.table, refTargetIdColumn,
						false, false,
						refTargetIdColumn, propDesc.column);
			}

			// add value to the select list
			if (select)
				valueSelectors.forEach(s => { queryTreeNode.addSelect(s); });

			// add used referred record properties
			if (propNode.hasChildren()) {
				for (let p of propNode.children)
					queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);
			}

			break;

		case 'array:string':
		case 'array:number':
		case 'array:boolean':
		case 'array:datetime':
		case 'map:string':
		case 'map:number':
		case 'map:boolean':
		case 'map:datetime':

			// check if aggregate map
			if (propDesc.isAggregate()) {

				// create and save value and key mappings
				const valueSql = AGGREGATE_FUNCS[propDesc.aggregateFunc].bind(
					null, propDesc.valueExpr.rebase(propNode.basePath));
				this._propsSql.set(propNode.path, valueSql);
				this._propsSql.set(propNode.path + '.value', valueSql);
				const keySql = ctx => (
					ctx.rebase(propNode.basePath).translatePropPath(
						propDesc.aggregatedPropPath + '.' +
							propDesc.keyPropertyName)
				);
				this._propsSql.set(propNode.path + '.$key', keySql);

				// mark the node with aggregation below
				this.makeAggregatedBelow(
					keySql,
					propNode.basePrefix + propDesc.aggregatedPropPath,
					(
						propDesc.filter &&
							propDesc.filter.rebase(propNode.basePath)
					)
				);

				// add value to the select list
				if (select) {
					this.addSelect(makeSelector(
						keySql,
						markupPrefix + propNode.desc.name
					));
					this.addSelect(makeSelector(
						valueSql,
						childrenMarkupCtx.prefix + 'value'
					));
				}

			} else { // not an aggregate map

				// add value table
				queryTreeNode = this.createChildNode(
					propNode, propDesc.table, null, true, propDesc.optional,
					propDesc.parentIdColumn, this._keyColumn,
					(select && propDesc.filter && propDesc.filter.rebase(
						propNode.basePath)),
					(select && propDesc.order && propDesc.order.rebase(
						propNode.basePath))
				);

				// create and save value and key mappings
				const valSql = queryTreeNode.tableAlias + '.' + propDesc.column;
				this._propsSql.set(propNode.path, valSql);
				this._propsSql.set(propNode.path + '.$value', valSql);
				const keySql = queryTreeNode.tableAlias + '.' + (
					propDesc.isMap() ?
						propDesc.keyColumn : propDesc.parentIdColumn);
				if (propDesc.isMap())
					this._propsSql.set(propNode.path + '.$key', keySql);

				// add value to the select list if neccesary
				if (select) {
					queryTreeNode.addSelect(makeSelector(
						keySql, markupPrefix + propNode.desc.name));
					queryTreeNode.addSelect(makeSelector(
						valSql, childrenMarkupCtx.prefix));
				}
			}

			break;

		case 'array:object':
		case 'map:object':

			// determine collection element key column
			keyColumn = (
				propDesc.isMap() ?
					getKeyColumn(propDesc, propDesc.nestedProperties) :
					getIdColumn(propDesc.nestedProperties)
			);

			// create child node for the objects table
			queryTreeNode = this.createChildNode(
				propNode, propDesc.table, (
					propDesc.nestedProperties.idPropertyName ?
						getIdColumn(propDesc.nestedProperties) :
						keyColumn
				), true, propDesc.optional,
				propDesc.parentIdColumn, this._keyColumn,
				(select && propDesc.filter && propDesc.filter.rebase(
					propNode.basePath)),
				(select && propDesc.order && propDesc.order.rebase(
					propNode.basePath)));

			// add anchor selector
			if (select)
				queryTreeNode.addSelect(makeSelector(
					queryTreeNode.tableAlias + '.' + keyColumn,
					markupPrefix + propDesc.name
				));

			// add selected child properties
			for (let p of propNode.children)
				queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

			break;

		case 'array:ref':
		case 'map:ref':

			// id included by the add collection helper function
			idIncluded = true;

			// check if implicit dependent record reference
			if (propDesc.implicitDependentRef) {

				// add referred record type table
				queryTreeNode = this.createChildNode(
					propNode, refTargetDesc.table, refTargetIdColumn,
					true, false,
					refTargetIdColumn, propDesc.column);

				// add the id property
				idIncluded = false;

			} else if (propDesc.reverseRefPropertyName) { // dependent reference

				// add the property
				if (reverseRefPropDesc.table) {

					// add the tables
					queryTreeNode = this._addRefPropertyViaLinkTable(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						reverseRefPropDesc.table, reverseRefPropDesc.column,
						reverseRefPropDesc.parentIdColumn, refTargetDesc, fetch
					);

				} else { // no link table

					// add referred record table
					queryTreeNode = this.createChildNode(
						propNode, refTargetDesc.table, refTargetIdColumn,
						true, propDesc.optional,
						reverseRefPropDesc.column, this._keyColumn,
						(select && propDesc.filter && propDesc.filter.rebase(
							propNode.basePath)),
						(select && propDesc.order && propDesc.order.rebase(
							propNode.basePath))
					);

					// create and save value and key mappings
					const valSql = queryTreeNode.tableAlias + '.' +
						refTargetIdColumn;
					this._propsSql.set(propNode.path, valSql);
					this._propsSql.set(
						propNode.path + '.' + refTargetDesc.idPropertyName,
						valSql
					);
					const keySql = queryTreeNode.tableAlias + '.' + (
						propDesc.isMap() ?
							getKeyColumn(propDesc, refTargetDesc) :
							refTargetIdColumn
					);
					if (propDesc.isMap())
						this._propsSql.set(propNode.path + '.$key', keySql);

					// add value to the select list if neccesary
					if (select) {
						queryTreeNode.addSelect(makeSelector(
							keySql, markupPrefix + propNode.desc.name +
								(fetch ? ':' : '')));
						queryTreeNode.addSelect(makeSelector(
							valSql, childrenMarkupCtx.prefix +
								refTargetDesc.idPropertyName));
					}
				}

			} else { // direct reference (via link table)

				// add the tables
				queryTreeNode = this._addRefPropertyViaLinkTable(
					propNode, select, markupPrefix,
					(childrenMarkupCtx && childrenMarkupCtx.prefix),
					propDesc.table, propDesc.parentIdColumn, propDesc.column,
					refTargetDesc, fetch
				);
			}

			// add used referred record properties
			if (propNode.hasChildren()) {
				for (let p of propNode.children)
					if (!idIncluded || !p.desc.isId())
						queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);
			}

			break;

		default: // should never happen
			throw new Error('Internal X2 error: unknown property type.');
		}
	}

	/**
	 * Helper function used by <code>addProperty</code> to add simple scalars.
	 *
	 * @private
	 * @param {module:x2node-dbos~PropertyTreeNode} propNode Scalar simple
	 * value property node.
	 * @param {string} markupPrefix Property's level markup prefix.
	 * @param {string} [valueTable] Value table name if the property is stored in
	 * its own table.
	 * @param {string} [valueTableParentIdColumn] Column in the value table, if
	 * any, that points back to the container table.
	 * @param {string} [parentTableIdColumn] Column in the container table, to
	 * which the value table, if any, points back.
	 * @param {?string} [valueTableKeyColumn] Column in the value table that can
	 * be used to join children to (for fetched references).
	 * @param {string} [valueColumn] Column that contains the property value (for
	 * stored properties).
	 * @param {boolean} fetchedRef <code>true</code> if fetched reference.
	 * @param {Array} valueSelectors Array, to which to add value selectors.
	 * @returns {module:x2node-dbos~QueryTreeNode} The leaf node possibly
	 * added by the method (or this node if no tables were added).
	 */
	_addScalarSimpleProperty(
		propNode, markupPrefix,
		valueTable, valueTableParentIdColumn, parentTableIdColumn,
		valueTableKeyColumn, valueColumn, fetchedRef, valueSelectors) {

		// the leaf node
		let queryTreeNode;

		// check if calculated
		let valueSelector;
		const propDesc = propNode.desc;
		if (propDesc.isAggregate()) {

			// create value selector
			valueSelector = makeSelector(
				AGGREGATE_FUNCS[propDesc.aggregateFunc].bind(
					null, propDesc.valueExpr.rebase(propNode.basePath)),
				markupPrefix + propDesc.name
			);

			// mark the node with aggregation below
			this.makeAggregatedBelow(
				null,
				propNode.basePrefix + propDesc.aggregatedPropPath,
				(propDesc.filter && propDesc.filter.rebase(propNode.basePath)));

			// add value to this query tree node
			queryTreeNode = this;

		} else if (propDesc.isCalculated()) {

			// create value selector
			valueSelector = makeSelector(
				propDesc.valueExpr.rebase(propNode.basePath),
				markupPrefix + propDesc.name
			);

			// add value to this query tree node
			queryTreeNode = this;

		} else { // stored value

			// check if stored in a separate table
			if (valueTable) {

				// create child node for the table
				queryTreeNode = this.createChildNode(
					propNode, valueTable, valueTableKeyColumn,
					false, propDesc.optional,
					valueTableParentIdColumn, parentTableIdColumn);

			} else { // stored in the same table

				// add value to this query tree node
				queryTreeNode = this;
			}

			// create value selector
			valueSelector = makeSelector(
				queryTreeNode.tableAlias + '.' + valueColumn,
				markupPrefix + propDesc.name + (fetchedRef ? ':' : '')
			);
		}

		// save value mapping
		this._propsSql.set(propNode.path, valueSelector.sql);

		// save value selector
		valueSelectors.push(valueSelector);

		// return the leaf node
		return queryTreeNode;
	}

	/**
	 * Helper function used by <code>addProperty</code> to add reference
	 * collection properties that use a link table.
	 *
	 * @private
	 * @param {module:x2node-dbos~PropertyTreeNode} propNode Reference
	 * collection property node.
	 * @param {boolean} select <code>true</code> if the property value is
	 * selected.
	 * @param {string} markupPrefix Property's level markup prefix.
	 * @param {string} [childrenMarkupPrefix] Children markup prefix (only if
	 * selected).
	 * @param {string} linkTable Link table name.
	 * @param {string} linkTableParentIdColumn Column in the link table that
	 * points back to the property table.
	 * @param {string} linkTableTargetIdColumn Column in the link table that
	 * points to the reference target record table.
	 * @param {module:x2node-records~RecordTypeDescriptor} refTargetDesc Referred
	 * record type descriptor.
	 * @param {boolean} fetchedRef <code>true</code> if fetched reference.
	 * @returns {module:x2node-dbos~QueryTreeNode} The leaf node added by the
	 * method.
	 */
	_addRefPropertyViaLinkTable(
		propNode, select, markupPrefix, childrenMarkupPrefix,
		linkTable, linkTableParentIdColumn, linkTableTargetIdColumn,
		refTargetDesc, fetchedRef) {

		// needs referred record table?
		let queryTreeNode, valSql, keySql;
		const propDesc = propNode.desc;
		if (propNode.hasChildren() || propDesc.keyPropertyName) {

			// add the link table
			queryTreeNode = this.createChildNode(
				propNode, linkTable, linkTableTargetIdColumn, true,
				propDesc.optional, linkTableParentIdColumn, this._keyColumn,
				(select && propDesc.filter && propDesc.filter.rebase(
					propNode.basePath)),
				(select && propDesc.order && propDesc.order.rebase(
					propNode.basePath))
			);

			// check if the key is in the link table
			if (propDesc.keyColumn) {
				keySql = queryTreeNode.tableAlias + '.' +
					propDesc.keyColumn;
			} else if (propDesc.isArray()) {
				keySql = queryTreeNode.tableAlias + '.' +
					linkTableTargetIdColumn;
			}

			// add referred record table
			const refTargetIdColumn = getIdColumn(refTargetDesc);
			queryTreeNode = queryTreeNode.createChildNode(
				propNode, refTargetDesc.table, refTargetIdColumn,
				false, false,
				refTargetIdColumn, linkTableTargetIdColumn
			);

			// create value and key expressions
			valSql = queryTreeNode.tableAlias + '.' + refTargetIdColumn;
			if (!keySql) {
				keySql = queryTreeNode.tableAlias + '.' +
					getKeyColumn(propDesc, refTargetDesc);
			}

		} else { // only the link table is needed

			// add the link table
			queryTreeNode = this.createChildNode(
				propNode, linkTable, linkTableTargetIdColumn, true,
				propDesc.optional, linkTableParentIdColumn, this._keyColumn,
				(select && propDesc.filter && propDesc.filter.rebase(
					propNode.basePath)),
				(select && propDesc.order && propDesc.order.rebase(
					propNode.basePath))
			);

			// create value and key expressions
			valSql = queryTreeNode.tableAlias + '.' + linkTableTargetIdColumn;
			keySql = queryTreeNode.tableAlias + '.' +
				(propDesc.keyColumn || linkTableTargetIdColumn);
		}

		// save value and key mappings
		this._propsSql.set(propNode.path, valSql);
		this._propsSql.set(
			propNode.path + '.' + refTargetDesc.idPropertyName, valSql);
		if (propDesc.isMap())
			this._propsSql.set(propNode.path + '.$key', keySql);

		// add value to the select list if neccesary
		if (select) {
			queryTreeNode.addSelect(makeSelector(
				keySql, markupPrefix + propNode.desc.name +
					(fetchedRef ? ':' : '')));
			queryTreeNode.addSelect(makeSelector(
				valSql, childrenMarkupPrefix + refTargetDesc.idPropertyName));
		}

		// return the node
		return queryTreeNode;
	}

	/**
	 * Assemble SELECT query from the tree starting at this node.
	 *
	 * @param {module:x2node-dbos~RecordsFilter} [filter] Filter to generate
	 * the WHERE clause.
	 * @param {module:x2node-dbos~RecordsOrder} [order] Order to generate the
	 * ORDER BY clause.
	 * @param {module:x2node-dbos~FilterParamsHandler} paramsHandler DBO
	 * parameters handler.
	 * @returns {string} The query SQL.
	 */
	assembleSelect(filter, order, paramsHandler) {

		// create query builder
		const queryBuilder = {
			select: new Array(),
			from: null,
			where: null,
			groupBy: new Array(),
			orderBy: new Array()
		};

		// translation context
		const ctx = new TranslationContext(
			this._recordTypes, '', this, paramsHandler);

		// add top filter if any
		if (filter)
			queryBuilder.where = filter.translate(ctx);

		// add top order if any
		if (order)
			order.elements.forEach(orderElement => {
				queryBuilder.orderBy.push(
					orderElement.translate(ctx));
			});

		// process query tree nodes
		this.addNodeToSelect(queryBuilder, false, false, ctx);

		// weed out repeats in the order
		const seen = new Set();
		queryBuilder.orderBy = queryBuilder.orderBy.filter(o => {
			const v = o.match(/^(.+?)(?:\s+(?:asc|desc))?$/i)[1];
			return (seen.has(v) ? false : (seen.add(v), true));
		});

		// assemble the query and return it
		return 'SELECT ' +
			queryBuilder.select.join(', ') +
			' FROM ' + queryBuilder.from +
			(
				queryBuilder.where ?
					' WHERE ' + queryBuilder.where : ''
			) +
			(
				queryBuilder.groupBy.length > 0 ?
					' GROUP BY ' + queryBuilder.groupBy.join(', ') : ''
			) +
			(
				queryBuilder.orderBy.length > 0 ?
					' ORDER BY ' + queryBuilder.orderBy.join(', ') : ''
			);
	}

	/**
	 * Recursively add the this node and its children to the SELECT query being
	 * built.
	 *
	 * @private
	 * @param {Object} queryBuilder The query builder.
	 * @param {boolean} forceOuter <code>true</code> to force outer join for the
	 * node's table.
	 * @param {boolean} aggregated <code>true</code> if being aggregated.
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 */
	addNodeToSelect(queryBuilder, forceOuter, aggregated, ctx) {

		// add SELECT clause elements
		this._select.forEach(s => {
			queryBuilder.select.push(
				((typeof s.sql) === 'function' ? s.sql(ctx) : s.sql) +
					' AS ' + ctx.dbDriver.safeLabel(s.markup));
		});

		// add node to the FROM chain
		if (this[PARENT_NODE]) {
			let joinCondition =
				this._tableAlias + '.' + this._joinByColumn + ' = ' +
				this[PARENT_NODE]._tableAlias + '.' + this._joinToColumn;
			if (this._joinCondition) {
				const joinConditionSql = this._joinCondition.translate(ctx);
				joinCondition += ' AND ' + (
					this._joinCondition.needsParen('AND') ?
						'(' + joinConditionSql + ')' : joinConditionSql);
			}
			queryBuilder.from +=
				(forceOuter || this._virtual ? ' LEFT OUTER' : ' INNER') +
				' JOIN ' + this._table + ' AS ' + this._tableAlias +
				' ON ' + joinCondition;
		} else { // top node
			queryBuilder.from = this._table + ' AS ' + this._tableAlias;
		}

		// add groupping
		if (this._aggregatedBelow) {
			const groupByChain = new Array();
			if (this._aggregatedKeySql)
				groupByChain.push(
					(typeof this._aggregatedKeySql) === 'function' ?
						this._aggregatedKeySql(ctx) : this._aggregatedKeySql
				);
			const addSingleRows = node => {
				node._singleRowChildren.forEach(cn => {
					addSingleRows(cn);
					groupByChain.push(cn._tableAlias + '.' + cn._keyColumn);
				});
			};
			for (let n = this; n && !n.anchor; n = n[PARENT_NODE]) {
				addSingleRows(n);
				groupByChain.push(n._tableAlias + '.' + n._keyColumn);
			}
			queryBuilder.groupBy = groupByChain.reverse();
		}

		// add order
		if (!aggregated)
			this._order.forEach(o => {
				queryBuilder.orderBy.push(
					((typeof o.sql) === 'function' ? o.sql(ctx) : o.sql));
			});

		// add children
		this._allChildren.forEach(childNode => {
			childNode.addNodeToSelect(
				queryBuilder, (forceOuter || this._virtual),
				(aggregated || this._aggregatedBelow), ctx)
		});
	}

	/**
	 * Assemble SELECT sub-query for an EXISTS condition from the tree starting
	 * at this node.
	 *
	 * @param {module:x2node-dbos~RecordsFilter} [filter] Filter to generate
	 * the WHERE clause.
	 * @param {module:x2node-dbos~FilterParamsHandler} paramsHandler DBO
	 * parameters handler.
	 * @returns {string} The sub-query SQL.
	 */
	assembleExistsSubquery(filter, paramsHandler) {

		// get the subquery root node
		const subqueryNode = this._allChildren[0];

		// create query builder
		const queryBuilder = {
			from: null,
			where: subqueryNode._tableAlias + '.' + subqueryNode._joinByColumn +
				' = ' + this._tableAlias + '.' + subqueryNode._joinToColumn
		};

		// translation context
		const ctx = new TranslationContext(
			this._recordTypes, '', this, paramsHandler);

		// add top filter if any
		if (filter) {
			const filterSql = filter.translate(ctx);
			queryBuilder.where += ' AND ' + (
				filter.needsParen('AND') ? '(' + filterSql + ')' : filterSql);
		}

		// process query tree nodes
		subqueryNode.addNodeToExistsSubquery(queryBuilder, ctx);

		// assemble the query and return it
		return 'SELECT TRUE' +
			' FROM ' + queryBuilder.from +
			' WHERE ' + queryBuilder.where
	}

	/**
	 * Recursively add the this node and its children to the SELECT sub-query for
	 * an EXISTS condition being built.
	 *
	 * @private
	 * @param {Object} queryBuilder The query builder.
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 */
	addNodeToExistsSubquery(queryBuilder, ctx) {

		// add node to the FROM chain
		if (queryBuilder.from) {
			let joinCondition =
				this._tableAlias + '.' + this._joinByColumn + ' = ' +
				this[PARENT_NODE]._tableAlias + '.' + this._joinToColumn;
			if (this._joinCondition) {
				const joinConditionSql = this._joinCondition.translate(ctx);
				joinCondition += ' AND ' + (
					this._joinCondition.needsParen('AND') ?
						'(' + joinConditionSql + ')' : joinConditionSql);
			}
			queryBuilder.from +=
				' INNER JOIN ' + this._table + ' AS ' + this._tableAlias +
				' ON ' + joinCondition;
		} else { // top node
			queryBuilder.from = this._table + ' AS ' + this._tableAlias;
		}

		// add children
		this._allChildren.forEach(childNode => {
			childNode.addNodeToExistsSubquery(queryBuilder, ctx)
		});
	}

	/**
	 * Find collection attachment node to attach a EXISTS condition subquery.
	 *
	 * @param {string} propPath Collection property path.
	 * @returns {module:x2node-dbos~QueryTreeNode} The attachment node.
	 */
	findAttachmentNode(propPath) {

		if ((this._propNode.path.length > 0) &&
			!propPath.startsWith(this._propNode.path + '.'))
			return null;

		let res = this;

		for (let childNode of this._allChildren) {
			const node = childNode.findAttachmentNode(propPath);
			if (node) {
				res = node;
				break;
			}
		}

		if (propPath.indexOf(
			(res._propNode.path.length > 0 ? res._propNode.path.length + 1 : 0),
			'.') >= 0)
			throw new Error(
				'Internal X2 error: cannot find collection attachment node.');

		return res;
	}

	/**
	 * All child nodes with the expanding node, if any, at the end of the list.
	 *
	 * @member {Array.<module:x2node-dbos~QueryTreeNode>}
	 * @readonly
	 */
	get _allChildren() {

		return (
			this._expandingChild ?
				this._singleRowChildren.concat(this._expandingChild) :
				this._singleRowChildren
		);
	}

	/**
	 * Name of the table represented by the node.
	 *
	 * @member {string}
	 * @readonly
	 */
	get table() { return this._table; }

	/**
	 * Alias of the table represented by the node.
	 *
	 * @member {string}
	 * @readonly
	 */
	get tableAlias() { return this._tableAlias; }

	/**
	 * Additional condition used to join the table associated with the node to
	 * its parent, if any.
	 *
	 * @member {module:x2node-dbos~RecordsFilter}
	 * @readonly
	 */
	get joinCondition() { return this._joinCondition; }

	/**
	 * Add property path to value SQL expression mapping to the tree.
	 *
	 * @param {string} propPath Property path.
	 * @param {string} sql Property value SQL expression.
	 */
	addPropSql(propPath, sql) {

		this._propsSql.set(propPath, sql);
	}
}


/**
 * Build query tree.
 *
 * @private
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree.
 * @param {module:x2node-dbos~QueryTreeNode} [anchorNode] Anchor table node,
 * if any.
 * @param {string[]} clauses The clauses to include from the properties tree.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 */
function buildQueryTree(dbDriver, recordTypes, propsTree, anchorNode, clauses) {

	// get and validate top records specification data
	const recordTypeDesc = recordTypes.getRecordTypeDesc(
		propsTree.desc.refTarget);
	const topIdPropName = recordTypeDesc.idPropertyName;
	const topIdColumn = recordTypeDesc.getPropertyDesc(topIdPropName).column;

	// create top query tree node
	const topNode = (
		anchorNode ?
			anchorNode.createChildNode(
				propsTree, recordTypeDesc.table, topIdColumn,
				false, false,
				topIdColumn, topIdColumn) :
			new QueryTreeNode(
				dbDriver, recordTypes, new Map(), new Map(), propsTree,
				true, recordTypeDesc.table, 'z', topIdColumn)
	);
	topNode.rootPropNode = propsTree;

	// add top record id property to have it in front of the select list
	const idPropSql = topNode.tableAlias + '.' + topIdColumn;
	topNode.addSelect(makeSelector(idPropSql, topIdPropName));
	topNode.addPropSql(topIdPropName, idPropSql);

	// add the rest of selected properties
	if (propsTree.hasChildren()) {
		const topMarkupCtx = {
			prefix: '',
			nextChildMarkupDisc: 'a'.charCodeAt(0)
		};
		for (let p of propsTree.children)
			if (!p.desc.isId()) // already included
				topNode.addProperty(p, clauses, topMarkupCtx);
	}

	// return the query tree
	return topNode;
}

/**
 * Build query tree for selecting record type records.
 *
 * @private
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.buildDirect = function(
	dbDriver, recordTypes, propsTree) {

	return buildQueryTree(
		dbDriver, recordTypes, propsTree, null,
		[ 'select', 'value', 'where', 'orderBy' ]);
};

/**
 * Build query tree for selecting record type records anchored at the specified
 * anchor table.
 *
 * @private
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @param {string} anchorTable Anchor table name.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.buildAnchored = function(
	dbDriver, recordTypes, propsTree, anchorTable) {

	const recordTypeDesc = recordTypes.getRecordTypeDesc(
		propsTree.desc.refTarget);

	const anchorNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), new Map(), propsTree, true,
		anchorTable, 'q',
		recordTypeDesc.getPropertyDesc(recordTypeDesc.idPropertyName).column);
	anchorNode.anchor = true;
	anchorNode.setTableAlias('q', '');
	anchorNode.rootPropNode = propsTree;

	buildQueryTree(
		dbDriver, recordTypes, propsTree, anchorNode,
		[ 'select', 'value', 'orderBy' ]);

	return anchorNode;
};

/**
 * Build query tree for selecting ids of record type records.
 *
 * @private
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.buildIdsOnly = function(dbDriver, recordTypes, propsTree) {

	return buildQueryTree(
		dbDriver, recordTypes, propsTree, null,
		[ 'where', 'orderBy' ]);
};

/**
 * Build query tree for EXISTS condition subquery.
 *
 * @private
 * @param {module:x2node-dbos~TranslationContext} translationCtx Query
 * translation context.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Properties tree.
 * @param {string} basePropPath Base property path.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 */
exports.buildExistsSubquery = function(translationCtx, propsTree, basePropPath) {

	basePropPath = translationCtx.rebasePropPath(basePropPath);

	const topNode = translationCtx.queryTree.findAttachmentNode(
		basePropPath).cloneWithoutChildren();
	topNode.setTableAlias(topNode.tableAlias, topNode.tableAlias + '_');
	topNode.rootPropNode = propsTree;

	topNode.addProperty(
		propsTree.findNode(basePropPath),
		[ 'where', 'value' ], {
			prefix: '',
			nextChildMarkupDisc: 'a'.charCodeAt(0)
		});

	return topNode;
};

/**
 * Build query tree for selecting super-properties.
 *
 * @private
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-dbos~PropertyTreeNode} superPropsTree Selected
 * super-properties tree. At the top is a pseudo-property with the super-type
 * container.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.buildSuperPropsQuery = function(dbDriver, recordTypes, superPropsTree) {

	const superNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), new Map(), superPropsTree, false,
		'$RECORD_TYPES', '$', '$RECORD_TYPE_NAME');
	superNode.setTableAlias('$', '');
	superNode.rootPropNode = superPropsTree;

	const recordsPropDesc = superPropsTree.findNode('records').desc;
	superNode.addSelect(makeSelector(
		dbDriver.stringLiteral(recordsPropDesc.refTarget),
		recordsPropDesc.container.idPropertyName));

	const topMarkupCtx = {
		prefix: '',
		nextChildMarkupDisc: 'a'.charCodeAt(0)
	};
	const clauses = [ 'select', 'value', 'where' ];
	for (let p of superPropsTree.children) {
		superNode.addProperty(p, clauses, topMarkupCtx);
	}

	const topNode = superNode.behead();
	topNode.anchor = true;

	return topNode;
};
