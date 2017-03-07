'use strict';

const common = require('x2node-common');

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
 * @param {(string|module:x2node-queries~Translatable|Function)} sql The
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
 * @param {(string|module:x2node-queries~Translatable|Function)} sql The
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
 * @type {Object.<string,Function>}
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
 * @memberof module:x2node-queries
 * @inner
 */
class TranslationContext {

	// TODO: add value expression functions

	/**
	 * Create new context.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} basePath Base path. When the context is asked to resolve a
	 * property path to the corresponding SQL, it prepends the base path to the
	 * property path before performing the lookup.
	 * @param {module:x2node-queries~QueryTreeNode} queryTree The query tree
	 * being translated.
	 * @param {module:x2node-queries~FilterParamsHandler} paramsHandler Filter
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
	 * @returns {module:x2node-queries~TranslationContext} The new context.
	 */
	rebase(basePath) {

		return new TranslationContext(
			this._recordTypes, basePath, this._queryTree, this._paramsHandler);
	}

	/**
	 * Query tree being translated.
	 *
	 * @type {module:x2node-queries~QueryTreeNode}
	 * @readonly
	 */
	get queryTree() { return this._queryTree; }

	/**
	 * The DB driver being used for the translation.
	 *
	 * @type {module:x2node-queries.DBDriver}
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
	 * @type {module:x2node-queries~FilterParamsHandler}
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
	 * @param {module:x2node-queries~Translatable} translatable The translatable.
	 * @returns {module:x2node-queries~Translatable} Rebased translatable.
	 */
	rebaseTranslatable(translatable) {

		return translatable.rebase(this._basePath);
	}

	/**
	 * Build properties tree for a subquery.
	 *
	 * @param {string} colPropPath Path of the collection property being
	 * subqueried. The context automatically adds its base path to it.
	 * @param {external:Iterable.<string>} propPaths Paths of the properties to
	 * include in the tree. The context automatically adds its base path to all
	 * of these.
	 * @param {string} clause The subqiery clause.
	 * @returns {module:x2node-queries~PropertyTreeNode} The properties tree.
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
 * @memberof module:x2node-queries
 * @inner
 */
class QueryTreeNode {

	/**
	 * Create new node. Used by the <code>createChildNode</code> method as well
	 * as once directly to create the top tree node.
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {external:Map.<string,string>} propsSql Map being populated with
	 * mappings between property paths and corresponding SQL value expressions.
	 * @param {external:Map.<string,module:x2node-queries~QueryFilter>} delayedJoinConditions
	 * Map used to delay attaching join conditions until a matching descendant
	 * node is added to the tree.
	 * @param {module:x2node-queries~PropertiesTreeNode} propNode Properties tree
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
	 * @returns {module:x2node-queries~QueryTreeNode} The node clone.
	 */
	cloneWithoutChildren() {

		return new QueryTreeNode(
			this._dbDriver, this._recordTypes, new Map(this._propsSql),
			new Map(), this._propNode, this._collection, this._table,
			this._tableAlias, this._keyColumn);
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
	 * @param {boolean} expanding <code>true</code> if may result in multiple
	 * rows.
	 * @param {boolean} many <code>true</code> if node's table is on the "many"
	 * side of the relation.
	 * @param {boolean} virtual <code>true</code> if may select no rows.
	 * @param {string} joinByColumn Name of the column in the node's table used
	 * to join to the parent table.
	 * @param {string} joinToColumn Name of the column in the parent node's table
	 * used for the join.
	 * @param {module:x2node-queries~QueryFilter} [joinCondition] Optional
	 * additional condition for the join. If provided, the node is made virtual
	 * regardless of the <code>virtual</code> flag.
	 * @param {module:x2node-queries~QueryOrder} [order] Optional additional
	 * ordering specification for the join.
	 * @returns {module:x2node-queries~QueryTreeNode} The new child node.
	 */
	createChildNode(
		propNode, table, keyColumn, expanding, many, virtual, joinByColumn,
		joinToColumn, joinCondition, order) {

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
		if (expanding) {
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
	 * @param {(string|Function)} [keySql] Aggregated map key SQL.
	 * @param {string} colPropPath Aggregated collection property path.
	 * @param {module:x2node-queries~QueryFilter} [colFilter] Optional aggregated
	 * collection property filter.
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
	 * @param {module:x2node-queries~PropertyTreeNode} propNode Child property
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
		const expanding = propNode.isExpanding();

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
		let queryTreeNode, valueSelector, anchorSelector, keyColumn;
		let valueSelectors = new Array();
		switch (
			(propDesc.isScalar() ? 'scalar' : (
				propDesc.isArray() ? 'array' : 'map')) +
				':' + propDesc.scalarValueType +
				':' + (propDesc.isPolymorph() ? 'poly' : 'mono')
		) {
		case 'scalar:string:mono':
		case 'scalar:number:mono':
		case 'scalar:boolean:mono':
		case 'scalar:datetime:mono':

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

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDesc.table) {

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propNode, propDesc.table, propDesc.parentIdColumn,
					expanding, false, propDesc.optional, propDesc.parentIdColumn,
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

		case 'scalar:object:poly':

			// TODO:...
			throw new Error('Polymorphs are not implemented yet.');

			break;

		case 'scalar:ref:mono':

			// check if dependent record reference
			if (propDesc.reverseRefPropertyName) {

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
							expanding, false, false,
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
						expanding, false, false,
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

		case 'scalar:ref:poly':

			// TODO:...
			throw new Error('Polymorphs are not implemented yet.');

			break;

		case 'array:string:mono':
		case 'array:number:mono':
		case 'array:boolean:mono':
		case 'array:datetime:mono':

			// add the property
			this._addCollectionSimpleProperty(
				propNode, select, markupPrefix,
				(childrenMarkupCtx && childrenMarkupCtx.prefix),
				propDesc.table, propDesc.parentIdColumn, this._keyColumn,
				null, propDesc.parentIdColumn, propDesc.column, 'value',
				false);

			break;

		case 'map:string:mono':
		case 'map:number:mono':
		case 'map:boolean:mono':
		case 'map:datetime:mono':

			// check if aggregate map
			if (propDesc.isAggregate()) {

				// create and save value and key mappings
				const valueSql = AGGREGATE_FUNCS[propDesc.aggregateFunc].bind(
					null, propDesc.valueExpr.rebase(propNode.basePath));
				this._propsSql.set(propNode.path, valueSql);
				this._propsSql.set(propNode.path + '.value', valueSql);
				const keySql = ctx => ctx.rebase(propNode.basePath)
					  .translatePropPath(
						  propDesc.aggregatedPropPath + '.' +
							  propDesc.keyPropertyName);
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

				// add the property
				this._addCollectionSimpleProperty(
					propNode, select, markupPrefix,
					(childrenMarkupCtx && childrenMarkupCtx.prefix),
					propDesc.table, propDesc.parentIdColumn, this._keyColumn,
					null, propDesc.keyColumn, propDesc.column, 'value',
					false);
			}

			break;

		case 'array:object:mono':
		case 'map:object:mono':

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
				), expanding, true, propDesc.optional,
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

		case 'array:object:poly':
		case 'map:object:poly':

			// TODO:...
			throw new Error('Polymorphs are not implemented yet.');

			break;

		case 'array:ref:mono':

			// check if dependent record reference
			if (propDesc.reverseRefPropertyName) {

				// add the property
				if (reverseRefPropDesc.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						reverseRefPropDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						reverseRefPropDesc.parentIdColumn,
						(
							propDesc.keyColumn ||
								reverseRefPropDesc.parentIdColumn
						),
						reverseRefPropDesc.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if used
					if (propNode.hasChildren())
						queryTreeNode = queryTreeNode.createChildNode(
							propNode, refTargetDesc.table, refTargetIdColumn,
							expanding, false, false,
							refTargetIdColumn,
							reverseRefPropDesc.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						refTargetDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						refTargetIdColumn, refTargetIdColumn, refTargetIdColumn,
						refTargetDesc.idPropertyName, fetch
					);
				}

			} else { // direct reference (via link table)

				// add the reference property
				queryTreeNode = this._addCollectionSimpleProperty(
					propNode, select, markupPrefix,
					(childrenMarkupCtx && childrenMarkupCtx.prefix),
					propDesc.table, propDesc.parentIdColumn, this._keyColumn,
					propDesc.column, (propDesc.keyColumn || propDesc.column),
					propDesc.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if used
				if (propNode.hasChildren())
					queryTreeNode = queryTreeNode.createChildNode(
						propNode, refTargetDesc.table, refTargetIdColumn,
						expanding, false, false,
						refTargetIdColumn, propDesc.column);
			}

			// add used referred record properties
			if (propNode.hasChildren()) {
				for (let p of propNode.children)
					if (!p.desc.isId())
						queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);
			}

			break;

		case 'map:ref:mono':

			// check if dependent record reference
			if (propDesc.reverseRefPropertyName) {

				// add the property
				if (reverseRefPropDesc.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						reverseRefPropDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						reverseRefPropDesc.parentIdColumn,
						getKeyColumn(propDesc, refTargetDesc),
						reverseRefPropDesc.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if used or needed for the key
					if (propNode.hasChildren() /*|| propDesc.keyPropertyName*/)
						queryTreeNode = queryTreeNode.createChildNode(
							propNode, refTargetDesc.table, refTargetIdColumn,
							expanding, false, false,
							refTargetIdColumn, reverseRefPropDesc.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						refTargetDesc.table, reverseRefPropDesc.column,
						this._keyColumn,
						refTargetIdColumn,
						getKeyColumn(propDesc, refTargetDesc),
						refTargetIdColumn,
						refTargetDesc.idPropertyName, fetch
					);
				}

			} else { // direct reference (via link table)

				// add the reference property
				queryTreeNode = this._addCollectionSimpleProperty(
					propNode, select, markupPrefix,
					(childrenMarkupCtx && childrenMarkupCtx.prefix),
					propDesc.table, propDesc.parentIdColumn, this._keyColumn,
					propDesc.column,
					getKeyColumn(propDesc, refTargetDesc),
					propDesc.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if used or needed for the key
				if (propNode.hasChildren() /*|| propDesc.keyPropertyName*/)
					queryTreeNode = queryTreeNode.createChildNode(
						propNode, refTargetDesc.table, refTargetIdColumn,
						expanding, false, false,
						refTargetIdColumn, propDesc.column);
			}

			// add used referred record properties
			if (propNode.hasChildren()) {
				for (let p of propNode.children)
					if (!p.desc.isId())
						queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);
			}

			break;

		case 'array:ref:poly':
		case 'map:ref:poly':

			// TODO:...
			throw new Error('Polymorphs are not implemented yet.');
		}
	}

	/**
	 * Helper function used by addProperty to add simple scalars.
	 *
	 * @private
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
					propNode.isExpanding(), false, propDesc.optional,
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
	 * Helper function used by addProperty to add simple collections.
	 *
	 * @private
	 */
	_addCollectionSimpleProperty(
		propNode, select, markupPrefix, childrenMarkupPrefix,
		valueTable, valueTableParentIdColumn, parentTableIdColumn,
		valueTableKeyColumn, keyColumn, valueColumn, valuePropName,
		fetchedRef) {

		// create child node for the table
		const propDesc = propNode.desc;
		const queryTreeNode = this.createChildNode(
			propNode, valueTable, valueTableKeyColumn,
			propNode.isExpanding(), true, propDesc.optional,
			valueTableParentIdColumn, parentTableIdColumn,
			(select && propDesc.filter && propDesc.filter.rebase(
				propNode.basePath)),
			(select && propDesc.order && propDesc.order.rebase(
				propNode.basePath)));

		// create and save value and key mappings
		const valueSql = queryTreeNode.tableAlias + '.' + valueColumn;
		this._propsSql.set(propNode.path, valueSql);
		this._propsSql.set(propNode.path + '.' + valuePropName, valueSql);
		const keySql = queryTreeNode.tableAlias + '.' + keyColumn;
		if (propDesc.isMap())
			this._propsSql.set(propNode.path + '.$key', keySql);

		// add value to the select list if neccesary
		if (select) {
			queryTreeNode.addSelect(makeSelector(
				keySql,
				markupPrefix + propNode.desc.name + (fetchedRef ? ':' : '')
			));
			queryTreeNode.addSelect(makeSelector(
				valueSql,
				childrenMarkupPrefix + valuePropName
			));
		}

		// return the leaf node
		return queryTreeNode;
	}

	/**
	 * Assemble a SELECT query from the tree starting at this node.
	 *
	 * @param {module:x2node-queries~QueryFilter} [filter] Filter to generate the
	 * WHERE clause.
	 * @param {module:x2node-queries~QueryOrder} [order] Order to generate the
	 * ORDER BY clause.
	 * @param {module:x2node-queries~FilterParamsHandler} paramsHandler Query
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
	 * @param {module:x2node-queries~TranslationContext} ctx Translation context.
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
	 * @type {module:x2node-queries~QueryTreeNode[]}
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
	 * @type {string}
	 * @readonly
	 */
	get table() { return this._table; }

	/**
	 * Alias of the table represented by the node.
	 *
	 * @type {string}
	 * @readonly
	 */
	get tableAlias() { return this._tableAlias; }

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
				true, false, false,
				topIdColumn, topIdColumn) :
			new QueryTreeNode(
				dbDriver, recordTypes, new Map(), new Map(), propsTree,
				true, recordTypeDesc.table, 'z', topIdColumn, topIdPropName)
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
 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-queries~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @returns {module:x2node-queries~QueryTreeNode} Top node of the query tree.
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
 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-queries~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @param {string} anchorTable Anchor table name.
 * @returns {module:x2node-queries~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.buildAnchored = function(
	dbDriver, recordTypes, propsTree, anchorTable) {

	const recordTypeDesc = recordTypes.getRecordTypeDesc(
		propsTree.desc.refTarget);
	const topIdPropName = recordTypeDesc.idPropertyName;

	const anchorNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), new Map(), propsTree, true,
		anchorTable, 'q',
		recordTypeDesc.getPropertyDesc(topIdPropName).column,
		topIdPropName);
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
 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-queries~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @returns {module:x2node-queries~QueryTreeNode} Top node of the query tree.
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
 * @param {module:x2node-queries~TranslationContext} translationCtx Query
 * translation context.
 * @param {module:x2node-queries~PropertyTreeNode} propsTree Properties tree.
 * @param {string} basePropPath Base property path.
 * @returns {module:x2node-queries~QueryTreeNode} Top node of the query tree.
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
