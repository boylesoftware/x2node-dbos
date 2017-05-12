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
 * @protected
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
		this._propValueColumns = queryTree._propValueColumns;
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
	 * Property value column descriptor.
	 *
	 * @protected
	 * @typedef {Object} module:x2node-dbos~TranslationContext~ColumnInfo
	 * @property {string} tableName Table name.
	 * @property {string} tableAlias Table alias.
	 * @property {string} columnName Column name.
	 */

	/**
	 * Get information about the database column used to store the specified
	 * property's value. Only properties whose value is stored in a column and
	 * can be updated by updating the column are available through this method,
	 * so properties of scalar value type "object" are never available nor
	 * calculated or dependent reference properties are.
	 *
	 * <p>A set special mappings is made available as well to allow access to
	 * columns that are not directly mapped to named properties. For non-object
	 * collection properties the value column can be found by adding ".$value" to
	 * the property path. For map properties, the key column can be found by
	 * adding ".$key" to the property path. For properties stored in their own
	 * tables (including nested object properties), the parent id column can be
	 * found by adding ".$parentId" to the property path.
	 *
	 * @param {string} propPath Property path.
	 * @returns {module:x2node-dbos~TranslationContext~ColumnInfo} Column
	 * information object.
	 */
	getPropValueColumn(propPath) {

		return this._propValueColumns.get(this._basePathPrefix + propPath);
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


/**
 * Parent node property name.
 *
 * @private
 * @constant {Symbol}
 */
const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * The query tree node.
 *
 * @protected
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
	 * @param {Map.<string,(string|function)>} propsSql Map being populated with
	 * mappings between property paths and corresponding SQL value expressions.
	 * @param {Map.<string,Object>} propValueColumns Map being populated with
	 * mappings between property paths and corresponding value columns info.
	 * @param {Map.<string,module:x2node-dbos~RecordsFilter>} delayedJoinConditions
	 * Map used to delay attaching join conditions until a matching descendant
	 * node is added to the tree.
	 * @param {boolean} singleAxis <code>true</code> to disallow having more than
	 * one expanding child for this node.
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
		dbDriver, recordTypes, propsSql, propValueColumns, delayedJoinConditions,
		singleAxis, propNode, collection, table, tableAlias, keyColumn) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._propsSql = propsSql;
		this._propValueColumns = propValueColumns;
		this._delayedJoinConditions = delayedJoinConditions;

		this._singleAxis = singleAxis;

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
		this._expandingChildren = new Array();
	}

	/**
	 * Clone the node without adding any of its children.
	 *
	 * @returns {module:x2node-dbos~QueryTreeNode} The node clone.
	 */
	cloneWithoutChildren() {

		return new QueryTreeNode(
			this._dbDriver, this._recordTypes, new Map(this._propsSql),
			new Map(this._propValueColumns), new Map(), this._singleAxis,
			this._propNode, this._collection, this._table, this._tableAlias,
			this._keyColumn);
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
			this._propValueColumns, this._delayedJoinConditions,
			this._singleAxis, propNode, many, table,
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
		else if (many && propNode.desc.indexColumn)
			childNode._order.push(makeOrderElement(
				childNode._tableAlias + '.' + propNode.desc.indexColumn));

		// set the child node parent
		childNode[PARENT_NODE] = this;

		// add the child to the parent children
		if (propNode.isExpanding()) {
			if (this._singleAxis && (this._expandingChildren.length > 1))
				throw new Error(
					'Internal X2 error: attempt to add more than one expanding' +
						' child to a query tree node.');
			this._expandingChildren.push(childNode);
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
	 * @param {Array.<string>} clauses List of clauses to include. The property
	 * is not added if it is not used in one of the listed clauses.
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

				// save parent id column mapping
				this.addPropValueColumn(
					propNode.path + '.$parentId', queryTreeNode.table,
					queryTreeNode.tableAlias, propDesc.parentIdColumn);

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
				this.addPropSql(propNode.path, valueSql);
				this.addPropSql(propNode.path + '.value', valueSql);
				const keySql = ctx => (
					ctx.rebase(propNode.basePath).translatePropPath(
						propDesc.aggregatedPropPath + '.' +
							propDesc.keyPropertyName)
				);
				this.addPropSql(propNode.path + '.$key', keySql);

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

				// save collection canonical mappings
				this._saveCollectionCanonicalMappings(queryTreeNode);

				// save value column and SQL mapping
				const valSql = queryTreeNode.tableAlias + '.' + propDesc.column;
				this.addPropSql(propNode.path, valSql);
				const valPath = propNode.path + '.$value';
				this.addPropSql(valPath, valSql);
				this.addPropValueColumn(
					valPath, queryTreeNode.table, queryTreeNode.tableAlias,
					propDesc.column);

				// add anchor and value selectors
				if (select) {
					const anchorSql = queryTreeNode.tableAlias + '.' + (
						propDesc.isMap() ?
							propDesc.keyColumn : propDesc.parentIdColumn);
					queryTreeNode.addSelect(makeSelector(
						anchorSql, markupPrefix + propNode.desc.name));
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

			// save collection canonical mappings
			this._saveCollectionCanonicalMappings(queryTreeNode);

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
					this.addPropSql(propNode.path, valSql);
					this.addPropSql(
						propNode.path + '.' + refTargetDesc.idPropertyName,
						valSql
					);
					const keySql = queryTreeNode.tableAlias + '.' + (
						propDesc.isMap() ?
							getKeyColumn(propDesc, refTargetDesc) :
							refTargetIdColumn
					);
					if (propDesc.isMap())
						this.addPropSql(propNode.path + '.$key', keySql);

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

			} else { // direct reference (always via link table)

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

		// save type column mapping for polymorphic container
		if (propDesc.isPolymorphObject()) {
			const polymorphProps = propDesc.nestedProperties;
			const typePropDesc = polymorphProps.getPropertyDesc(
				polymorphProps.typePropertyName);
			if (typePropDesc.column) {
				const typePropPath =
					propNode.path + '.' + polymorphProps.typePropertyName;
				this.addPropValueColumn(
					typePropPath, queryTreeNode.table,
					queryTreeNode.tableAlias, typePropDesc.column);
				this.addPropSql(
					typePropPath,
					queryTreeNode.tableAlias + '.' + typePropDesc.column
				);
			}
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

				// save parent id column mapping
				this.addPropValueColumn(
					propNode.path + '.$parentId', queryTreeNode.table,
					queryTreeNode.tableAlias, valueTableParentIdColumn);

			} else { // stored in the same table

				// add value to this query tree node
				queryTreeNode = this;
			}

			// save value column mapping
			this.addPropValueColumn(
				propNode.path, queryTreeNode.table, queryTreeNode.tableAlias,
				valueColumn);

			// create value selector
			valueSelector = makeSelector(
				queryTreeNode.tableAlias + '.' + valueColumn,
				markupPrefix + propDesc.name + (fetchedRef ? ':' : '')
			);
		}

		// save value mapping
		this.addPropSql(propNode.path, valueSelector.sql);

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
		let queryTreeNode;
		let valTableName, valTableAlias, valColumn;
		let keyTableName, keyTableAlias, keyColumn;
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

			// save parent id column mapping
			this.addPropValueColumn(
				propNode.path + '.$parentId', queryTreeNode.table,
				queryTreeNode.tableAlias, linkTableParentIdColumn);

			// check if the key is in the link table
			if (propDesc.keyColumn) {
				keyTableName = queryTreeNode.table;
				keyTableAlias = queryTreeNode.tableAlias;
				keyColumn = propDesc.keyColumn;
			} else if (propDesc.isArray()) {
				keyTableName = queryTreeNode.table;
				keyTableAlias = queryTreeNode.tableAlias;
				keyColumn = linkTableTargetIdColumn;
			}

			// add referred record table
			const refTargetIdColumn = getIdColumn(refTargetDesc);
			queryTreeNode = queryTreeNode.createChildNode(
				propNode, refTargetDesc.table, refTargetIdColumn,
				false, false,
				refTargetIdColumn, linkTableTargetIdColumn
			);

			// create value and key expressions
			valTableName = queryTreeNode.table;
			valTableAlias = queryTreeNode.tableAlias;
			valColumn = refTargetIdColumn;
			if (!keyTableName) {
				keyTableName = queryTreeNode.table;
				keyTableAlias = queryTreeNode.tableAlias;
				keyColumn = getKeyColumn(propDesc, refTargetDesc);
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

			// save parent id column mapping
			this.addPropValueColumn(
				propNode.path + '.$parentId', queryTreeNode.table,
				queryTreeNode.tableAlias, linkTableParentIdColumn);

			// create value and key expressions
			valTableName = queryTreeNode.table;
			valTableAlias = queryTreeNode.tableAlias;
			valColumn = linkTableTargetIdColumn;
			keyTableName = queryTreeNode.table;
			keyTableAlias = queryTreeNode.tableAlias;
			keyColumn = (propDesc.keyColumn || linkTableTargetIdColumn);
		}

		// save value column and SQL mappings
		const valSql = valTableAlias + '.' + valColumn;
		this.addPropSql(propNode.path, valSql);
		const valPath = propNode.path + '.$value';
		this.addPropSql(valPath, valSql);
		this.addPropSql(
			propNode.path + '.' + refTargetDesc.idPropertyName, valSql);
		this.addPropValueColumn(valPath, valTableName, valTableAlias, valColumn);

		// save map key column and SQL mappings
		const keySql = keyTableAlias + '.' + keyColumn;
		if (propDesc.isMap()) {
			const keyPath = propNode.path + '.$key';
			this.addPropSql(keyPath, keySql);
			this.addPropValueColumn(
				keyPath, keyTableName, keyTableAlias, keyColumn);
		}

		// save array index column and SQL mapping
		if (propDesc.isArray() && propDesc.indexColumn) {
			const indPath = propNode.path + '.$index';
			this.addPropSql(
				indPath, keyTableAlias + '.' + propDesc.indexColumn);
			this.addPropValueColumn(
				indPath, keyTableName, keyTableAlias, propDesc.indexColumn);
		}

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
	 * Register canonical mappings "$parentId", "$key" and "$index" for a
	 * collection property.
	 *
	 * @private
	 * @param {module:x2node-dbos~QueryTreeNode} queryTreeNode Query tree node
	 * corresponding to the collection property table.
	 */
	_saveCollectionCanonicalMappings(queryTreeNode) {

		const propNode = queryTreeNode._propNode;
		const propDesc = propNode.desc;

		// save parent id column mapping
		this.addPropValueColumn(
			propNode.path + '.$parentId',
			queryTreeNode.table, queryTreeNode.tableAlias,
			propDesc.parentIdColumn);

		// save map key column and SQL mapping
		if (propDesc.isMap()) {
			const keyPath = propNode.path + '.$key';
			const keyColumn = getKeyColumn(propDesc, propDesc.nestedProperties);
			this.addPropSql(
				keyPath, queryTreeNode.tableAlias + '.' + keyColumn);
			this.addPropValueColumn(
				keyPath,
				queryTreeNode.table, queryTreeNode.tableAlias,
				keyColumn);
		}

		// save array index column and SQL mapping
		if (propDesc.isArray() && propDesc.indexColumn) {
			const indPath = propNode.path + '.$index';
			this.addPropSql(
				indPath, queryTreeNode.tableAlias + '.' + propDesc.indexColumn);
			this.addPropValueColumn(
				indPath,
				queryTreeNode.table, queryTreeNode.tableAlias,
				propDesc.indexColumn);
		}
	}

	/**
	 * Callback for the tree walk methods.
	 *
	 * @callback module:x2node-dbos~QueryTreeNode~walkCallback
	 * @param {module:x2node-dbos~PropertyTreeNode} propNode Property tree node
	 * associated with the query tree node being visited.
	 * @param {module:x2node-dbos~QueryTreeNode~TableDesc} tableDesc Descriptor
	 * of the table associated with the node being visited.
	 * @param {Array.<module:x2node-dbos~QueryTreeNode~TableDesc>} tableChain
	 * Chain of table descrpitors leading down to the node being visited from the
	 * top tree node. When the top tree node is visited, the chain is an empty
	 * array.
	 */

	/**
	 * Table descriptor passed to the tree walk methods.
	 *
	 * @typedef {Object} module:x2node-dbos~QueryTreeNode~TableDesc
	 * @property {string} tableName Table name.
	 * @property {string} tableAlias Table alias.
	 * @property {Array.<Object>} selectElements Array of <code>SELECT</code>
	 * clause elements that come from the table.
	 * @property {string} selectElements.valueExpr SQL value expression.
	 * @property {string} selectElements.markup Result set parser markup.
	 * @property {string} [basicJoinCondition] Boolean SQL expression used to
	 * join table to its parent using only the key columns, or
	 * <code>undefined</code> if top query tree node.
	 * @property {string} [joinCondition] Boolean SQL expression used to join the
	 * table to its parent, or <code>undefined</code> if top query tree node.
	 * @property {boolean} outerJoin <code>true</code> if the table is joined to
	 * its parent using an outer join.
	 * @property {boolean} aggregated <code>true</code> if the table is
	 * aggregated.
	 * @property {Array.<string>} [groupByElements] If the table is the first in
	 * the aggregated tables chain joined to it, this is the list of elements for
	 * the <code>GROUP BY</code> clause. If present, the <code>aggregated</code>
	 * flag is also <code>true</code> on the table descriptor.
	 * @property {Array.<string>} [orderByElements] Elements for the
	 * <code>ORDER BY</code> clause.
	 * @property {boolean} referred <code>true</code> if the table belongs to a
	 * referred record type.
	 */

	/**
	 * Visit every tree node starting from the top descending to the children.
	 *
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {module:x2node-dbos~QueryTreeNode~walkCallback} callback Callback
	 * function called for every node being visited.
	 * @returns {module:x2node-dbos~QueryTreeNode} This node.
	 */
	walk(ctx, callback) {

		this._walkNode(ctx, false, new Array(), callback);

		return this;
	}

	/**
	 * Visit every tree node in reverse order starting from the leaf nodes and
	 * ascending to the top node (therefore always visited last).
	 *
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {module:x2node-dbos~QueryTreeNode~walkCallback} callback Callback
	 * function called for every node being visited.
	 * @returns {module:x2node-dbos~QueryTreeNode} This node.
	 */
	walkReverse(ctx, callback) {

		this._walkNode(ctx, true, new Array(), callback);

		return this;
	}

	/**
	 * Recursively walk the tree from this node down.
	 *
	 * @private
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {boolean} reverse <code>true</code> for reverse walk order.
	 * @param {Array.<module:x2node-dbos~QueryTreeNode~tableDesc>} tableChain
	 * Table chain leading down to this node.
	 * @param {module:x2node-dbos~QueryTreeNode~walkCallback} callback Node
	 * visitor callback.
	 */
	_walkNode(ctx, reverse, tableChain, callback) {

		const parentTableDesc = (
			(tableChain.length > 0) && tableChain[tableChain.length - 1]);

		const tableDesc = {
			tableName: this._table,
			tableAlias: this._tableAlias,
			selectElements: this._select.map(s => ({
				valueExpr: ((typeof s.sql) === 'function' ? s.sql(ctx) : s.sql),
				markup: s.markup
			})),
			basicJoinCondition: this._buildBasicJoinCondition(),
			joinCondition: this._buildFullJoinCondition(ctx),
			outerJoin: (parentTableDesc && parentTableDesc.outerJoin) ||
				this._virtual,
			aggregated: (parentTableDesc && parentTableDesc.aggregated) ||
				this._aggregatedBelow,
			orderByElements: this._order.map(
				o => ((typeof o.sql) === 'function' ? o.sql(ctx) : o.sql)),
			referred: (parentTableDesc && parentTableDesc.referred) || (
				parentTableDesc && this._propNode.desc.isRef() &&
					(this._propNode.desc.nestedProperties.table === this._table))
		};

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
			tableDesc.groupByElements = groupByChain.reverse();
		}

		if (!reverse)
			callback(this._propNode, tableDesc, tableChain);

		tableChain.push(tableDesc);
		this._allChildren.forEach(childNode => {
			childNode._walkNode(ctx, reverse, tableChain, callback);
		});
		tableChain.pop();

		if (reverse)
			callback(this._propNode, tableDesc, tableChain);
	}

	/**
	 * Find the node corresponding to the specified table alias and call the
	 * provided callback function for it.
	 *
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {string} tableAlias Table alias to find.
	 * @param {module:x2node-dbos~QueryTreeNode~walkCallback} callback Callback
	 * function called for the node matching the specified table alias.
	 * @returns {*} The result of the callback function or <code>undefined</code>
	 * if node was not found and the callback was not called.
	 */
	forTableAlias(ctx, tableAlias, callback) {

		return this._findNodeForTableAlias(
			ctx, tableAlias, new Array(), callback);
	}

	/**
	 * Recursive implementation of the find node by table alias method.
	 *
	 * @private
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @param {string} tableAlias Table alias to find.
	 * @param {Array.<module:x2node-dbos~QueryTreeNode~TableDesc>} tableChain
	 * Current table chain.
	 * @param {module:x2node-dbos~QueryTreeNode~walkCallback} callback The
	 * callback.
	 * @returns {*} The result of the callback function or
	 * <code>undefined</code>.
	 */
	_findNodeForTableAlias(ctx, tableAlias, tableChain, callback) {

		// check if match
		if (this._tableAlias === tableAlias)
			return callback(this._propNode, {
				tableName: this._table,
				tableAlias: this._tableAlias,
				joinCondition: this._buildFullJoinCondition(ctx)
			}, tableChain);

		// search children
		for (let childNode of this._allChildren) {
			if (tableAlias.startsWith(childNode._tableAlias)) {
				tableChain.push({
					tableName: this._table,
					tableAlias: this._tableAlias,
					joinCondition: this._buildFullJoinCondition(ctx)
				});
				return childNode._findNodeForTableAlias(
					ctx, tableAlias, tableChain, callback);
			}
		}

		// return undefined
	}

	/**
	 * Build Boolean SQL expression that can be used to join the table
	 * represented by this node to its parent table. The condition does not take
	 * into account any additional join conditions associated with the node and
	 * only uses the key columns.
	 *
	 * @private
	 * @returns {string} Boolean SQL expression for the join condition.
	 */
	_buildBasicJoinCondition() {

		if (!this[PARENT_NODE])
			return undefined;

		return this._tableAlias + '.' + this._joinByColumn + ' = ' +
			this[PARENT_NODE]._tableAlias + '.' + this._joinToColumn;
	}

	/**
	 * Build Boolean SQL expression that is the complete join condition for the
	 * node to its parent. The condition includes the link to the parent table as
	 * well as the node's scoped join condition, if any.
	 *
	 * @private
	 * @param {module:x2node-dbos~TranslationContext} ctx Translation context.
	 * @returns {string} Boolean SQL expression for the join condition.
	 */
	_buildFullJoinCondition(ctx) {

		if (!this[PARENT_NODE])
			return undefined;

		let joinCondition = this._buildBasicJoinCondition();
		if (this._joinCondition) {
			const joinConditionSql = this._joinCondition.translate(ctx);
			joinCondition += ' AND ' + (
				this._joinCondition.needsParen('AND') ?
					'(' + joinConditionSql + ')' : joinConditionSql);
		}

		return joinCondition;
	}

	/**
	 * Find collection attachment node to attach an EXISTS condition subquery.
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
	 * @private
	 * @member {Array.<module:x2node-dbos~QueryTreeNode>}
	 * @readonly
	 */
	get _allChildren() {

		return (
			this._expandingChildren.length > 0 ?
				this._singleRowChildren.concat(this._expandingChildren) :
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
	 * Name of the key (such as a primary key) column in the table represented by
	 * the node. Generally speaking, the column can be used to join child tables
	 * to. For a main record type table, this is the record id column.
	 *
	 * @member {string}
	 * @readonly
	 */
	get keyColumn() { return this._keyColumn; }

	/**
	 * Additional condition used to join the table associated with the node to
	 * its parent, if any.
	 *
	 * @member {module:x2node-dbos~RecordsFilter}
	 * @readonly
	 */
	get joinCondition() { return this._joinCondition; }

	/**
	 * Add property path to value column mapping to the tree.
	 *
	 * @param {string} propPath Property path.
	 * @param {string} tableName Table name.
	 * @param {string} tableAlias Table alias.
	 * @param {string} columnName Value column name.
	 */
	addPropValueColumn(propPath, tableName, tableAlias, columnName) {

		this._propValueColumns.set(propPath, {
			tableName: tableName,
			tableAlias: tableAlias,
			columnName: columnName
		});
	}

	/**
	 * Add property path to value SQL expression mapping to the tree.
	 *
	 * @param {string} propPath Property path.
	 * @param {(string|function)} sql Property value SQL expression.
	 */
	addPropSql(propPath, sql) {

		this._propsSql.set(propPath, sql);
	}

	/**
	 * Get top translation context for the tree (normally called on the top tree
	 * node).
	 *
	 * @param {module:x2node-dbos~FilterParamsHandler} paramsHandler DBO
	 * parameters handler.
	 * @returns {module:x2node-dbos~TranslationContext} The context.
	 */
	getTopTranslationContext(paramsHandler) {

		return new TranslationContext(
			this._recordTypes, '', this, paramsHandler);
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
 * @param {Array.<string>} clauses The clauses to include from the properties
 * tree.
 * @param {boolean} singleAxis <code>true</code> to enforce single axis query
 * tree.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 */
function buildQueryTree(
	dbDriver, recordTypes, propsTree, anchorNode, clauses, singleAxis) {

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
				dbDriver, recordTypes, new Map(), new Map(), new Map(),
				singleAxis, propsTree, true, recordTypeDesc.table, 'z',
				topIdColumn)
	);
	topNode.rootPropNode = propsTree;

	// add top record id property to have it in front of the select list
	const idPropSql = topNode.tableAlias + '.' + topIdColumn;
	topNode.addSelect(makeSelector(idPropSql, topIdPropName));
	topNode.addPropSql(topIdPropName, idPropSql);
	topNode.addPropValueColumn(
		topIdPropName, topNode.table, topNode.tableAlias, topIdColumn);

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
 * Build query tree for directly selecting record type records.
 *
 * @protected
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {string} queryClause Main query clause (use "select" for
 * <code>SELECT</code> query).
 * @param {boolean} singleAxis <code>true</code> to enforce single axis query
 * tree.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.forDirectQuery = function(
	dbDriver, recordTypes, queryClause, singleAxis, propsTree) {

	return buildQueryTree(
		dbDriver, recordTypes, propsTree, null,
		[ queryClause, 'value', 'where', 'orderBy' ], singleAxis);
};

/**
 * Build query tree for selecting record type records anchored at the specified
 * anchor table.
 *
 * @protected
 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {string} queryClause Main query clause (use "select" for
 * <code>SELECT</code> query).
 * @param {boolean} singleAxis <code>true</code> to enforce single axis query
 * tree.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Selected properties
 * tree. The top node must be for a property that is a reference to the selected
 * record type.
 * @param {string} anchorTable Anchor table name.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.forAnchoredQuery = function(
	dbDriver, recordTypes, queryClause, singleAxis, propsTree, anchorTable) {

	const anchorNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), new Map(), new Map(), singleAxis,
		propsTree, true, anchorTable, 'q', 'id');
	anchorNode.anchor = true;
	anchorNode.setTableAlias('q', '');
	anchorNode.rootPropNode = propsTree;

	buildQueryTree(
		dbDriver, recordTypes, propsTree, anchorNode,
		[ queryClause, 'value', 'orderBy' ], singleAxis);

	return anchorNode;
};

/**
 * Build query tree for selecting ids of record type records.
 *
 * @protected
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
exports.forIdsOnlyQuery = function(dbDriver, recordTypes, propsTree) {

	return buildQueryTree(
		dbDriver, recordTypes, propsTree, null,
		[ 'where', 'orderBy' ], true);
};

/**
 * Build query tree for EXISTS condition subquery.
 *
 * @protected
 * @param {module:x2node-dbos~TranslationContext} translationCtx Query
 * translation context.
 * @param {module:x2node-dbos~PropertyTreeNode} propsTree Properties tree.
 * @param {string} basePropPath Base property path.
 * @returns {module:x2node-dbos~QueryTreeNode} Top node of the query tree.
 */
exports.forExistsSubquery = function(translationCtx, propsTree, basePropPath) {

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
 * @protected
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
exports.forSuperPropsQuery = function(dbDriver, recordTypes, superPropsTree) {

	const superNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), new Map(), new Map(), true,
		superPropsTree, false, '$RECORD_TYPES', '$', '$RECORD_TYPE_NAME');
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
