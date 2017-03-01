'use strict';

const common = require('x2node-common');

const ValueExpression = require('./value-expression.js');


// validation and other query tree building functions:

function invalidPropDef(propDesc, msg) {
	return new common.X2UsageError(
		'Property ' + propDesc.container.nestedPath + propDesc.name +
			' of record type ' + propDesc.container.recordTypeName +
			' has invalid definition: ' + msg);
}

function validatePropDef(propDesc, calculated) {
	const propDef = propDesc.definition;
	if (calculated) {
		if (propDef.column || propDef.table || propDef.parentIdColumn)
			throw invalidPropDef(
				propDesc,
				'calculated value or aggregate property may not have storage' +
					' attributes such as column, table and parentIdColumn in' +
					' its definition.');
	} else if (propDesc.scalarValueType === 'object') {
		if (propDef.column)
			throw invalidPropDef(
				propDesc, 'object property may not have column attribute.');
		if (propDef.table && !propDef.parentIdColumn)
			throw invalidPropDef(
				propDesc, 'must have parentIdColumn attribute.');
		if (!propDesc.isScalar() && !propDef.table)
			throw invalidPropDef(
				propDesc, 'must have table attribute.');
	} else if (propDesc.isRef() && propDef.reverseRefProperty) {
		//...
	} else {
		if (!propDef.column)
			throw invalidPropDef(
				propDesc, 'must have column attribute.');
		if (propDef.table && !propDef.parentIdColumn)
			throw invalidPropDef(
				propDesc, 'must have parentIdColumn attribute.');
		if (!propDesc.isScalar() && !propDef.table)
			throw invalidPropDef(
				propDesc, 'must have table attribute.');
		if (propDesc.isMap() && !propDef.keyColumn)
			throw invalidPropDef(
				propDesc, 'must have keyColumn attribute.');
	}
}

function getRefTargetDesc(recordTypes, propDesc) {
	const refTargetDesc = recordTypes.getRecordTypeDesc(propDesc.refTarget);
	if (!refTargetDesc.definition.table)
		throw new common.X2UsageError(
			'Record type ' + refTargetDesc.name +
				' does not have a database table associated with it' +
				' but is used as a target of reference property ' +
				propDesc.container.nestedPath + propDesc.name + ' of ' +
				propDesc.container.recordTypeName + '.');
	return refTargetDesc;
}

function getIdColumn(container) {
	const idPropDesc = container.getPropertyDesc(container.idPropertyName);
	const idColumn = idPropDesc.definition.column;
	if (!idColumn)
		throw invalidPropDef(
			idPropDesc, 'must have column attribute.');
	return idColumn;
}

function getReverseRefPropDesc(propDesc, refTargetDesc) {
	if (propDesc.container.nestedPath.length > 0)
		throw invalidPropDef(
			propDesc,
			'only top level reference property may have reverseRefProperty' +
				' attribute.');
	const propDef = propDesc.definition;
	if (!refTargetDesc.hasProperty(propDef.reverseRefProperty))
		throw invalidPropDef(
			propDesc,
			'reference target record type ' + propDesc.refTarget +
				' does not have reverse reference property ' +
				propDef.reverseRefProperty + '.');
	const reverseRefPropDesc = refTargetDesc.getPropertyDesc(
		propDef.reverseRefProperty);
	const myRecordTypeName = propDesc.container.recordTypeName;
	if (!reverseRefPropDesc.isRef() ||
		!reverseRefPropDesc.isScalar() ||
		reverseRefPropDesc.isPolymorph() || // TODO: may be unnecessary after polymorph props refactor
		(reverseRefPropDesc.refTarget !== myRecordTypeName) ||
		reverseRefPropDesc.definition.valueExpr ||
		reverseRefPropDesc.definition.aggregate)
		throw invalidPropDef(
			propDesc,
			'reverse reference property ' + propDef.reverseRefProperty + ' of ' +
				propDesc.refTarget + ' is not a scalar non-polymorphic' +
				' non-calculated reference property pointing at ' +
				myRecordTypeName + ' record type.');
	if (reverseRefPropDesc.definition.reverseRefProperty)
		throw new common.X2UsageError(
			'Property ' + propDef.reverseRefProperty +
				' of record type ' + propDesc.refTarget +
				' may not have reverse reference in its definition as' +
				' it is itself is used as a reverse reference for ' +
				' property ' + propDesc.name + ' of ' +
				propDesc.container.recordTypeName + '.');
	return reverseRefPropDesc;
}

function getKeyColumn(propDesc, keyPropContainer) {
	const propDef = propDesc.definition;
	if (propDef.keyColumn)
		return propDef.keyColumn;
	if (!propDesc.keyPropertyName)
		throw invalidPropDef(
			propDesc,
			'must have either keyColumn or keyPropertyName attribute.');
	const keyPropDesc = keyPropContainer.getPropertyDesc(
		propDesc.keyPropertyName);
	if (!keyPropDesc.definition.column || keyPropDesc.definition.table)
		throw invalidPropDef(
			propDesc, 'key property ' + keyPropDesc.name +
				' must be have column attribute and must not have table' +
				' attribute.');
	return keyPropDesc.definition.column;
}


/**
 * Create and return an object for the select list element.
 *
 * @private
 * @param {(string|module:x2node-queries~ValueExpression|Function)} sql The
 * value, which can be a SQL expression, a value expression object or a SQL
 * translation function.
 * @param {string} markup Markup for the result set parser.
 * @returns {Object} The select list element descriptor.
 */
function makeSelector(sql, markup) {
	return {
		sql: (sql instanceof ValueExpression ? ctx => sql.translate(ctx) : sql),
		markup: markup
	};
}

/**
 * Create and return an object for the order list element.
 *
 * @private
 * @param {(string|module:x2node-queries~ValueExpression|Function)} sql The
 * value, which can be a SQL expression, a value expression object or a SQL
 * translation function.
 * @param {boolean} reverse <code>true</code> for the descending order.
 * @returns {Object} The order list element descriptor.
 */
function makeOrderElement(sql, reverse) {
	return {
		sql: (sql instanceof ValueExpression ? ctx => sql.translate(ctx) : sql),
		reverse: reverse
	};
}


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

	constructor(queryTree, paramsHandler) {

		this._queryTree = queryTree;
		this._dbDriver = queryTree._dbDriver;
		this._propsSql = queryTree._propsSql;
		this._paramsHandler = paramsHandler;
	}

	get queryTree() { return this._queryTree; }

	get dbDriver() { return this._dbDriver; }

	translatePropPath(propPath) {

		const sql = this._propsSql.get(propPath);

		return ((typeof sql) === 'function' ? sql(this) : sql);
	}

	get paramsHandler() { return this._paramsHandler; }
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
	 * @param {string} propPath Path of the property, for which the node was
	 * created (the first node that needs to be included in order to get the
	 * property and any of its children).
	 * @param {boolean} collection <code>true</code> if the top table of a
	 * collection property.
	 * @param {string} table The table, for which the node is being created.
	 * @param {string} tableAlias Table alias.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 */
	constructor(
		dbDriver, recordTypes, propsSql, delayedJoinConditions, propPath,
		collection, table, tableAlias, keyColumn) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._propsSql = propsSql;
		this._delayedJoinConditions = delayedJoinConditions;

		this._propPath = propPath;
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

	cloneWithoutChildren() {

		return new QueryTreeNode(
			this._dbDriver, this._recordTypes, new Map(this._propsSql),
			new Map(), this._propPath, this._collection, this._table,
			this._tableAlias, this._keyColumn);
	}

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
	 * @param {string} propPath Node's property path.
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
	 * @param {module:x2node-queries~QueryOrder[]} [order] Optional additional
	 * ordering specification for the join.
	 * @returns {module:x2node-queries~QueryTreeNode} The new child node.
	 */
	createChildNode(
		propPath, table, keyColumn, expanding, many, virtual, joinByColumn,
		joinToColumn, joinCondition, order) {

		// create child node
		const childNode = new QueryTreeNode(
			this._dbDriver, this._recordTypes, this._propsSql,
			this._delayedJoinConditions, propPath, many, table,
			this._childTableAliasPrefix +
				String.fromCharCode(this._nextChildTableAliasDisc++),
			keyColumn
		);

		// add join parameters
		childNode._virtual = (virtual || (joinCondition !== undefined));
		childNode._joinByColumn = joinByColumn;
		childNode._joinToColumn = joinToColumn;

		// add join condition if any
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
				childNode._tableAlias + '.' + childNode._keyColumn, false));
		} else if (many) {
			let anchorNode = this;
			while (!anchorNode._collection)
				anchorNode = anchorNode[PARENT_NODE];
			if (!anchorNode.anchor)
				childNode._order.push(makeOrderElement(
					anchorNode._tableAlias + '.' + anchorNode._keyColumn,
					false));
		}

		// add scoped order if any
		if (order)
			order.forEach(o => {
				childNode._order.push(makeOrderElement(
					o.valueExpr, o.isReverse()));
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
	 * @param {string} colPropPath Aggregated collection property path.
	 * @param {module:x2node-queries~QueryFilter} [colFilter] Optional aggregated
	 * collection property filter.
	 */
	makeAggregatedBelow(colPropPath, colFilter) {

		this._aggregatedBelow = true;

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
		const propDef = propDesc.definition;
		const expanding = propNode.isExpanding();

		// basic property definition validation
		validatePropDef(propDesc, propNode.valueExpr);

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
		let refTargetDesc, refTargetDef, refTargetIdColumn, fetch;
		let reverseRefPropDesc, reverseRefPropDef;
		let valueSelectors = new Array();
		if (propDesc.isRef()) {
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetDef = refTargetDesc.definition;
			refTargetIdColumn = getIdColumn(refTargetDesc);
			fetch = (
				propNode.hasChildren() && Array.from(propNode.children).some(
					childPropNode => childPropNode.isSelected())
			);
			if (propDef.reverseRefProperty) {
				reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				reverseRefPropDef = reverseRefPropDesc.definition;
				validatePropDef(reverseRefPropDesc, false);
			}
		}

		// process property node depending on its type
		let queryTreeNode, valueSelector, anchorSelector, keyColumn;
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
				propDef.table, propDef.parentIdColumn, this._keyColumn,
				null, propDef.column, false,
				valueSelectors
			);

			// add value to the select list
			if (select)
				valueSelectors.forEach(s => { queryTreeNode.addSelect(s); });

			break;

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propNode.path, propDef.table, propDef.parentIdColumn,
					expanding, false, propDesc.optional, propDef.parentIdColumn,
					this._keyColumn);

				// create anchor selector
				anchorSelector = makeSelector(
					queryTreeNode.tableAlias + '.' + propDef.parentIdColumn,
					markupPrefix + propDesc.name
				);

			} else { // stored in the same table

				// create anchor selector
				anchorSelector = makeSelector(
					(
						propNode.presenceTest ?
							ctx => ctx.dbDriver.booleanToNull(
								propNode.presenceTest.translate(ctx)) :
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
			if (propDef.reverseRefProperty) {

				// add the property
				if (reverseRefPropDef.table) {

					// add the reference property
					queryTreeNode = this._addScalarSimpleProperty(
						propNode, markupPrefix,
						reverseRefPropDef.table, reverseRefPropDef.column,
						this._keyColumn,
						reverseRefPropDef.parentIdColumn,
						reverseRefPropDef.parentIdColumn, fetch,
						valueSelectors
					);

					// add referred record table if used
					if (propNode.hasChildren())
						queryTreeNode = queryTreeNode.createChildNode(
							propNode.path, refTargetDef.table, refTargetIdColumn,
							expanding, false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addScalarSimpleProperty(
						propNode, markupPrefix,
						refTargetDef.table, reverseRefPropDef.column,
						this._keyColumn,
						refTargetIdColumn, refTargetIdColumn, fetch,
						valueSelectors
					);
				}

			} else { // direct reference

				// add the reference property
				queryTreeNode = this._addScalarSimpleProperty(
					propNode, markupPrefix,
					propDef.table, propDef.parentIdColumn, this._keyColumn,
					propDef.column, propDef.column, fetch,
					valueSelectors
				);

				// add referred record table if used
				if (propNode.hasChildren())
					queryTreeNode = queryTreeNode.createChildNode(
						propNode.path, refTargetDef.table, refTargetIdColumn,
						expanding, false, false,
						refTargetIdColumn, propDef.column);
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
				propDef.table, propDef.parentIdColumn, this._keyColumn,
				null, propDef.parentIdColumn, propDef.column, 'value',
				false);

			break;

		case 'map:string:mono':
		case 'map:number:mono':
		case 'map:boolean:mono':
		case 'map:datetime:mono':

			// check if aggregate map
			if (propNode.aggregateFunc) {

				// TODO: process aggregate
				throw Error('Aggregates not implemented yet.');

			} else { // not an aggregate map

				// add the property
				this._addCollectionSimpleProperty(
					propNode, select, markupPrefix,
					(childrenMarkupCtx && childrenMarkupCtx.prefix),
					propDef.table, propDef.parentIdColumn, this._keyColumn,
					null, propDef.keyColumn, propDef.column, 'value',
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
				propNode.path, propDef.table, (
					propDesc.nestedProperties.idPropertyName ?
						getIdColumn(propDesc.nestedProperties) :
						keyColumn
				), expanding, true, propDesc.optional,
				propDef.parentIdColumn, this._keyColumn,
				(select && propNode.filter), (select && propNode.order));

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
			if (propDef.reverseRefProperty) {

				// add the property
				if (reverseRefPropDef.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						reverseRefPropDef.table, reverseRefPropDef.column,
						this._keyColumn,
						reverseRefPropDef.parentIdColumn,
						(propDef.keyColumn || reverseRefPropDef.parentIdColumn),
						reverseRefPropDef.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if used
					if (propNode.hasChildren())
						queryTreeNode = queryTreeNode.createChildNode(
							propNode.path, refTargetDef.table, refTargetIdColumn,
							expanding, false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						refTargetDef.table, reverseRefPropDef.column,
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
					propDef.table, propDef.parentIdColumn, this._keyColumn,
					propDef.column, (propDef.keyColumn || propDef.column),
					propDef.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if used
				if (propNode.hasChildren())
					queryTreeNode = queryTreeNode.createChildNode(
						propNode.path, refTargetDef.table, refTargetIdColumn,
						expanding, false, false,
						refTargetIdColumn, propDef.column);
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
			if (propDef.reverseRefProperty) {

				// add the property
				if (reverseRefPropDef.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						reverseRefPropDef.table, reverseRefPropDef.column,
						this._keyColumn,
						reverseRefPropDef.parentIdColumn,
						getKeyColumn(propDesc, refTargetDesc),
						reverseRefPropDef.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if used or needed for the key
					if (propNode.hasChildren() /*|| propDesc.keyPropertyName*/)
						queryTreeNode = queryTreeNode.createChildNode(
							propNode.path, refTargetDef.table, refTargetIdColumn,
							expanding, false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, select, markupPrefix,
						(childrenMarkupCtx && childrenMarkupCtx.prefix),
						refTargetDef.table, reverseRefPropDef.column,
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
					propDef.table, propDef.parentIdColumn, this._keyColumn,
					propDef.column,
					getKeyColumn(propDesc, refTargetDesc),
					propDef.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if used or needed for the key
				if (propNode.hasChildren() /*|| propDesc.keyPropertyName*/)
					queryTreeNode = queryTreeNode.createChildNode(
						propNode.path, refTargetDef.table, refTargetIdColumn,
						expanding, false, false,
						refTargetIdColumn, propDef.column);
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
		if (propNode.valueExpr) {

			// create value selector
			if (propNode.aggregateFunc) {
				valueSelector = makeSelector(
					AGGREGATE_FUNCS[propNode.aggregateFunc].bind(
						null, propNode.valueExpr),
					markupPrefix + propNode.desc.name
				);
				this.makeAggregatedBelow(
					propNode.aggregatedPropPath, propNode.filter);
			} else { // calculated value property
				valueSelector = makeSelector(
					propNode.valueExpr,
					markupPrefix + propNode.desc.name
				);
			}

			// add value to this query tree node
			queryTreeNode = this;

		} else { // stored value

			// check if stored in a separate table
			if (valueTable) {

				// create child node for the table
				queryTreeNode = this.createChildNode(
					propNode.path, valueTable, valueTableKeyColumn,
					propNode.isExpanding(), false, propNode.desc.optional,
					valueTableParentIdColumn, parentTableIdColumn);

			} else { // stored in the same table

				// add value to this query tree node
				queryTreeNode = this;
			}

			// create value selector
			valueSelector = makeSelector(
				queryTreeNode.tableAlias + '.' + valueColumn,
				markupPrefix + propNode.desc.name + (fetchedRef ? ':' : '')
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
		const queryTreeNode = this.createChildNode(
			propNode.path, valueTable, valueTableKeyColumn,
			propNode.isExpanding(), true, propNode.desc.optional,
			valueTableParentIdColumn, parentTableIdColumn,
			(select && propNode.filter), (select && propNode.order));

		// create and save the value mappings
		const propSql = queryTreeNode.tableAlias + '.' + valueColumn;
		this._propsSql.set(propNode.path, propSql);
		this._propsSql.set(propNode.path + '.' + valuePropName, propSql);

		// add value to the select list if neccesary
		if (select) {
			queryTreeNode.addSelect(makeSelector(
				queryTreeNode.tableAlias + '.' + keyColumn,
				markupPrefix + propNode.desc.name + (fetchedRef ? ':' : '')
			));
			queryTreeNode.addSelect(makeSelector(
				propSql,
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
	 * @param {module:x2node-queries~QueryOrder[]} [order] Order to generate the
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
		const ctx = new TranslationContext(this, paramsHandler);

		// add top filter if any
		if (filter)
			queryBuilder.where = filter.translate(ctx);

		// add top order if any
		if (order)
			order.forEach(orderSpec => {
				queryBuilder.orderBy.push(
					orderSpec.valueExpr.translate(ctx) +
						(orderSpec.isReverse() ? ' DESC' : ''));
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
					((typeof o.sql) === 'function' ? o.sql(ctx) : o.sql) +
						(o.reverse ? ' DESC' : ''));
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
		const ctx = new TranslationContext(this, paramsHandler);

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

		if ((this._propPath.length > 0) &&
			!propPath.startsWith(this._propPath + '.'))
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
			(res._propPath.length > 0 ? res._propPath.length + 1 : 0), '.') >= 0)
			throw new Error(
				'Internal X2 error: cannot find collection attachment node.');

		return res;
	}

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
	const recordTypeDef = recordTypeDesc.definition;
	if (!recordTypeDef.table)
		throw new common.X2UsageError(
			'Specified top record type ' + recordTypeDesc.name +
				' does not have database table associated with it.');
	const topIdPropName = recordTypeDesc.idPropertyName;
	const topIdColumn = recordTypeDesc.getPropertyDesc(topIdPropName)
		  .definition.column;
	if (!topIdColumn)
		throw new common.X2UsageError(
			'Id property of the specified top record type ' +
				recordTypeDesc.name +
				' does not have a column associated with it.');

	// create top query tree node
	const topNode = (
		anchorNode ?
			anchorNode.createChildNode(
				propsTree.path, recordTypeDef.table, topIdColumn,
				true, false, false,
				topIdColumn, topIdColumn) :
			new QueryTreeNode(
				dbDriver, recordTypes, new Map(), new Map(), propsTree.path,
				true, recordTypeDef.table, 'z', topIdColumn, topIdPropName)
	);

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
		dbDriver, recordTypes, new Map(), new Map(), propsTree.path, true,
		anchorTable, 'q',
		recordTypeDesc.getPropertyDesc(topIdPropName).definition.column,
		topIdPropName);
	anchorNode.anchor = true;
	anchorNode.setTableAlias('q', '');

	buildQueryTree(
		dbDriver, recordTypes, propsTree, anchorNode,
		[ 'select', 'value', 'orderBy' ]);

	return anchorNode;
};

/**
 * Build query tree for selecting ids of record type records.
 *
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

exports.buildExistsSubquery = function(translationCtx, propsTree, basePropPath) {

	const topNode = translationCtx.queryTree.findAttachmentNode(
		basePropPath).cloneWithoutChildren();
	topNode.setTableAlias(topNode.tableAlias, topNode.tableAlias + '_');

	topNode.addProperty(
		propsTree.findNode(basePropPath),
		[ 'where', 'value' ], {
			prefix: '',
			nextChildMarkupDisc: 'a'.charCodeAt(0)
		});

	return topNode;
};
