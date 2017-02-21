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
		sql: (
			sql instanceof ValueExpression ?
				function(dbDriver, propsResolver, funcResolvers, paramsHandler) {
					return sql.translate(propsResolver, funcResolvers);
				}
			: sql
		),
		markup: markup
	};
}

function makeOrderElement(sql, reverse) {
	return {
		sql: (
			sql instanceof ValueExpression ?
				function(dbDriver, propsResolver, funcResolvers, paramsHandler) {
					return sql.translate(propsResolver, funcResolvers);
				}
			: sql
		),
		reverse: reverse
	};
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
	 * @param {string} table The table, for which the node is being created.
	 * @param {string} tableAlias Table alias.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 */
	constructor(dbDriver, recordTypes, propsSql, table, tableAlias, keyColumn) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._propsSql = propsSql;

		this._table = table;
		this._tableAlias = tableAlias;
		this._keyColumn = keyColumn;

		this._nextChildTableAliasDisc = 'a'.charCodeAt(0);

		this._select = new Array();
		this._order = new Array();

		this._singleRowChildren = new Array();
	}

	/**
	 * Create child node.
	 *
	 * @param {string} table The table, for which the node is being created.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 * @param {boolean} expanding <code>true</code> if may result in multiple
	 * rows.
	 * @param {boolean} collection <code>true</code> if adding for a collection
	 * property.
	 * @param {boolean} virtual <code>true</code> if may select no rows.
	 * @param {string} joinByColumn Name of the column in the node's table used
	 * to join to the parent table.
	 * @param {string} joinToColumn Name of the column in the parent node's table
	 * used for the join.
	 * @param {module:x2node-queries~QueryFilterSpec} [joinCondition] Optional
	 * additional condition for the join. If provided, the node is made virtual
	 * regardless of the <code>virtual</code> flag.
	 * @param {module:x2node-queries~QueryOrderSpec[]} [order] Optional
	 * additional ordering specification for the join.
	 * @returns {module:x2node-queries~QueryTreeNode} The new child node.
	 */
	createChildNode(
		table, keyColumn, expanding, collection, virtual, joinByColumn,
		joinToColumn, joinCondition, order) {

		const childNode = new QueryTreeNode(
			this._dbDriver, this._recordTypes, this._propsSql,
			table,
			this._tableAlias +
				String.fromCharCode(this._nextChildTableAliasDisc++),
			keyColumn
		);

		childNode._virtual = (virtual || (joinCondition !== undefined));
		childNode._joinByColumn = joinByColumn;
		childNode._joinToColumn = joinToColumn;

		childNode._joinCondition = joinCondition;

		if (collection)
			childNode._order.push(makeOrderElement(
				this._tableAlias + '.' + this._keyColumn, false));
		if (order)
			order.forEach(o => {
				// TODO: optimize: don't include is same as parent id
				childNode._order.push(makeOrderElement(
					o.valueExpr, o.isReverse()));
			});

		childNode[PARENT_NODE] = this;

		if (expanding) {
			if (this._expandingChild) // should not happen
				throw new Error(
					'Attempt to add more than one expanding child to a query' +
						' tree node.');
			this._expandingChild = childNode;
		} else {
			this._singleRowChildren.push(childNode);
		}

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

		// property basics
		const propDesc = propNode.desc;
		const propDef = propDesc.definition;
		const expanding = propNode.isExpanding();

		// basic property definition validation
		validatePropDef(propDesc, propNode.valueExpr);

		// create markup context for possible children
		const markupPrefix = markupCtx.prefix;
		const childrenMarkupCtx = (
			propNode.hasChildren() || (
				propNode.isSelected() && !propDesc.isScalar()
			) ? {
				prefix: markupPrefix.substring(0, markupPrefix.length - 1) +
					String.fromCharCode(markupCtx.nextChildMarkupDisc++) + '$',
				nextChildMarkupDisc: 'a'.charCodeAt(0)
			} :
			undefined
		);

		// get reference related data
		let refTargetDesc, refTargetDef, refTargetIdColumn, fetch;
		let reverseRefPropDesc, reverseRefPropDef;
		if (propDesc.isRef()) {
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetDef = refTargetDesc.definition;
			refTargetIdColumn = getIdColumn(refTargetDesc);
			fetch = propNode.hasChildren();
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
			this._addScalarSimpleProperty(
				propNode, markupPrefix,
				propDef.table, propDef.parentIdColumn, this.keyColumn,
				null, propDef.column, false
			);

			break;

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propDef.table, propDef.parentIdColumn, expanding, false,
					propDesc.optional, propDef.parentIdColumn, this.keyColumn);

				// create anchor selector
				anchorSelector = makeSelector(
					queryTreeNode.tableAlias + '.' + queryTreeNode.keyColumn,
					markupPrefix + propDesc.name
				);

			} else { // stored in the same table

				// create anchor selector
				anchorSelector = makeSelector(
					(
						propNode.presenceTest ?
							function(
								dbDriver, propsResolver, funcResolvers,
								paramsHandler) {
								return dbDriver.booleanToNull(
									propNode.presenceTest.translate(
										dbDriver, propsResolver, funcResolvers,
										paramsHandler)
								)
							}
						: this._dbDriver.booleanLiteral(true)
					),
					markupPrefix + propDesc.name
				);

				// add child properties to the same node
				queryTreeNode = this;
			}

			// add anchor selector
			if (propNode.isSelected())
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
						this.keyColumn,
						reverseRefPropDef.parentIdColumn,
						reverseRefPropDef.parentIdColumn, fetch
					);

					// add referred record table if fetched
					if (fetch)
						queryTreeNode = queryTreeNode.createChildNode(
							refTargetDef.table, refTargetIdColumn, expanding,
							false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addScalarSimpleProperty(
						propNode, markupPrefix,
						refTargetDef.table, reverseRefPropDef.column,
						this.keyColumn,
						refTargetIdColumn, refTargetIdColumn, fetch
					);
				}

			} else { // direct reference

				// add the reference property
				queryTreeNode = this._addScalarSimpleProperty(
					propNode, markupPrefix,
					propDef.table, propDef.parentIdColumn, this.keyColumn,
					propDef.column, propDef.column, fetch
				);

				// add referred record table if fetched
				if (fetch)
					queryTreeNode = queryTreeNode.createChildNode(
						refTargetDef.table, refTargetIdColumn, expanding, false,
						false, refTargetIdColumn, propDef.column);
			}

			// add fetched referred record
			if (fetch) {
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
				propNode, markupPrefix, childrenMarkupCtx.prefix,
				propDef.table, propDef.parentIdColumn, this.keyColumn,
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
					propNode, markupPrefix, childrenMarkupCtx.prefix,
					propDef.table, propDef.parentIdColumn, this.keyColumn,
					null, propDef.keyColumn, propDef.column, 'value',
					false);
			}

			break;

		case 'array:object:mono':
		case 'map:object:mono':

			// TODO: add join condition and order if any

			// determine collection element key column
			keyColumn = (
				propDesc.isMap() ?
					getKeyColumn(propDesc, propDesc.nestedProperties) :
					getIdColumn(propDesc.nestedProperties)
			);

			// create child node for the objects table
			queryTreeNode = this.createChildNode(
				propDef.table, (
					propDesc.nestedProperties.idPropertyName ?
						getIdColumn(propDesc.nestedProperties) :
						keyColumn
				), expanding, true, propDesc.optional,
				propDef.parentIdColumn, this.keyColumn);

			// add anchor selector
			if (propNode.isSelected())
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

			// TODO: add join condition and order if any

			// check if dependent record reference
			if (propDef.reverseRefProperty) {

				// add the property
				if (reverseRefPropDef.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, markupPrefix, childrenMarkupCtx.prefix,
						reverseRefPropDef.table, reverseRefPropDef.column,
						this.keyColumn,
						reverseRefPropDef.parentIdColumn,
						(propDef.keyColumn || reverseRefPropDef.parentIdColumn),
						reverseRefPropDef.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if fetched
					if (fetch)
						queryTreeNode = queryTreeNode.createChildNode(
							refTargetDef.table, refTargetIdColumn, expanding,
							false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, markupPrefix, childrenMarkupCtx.prefix,
						refTargetDef.table, reverseRefPropDef.column,
						this.keyColumn,
						refTargetIdColumn, refTargetIdColumn, refTargetIdColumn,
						refTargetDesc.idPropertyName, fetch
					);
				}

			} else { // direct reference (via link table)

				// add the reference property
				queryTreeNode = this._addCollectionSimpleProperty(
					propNode, markupPrefix, childrenMarkupCtx.prefix,
					propDef.table, propDef.parentIdColumn, this.keyColumn,
					propDef.column, (propDef.keyColumn || propDef.column),
					propDef.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if fetched
				if (fetch)
					queryTreeNode = queryTreeNode.createChildNode(
						refTargetDef.table, refTargetIdColumn, expanding, false,
						false, refTargetIdColumn, propDef.column);
			}

			// add fetched referred record
			if (fetch) {
				for (let p of propNode.children)
					if (!p.desc.isId())
						queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);
			}

			break;

		case 'map:ref:mono':

			// TODO: add join condition and order if any

			// check if dependent record reference
			if (propDef.reverseRefProperty) {

				// add the property
				if (reverseRefPropDef.table) {

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, markupPrefix, childrenMarkupCtx.prefix,
						reverseRefPropDef.table, reverseRefPropDef.column,
						this.keyColumn,
						reverseRefPropDef.parentIdColumn,
						getKeyColumn(propDesc, refTargetDesc),
						reverseRefPropDef.parentIdColumn,
						refTargetDesc.idPropertyName, fetch
					);

					// add referred record table if fetched or needed for the key
					if (fetch || propDesc.keyPropertyName)
						queryTreeNode = queryTreeNode.createChildNode(
							refTargetDef.table, refTargetIdColumn, expanding,
							false, false,
							refTargetIdColumn, reverseRefPropDef.parentIdColumn);

				} else { // no link table

					// add the reference property
					queryTreeNode = this._addCollectionSimpleProperty(
						propNode, markupPrefix, childrenMarkupCtx.prefix,
						refTargetDef.table, reverseRefPropDef.column,
						this.keyColumn,
						refTargetIdColumn,
						getKeyColumn(propDesc, refTargetDesc),
						refTargetIdColumn,
						refTargetDesc.idPropertyName, fetch
					);
				}

			} else { // direct reference (via link table)

				// add the reference property
				queryTreeNode = this._addCollectionSimpleProperty(
					propNode, markupPrefix, childrenMarkupCtx.prefix,
					propDef.table, propDef.parentIdColumn, this.keyColumn,
					propDef.column,
					getKeyColumn(propDesc, refTargetDesc),
					propDef.column, refTargetDesc.idPropertyName, fetch
				);

				// add referred record table if fetched or needed for the key
				if (fetch || propDesc.keyPropertyName)
					queryTreeNode = queryTreeNode.createChildNode(
						refTargetDef.table, refTargetIdColumn, expanding, false,
						false, refTargetIdColumn, propDef.column);
			}

			// add fetched referred record
			if (fetch) {
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

	_addScalarSimpleProperty(
		propNode, markupPrefix,
		valueTable, valueTableParentIdColumn, parentTableIdColumn,
		valueTableKeyColumn, valueColumn, fetchedRef) {

		// the leaf node
		let queryTreeNode;

		// check if calculated
		let valueSelector;
		if (propNode.valueExpr) {

			// TODO: process aggregate
			if (propNode.aggregateFunc)
				throw Error('Aggregates not implemented yet.');

			// create value selector
			valueSelector = makeSelectorFor(
				propNode.valueExpr,
				markupPrefix + propNode.desc.name
			);

			// add value to this query tree node
			queryTreeNode = this;

		} else { // stored value

			// check if stored in a separate table
			if (valueTable) {

				// create child node for the table
				queryTreeNode = this.createChildNode(
					valueTable, valueTableKeyColumn, propNode.isExpanding(),
					false, propNode.desc.optional,
					valueTableParentIdColumn, parentTableIdColumn);

			} else { // stored in the same table

				// add value to this query tree node
				queryTreeNode = this;
			}

			// create value selector
			const propSql = queryTreeNode.tableAlias + '.' + valueColumn;
			valueSelector = makeSelector(
				propSql,
				markupPrefix + propNode.desc.name + (fetchedRef ? ':' : '')
			);

			// save value mapping
			this._propsSql.set(propNode.path, propSql);
		}

		// add value to the select list
		if (propNode.isSelected())
			queryTreeNode.addSelect(valueSelector);

		// return the leaf node
		return queryTreeNode;
	}

	_addCollectionSimpleProperty(
		propNode, markupPrefix, childrenMarkupPrefix,
		valueTable, valueTableParentIdColumn, parentTableIdColumn,
		valueTableKeyColumn, keyColumn, valueColumn, valuePropName,
		fetchedRef) {

		// create child node for the table
		const queryTreeNode = this.createChildNode(
			valueTable, valueTableKeyColumn, propNode.isExpanding(),
			true, propNode.desc.optional,
			valueTableParentIdColumn, parentTableIdColumn);

		// create and save the value mapping
		const propSql = queryTreeNode.tableAlias + '.' + valueColumn;
		this._propsSql.set(propNode.path, propSql);

		// add value to the select list if neccesary
		if (propNode.isSelected()) {
			queryTreeNode.addSelect(makeSelector(
				queryTreeNode.tableAlias + '.' + keyColumn,
				markupPrefix + propNode.desc.name + (fetchedRef ? ':' : '')
			));
			queryTreeNode.addSelect(makeSelector(
				queryTreeNode.tableAlias + '.' + valueColumn,
				childrenMarkupPrefix + valuePropName
			));
		}

		// return the leaf node
		return queryTreeNode;
	}

	get select() { return this._select; }

	get table() { return this._table; }

	get tableAlias() { return this._tableAlias; }

	get keyColumn() { return this._keyColumn; }

	isVirtual() { return this._virtual; }

	get joinByColumn() { return this._joinByColumn; }

	get joinToColumn() { return this._joinToColumn; }

	get joinCondition() { return this._joinCondition; }

	get order() { return this._order; }

	get children() {

		return (
			this._expandingChild ?
				this._singleRowChildren.concat(this._expandingChild) :
				this._singleRowChildren
		);
	}

	get propsSql() { return this._propsSql; }
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
				recordTypeDef.table, topIdColumn, true, false, false,
				topIdColumn, topIdColumn) :
			new QueryTreeNode(
				dbDriver, recordTypes, new Map(), recordTypeDef.table, 'z',
				topIdColumn)
	);

	// fix top table alias (note for a perfectionist: KLUDGY!)
	if (anchorNode)
		topNode._tableAlias = 'z';

	// add top record id property to have it in front of the select list
	const idPropSql = topNode.tableAlias + '.' + topIdColumn;
	topNode.addSelect(makeSelector(idPropSql, topIdPropName));
	topNode.propsSql.set(topIdPropName, idPropSql);

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

	const anchorNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), anchorTable, 'q',
		recordTypeDesc.getPropertyDesc(
			recordTypeDesc.idPropertyName).definition.column);

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

exports.buildSubquery = function() {

	//...
};
