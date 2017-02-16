'use strict';

const common = require('x2node-common');


// validation and other query tree building functions:

function mustHaveParentIdColumn(propDesc) {
	if (!propDesc.definition.parentIdColumn)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' must have a parent id column in its definition.');
}

function mustHaveTable(propDesc) {
	if (!propDesc.definition.table)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' is not scalar and must be stored in a separate' +
				' database table.');
	mustHaveParentIdColumn(propDesc);
}

function mustHaveColumn(propDesc) {
	if (!propDesc.definition.column)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' does not have a database table column associated with it.');
}

function mustHaveKeyColumn(propDesc) {
	if (!propDesc.definition.keyColumn)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' does not have a map key database table column' +
				' associated with it.');
}

function getIdColumn(container) {
	const idColumn = container.getPropertyDesc(
		container.idPropertyName).definition.column;
	if (!idColumn)
		throw new common.X2UsageError(
			'Property ' + container.nestedPath + container.idPropertyName +
				' of record type ' + container.recordTypeName +
				' does not have a database table column associated with it.');
	return idColumn;
}

function getKeyColumn(propDesc, keyPropContainer) {
	const propDef = propDesc.definition;
	if (propDef.keyColumn)
		return propDef.keyColumn;
	if (!propDesc.keyPropertyName)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' does not have a map key database table column' +
				' nor key property associated with it.');
	const keyPropDesc = keyPropContainer.getPropertyDesc(
		propDesc.keyPropertyName);
	if (!keyPropDesc.definition.column || keyPropDesc.definition.table)
		throw new common.X2UsageError(
			'Invalid definition of property ' +
				propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				': key property ' + keyPropDesc.name +
				' must be have a database table column associated with' +
				' it and must be stored in the same table as the record.');
	return keyPropDesc.definition.column;
}

function getRefTargetDesc(recordTypes, propDesc) {
	const refTargetDesc = recordTypes.getRecordTypeDesc(propDesc.refTarget);
	if (!refTargetDesc.definition.table)
		throw new common.X2UsageError(
			'Record type ' + refTargetDesc.name +
				' does not have a database table associated with it' +
				' but is used as a target of reference property ' +
				propDesc.name + ' of ' +
				propDesc.container.recordTypeName + '.');
	return refTargetDesc;
}

function getReverseRefPropDesc(propDesc, refTargetDesc) {
	if (propDesc.container.nestedPath.length > 0)
		throw new common.X2UsageError(
			'Invalid definition of property ' +
				propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				': only top level record type reference property may' +
				' have reverse reference property in its definition.');
	const propDef = propDesc.definition;
	if (!refTargetDesc.hasProperty(propDef.reverseRefProperty))
		throw new common.X2UsageError(
			'Invalid definition of property ' + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				': reference target record type ' + propDesc.refTarget +
				' does not have reverse reference property ' +
				propDef.reverseRefProperty + '.');
	const reverseRefPropDesc = refTargetDesc.getPropertyDesc(
		propDef.reverseRefProperty);
	const myRecordTypeName = propDesc.container.recordTypeName;
	if (!reverseRefPropDesc.isRef() ||
		!reverseRefPropDesc.isScalar() ||
		reverseRefPropDesc.isPolymorph() ||
		(reverseRefPropDesc.refTarget !== myRecordTypeName))
		throw new common.X2UsageError(
			'Invalid definition of property ' + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				': the reverse reference property ' +
				propDef.reverseRefProperty + ' of ' +
				propDesc.refTarget + ' is not a scalar non-polymorphic' +
				' reference property pointing at ' +
				propDesc.container.recordTypeName + ' record type.');
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


/**
 * Represents a selected value.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class SelectedValue {

	constructor(sql, markup) {

		this._sql = sql;
		this._markup = markup;
	}

	get sql() { return this._sql; }

	//...
}

function makeSqlSelectedValue() {}
function makeValueExprSelectedValue() {}

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
	 * @param {boolean} virtual <code>true</code> if may select no rows.
	 * @param {string} joinByColumn Name of the column in the node's table used
	 * to join to the parent table.
	 * @param {string} joinToColumn Name of the column in the parent node's table
	 * used for the join.
	 * @returns {module:x2node-queries~QueryTreeNode} The new child node.
	 */
	createChildNode(
		table, keyColumn, expanding, virtual, joinByColumn, joinToColumn) {

		const childNode = new QueryTreeNode(
			this._dbDriver, this._recordTypes, this._propsSql,
			table,
			this._tableAlias + String.fromCharCode(this._nextTableAliasDisc++),
			keyColumn
		);

		childNode._virtual = virtual;
		childNode._joinByColumn = joinByColumn;
		childNode._joinToColumn = joinToColumn;

		childNode[PARENT_NODE] = this;

		if (expanding)
			this._expandingChild = childNode;
		else
			this._singleRowChildren.push(childNode);

		return childNode;
	}

	/**
	 * Add element to the select list.
	 *
	 * @param {string} sql Value SQL.
	 * @param {string} markup Column markup for the result set parser.
	 * @returns {module:x2node-queries~SelectedValue} Added selected value
	 * object.
	 */
	addSelect(sql, markup) {

		const val = new SelectedValue(sql, markup);

		this._select.push(val);

		return val;
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

		// create markup context for possible children
		const markupPrefix = markupCtx.prefix;
		const childrenMarkupCtx = (
			propNode.hasChildren() ? {
				prefix: markupPrefix.substring(0, markupPrefix.length - 1) +
					String.fromCharCode(markupCtx.nextChildMarkupDisc++) + '$',
				nextChildMarkupDisc: 'a'.charCodeAt(0)
			} :
			undefined
		);

		// process property node depending on its type
		const propDesc = propNode.desc;
		const propDef = propDesc.definition;
		let queryTreeNode;
		let fetch, keyColumn, anchor, valueExpr;
		let refTargetDesc, refTargetIdColumn;
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

			// check if stored in a separate table
			if (propDef.table) {

				// validate definition
				mustHaveColumn(propDesc);
				mustHaveParentIdColumn(propDesc);

				// create child node for the value table
				queryTreeNode = this.createChildNode(
					propDef.table, null, false, propDesc.optional,
					propDef.parentIdColumn, this.keyColumn);

			} else { // stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// add value to this query tree node
				queryTreeNode = this;
			}

			// save value mapping
			valueExpr = queryTreeNode.tableAlias + '.' + propDef.column;
			this._propsSql.set(propNode.path, valueExpr);

			// add value selector
			if (propNode.isSelected())
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix + propDesc.name
				});

			break;

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// validate definition
				mustHaveParentIdColumn(propDesc);

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propDef.table, propDef.parentIdColumn, false,
					propDesc.optional, propDef.parentIdColumn, this.keyColumn);

				// create anchor selector
				anchor = {
					expr: queryTreeNode.tableAlias + '.' +
						queryTreeNode.keyColumn,
					markup: markupPrefix + propDesc.name
				};

			} else { // stored in the parent table

				// TODO: support presence expression
				if (propDesc.optional)
					throw new common.X2UsageError(
						'Property ' + propDesc.container.nestedPath +
							propDesc.name + ' of record type ' +
							propDesc.container.recordTypeName +
							' may not be optional as it is stored in the' +
							' parent record table.');

				// create anchor selector
				anchor = {
					expr: this._dbDriver.booleanLiteral(true),
					markup: markupPrefix + propDesc.name
				};

				// add child properties to the same node
				queryTreeNode = this;
			}

			// add anchor selector
			if (propNode.isSelected()) {
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

			// add selected child properties
			for (let p of propNode.children)
				queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

			break;

		case 'scalar:object:poly':

			// TODO:...

			break;

		case 'scalar:ref:mono':

			// get target record info
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetIdColumn = getIdColumn(refTargetDesc);

			// check if fetched
			fetch = propNode.hasChildren();

			// check if dependent reference
			if (propDef.reverseRefProperty) {

				// get reverse reference property descriptor
				const reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				const reverseRefPropDef = reverseRefPropDesc.definition;

				// check how the reverse reference property is stored
				if (reverseRefPropDef.table) {

					this._addLinkTableScalarRefProp(
						propNode, reverseRefPropDesc,
						reverseRefPropDef.column,
						reverseRefPropDef.parentIdColumn, fetch, markupPrefix,
						refTargetDesc, refTargetIdColumn);

				} else { // stored in the target record table

					// validate definition
					mustHaveColumn(reverseRefPropDesc);

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, refTargetIdColumn,
						false, propDesc.optional,
						reverseRefPropDef.column, this.keyColumn);

					// save value mapping
					valueExpr =
						queryTreeNode.tableAlias + '.' + refTargetIdColumn;
					this._propsSql.set(propNode.path, valueExpr);

					// add selected values
					if (propNode.isSelected()) {

						// add anchor/value selector
						anchor = {
							expr: valueExpr,
							markup: markupPrefix + propDesc.name +
								(fetch ? ':' : '')
						};
						queryTreeNode.props.push(anchor);

						// add anchor to the keys if fetched
						if (fetch)
							queryTreeNode.keys.push(anchor);
					}
				}

			} else if (propDef.table) { // direct reference in a link table

				this._addLinkTableScalarRefProp(
					propNode, propDesc, propDef.parentIdColumn,
					propDef.column, fetch, markupPrefix, refTargetDesc,
					refTargetIdColumn);

			} else { // direct reference stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// save value mapping
				valueExpr = this.tableAlias + '.' + propDef.column;
				this._propsSql.set(propNode.path, valueExpr);

				// create anchor/value selector
				anchor = {
					expr: valueExpr,
					markup: markupPrefix + propDesc.name +
						(fetch ? ':' : '')
				};

				// add child node if fetched
				if (fetch) {

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, refTargetIdColumn,
						false, propDesc.optional,
						refTargetIdColumn, propDef.column);

					// add anchor and value
					if (propNode.isSelected()) {
						queryTreeNode.keys.push(anchor);
						queryTreeNode.props.push(anchor);
					}

				} else { // no fetch

					// add value
					if (propNode.isSelected())
						this.props.push(anchor);
				}
			}

			// add fetched target properties
			if (fetch)
				for (let p of propNode.children)
					queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

			break;

		case 'scalar:ref:poly':

			// TODO:...

			break;

		case 'array:string:mono':
		case 'array:number:mono':
		case 'array:boolean:mono':
		case 'array:datetime:mono':
		case 'map:string:mono':
		case 'map:number:mono':
		case 'map:boolean:mono':
		case 'map:datetime:mono':

			// validate definition
			mustHaveTable(propDesc);
			mustHaveColumn(propDesc);
			if (propDesc.isMap())
				mustHaveKeyColumn(propDesc);

			// create child node for the values table
			queryTreeNode = this.createChildNode(
				propDef.table, null, true, propDesc.optional,
				propDef.parentIdColumn, this.keyColumn);

			// save value mapping
			valueExpr = queryTreeNode.tableAlias + '.' + propDef.column;
			this._propsSql.set(propNode.path, valueExpr);

			// add anchor and value selectors
			if (propNode.isSelected()) {
				queryTreeNode.props.push({
					expr: queryTreeNode.tableAlias + '.' + (
						propDesc.isMap() ? propDef.keyColumn :
							propDef.parentIdColumn),
					markup: markupPrefix + propDesc.name
				});
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix.substring(0, markupPrefix.length - 1) +
						String.fromCharCode(markupCtx.nextChildMarkupDisc++) +
						'$'
				});
			}

			break;

		case 'array:object:mono':
		case 'map:object:mono':

			// validate definition
			mustHaveTable(propDesc);

			// create child node for the objects table
			keyColumn = (
				propDesc.isMap() ?
					getKeyColumn(propDesc, propDesc.nestedProperties) :
					getIdColumn(propDesc.nestedProperties)
			);
			queryTreeNode = this.createChildNode(
				propDef.table, keyColumn, true, propDesc.optional,
				propDef.parentIdColumn, this.keyColumn);

			// add anchor selector
			if (propNode.isSelected()) {
				anchor = {
					expr: queryTreeNode.tableAlias + '.' + keyColumn,
					markup: markupPrefix + propDesc.name
				};
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

			// add selected child properties
			for (let p of propNode.children)
				queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

			break;

		case 'array:object:poly':
		case 'map:object:poly':

			// TODO:...

			break;

		case 'array:ref:mono':
		case 'map:ref:mono':

			// get target record info
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetIdColumn = getIdColumn(refTargetDesc);

			// check if fetched
			fetch = propNode.hasChildren();

			// check if dependent reference
			if (propDef.reverseRefProperty) {

				// get reverse reference property descriptor
				const reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				const reverseRefPropDef = reverseRefPropDesc.definition;

				// check how the reverse reference property is stored
				if (reverseRefPropDef.table) {

					this._addLinkTableCollectionRefProp(
						propNode, clauses,
						reverseRefPropDesc, reverseRefPropDef.column,
						reverseRefPropDef.parentIdColumn, fetch, markupCtx,
						refTargetDesc, refTargetIdColumn, childrenMarkupCtx);

				} else { // stored in the target record table

					// validate definition
					mustHaveColumn(reverseRefPropDesc);

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, refTargetIdColumn,
						true, propDesc.optional,
						reverseRefPropDef.column, this.keyColumn);

					// save value mapping
					valueExpr =
						queryTreeNode.tableAlias + '.' + refTargetIdColumn;
					this._propsSql.set(propNode.path, valueExpr);

					// add selected values
					if (propNode.isSelected()) {

						// add anchor selector
						anchor = {
							expr: queryTreeNode.tableAlias + '.' +  (
								propDesc.isMap() ?
									getKeyColumn(propDesc, refTargetDesc) :
									refTargetIdColumn
							),
							markup: markupPrefix + propDesc.name +
								(fetch ? ':' : '')
						};
						queryTreeNode.props.push(anchor);

						// add value selector(s)
						if (fetch) {

							// add anchor to the keys
							queryTreeNode.keys.push(anchor);

							// add id property selector
							const idSelector = {
								expr: valueExpr,
								markup: childrenMarkupCtx.prefix +
									refTargetDesc.idPropertyName
							};
							queryTreeNode.keys.push(idSelector);
							queryTreeNode.props.push(idSelector);

						} else { // no fetch

							// add value selector
							queryTreeNode.props.push({
								expr: valueExpr,
								markup: markupPrefix.substring(
									0, markupPrefix.length - 1) +
									String.fromCharCode(
										markupCtx.nextChildMarkupDisc++) + '$'
							});
						}
					}

					// add the rest of the fetched properties
					if (fetch)
						for (let p of propNode.children)
							if (!p.desc.isId())
								queryTreeNode.addProperty(
									p, clauses, childrenMarkupCtx);
				}

			} else { // direct reference (must have link table)

				this._addLinkTableCollectionRefProp(
					propNode, clauses,
					propDesc, propDef.parentIdColumn, propDef.column, fetch,
					markupCtx, refTargetDesc, refTargetIdColumn,
					childrenMarkupCtx);
			}

			break;

		case 'array:ref:poly':
		case 'map:ref:poly':

			// TODO:...
		}
	}

	/**
	 * Add scalar reference property attached via a link table.
	 *
	 * @private
	 * @todo describe params
	 */
	_addLinkTableScalarRefProp(
		propNode, linkTablePropDesc, linkTableJoinByColumn,
		linkTableKeyColumn, fetch, markupPrefix, refTargetDesc,
		refTargetIdColumn) {

		const propDesc = propNode.desc;

		// validate definition
		mustHaveColumn(linkTablePropDesc);
		mustHaveParentIdColumn(linkTablePropDesc);

		// create child node for the link table
		const queryTreeNode = this.createChildNode(
			linkTablePropDesc.definition.table, linkTableKeyColumn,
			false, propDesc.optional,
			linkTableJoinByColumn, this.keyColumn);

		// save value mapping
		const valueExpr =
			queryTreeNode.tableAlias + '.' + queryTreeNode.keyColumn;
		this._propsSql.set(propNode.path, valueExpr);

		// create anchor/value selector
		const anchor = {
			expr: valueExpr,
			markup: markupPrefix + propDesc.name + (fetch ? ':' : '')
		};

		// add child node if fetched
		if (fetch) {

			// create child node for the referred records table
			queryTreeNode = queryTreeNode.createChildNode(
				refTargetDesc.definition.table, refTargetIdColumn,
				false, queryTreeNode.virtual,
				refTargetIdColumn, queryTreeNode.keyColumn);

			// add anchor and value
			if (propNode.isSelected()) {
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

		} else { // no fetch

			// add value
			if (propNode.isSelected())
				queryTreeNode.props.push(anchor);
		}
	}

	/**
	 * Add collection reference property attached via a link table.
	 *
	 * @private
	 * @todo describe params
	 */
	_addLinkTableCollectionRefProp(
		propNode, clauses, linkTablePropDesc,
		linkTableJoinByColumn, linkTableKeyColumn, fetch, markupCtx,
		refTargetDesc, refTargetIdColumn, childrenMarkupCtx) {

		const propDesc = propNode.desc;
		const propDef = propDesc.definition;
		const markupPrefix = markupCtx.prefix;

		// validate definition
		mustHaveTable(linkTablePropDesc);
		mustHaveColumn(linkTablePropDesc);

		// create child node for the link table
		let queryTreeNode = this.createChildNode(
			linkTablePropDesc.definition.table, linkTableKeyColumn,
			true, propDesc.optional,
			linkTableJoinByColumn, this.keyColumn);

		// create anchor selector
		const anchor = {
			markup: markupPrefix + propDesc.name + (fetch ? ':' : '')
		};
		let targetTableAdded = false, keyColumn;
		if (propDesc.isMap()) {
			keyColumn = getKeyColumn(propDesc, refTargetDesc);
			if (propDesc.keyPropertyName) {
				queryTreeNode = queryTreeNode.createChildNode(
					refTargetDesc.definition.table, refTargetIdColumn,
					false, false,
					refTargetIdColumn, queryTreeNode.keyColumn);
				targetTableAdded = true;
			}
		} else { // array
			keyColumn = (
				propDef.keyColumn ? propDef.keyColumn : linkTableKeyColumn);
		}
		anchor.expr = queryTreeNode.tableAlias + '.' + keyColumn;

		// add value column(s)
		let valueExpr;
		if (fetch) {

			// create node for the referred records table if necessary
			if (!targetTableAdded)
				queryTreeNode = queryTreeNode.createChildNode(
					refTargetDesc.definition.table, refTargetIdColumn,
					false, false,
					refTargetIdColumn, queryTreeNode.keyColumn);

			// create value expression
			valueExpr = queryTreeNode.tableAlias + '.' + refTargetIdColumn;

			// add selected values
			if (propNode.isSelected()) {

				// add anchor and value selectors
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);

				// add id property selector
				const idSelector = {
					expr: valueExpr,
					markup: childrenMarkupCtx.prefix +
						refTargetDesc.idPropertyName
				};
				queryTreeNode.keys.push(idSelector);
				queryTreeNode.props.push(idSelector);
			}

			// add the rest of the fetched properties
			for (let p of propNode.children)
				if (!p.desc.isId())
					queryTreeNode.addProperty(p, clauses, childrenMarkupCtx);

		} else { // no fetch

			// create value expression
			valueExpr = queryTreeNode.tableAlias + '.' + (
				targetTableAdded ? refTargetIdColumn : linkTableKeyColumn);

			// add anchor and value selectors
			if (propNode.isSelected()) {
				queryTreeNode.props.push(anchor);
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix.substring(0, markupPrefix.length - 1) +
						String.fromCharCode(markupCtx.nextChildMarkupDisc++) +
						'$'
				});
			}
		}

		// save value mapping
		this._propsSql.set(propNode.path, valueExpr);
	}

	get table() { return this._table; }

	get tableAlias() { return this._tableAlias; }

	get keyColumn() { return this._keyColumn; }

	isVirtual() { return this._virtual; }

	get joinByColumn() { return this._joinByColumn; }

	get joinToColumn() { return this._joinToColumn; }

	get propsSql() { return this._propsSql; }
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
exports.buildDirectRecordsQueryTree = function(
	dbDriver, recordTypes, propsTree) {

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
	const topNode = new QueryTreeNode(
		dbDriver, recordTypes, new Map(), recordTypeDef.table, 'z', topIdColumn);

	// add top record id property to have it in front of the select list
	topNode.propsSql.set(
		topIdPropName,
		topNode.addSelect(
			topNode.tableAlias + '.' + topIdColumn, topIdPropName
		).sql
	);

	// add the rest of selected properties
	const topMarkupCtx = {
		prefix: '',
		nextChildMarkupDisc: 'a'.charCodeAt(0)
	};
	const clauses = [ 'select', 'value', 'where', 'orderBy' ];
	for (let p of propsTree.children)
		if (!p.desc.isId()) // already included
			topNode.addProperty(p, clauses, topMarkupCtx);

	// return the query tree
	return topNode;
};

exports.buildAnchoredRecordsQueryTree = function() {};

exports.buildIdsQueryTree = function() {};

exports.buildSubqueryTree = function() {};
