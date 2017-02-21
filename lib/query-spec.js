'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const placeholders = require('./placeholders.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * Node in the selected properties tree.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class PropertyTreeNode {

	constructor(topPropDesc, topValueExprCtx, clause) {

		if (arguments.length === 0)
			return;

		this._path = topValueExprCtx.basePath;
		this._desc = topPropDesc;
		this._containerChain = topValueExprCtx.containerChain;
		this._childrenContainer =
			this._containerChain[this._containerChain.length - 1];

		this._clauses = new Set();
		if (clause)
			this._clauses.add(clause);
	}

	newTree(clause) {

		const newTree = new PropertyTreeNode();

		newTree._path = this._path;
		newTree._desc = this._desc;
		newTree._containerChain = this._containerChain;
		newTree._childrenContainer = this._childrenContainer;

		newTree._clauses = new Set();
		newTree._clauses.add(clause);

		return newTree;
	}

	addChild(recordTypes, propName, clause, srcPropPattern) {

		// invalid pattern function
		function invalidPropPattern(msg) {
			return new common.X2UsageError(
				'Invalid property path or pattern "' + srcPropPattern +
					'": ' + msg);
		}

		// make sure the property may have children
		if (!this._childrenContainer)
			throw invalidPropPattern(
				'property ' + this._desc.container.nestedPath +
					this._desc.name + ' of ' +
					String(this._desc.container.recordTypeName) +
					' is not an object nor reference and cannot be used as' +
					' an intermediate property in a path.');

		// cannot fetch calculated reference
		if (this._valueExpr)
			throw invalidPropPattern(
				'calculated or aggregate reference property may not be' +
					' fetched.');

		// get property descriptor
		if (!this._childrenContainer.hasProperty(propName))
			throw invalidPropPattern(
				'record type ' + this._childrenContainer.recordTypeName +
					' does not have property ' +
					this._childrenContainer.nestedPath + propName + '.');
		const propDesc = this._childrenContainer.getPropertyDesc(propName);

		// create the child node
		const childNode = new PropertyTreeNode();
		childNode[PARENT_NODE] = this;
		childNode._path = (
			this._path.length > 0 ? this._path + '.' + propName : propName);
		childNode._desc = propDesc;
		childNode._clauses = new Set();
		childNode._clauses.add(clause);

		// determine property children container, if applicable
		switch (propDesc.scalarValueType) {
		case 'object':
			childNode._childrenContainer = propDesc.nestedProperties;
			break;
		case 'ref':
			childNode._childrenContainer = recordTypes.getRecordTypeDesc(
				propDesc.refTarget);
		}
		if (childNode._childrenContainer)
			childNode._containerChain = this._containerChain.concat(
				childNode._childrenContainer);

		// process property definition
		const invalidPropDef = msg => new common.X2UsageError(
			'Property ' + this._childrenContainer.nestedPath + propName +
				' of record type ' + this._childrenContainer.recordTypeName +
				' has invalid definition: ' + msg);
		const propDef = propDesc.definition;
		if (propDef.valueExpr) {

			// allowed only in select clause
			if (clause !== 'select')
				throw invalidPropPattern(
					'calculated value property may only be used in selected' +
						' property patterns.');

			// may not be aggregate or stored
			if (propDef.aggregate || propDef.table || propDef.column ||
				propDef.presenceTest)
				throw invalidPropDef(
					'calculated value property may not be an aggregate, have a' +
						' presence test or have database table or column' +
						' associated with it.');

			// must be scalar and not an object
			if (!propDesc.isScalar() || (propDesc.scalarValueType === 'object'))
				throw invalidPropDef(
					'calculated value property must be scalar and may not be' +
						' an object.');

			// compile the property value expression
			childNode._valueExpr = new ValueExpression(
				new ValueExpressionContext(this._path, this._containerChain),
				propDef.valueExpr
			);

			// get used properties paths
			childNode._usedPropertyPaths = new Set(
				childNode._valueExpr.usedPropertyPaths);

		} else if (propDef.aggregate) {

			// allowed only in select clause
			if (clause !== 'select')
				throw invalidPropPattern(
					'aggregate property may only be used in selected' +
						' property patterns.');

			// may not be calculated value or stored
			if (propDef.table || propDef.column)
				throw invalidPropDef(
					'aggregate property may not have database table or column' +
						' associated with it.');

			// must be scalar or map and not an object
			if ((!propDesc.isScalar() && !propDesc.isMap()) ||
				(propDesc.scalarValueType === 'object'))
				throw invalidPropDef(
					'aggregate property must be scalar or a map and may not be' +
						' an object.');

			// may not have filter or order
			if (propDef.filter || propDef.order)
				throw invalidPropDef(
					'aggregate property may not have a scoped filter or order'+
						' on it.');

			// get aggregated collection path
			const aggColPath = propDef.aggregate.collection;
			if (!aggColPath)
				throw invalidPropDef(
					'aggregate property must specify aggregated collection.');

			// TODO: ...
			throw new Error('Aggregates not implemented yet.');
		}

		if (propDef.presenceTest) {

			// must be scalar optional object
			if (!propDesc.isScalar() ||
				(propDesc.scalarValueType !== 'object') || !propDesc.optional ||
				propDef.table)
				throw invalidPropDef(
					'presence test may only be specified on an optional scalar' +
						' object property stored in the parent record\'s' +
						' table.');

			// parse the test
			let topNode = this;
			while (topNode[PARENT_NODE])
				topNode = topNode[PARENT_NODE];
			childNode._usedPropertyPaths = new Set();
			childNode._presenceTest = filterBuilder.parseFilterSpec(
				recordTypes,
				topNode.desc,
				new ValueExpressionContext(
					childNode._path, childNode._containerChain),
				[ ':and', propDef.presenceTest ],
				childNode._usedPropertyPaths
			);

		} else if (
			propDesc.isScalar() &&
				(propDesc.scalarValueType === 'object') && propDesc.optional &&
				!propDef.table) {
			throw invalidPropDef(
				'optional scalar object property stored in the parent' +
					' record\'s table must have a presence test associated' +
					' with it.');
		}

		if (propDef.filter || propDef.order) {

			// must be a collection
			if (propDesc.isScalar())
				throw invalidPropDef(
					'scoped filters and orders are only allowed on non-scalar' +
						' properties.');

			// create value expression context
			const valueExprCtx = new ValueExpressionContext(
				childNode._path, childNode._containerChain);

			// collect used properties
			childNode._usedPropertyPaths = new Set();

			// process filter
			if (propDef.filter) {

				// must be a iview
				if (!propDesc.isView())
					throw invalidPropDef(
						'scoped filters are only allowed on views.');

				// parse the filter
				let topNode = this;
				while (topNode[PARENT_NODE])
					topNode = topNode[PARENT_NODE];
				childNode._filter = filterBuilder.parseFilterSpec(
					recordTypes, topNode.desc, valueExprCtx,
					[ ':and', propDef.filter ], childNode._usedPropertyPaths);
			}

			// process order
			if (propDef.order)
				childNode._order = orderBuilder.parseOrderSpec(
					valueExprCtx, propDef.order, childNode._usedPropertyPaths);
		}

		// add node to the tree
		if (!this._children)
			this._children = new Map();
		if (!this._includedPropPaths)
			this._includedPropPaths = new Set();
		this._children.set(propName, childNode);
		for (let n = this; n; n = n[PARENT_NODE])
			n._includedPropPaths.add(childNode._path);

		// return the child node
		return childNode;
	}

	/**
	 * Mark the node as used in the specified clause.
	 *
	 * @param {string} clause The clause ("select", "where", "orderBy" or
	 * "value").
	 */
	addClause(clause) {

		this._clauses.add(clause);
	}

	debranch(branches) {

		if (!branches)
			branches = new Array();

		let branch = this._cloneWithoutChildren();
		branch._expanding = !branch._desc.isScalar();
		branches.push(branch);

		if (this._children) {
			branch._includedPropPaths = new Set();
			branch._children = new Map();
			const childBranches = new Array();
			this._children.forEach((childNode, childPropName) => {
				childBranches.length = 0;
				childNode.debranch(childBranches);
				childBranches.forEach(childBranch => {
					if (!childBranch._expanding) {
						childBranch[PARENT_NODE] = branch;
						if (childBranch._includedPropPaths)
							childBranch._includedPropPaths.forEach(v => {
								branch._includedPropPaths.add(v);
							});
						branch._includedPropPaths.add(childBranch._path);
						branch._children.set(childPropName, childBranch);
					}
				});
				childBranches.forEach(childBranch => {
					if (childBranch._expanding) {
						if (branch._expandingChild) {
							branch = this._cloneWithoutChildren();
							branch._expanding = !branch._desc.isScalar();
							branch._includedPropPaths = new Set();
							branch._children = new Map();
							branches.push(branch);
						}
						childBranch[PARENT_NODE] = branch;
						if (childBranch._includedPropPaths)
							childBranch._includedPropPaths.forEach(v => {
								branch._includedPropPaths.add(v);
							});
						branch._includedPropPaths.add(childBranch._path);
						branch._children.set(childPropName, childBranch);
						branch._expandingChild = childBranch;
						branch._expanding = true;
					}
				});
			});
		}

		return branches;
	}

	combine(otherNode) {

		const combined = this._cloneWithoutChildren();

		if (this._includedPropPaths)
			combined._includedPropPaths = new Set(this._includedPropPaths);

		if (otherNode) {

			otherNode._clauses.forEach(clause => {
				combined._clauses.add(clause);
			});

			if (otherNode._includedPropPaths) {
				if (!combined._includedPropPaths)
					combined._includedPropPaths = new Set();
				otherNode._includedPropPaths.forEach(propPath => {
					combined._includedPropPaths.add(propPath);
				});
			}
		}

		const otherChildren = (otherNode && otherNode._children);
		if (this._children || otherChildren) {

			combined._children = new Map();

			if (this._children)
				this._children.forEach((childNode, childPropName) => {
					const combinedChildNode = childNode.combine(
						otherChildren && otherChildren.get(childPropName));
					combinedChildNode[PARENT_NODE] = combined;
					combined._children.set(childPropName, combinedChildNode);
				});

			if (otherChildren)
				otherChildren.forEach((childNode, childPropName) => {
					if (!combined._children.has(childPropName)) {
						const combinedChildNode = childNode.combine(null);
						combinedChildNode[PARENT_NODE] = combined;
						combined._children.set(childPropName, combinedChildNode);
					}
				});
		}

		return combined;
	}

	_cloneWithoutChildren() {

		const clone = new PropertyTreeNode();

		clone._path = this._path;
		clone._desc = this._desc;
		if (this._containerChain) {
			clone._containerChain = this._containerChain;
			clone._childrenContainer = this._childrenContainer;
		}

		clone._clauses = new Set(this._clauses);
		if (this._usedPropertyPaths)
			clone._usedPropertyPaths = new Set(this._usedPropertyPaths);

		clone._valueExpr = this._valueExpr;
		clone._aggregateFunc = this._aggregateFunc;
		clone._presenceTest = this._presenceTest;
		clone._filter = this._filter;
		clone._order = this._order;

		return clone;
	}

	/**
	 * Property path associated with the node. The path is relative to the top
	 * query record (includes the base path of the top value expression
	 * context).
	 *
	 * @type {string}
	 * @readonly
	 */
	get path() { return this._path; }

	/**
	 * Descriptor of the property represented by the node.
	 *
	 * @type {module:x2node-records~PropertyDescriptor}
	 * @readonly
	 */
	get desc() { return this._desc; }

	/**
	 * Tell if the tree below this node includes a node for the specified
	 * property path (excluding this node).
	 *
	 * @param {string} propPath Property path.
	 * @returns {boolean} <code>true</code> if the tree includes the property.
	 */
	includesProp(propPath) { return this._includedPropPaths.has(propPath); }

	/**
	 * If the property is an object or a reference, this is container for child
	 * properties.
	 *
	 * @type {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get childrenContainer() { return this._childrenContainer; }

	/**
	 * Tells if the calculation of the node property's value (including scoped
	 * filters and orders for collection properties) involves other properties.
	 *
	 * @returns {boolean} <code>true</code> if uses other properties.
	 */
	usesOtherProperties() {

		return (this._usedPropertyPaths && (this._usedPropertyPaths.size > 0));
	}

	/**
	 * If the node's property uses other properties for the value calculation,
	 * this is the paths of the used properties.
	 *
	 * @type {external:Set.<string>}
	 * @readonly
	 */
	get usedPropertyPaths() { return this._usedPropertyPaths; }

	/**
	 * Tell if the node is used in the specified clause.
	 *
	 * @param {string} clause The clause.
	 * @returns {boolean} <code>true</code> if used in the clause.
	 */
	isUsedIn(clause) { return this._clauses.has(clause); }

	/**
	 * Tell if the node is used in the select clause (shortcut for
	 * <code>isUsedIn('select')</code>).
	 *
	 * @returns {boolean} <code>true</code> if used in the select clause.
	 */
	isSelected() { return this.isUsedIn('select'); }

	/**
	 * For a calculated value or aggregate property, the value expression. Can be
	 * present only on a scalar, non-object property. For an aggregate, can be
	 * also on a non-object value map.
	 *
	 * @type {module:x2node-queries~ValueExpression}
	 * @readonly
	 */
	get valueExpr() { return this._valueExpr; }

	/**
	 * For an aggregate property, the aggregation function name.
	 *
	 * @type {string}
	 * @readonly
	 */
	get aggregateFunc() { return this._aggregateFunc; }

	/**
	 * Filter specification used to test the object presence. Can be present only
	 * on an optional scalar object property.
	 *
	 * @type {module:x2node-queries~QueryFilterSpec}
	 * @readonly
	 */
	get presenceTest() { return this._presenceTest; }

	/**
	 * The scoped filter specification. Can be present only on a collection view
	 * property.
	 *
	 * @type {module:x2node-queries~QueryFilterSpec}
	 * @readonly
	 */
	get filter() { return this._filter; }

	/**
	 * The scoped order specification. Can be present only on a collection
	 * property.
	 *
	 * @type {module:x2node-queries~QueryOrderSpec[]}
	 * @readonly
	 */
	get order() { return this._order; }

	/**
	 * Tell if the node has children.
	 *
	 * @returns {boolean} <code>true</code> if has children.
	 */
	hasChildren() { return (this._children && (this._children.size > 0)); }

	/**
	 * Get child node.
	 *
	 * @param {string} propName Child property name.
	 * @returns {module:x2node-queries~PropertyTreeNode} Child node, or
	 * <code>undefined</code> if none.
	 */
	getChild(propName) {

		return (this._children ? this._children.get(propName) : undefined);
	}

	/**
	 * Iterator for node children.
	 *
	 * @type {Iterator.<module:x2node-queries~PropertyTreeNode>}
	 * @readonly
	 */
	get children() {

		return (this._children ? this._children.values() : undefined);
	}

	hasExpandingChild() { return this._expandingChild; }

	isExpanding() { return this._expanding; }
}

/**
 * Query range specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryRangeSpec {

	constructor(offset, limit) {

		this._offset = offset;
		this._limit = limit;
	}

	get offset() { return this._offset; }

	get limit() { return this._limit; }
}

/**
 * Query specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QuerySpec {

	/**
	 * Create new query specification object.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor
	 * of the top property, values of which are being fetched by the query. When
	 * the query specification is built for a record type, this is the descriptor
	 * of the "records" property in the super-type.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Context
	 * for any value expressions that appear in the filter and order
	 * specifications.
	 * @param {string[]} [selectedPropsSpec] Selected property patterns. If not
	 * specified, <code>['*']</code> is assumed.
	 * @param {Array[]} [filterSpec] Optional specification of the filter to
	 * apply to the selected records.
	 * @param {string[]} [orderSpec] Optional order specification to apply to the
	 * selected records.
	 * @param {number[]} [rangeSpec] Optional range specification to apply to the
	 * selected records.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specifications are invalid.
	 */
	constructor(
		recordTypes, topPropDesc, valueExprCtx, selectedPropsSpec, filterSpec,
		orderSpec, rangeSpec) {

		this._recordTypes = recordTypes;

		// create complete branching tree of selected properties
		const branchingSelectedPropsTree = new PropertyTreeNode(
			topPropDesc, valueExprCtx, 'select');
		const scopedPropTrees = new Map();
		this._parseSelectedProps(
			branchingSelectedPropsTree,
			valueExprCtx,
			(selectedPropsSpec ? selectedPropsSpec : [ '*' ]),
			scopedPropTrees
		);

		// de-branch the selected properties tree and add scoped property trees
		const scopedPropTreesArray = Array.from(scopedPropTrees.entries());
		this._selectedPropsTrees = branchingSelectedPropsTree.debranch().map(
			selectedPropsTree => scopedPropTreesArray.reduce((res, pair) => {
				const scopePropPath = pair[0];
				const scopedPropTree = pair[1];
				if (!res.includesProp(scopePropPath))
					return res;
				const combinedTrees = res.combine(scopedPropTree).debranch();
				if (combinedTrees.length > 1)
					throw new common.X2UsageError(
						'Invalid property "' + scopePropPath +
							'" definition: refers to other properties that do' +
							' not lay on the same collection axis.');
				return combinedTrees[0];
			}, selectedPropsTree)
		);

		// parse top filter specification
		if (filterSpec) {
			const usedPropPaths = new Set();
			this._filter = filterBuilder.parseFilterSpec(
				recordTypes, topPropDesc, valueExprCtx, [ ':and', filterSpec ],
				usedPropPaths);
			if (this._filter) {
				this._filterPropsTree =
					branchingSelectedPropsTree.newTree('where');
				usedPropPaths.forEach(p => {
					this._addProperty(
						this._filterPropsTree, this._filterPropsTree.path, p,
						'where');
				});
			}
		}

		// parse top order specification
		if (orderSpec) {
			const usedPropPaths = new Set();
			this._order = orderBuilder.parseOrderSpec(
				valueExprCtx, orderSpec, usedPropPaths);
			if (this._order) {
				this._orderPropsTree =
					branchingSelectedPropsTree.newTree('orderBy');
				usedPropPaths.forEach(p => {
					this._addProperty(
						this._orderPropsTree, this._orderPropsTree.path, p,
						'orderBy');
				});
			}
		}

		// parse range specification
		if (rangeSpec)
			this._range = this._parseRange(rangeSpec);
	}

	/**
	 * Parse selected property patterns and build out the tree.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top selected
	 * properties tree node.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
	 * expression context to use.
	 * @param {string[]} propPatterns List of selected property patterns to
	 * parse.
	 * @param {external:Map.<string,module:x2node-queries~SingleAxisPropertyTreeNode>} scopedPropTrees
	 * Map, to which to add generated scoped property trees.
	 * @throws {module:x2node-common.X2UsageError} If the any of the patterns is
	 * invalid.
	 */
	_parseSelectedProps(topNode, valueExprCtx, propPatterns, scopedPropTrees) {

		// process direct patterns
		const excludedPaths = new Set();
		let wcPatterns = new Array();
		propPatterns.forEach(propPattern => {

			if (propPattern.startsWith('-')) { // collect exclusion pattern

				excludedPaths.add(propPattern.substring(1));

			} else { // process regular inclusion pattern

				this._addProperty(
					topNode, null, propPattern, 'select', scopedPropTrees,
					wcPatterns);
			}
		});

		// process wildcard patterns
		while (wcPatterns.length > 0) {
			const wcPatterns2 = new Array();
			wcPatterns.forEach(propPattern => {
				if (!excludedPaths.has(propPattern))
					this._addProperty(
						topNode, null, propPattern, 'select', scopedPropTrees,
						wcPatterns2);
			});
			wcPatterns = wcPatterns2;
		}
	}

	/**
	 * Add property to the selected properties tree.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top selected
	 * properties tree node.
	 * @param {?string} scopeColPath Path of the scope collection property. If
	 * provided, the pattern may only belong to the scope collection property's
	 * axis.
	 * @param {string} propPattern Property pattern. May be a wildcard pattern if
	 * the <code>clause</code> argument is "select".
	 * @param {string} clause The query clause where the property is used.
	 * @param {string[]} wcPatterns Array, to which to add extra patterns
	 * resulting in the wildcard pattern expansion. Not used unless the
	 * <code>clause</code> argument is "select".
	 * @returns {module:x2node-queries~PropertyTreeNode} The leaf node
	 * representing the property.
	 */
	_addProperty(
		topNode, scopeColPath, propPattern, clause, scopedPropTrees,
		wcPatterns) {

		// check if adding to the SELECT clause
		const selected = (clause === 'select');

		// process the pattern parts
		let expandChildren = false;
		const propPatternParts = propPattern.split('.');
		const numParts = propPatternParts.length;
		let parentNode = topNode;
		let patternPrefix = topNode.path, patternSuffix = '';
		for (let i = 0; i < numParts; i++) {
			const propName = propPatternParts[i];
			const parentPropDesc = parentNode.desc;

			// check if wildcard
			if (propName === '*') {

				// wildcards are only allowed for selected property patterns
				if (!selected)
					throw new common.X2UsageError(
						'Invalid property path "' + propPattern +
							'": wild cards are only allowed in selected' +
							' property patterns.');

				// set up the expansion
				expandChildren = true;
				if (i < numParts - 1) {
					patternSuffix =
						'.' + propPatternParts.slice(i + 1).join('.');
				}

				// done looping through pattern parts
				break;
			}

			// get the child node
			let node = parentNode.getChild(propName);

			// need to create a new node?
			if (!node) {

				// create new child node
				node = parentNode.addChild(
					this._recordTypes, propName, clause, propPattern);

				// create scoped value trees if necessary
				if ((clause !== 'value') && node.usesOtherProperties()) {
					const scopedPropTree = topNode.newTree('value');
					node.usedPropertyPaths.forEach(p => {
						this._addProperty(
							scopedPropTree, node.path, p, 'value');
					});
					scopedPropTrees.set(node.path, scopedPropTree);
				}

			} else { // existing node

				// include the existing node in the clause
				node.addClause(clause);
			}

			// make sure the node is on the scope collection axis
			if (scopeColPath && !node.desc.isScalar() &&
				!((scopeColPath === node.path) || scopeColPath.startsWith(
					node.path + '.')))
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": the property must be on the same collection axis' +
						' as ' + scopeColPath + '.');

			// add part to the reconstructed pattern prefix
			patternPrefix += propName + '.';

			// advance down the tree
			parentNode = node;
		}

		// expand selected object
		if (!expandChildren && (parentNode.desc.scalarValueType === 'object')) {

			// may not expand unless selected
			if (!selected)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": object properties are only allowed in selected' +
						' property patterns.');

			// set up the expansion
			expandChildren = true;
		}

		// generate expanded patterns
		if (expandChildren) {

			// make sure there is a children container
			if (!parentNode.childrenContainer)
				throw new common.X2UsageError(
					'Invalid property pattern "' + propPattern
						+ '": property ' + parentNode.desc.container.nestedPath +
						parentNode.desc.name + ' of ' +
						String(parentNode.desc.container.recordTypeName) +
						' is not an object nor reference and cannot be used as' +
						' an intermediate property in a path.');

			// cannot fetch calculated reference
			if (parentNode.valueExpr)
				throw new common.X2UsageError(
					'Invalid property pattern "' + propPattern +
						'": calculated or aggregate reference property may' +
						' not be fetched.');

			// generate patterns for all nested properties included by default
			parentNode.childrenContainer.allPropertyNames.forEach(propName => {
				const propDesc = parentNode.childrenContainer.getPropertyDesc(
					propName);
				const propDef = propDesc.definition;
				if (((propDef.fetchByDefault === undefined) &&
						!propDesc.isView() && !propDef.valueExpr &&
							!propDef.aggregate) ||
					propDef.fetchByDefault)
					wcPatterns.push(patternPrefix + propName + patternSuffix);
			});
		}

		// return the leaf node
		return parentNode;
	}

	/**
	 * Parse raw query range specification.
	 *
	 * @private
	 * @param {number[]} rangeSpec Two-element number array of the raw range
	 * specification.
	 * @returns {module:x2node-queries~QueryRangeSpec} Range specification
	 * object.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_parseRange(rangeSpec) {

		// validate range specification array
		if (!Array.isArray(rangeSpec) || (rangeSpec.length !== 2))
			throw new common.X2UsageError(
				'Query range specification is not a two-element array.');
		const offset = rangeSpec[0];
		const limit = rangeSpec[1];

		// validate range values
		if (((typeof offset) !== 'number') ||
			Number.isNaN(offset) || (offset < 0) ||
			((typeof limit) !== 'number') ||
			Number.isNaN(limit) || (limit < 0))
			throw new common.X2UsageError(
				'Invalid query range specification: the offset or the limit' +
					' value is not a number or is negative.');

		// create and return the range specification object
		return new QueryRangeSpec(offset, limit);
	}

	/**
	 * Single axis trees for all selected properties.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode[]}
	 * @readonly
	 */
	get selectedPropsTrees() { return this._selectedPropsTrees; }

	/**
	 * Top filter specification, or <code>undefined</code> if none.
	 *
	 * @type {module:x2node-queries~QueryFilterSpec}
	 * @readonly
	 */
	get filter() { return this._filter; }

	/**
	 * Tree of properties used in the top filter.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode}
	 * @readonly
	 */
	get filterPropsTree() { return this._filterPropsTree; }

	/**
	 * Top order specification, or <code>undefined</code> if none.
	 *
	 * @type {module:x2node-queries~QueryOrderSpec[]}
	 * @readonly
	 */
	get order() { return this._order; }

	/**
	 * Tree of properties used in the top order.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode}
	 * @readonly
	 */
	get orderPropsTree() { return this._orderPropsTree; }

	/**
	 * Range spefification, or <code>undefined</code> if none. Range
	 * specification has two number properties: zero-based <code>offset</code>
	 * and <code>limit</code>.
	 *
	 * @type {Object.<string,number>}
	 * @readonly
	 */
	get range() { return this._range; }
}

module.exports = QuerySpec;
