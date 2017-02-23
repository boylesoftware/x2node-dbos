'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * Properties tree node.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class PropertyTreeNode {

	/**
	 * Create new node. Not used from outside of the class.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} propPath Property path including the tree's base value
	 * expression context base path.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Descriptor of
	 * the property represented by the node.
	 * @param {module:x2node-records~PropertiesContainer[]} containerChain Chain
	 * of property containers leading to the property. The first element in the
	 * chain is the container corresponding to the property with empty path. The
	 * last element is the container of the node property's children, or
	 * <code>null</code> if the property cannot have children (not an object nor
	 * a reference).
	 * @param {external:Set.<string>} clauses The clauses set.
	 */
	constructor(recordTypes, propPath, propDesc, containerChain, clauses) {

		this._recordTypes = recordTypes;
		this._path = propPath;
		this._desc = propDesc;
		this._containerChain = containerChain;

		this._clauses = clauses;

		this._includedPropPaths = new Set();

		this._childrenContainer = containerChain[containerChain.length - 1];
		if (this._childrenContainer)
			this._children = new Map();
	}

	/**
	 * Create top node of a tree.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor
	 * of the top property, values of which are being fetched by the query. When
	 * the query specification is built for a record type, this is the descriptor
	 * of the "records" super-property.
	 * @param {module:x2node-queries~ValueExpressionContext} baseValueExprCtx
	 * Context for any value expressions used by the properties in the tree.
	 * @param {string} [clause] Optional clause to associate the node with.
	 * @returns {module:x2node-queries~PropertyTreeNode} The top tree node.
	 */
	static createTopNode(recordTypes, topPropDesc, baseValueExprCtx, clause) {

		const topNode = new PropertyTreeNode(
			recordTypes, baseValueExprCtx.basePath, topPropDesc,
			baseValueExprCtx.containerChain, new Set());

		if (clause)
			topNode.addClause(clause);

		return topNode;
	}

	/**
	 * Create new tree by cloning this node and using it as the top node for the
	 * new tree. Children and clauses are not carried over.
	 *
	 * @param {string} clause Clause to associate the new node with.
	 * @returns {module:x2node-queries~PropertyTreeNode} The new top tree node.
	 */
	newTree(clause) {

		const newTree = new PropertyTreeNode(
			this._recordTypes, this._path, this._desc, this._containerChain,
			new Set());

		newTree.addClause(clause);

		return newTree;
	}

	/**
	 * Create and add a child property node to this node.
	 *
	 * @param {string} propName Property name. Must be present in the node's
	 * children container.
	 * @param {string} clause Clause to associate the new child node with.
	 * @param {string} srcPropPattern Property pattern that caused adding the
	 * child node. Used only for error messages.
	 * @returns {module:x2node-queries~PropertyTreeNode} The new child node.
	 */
	addChild(propName, clause, srcPropPattern) {

		// invalid pattern function
		const invalidPropPattern = msg => new common.X2UsageError(
			'Invalid property path or pattern "' + srcPropPattern + '": ' + msg);

		// make sure the property may have children
		if (!this._childrenContainer)
			throw invalidPropPattern(
				'property ' + this._desc.container.nestedPath +
					this._desc.name + ' of ' +
					String(this._desc.container.recordTypeName) +
					' is not an object nor reference and cannot be used as' +
					' an intermediate element in a path.');

		// may not use children of a calculated property
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

		// determine property children container, if applicable
		let childrenContainer = null;
		switch (propDesc.scalarValueType) {
		case 'object':
			childrenContainer = propDesc.nestedProperties;
			break;
		case 'ref':
			childrenContainer = this._recordTypes.getRecordTypeDesc(
				propDesc.refTarget);
		}

		// create the child node
		const childNode = new PropertyTreeNode(
			this._recordTypes,
			(this._path.length > 0 ? this._path + '.' + propName : propName),
			propDesc,
			this._containerChain.concat(childrenContainer),
			new Set()
		);
		childNode[PARENT_NODE] = this;

		// add the child node clause
		childNode.addClause(clause);

		// add node to the tree
		this._children.set(propName, childNode);
		for (let n = this; n; n = n[PARENT_NODE])
			n._includedPropPaths.add(childNode._path);

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
			childNode._presenceTest = filterBuilder.buildFilter(
				this._recordTypes,
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
				childNode._filter = filterBuilder.buildFilter(
					this._recordTypes, topNode.desc, valueExprCtx,
					[ ':and', propDef.filter ], childNode._usedPropertyPaths);
			}

			// process order
			if (propDef.order)
				childNode._order = orderBuilder.buildOrder(
					valueExprCtx, propDef.order, childNode._usedPropertyPaths);
		}

		// return the child node
		return childNode;
	}

	/**
	 * Mark the node as used in the specified clause.
	 *
	 * @param {string} clause The clause.
	 */
	addClause(clause) {

		this._clauses.add(clause);
	}

	/**
	 * Create new trees, called branches, by cloning this node, using it as
	 * the top node for each branch and recursively proceeding into its children.
	 * All properties included in a single branch are guaranteed to lay on a
	 * single collection axis. The method does not change this tree.
	 *
	 * @param {Array} [branches] Array, to which to add the generated branches.
	 * If not provided, new array is automatically created.
	 * @returns {module:x2node-queries~PropertyTreeNode[]} The array containing
	 * the generated branches.
	 */
	debranch(branches) {

		if (!branches)
			branches = new Array();

		let branch = this._cloneWithoutChildren();
		branch._expanding = !branch._desc.isScalar();
		branches.push(branch);

		if (this._children) {
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

	/**
	 * Recursively combine this node with another node and return the new
	 * combined tree. The method does not change this node.
	 *
	 * @param {module:x2node-queries~PropertyTreeNode} [otherNode] The other
	 * node. If not provided, the (sub)tree is cloned without combining with
	 * anything.
	 * @returns {module:x2node-queries~PropertyTreeNode} New combined tree.
	 */
	combine(otherNode) {

		const combined = this._cloneWithoutChildren();

		if (this._includedPropPaths)
			combined._includedPropPaths = new Set(this._includedPropPaths);

		if (otherNode) {

			if (otherNode._path !== combined._path)
				throw new common.X2UsageError(
					'Combining nodes requires them to share the same' +
						' property path.');

			otherNode._clauses.forEach(clause => {
				combined.addClause(clause);
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

	/**
	 * Clone this node without carrying over its children.
	 *
	 * @private
	 * @returns {module:x2node-queries~PropertyTreeNode} The node clone.
	 */
	_cloneWithoutChildren() {

		const clone = new PropertyTreeNode(
			this._recordTypes, this._path, this._desc, this._containerChain,
			new Set(this._clauses));

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
	 * Property path associated with the node. The path includes the tree's base
	 * value expression context base path.
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
	 * Tell if the tree <em>below</em> this node includes a node for the specified
	 * property path (this node is excluded).
	 *
	 * @param {string} propPath Property path.
	 * @returns {boolean} <code>true</code> if the tree includes the property.
	 */
	includesProp(propPath) { return this._includedPropPaths.has(propPath); }

	/**
	 * If the property is an object or a reference, this is container for child
	 * properties. Otherwise, it's <code>null</code>.
	 *
	 * @type {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get childrenContainer() { return this._childrenContainer; }

	/**
	 * Tell if the calculation of the node property's value involves other
	 * properties. This involves property value and aggregation expressions,
	 * scoped collection filters and orders and embedded optional object presence
	 * tests, all of which may refer to other properties.
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
	 * @type {module:x2node-queries~QueryFilter}
	 * @readonly
	 */
	get presenceTest() { return this._presenceTest; }

	/**
	 * The scoped filter specification. Can be present only on a collection view
	 * property.
	 *
	 * @type {module:x2node-queries~QueryFilter}
	 * @readonly
	 */
	get filter() { return this._filter; }

	/**
	 * The scoped order specification. Can be present only on a collection
	 * property.
	 *
	 * @type {module:x2node-queries~QueryOrder[]}
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

		return this._children.get(propName);
	}

	/**
	 * Iterator for node children.
	 *
	 * @type {external:Iterator.<module:x2node-queries~PropertyTreeNode>}
	 * @readonly
	 */
	get children() {

		return this._children.values();
	}

	/**
	 * Tell if the subtree rooted at this node is expanding (this node is for a
	 * collection property or there are collection properties among its
	 * descendants).
	 *
	 * @returns {boolean} <code>true</code> if expanding.
	 */
	isExpanding() { return this._expanding; }

	/**
	 * Tell if the tree <em>below</em> this node is expanding (has any collection
	 * property descendants).
	 */
	hasExpandingChild() { return this._expandingChild; }
}


/**
 * Build selected properties tree according to the specified property patterns
 * and debranch it.
 *
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top property, values of which are being fetched by the query. When the
 * query specification is built for a record type, this is the descriptor of the
 * "records" super-property.
 * @param {module:x2node-queries~ValueExpressionContext} baseValueExprCtx
 * Context for any value expressions used by the properties in the tree.
 * @param {string[]} propPatterns Property patterns.
 * @returns {module:x2node-queries~PropertyTreeNode[]} Array containing the tree
 * branches.
 * @throws {module:x2node-common.X2UsageError} If the provided specifications are
 * invalid.
 */
function buildSelectTreeBranches(
	recordTypes, topPropDesc, baseValueExprCtx, propPatterns) {

	// create complete branching tree
	const branchingPropsTree = PropertyTreeNode.createTopNode(
		recordTypes, topPropDesc, baseValueExprCtx, 'select');

	// process direct patterns
	const valuePropsTrees = new Map();
	const excludedPaths = new Set();
	let wcPatterns = new Array();
	propPatterns.forEach(propPattern => {
		if (propPattern.startsWith('-'))
			excludedPaths.add(propPattern.substring(1));
		else
			addProperty(
				branchingPropsTree, null, propPattern, 'select',
				valuePropsTrees, wcPatterns);
	});

	// process wildcard patterns
	while (wcPatterns.length > 0) {
		const wcPatterns2 = new Array();
		wcPatterns.forEach(propPattern => {
			if (!excludedPaths.has(propPattern))
				addProperty(
					branchingPropsTree, null, propPattern, 'select',
					valuePropsTrees, wcPatterns2);
		});
		wcPatterns = wcPatterns2;
	}

	// de-branch the tree and add value trees
	const valuePropsTreesArray = Array.from(valuePropsTrees.entries());
	return branchingPropsTree.debranch().map(
		branch => valuePropsTreesArray.reduce((res, pair) => {
			const valuePropPath = pair[0];
			const valuePropsTree = pair[1];
			if (!res.includesProp(valuePropPath))
				return res;
			const combinedTrees = res.combine(valuePropsTree).debranch();
			if (combinedTrees.length > 1)
				throw new common.X2UsageError(
					'Invalid property "' + valuePropPath +
						'" definition: refers to other properties that do' +
						' not lay on the same collection axis.');
			return combinedTrees[0];
		}, branch)
	);
};

/**
 * Build supporting properties tree. The resulting tree is not debranched.
 *
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top property, values of which are being fetched by the query.
 * @param {module:x2node-queries~ValueExpressionContext} baseValueExprCtx
 * Context for any value expressions used by the properties in the tree.
 * @param {string} clause The supporting clause.
 * @param {external:Iterable.<string>} propPaths Used property paths.
 * @returns {module:x2node-queries~PropertyTreeNode} The tree.
 */
function buildSupportingTree(
	recordTypes, topPropDesc, baseValueExprCtx, clause, propPaths) {

	// create the tree
	const propsTree = PropertyTreeNode.createTopNode(
		recordTypes, topPropDesc, baseValueExprCtx, clause);

	// add the properties
	for (let propPath of propPaths)
		addProperty(propsTree, baseValueExprCtx.basePath, propPath, clause);

	// return the tree
	return propsTree;
}

/**
 * Add property to the properties tree.
 *
 * @private
 * @param {module:x2node-queries~PropertyTreeNode} topNode Top node of the
 * properties tree.
 * @param {?string} scopeColPath Path of the scope collection property. If
 * provided, the pattern may only belong to the scope collection property's axis.
 * @param {string} propPattern Property pattern. May be a wildcard pattern if the
 * <code>clause</code> argument is "select".
 * @param {string} clause The query clause where the property is used.
 * @param {external:Map.<string,module:x2node-queries~PropertyTreeNode>} valuePropsTrees
 * Map, to which to add generated value property trees.
 * @param {string[]} wcPatterns Array, to which to add extra patterns resulting
 * in the wildcard pattern expansion. Not used unless the <code>clause</code>
 * argument is "select".
 * @returns {module:x2node-queries~PropertyTreeNode} The leaf node representing
 * the property.
 */
function addProperty(
	topNode, scopeColPath, propPattern, clause, valuePropsTrees, wcPatterns) {

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
			if (i < numParts - 1)
				patternSuffix = '.' + propPatternParts.slice(i + 1).join('.');

			// done looping through pattern parts
			break;
		}

		// get the child node
		let node = parentNode.getChild(propName);

		// need to create a new node?
		if (!node) {

			// create new child node
			node = parentNode.addChild(propName, clause, propPattern);

			// create scoped value trees if necessary
			if ((clause !== 'value') && node.usesOtherProperties()) {
				const scopedPropTree = topNode.newTree('value');
				node.usedPropertyPaths.forEach(p => {
					addProperty(scopedPropTree, node.path, p, 'value');
				});
				valuePropsTrees.set(node.path, scopedPropTree);
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
					'": the property must be on the same collection axis as ' +
					scopeColPath + '.');

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
					!propDesc.isView() &&
					!propDef.valueExpr &&
					!propDef.aggregate) || propDef.fetchByDefault)
				wcPatterns.push(patternPrefix + propName + patternSuffix);
		});
	}

	// return the leaf node
	return parentNode;
}

// export the builder functions
exports.buildSelectTreeBranches = buildSelectTreeBranches;
exports.buildSupportingTree = buildSupportingTree;
