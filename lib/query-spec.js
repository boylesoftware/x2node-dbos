'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const placeholders = require('./placeholders.js');


/**
 * Node in the selected properties tree.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class PropertyTreeNode {

	constructor(propPath, propDesc, containerChain) {

		this._path = propPath;
		this._desc = propDesc;
		this._usedIn = new Set();
		if (containerChain) {
			this._containerChain = containerChain;
			this._childrenContainer = containerChain[containerChain.length - 1];
		}
	}

	addClause(clause) {

		this._usedIn.add(clause);
	}

	addChild(childPropName, node) {

		if (!this._children)
			this._children = new Map();

		this._children.set(childPropName, node);
	}

	get path() { return this._path; }

	get desc() { return this._desc; }

	isUsedIn(clause) { return this._usedIn.has(clause); }

	hasChildren() { return (this._children && (this._children.size > 0)); }

	get children() { return this._children.values(); }

	getChild(childPropName) {

		return (this._children ? this._children.get(childPropName) : undefined);
	}

	get childrenContainer() { return this._childrenContainer; }

	get containerChain() { return this._containerChain; }

	get filter() { return this._filter; }
	set filter(filter) { this._filter = filter; }

	get order() { return this._order; }
	set order(order) { this._order = order; }
}

/**
 * Query filter specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @abstract
 */
class QueryFilterSpec {

	constructor(type) {

		this._type = type;
	}

	get type() { return this._type; }
}

/**
 * Query filter logical junction specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends module:x2node-queries~QueryFilterSpec
 */
class QueryFilterJuncSpec extends QueryFilterSpec {

	constructor(juncType, invert) {
		super('junction');

		this._juncType = juncType;
		this._invert = invert;
	}

	addElement(element) {

		if (!this._elements)
			this._elements = new Array();

		this._elements.push(element);
	}

	isInverted() { return this._invert; }

	isEmpty() {

		return (
			(this._elements === undefined) || (this._elements.length === 0));
	}

	get elements() { return this._elements; }
}

/**
 * Query filter single value expression test specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends module:x2node-queries~QueryFilterSpec
 */
class QueryFilterValueTestSpec extends QueryFilterSpec {

	constructor(valueExpr, testType, invert, testParams) {
		super('valueTest');

		this._valueExpr = valueExpr;
		this._testType = testType;
		this._invert = invert;
		this._testParams = testParams;
	}

	get valueExpr() { return this._valueExpr; }

	get testType() { return this._testType; }

	isInverted() { return this._invert; }

	get testParams() { return this._testParams; }
}

/**
 * Query filter collection property test specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends module:x2node-queries~QueryFilterSpec
 */
class QueryFilterCollectionTestSpec extends QueryFilterSpec {

	constructor(propPath, invert, subquerySpec) {
		super('collectionTest');

		this._propPath = propPath;
		this._invert = invert;
		this._subquerySpec = subquerySpec;
	}

	get propPath() { return this._propPath; }

	isInverted() { return this._invert; }

	get subquerySpec() { return this._subquerySpec; }
}

/**
 * Single query order specification element. The full specification is an array
 * of these.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryOrderSpec {

	constructor(valueExpr, reverse) {

		this._valueExpr = valueExpr;
		this._reverse = reverse;
	}

	get valueExpr() { return this._valueExpr; }

	isReverse() { return this._reverse; }
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
	 * @param {module:x2node-records~PropertyDescriptor} topColPropDesc
	 * Descriptor of the top collection property. When the query specification is
	 * built for a record type, this is the descriptor of the "records" property
	 * in the super-type.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Context
	 * for property references used in the query specification.
	 * @param {Object} querySpec Raw query specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the top record type is unknown.
	 */
	constructor(recordTypes, topColPropDesc, valueExprCtx, querySpec) {

		this._recordTypes = recordTypes;
		this._valueExprCtx = valueExprCtx;

		// build selected properties tree
		this._selectedProps = this._buildSelectedProps(
			topColPropDesc, querySpec.props);

		// build filter specification and add it to the tree
		const topNode = this._selectedProps.get(topColPropDesc.name);
		if (querySpec.filter)
			topNode.filter = this._parseFilter(
				topNode, valueExprCtx, [ ':and', querySpec.filter ]);

		// build order specification and add it to the tree
		if (querySpec.order)
			topNode.order = this._parseOrder(
				topNode, valueExprCtx, querySpec.order);

		// parse range specification
		if (querySpec.range)
			this._range = this._parseRange(querySpec.range);
	}

	/**
	 * Build selected properties tree.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} topColPropDesc
	 * Descriptor of the top records collection property.
	 * @param {string[]} propPatterns List of selected property patterns to
	 * parse.
	 * @returns {external:Map.<string,Object>} Top of the selected properties
	 * tree.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_buildSelectedProps(topColPropDesc, propPatterns) {

		// start building selected properties tree
		const selectedProps = new Map();

		// add top collection to the tree
		const topNode = new PropertyTreeNode(
			this._valueExprCtx.basePath, topColPropDesc,
			this._valueExprCtx.containerChain);
		selectedProps.set(topColPropDesc.name, topNode);

		// process direct patterns
		const excludedPaths = new Map();
		let wcPatterns = new Array();
		(propPatterns ? propPatterns : [ '*' ]).forEach(propPattern => {

			if (propPattern.startsWith('-')) { // collect exclusion pattern

				excludedPaths.set(propPattern.substring(1), true);

			} else if (propPattern.startsWith('.')) { // process super-view

				// TODO: process super-view
				throw new common.X2UsageError(
					'Fetching super-views is not implemented yet.');

			} else { // process regular inclusion pattern

				this._addProperty(
					topNode, null, propPattern, 'select', wcPatterns);
			}
		});

		// process wildcard patterns
		while (wcPatterns.length > 0) {
			const wcPatterns2 = new Array();
			wcPatterns.forEach(propPattern => {
				if (!excludedPaths.has(propPattern))
					this._addProperty(
						topNode, null, propPattern, 'select', wcPatterns2);
			});
			wcPatterns = wcPatterns2;
		}

		// return the result tree
		return selectedProps;
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
	 * @returns {Object} The leaf node representing the property.
	 */
	_addProperty(topNode, scopeColPath, propPattern, clause, wcPatterns) {

		// check if adding to the SELECT clause
		const selected = (clause === 'select');

		// create scoped clause
		const scopedClause = (
			scopeColPath ? clause + '/' + scopeColPath : clause);

		// process the pattern parts
		let expandChildren = false;
		const propPatternParts = propPattern.split('.');
		const numParts = propPatternParts.length;
		let parentNode = topNode;
		let patternPrefix = topNode.path, patternSuffix = '';
		const containerChain = new Array();
		for (let i = 0; i < numParts; i++) {
			const propName = propPatternParts[i];
			const parentPropDesc = parentNode.desc;

			// get container from the parent node
			const container = parentNode.childrenContainer;
			if (!container)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern
						+ '": property ' + parentPropDesc.container.nestedPath +
						parentPropDesc.name + ' of ' +
						String(parentPropDesc.container.recordTypeName) +
						' is not an object nor reference and cannot be used as' +
						' an intermediate property in a path.');
			containerChain.push(container);

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

				// done here
				break;
			}

			// try to get existing node
			let node = parentNode.getChild(propName);

			// create new node is does not yet exist
			if (!node) {

				// get property descriptor
				if (!container.hasProperty(propName))
					throw new common.X2UsageError(
						'Invalid property path "' + propPattern +
							'": record type ' + container.recordTypeName +
							' does not have property ' + container.nestedPath +
							propName + '.');
				const propDesc = container.getPropertyDesc(propName);

				// determine property children container, if applicable
				let childrenContainer;
				switch (propDesc.scalarValueType) {
				case 'object':
					childrenContainer = propDesc.nestedProperties;
					break;
				case 'ref':
					childrenContainer =
						this._recordTypes.getRecordTypeDesc(propDesc.refTarget);
				}

				// create node for the property
				node = new PropertyTreeNode(
					patternPrefix + propName, propDesc,
					(
						childrenContainer ?
							containerChain.concat(childrenContainer) :
							undefined
					)
				);

				// add node to the tree
				parentNode.addChild(propName, node);

				// add collection property filtering and ordering if any
				if (selected && !propDesc.isScalar()) {
					const propDef = propDesc.definition;
					if (propDef.filter || propDef.order) {

						// create value expressions context
						const valueExprCtx = new ValueExpressionContext(
							node.path, node.containerChain);

						// add filter if any
						if (propDef.filter)
							node.filter = this._parseFilter(
								topNode, valueExprCtx,
								[ ':and', propDef.filter ]);

						// add order if any
						if (propDef.order)
							node.order = this._parseOrder(
								topNode, valueExprCtx, propDef.order);
					}
				}
			}

			// make sure the node is on the scope collection axis
			if (scopeColPath && !node.desc.isScalar() &&
				!((scopeColPath === node.path) || scopeColPath.startsWith(
					node.path + '.')))
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": the property must be on the same collection axis' +
						' as ' + scopeColPath + '.');

			// set node usage flag
			node.addClause(scopedClause);

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
			parentNode.childrenContainer.allPropertyNames.forEach(propName => {
				const propDesc = parentNode.childrenContainer.getPropertyDesc(
					propName);
				const propDef = propDesc.definition;
				if ((propDef.fetchByDefault === undefined) ||
					propDef.fetchByDefault)
					wcPatterns.push(patternPrefix + propName + patternSuffix);
			});
		}

		// return the leaf node
		return parentNode;
	}

	/**
	 * Parse raw query filter specification.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top node of the
	 * selected properties tree.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
	 * expression context to use.
	 * @param {Array} testSpec Single raw filter test specification.
	 * @returns {module:x2node-queries~QueryFilterSpec} Parsed filter test
	 * specification. May return <code>undefined</code>.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_parseFilter(topNode, valueExprCtx, testSpec) {

		// error function
		function error(msg) {
			return new common.X2UsageError(
				'Invalid filter specification' +
					(
						valueExprCtx.basePath.length > 0 ?
							' on ' + valueExprCtx.basePath : ''
					) + ': ' + msg);
		}

		// validate test specification array
		if (!Array.isArray(testSpec) || (testSpec.length === 0))
			throw error(
				'filter test specification must be an array and must' +
					' not be empty.');

		// parse the predicate
		const predParts = testSpec[0].match(
				/^\s*(?:(?::(!?\w+)\s*)|([^:|\s].*?)\s*(?:\|\s*(!?\w+)\s*)?)$/i);
		if (predParts === null)
			throw error('predicate "' + testSpec[0] + '" has invalid syntax.');

		// check if junction
		if (predParts[1] !== undefined) {

			// validate junction conditions array
			if (!Array.isArray(testSpec[1]) || (testSpec.length > 2))
				throw error(
					'logical junction must be followed by exactly one' +
						' array of nested tests.');

			// determine type of the logical junction
			let juncType, invert;
			switch (predParts[1].toLowerCase()) {
			case 'or':
			case 'any':
			case '!none':
				juncType = 'OR'; invert = false;
				break;
			case '!or':
			case '!any':
			case 'none':
				juncType = 'OR'; invert = true;
				break;
			case 'and':
			case 'all':
				juncType = 'AND'; invert = false;
				break;
			case '!and':
			case '!all':
				juncType = 'AND'; invert = true;
				break;
			default:
				throw error('unknown junction type "' + predParts[1] + '".');
			}

			// create junction test object
			const junc = new QueryFilterJuncSpec(juncType, invert);

			// add junction elements
			testSpec[1].forEach(nestedTestSpec => {
				const nestedTest = this._parseFilter(
					topNode, valueExprCtx, nestedTestSpec);
				if (nestedTestSpec)
					junc.addElement(nestedTestSpec);
			});

			// return the result if not empty junction
			return (junc.isEmpty() ? undefined : junc);
		}

		// test, not a junction:

		// parse the value expression
		const valueExpr = new ValueExpression(valueExprCtx, predParts[2]);

		// check if single property expression
		let singlePropDesc, singlePropPath, singlePropContainerChain;
		if (valueExpr.usedPropertyPaths.size === 1) {
			singlePropPath = valueExpr.usedPropertyPaths.values().next().value;
			singlePropContainerChain = Array.from(valueExprCtx.containerChain);
			singlePropDesc = singlePropPath.split('.').reduce((desc, name) => {
				let childrenContainer;
				switch (desc.scalarValueType) {
				case 'object':
					childrenContainer = desc.nestedProperties;
					break;
				case 'ref':
					childrenContainer = this._recordTypes.getRecordTypeDesc(
						desc.refTarget);
					break;
				default:
					throw error(
						'intermediate property in property path "' +
							singlePropPath + '" is not an object nor' +
							' a reference.');
				}
				if (!childrenContainer.hasProperty(name))
					throw error(
						'property path "' + singlePropPath +
							'" refers to non-existent property ' +
							childrenContainer.nestedPath + name +
							' of record type ' +
							String(childrenContainer.recordTypeName) + '.');
				singlePropContainerChain.push(childrenContainer);
				return childrenContainer.getPropertyDesc(name);
			}, topNode.desc);
		}

		// check if collection test
		if (singlePropDesc && !singlePropDesc.isScalar()) {

			// check that no functions
			if (predParts[2] !== singlePropPath)
				throw error(
					'collection test "' + testSpec[0] +
						' may not have functions.');

			// parse and validate the test
			const testType = (
				predParts[3] !== undefined ?
					predParts[3].toLowerCase() : '!empty'
			);
			let invert;
			switch (testType) {
			case 'empty':
				invert = true; // not exists
				break;
			case '!empty':
				invert = false; // exists
				break;
			default:
				throw error(
					'invalid collection test "' + testSpec[0] +
						'" as it may only be "empty" or "!empty".');
			}

			// validate test arguments
			const colFilterSpec = testSpec[1];
			if ((testSpec.length > 2) || (
				(colFilterSpec !== undefined) && !Array.isArray(colFilterSpec)))
				throw error(
					'collection test may only have none or a single' +
						' array argument.');

			// create and return collection test specification
			return new QueryFilterCollectionTestSpec(
				singlePropPath, invert, new QuerySpec(
					this._recordTypes,
					singlePropDesc,
					new ValueExpressionContext(
						singlePropPath,
						singlePropContainerChain
					), {
						props: [],
						filter: colFilterSpec
					}
				)
			);
		}

		// single value test:

		// used properties collector
		const allUsedPropertyPaths = Array.from(valueExpr.usedPropertyPaths);

		// determine the test type
		let testType, invert = false, testParams;
		const rawTestType = (
			predParts[3] ? predParts[3] : (
				testSpec.length > 1 ? 'is' : 'present'));
		function getSingleTestParam() {
			const v = testSpec[1];
			if ((testSpec.length > 2) || (v === undefined) ||
				(v === null) || Array.isArray(v))
				throw error(
					'test "' + rawTestType +
						'" expects a single non-null, non-array argument.');
			return [ v ];
		}
		function getTwoTestParams() {
			let v1, v2;
			switch (testSpec.length) {
			case 2:
				if (Array.isArray(testSpec[1]) &&
					(testSpec[1].length === 2)) {
					v1 = testSpec[1][0];
					v2 = testSpec[1][1];
				}
				break;
			case 3:
				v1 = testSpec[1];
				v2 = testSpec[2];
			}
			if ((v1 === undefined) || (v2 === undefined) ||
				(v1 === null) || (v2 === null) ||
				Array.isArray(v1) || Array.isArray(v2))
				throw error(
					'test "' + rawTestType +
						'" expects two non-null, non-array arguments.');
			return [ v1, v2 ];
		}
		function getListTestParams() {
			const f = (a, v) => {
				if (v === null)
					throw new error(
						'test "' + rawTestType +
							'" expects a list of non-null arguments.');
				if (Array.isArray(v))
					v.forEach(vv => { f(a, vv); });
				else
					a.push(v);
				return a;
			};
			const a = testSpec.slice(1).reduce(f, new Array());
			if (a.length === 0)
				throw error(
					'test "' + rawTestType +
						'" expects a list of non-null arguments.');
			return a;
		}
		switch (rawTestType.toLowerCase()) {
		case 'is':
		case 'eq':
			testType = 'eq';
			testParams = getSingleTestParam();
			break;
		case 'not':
		case 'ne':
		case '!eq':
			testType = 'ne';
			testParams = getSingleTestParam();
			break;
		case 'min':
		case 'ge':
		case '!lt':
			testType = 'ge';
			testParams = getSingleTestParam();
			break;
		case 'max':
		case 'le':
		case '!gt':
			testType = 'le';
			testParams = getSingleTestParam();
			break;
		case 'gt':
			testType = 'gt';
			testParams = getSingleTestParam();
			break;
		case 'lt':
			testType = 'lt';
			testParams = getSingleTestParam();
			break;
		case '!in':
		case '!oneof':
			invert = true;
		case 'in':
		case 'oneof':
		case 'alt':
			testType = 'in';
			testParams = getListTestParams();
			break;
		case '!between':
			invert = true;
		case 'between':
			testType = 'between';
			testParams = getTwoTestParams();
			break;
		case '!contains':
			invert = true;
		case 'contains':
			testType = 'contains';
			testParams = getSingleTestParam();
			break;
		case '!containsi':
		case '!substring':
			invert = true;
		case 'containsi':
		case 'substring':
			testType = 'containsi';
			testParams = getSingleTestParam();
			break;
		case '!starts':
			invert = true;
		case 'starts':
			testType = 'starts';
			testParams = getSingleTestParam();
			break;
		case '!startsi':
		case '!prefix':
			invert = true;
		case 'startsi':
		case 'prefix':
			testType = 'startsi';
			testParams = getSingleTestParam();
			break;
		case '!matches':
			invert = true;
		case 'matches':
			testType = 'matches';
			testParams = getSingleTestParam();
			break;
		case '!matchesi':
		case '!pattern':
		case '!re':
			invert = true;
		case 'matchesi':
		case 'pattern':
		case 're':
			testType = 'matchesi';
			testParams = getSingleTestParam();
			break;
		case '!empty':
		case 'present':
			invert = true;
		case 'empty':
			testType = 'empty';
			if (testSpec.length > 1)
				throw error('test "' + rawTestType + '" expects no arguments.');
			break;
		default:
			throw error('unknown test "' + rawTestType + '".');
		}

		// compile value expressions in test params if any
		if (testParams) {
			for (let i = 0, len = testParams.length; i < len; i++) {
				const testParam = testParams[i];
				if (placeholders.isExpr(testParam)) {
					testParams[i] = new ValueExpression(
						valueExprCtx, testParam.expr);
					testParams[i].usedPropertyPaths.forEach(propPath => {
						allUsedPropertyPaths.push(propPath);
					});
				}
			}
		}

		// add all used properties to the tree
		allUsedPropertyPaths.forEach(propPath => {
			const propNode = this._addProperty(
				topNode, valueExprCtx.basePath, propPath, 'where');
			if (!propNode.desc.isScalar())
				throw error(
					'test "' + testSpec + '" refers to a non-scalar property "' +
						propPath + '".');
		});

		// create and return the resulting test specification
		return new QueryFilterValueTestSpec(
			valueExpr, testType, invert, testParams);
	}

	/**
	 * Parse raw query order specification.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top node of the
	 * selected properties tree.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
	 * expression context to use.
	 * @param {string[]} orderSpecs Raw order specification.
	 * @returns {module:x2node-queries~QueryOrderSpec[]} Parsed order
	 * specification elements.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_parseOrder(topNode, valueExprCtx, orderSpecs) {

		// error function
		function error(msg) {
			return new common.X2UsageError(
				'Invalid order specification' +
					(
						valueExprCtx.basePath.length > 0 ?
							' on ' + valueExprCtx.basePath : ''
					) + ': ' + msg);
		}

		// validate order specification array
		if (!Array.isArray(orderSpecs))
			throw error('order specification must be an array.');

		// parse order specifications
		return orderSpecs.map(orderSpec => {

			// validate the specification element
			if ((typeof orderSpec) !== 'string')
				throw error('order specification element is not a string.');

			// parse the specification element
			const orderSpecParts = orderSpec.match(
					/^\s*([^|\s].*?)\s*(?:\|\s*(asc|desc)\s*)?$/i);
			if (orderSpecParts === null)
				throw error(
					'order specification element "' + orderSpec +
						'" has invalid syntax.');

			// parse the value expression
			const valueExpr = new ValueExpression(
				valueExprCtx, orderSpecParts[1]);

			// add used properties to the tree
			valueExpr.usedPropertyPaths.forEach(propPath => {
				const propNode = this._addProperty(
					topNode, valueExprCtx.basePath, propPath, 'orderBy');
				if (!propNode.desc.isScalar())
					throw error(
						'element "' + orderSpec +
							'" refers to a non-scalar property "' +
							propPath + '".');
			});

			// return order specification element object
			return new QueryOrderSpec(
				valueExpr,
				(orderSpecParts[2] && (
					orderSpecParts[2].toLowerCase() === 'desc')));
		});
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
	 * Record types library.
	 *
	 * @type {module:x2node-records~RecordTypesLibrary}
	 * @readonly
	 */
	get recordTypes() { return this._recordTypes; }

	/**
	 * Tree of selected properties. The keys in the map are record property
	 * names, the values are objects that have properties: <code>desc</code> for
	 * the property descriptor and <code>children</code> for the map of selected
	 * nested properties. TODO: add descriptions of more properties.
	 *
	 * @type {external:Map.<string,Object>}
	 * @readonly
	 */
	get selectedProps() { return this._selectedProps; }

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
