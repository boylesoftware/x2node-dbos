'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');


/**
 * Regular expression used to parse fitler specification predicate element.
 *
 * @private
 * @type {external:RegExp}
 */
const FILTER_PRED_RE = new RegExp(
	'^(?:' +
		'(?::(!?(?:or|any|none|and|all)))' +
		'|(?:' +
			'([^:|*.-][^|*]*)' +
			'(?:\\s*\\|\\s*(val|len|lc|uc|sub:\\d+(?::\\d+)?' +
				'|lpad:\\d+(?::[^|:\\s])?))?' +
			'(?:\\s*\\|\\s*(!?(?:eq|ne|min|max|gt|ge|lt|le|between|rng|alt|in' +
				'|oneof|containsi?|sub|prefixi?|pre|matchesi?|pat|re|empty)))?' +
		')' +
	')$', 'i'
);

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
	 * @param {module:x2node-records~PropertyDescriptor} recordsColPropDesc
	 * Descriptor of the top records collection property.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Context
	 * for property references used in the query specification.
	 * @param {Object} querySpec Raw query specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the top record type is unknown.
	 */
	constructor(recordTypes, recordsColPropDesc, valueExprCtx, querySpec) {

		this._recordTypes = recordTypes;
		this._valueExprCtx = valueExprCtx;

		// build selected properties tree
		this._selectedProps = this._buildSelectedProps(
			recordsColPropDesc, valueExprCtx.baseContainer, querySpec.props);

		// build filter specification and add it to the tree
		const topRecordsNode = this._selectedProps.get(recordsColPropDesc.name);
		this._addFilter(topRecordsNode, querySpec.filter);

		// build order specification and add it to the tree
		this._addOrder(topRecordsNode, querySpec.order);

		// parse range specification
		this._range = this._parseRange(querySpec.range);
	}

	/**
	 * Build selected properties tree.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} recordsColPropDesc
	 * Descriptor of the top records collection property.
	 * @param {module:x2node-records~PropertiesContainer} recordPropsContainer
	 * Container of the top record properties.
	 * @param {string[]} propPatterns List of selected property patterns to
	 * parse.
	 * @returns {external:Map.<string,Object>} Top of the selected properties
	 * tree.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_buildSelectedProps(recordsColPropDesc, recordPropsContainer, propPatterns) {

		// start building selected properties tree
		const selectedProps = new Map();

		// add top collection to the tree
		const topRecordsNode = {
			propPath: '', // TODO: different for subqueries
			desc: recordsColPropDesc,
			childrenContainer: recordPropsContainer,
			children: new Map()
		};
		selectedProps.set(recordsColPropDesc.name, topRecordsNode);

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

				this._parseInclusionPattern(
					topRecordsNode, propPattern, 'inSelect', wcPatterns);
			}
		});

		// process wildcard patterns
		while (wcPatterns.length > 0) {
			const wcPatterns2 = new Array();
			wcPatterns.forEach(propPattern => {
				if (!excludedPaths.has(propPattern))
					this._parseInclusionPattern(
						topRecordsNode, propPattern, 'inSelect', wcPatterns2);
			});
			wcPatterns = wcPatterns2;
		}

		// return the result tree
		return selectedProps;
	}

	/**
	 * Parse raw query filter specification and, if any, add it to the specified
	 * top tree node.
	 *
	 * @private
	 * @param {Object} topRecordsNode Selected properties tree node for the
	 * subtree top collection property (the top "records" node for the top
	 * filter).
	 * @param {Array[]} [testSpecs] Raw filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_addFilter(topRecordsNode, testSpecs) {

		// check if no filter
		if (testSpecs === undefined)
			return;

		// validate filter specification array
		if (!Array.isArray(testSpecs))
			throw new common.X2UsageError(
				'Filter specification must be an array.');

		// build the filter
		const filter = this._processFilterTests(
			topRecordsNode, 'AND', false, testSpecs);

		// add the filter, if any, to the top tree node
		if (filter)
			topRecordsNode.filter = filter;
	}

	/**
	 * Recursively process an array of raw filter test specifications and build
	 * the filter term object from them.
	 *
	 * @private
	 * @param {Object} topRecordsNode Selected properties tree node for the
	 * subtree top collection property (the top "records" node for the top
	 * filter).
	 * @param {string} juncType Junction type, which is either "AND" or "OR".
	 * @param {boolean} invert <code>true</code> to invert the junction.
	 * @param {Array[]} testSpecs Array of raw filter test specifications. May be
	 * empty, but must be a valid array.
	 * @returns {Object} Resulting filter term object for the whole junction,
	 * or <code>null</code> if results in no test.
	 */
	_processFilterTests(topRecordsNode, juncType, invert, testSpecs) {

		// build junction terms
		const terms = testSpecs.reduce((res, testSpec) => {
			const term = this._parseFilterTest(topRecordsNode, testSpec);
			if (term !== null)
				res.push(term);
			return res;
		}, new Array());

		// check if empty
		if (terms.length === 0)
			return null;

		// create and return filter term object for the junction
		return {
			type: 'junction',
			juncType: juncType,
			invert: invert,
			terms: terms
		};
	}

	/**
	 * Parse raw filter test specification and build the filter term object for
	 * it.
	 *
	 * @private
	 * @param {Object} topRecordsNode Selected properties tree node for the
	 * subtree top collection property (the top "records" node for the top
	 * filter).
	 * @param {Array} testSpec Raw test specification to parse.
	 * @returns {Object} Resulting filter term object, or <code>null</code> if
	 * results in no test (the test is an empty junction).
	 */
	_parseFilterTest(topRecordsNode, testSpec) {

		// validate raw test specification array
		if (!Array.isArray(testSpec) || (testSpec.length === 0))
			throw new common.X2UsageError(
				'An element in the query filter specification is not an' +
					' array or is empty.');

		// parse the predicate
		const predParts = FILTER_PRED_RE.exec(testSpec[0]);
		if (predParts === null)
			throw new common.X2UsageError(
				'Invalid filter predicate "' + testSpec[0] +
					'": unrecognized syntax.');

		// start building the filter term object
		let filterTerm;

		// check if junction
		if (predParts[1] !== undefined) {

			// validate junction conditions array
			if (!Array.isArray(testSpec[1]) || (testSpec.length > 2))
				throw new common.X2UsageError(
					'Filter junction ' + predParts[1] +
						' must be followed by exactly one array of nested' +
						' tests.');

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
			}

			// process junction nested tests
			filterTerm = this._processFilterTests(
				topRecordsNode, juncType, invert, testSpec[1]);

		} else { // property test

			// add tested property path to the selected properties tree
			const propNode = this._parseInclusionPattern(
				topRecordsNode, predParts[2], 'inWhere')

			// check if scalar
			if (propNode.desc.isScalar()) {

				// parse and validate the value function
				const valueFuncParts = (
					predParts[3] !== undefined ?
						predParts[3].split(':') : [ 'val' ]
				);
				const valueFuncName = valueFuncParts[0].toLowerCase();
				let valueFuncParams;
				switch (valueFuncName) {
				case 'sub':
					if ((valueFuncParts.length < 2) ||
						(valueFuncParts.length > 3))
						throw new common.X2UsageError(
							'Invalid filter predicate "' + testSpec[0] +
								'": value function "sub" requires one or two' +
								' parameters.');
					valueFuncParams = [
						Number(valueFuncParts[1]),
						Number(valueFuncParts[2]) // may be undefined
					];
					break;
				case 'lpad':
					if ((valueFuncParts.length < 2) ||
						(valueFuncParts.length > 3))
						throw new common.X2UsageError(
							'Invalid filter predicate "' + testSpec[0] +
								'": value function "lpad" requires one or two' +
								' parameters.');
					valueFuncParams = [
						Number(valueFuncParts[1]),
						(
							valueFuncParts[2] !== undefined ?
								valueFuncParts[2] : ' '
						)
					];
					break;
				default: // all other functions
					if (valueFuncParts.length > 1)
						throw new common.X2UsageError(
							'Invalid filter predicate "' + testSpec[0] +
								'": value function "' + valueFuncName +
								'" does not take any parameters.');
				}

				// parse and validate the test
				const testType = (
					predParts[4] !== undefined ?
						predParts[4].toLowerCase() :
						(testSpec.length > 1 ? 'eq' : '!empty')
				);
				function getSingleTestParam() {
					const v = testSpec[1];
					if ((testSpec.length > 2) || (v === undefined) ||
						(v === null) || Array.isArray(v))
						throw new common.X2UsageError(
							'Filter test "' + testType +
								'" expects a single non-null, non-array' +
								' argument.');
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
						throw new common.X2UsageError(
							'Filter test "' + testType +
								'" expects two non-null, non-array arguments.');
					return [ v1, v2 ];
				}
				function getListTestParams() {
					const f = (a, v) => {
						if (v === null)
							throw new common.X2UsageError(
								'Filter test "' + testType +
									'" expects a list of non-null arguments.');
						if (Array.isArray(v))
							v.forEach(vv => { f(a, vv); });
						else
							a.push(v);
						return a;
					};
					const a = testSpec.slice(1).reduce(f, new Array());
					if (a.length === 0)
						throw new common.X2UsageError(
							'Filter test "' + testType +
								'" expects a list of non-null arguments.');
					return a;
				}
				let canonicalTestType, invert = false, testParams;
				switch (testType) {
				case 'eq':
				case '!ne':
					canonicalTestType = 'eq';
					testParams = getSingleTestParam();
					break;
				case 'ne':
				case '!eq':
					canonicalTestType = 'ne';
					testParams = getSingleTestParam();
					break;
				case 'ge':
				case '!lt':
				case 'min':
					canonicalTestType = 'ge';
					testParams = getSingleTestParam();
					break;
				case 'le':
				case '!gt':
				case 'max':
					canonicalTestType = 'le';
					testParams = getSingleTestParam();
					break;
				case 'gt':
					canonicalTestType = 'gt';
					testParams = getSingleTestParam();
					break;
				case 'lt':
					canonicalTestType = 'lt';
					testParams = getSingleTestParam();
					break;
				case 'in':
				case '!in':
				case 'oneof':
				case '!oneof':
				case 'alt':
				case '!alt':
					canonicalTestType = 'in';
					invert = testType.startsWith('!');
					testParams = getListTestParams();
					break;
				case 'between':
				case '!between':
				case 'rng':
				case '!rng':
					canonicalTestType = 'between';
					invert = testType.startsWith('!');
					testParams = getTwoTestParams();
					break;
				case 'contains':
				case '!contains':
					canonicalTestType = 'contains';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'containsi':
				case '!containsi':
				case 'sub':
				case '!sub':
					canonicalTestType = 'containsi';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'prefix':
				case '!prefix':
					canonicalTestType = 'prefix';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'prefixi':
				case '!prefixi':
				case 'pre':
				case '!pre':
					canonicalTestType = 'prefixi';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'matches':
				case '!matches':
					canonicalTestType = 'matches';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'matchesi':
				case '!matchesi':
				case 'pat':
				case '!pat':
				case 're':
				case '!re':
					canonicalTestType = 'matchesi';
					invert = testType.startsWith('!');
					testParams = getSingleTestParam();
					break;
				case 'empty':
				case '!empty':
					canonicalTestType = 'empty';
					invert = testType.startsWith('!');
					if (testSpec.length > 1)
						throw new common.X2UsageError(
							'Filter test "' + testType +
								'" expects no arguments.');
					testParams = [];
					break;
				default:
					throw new common.X2UsageError(
						'Invalid filter predicate "' + testSpec[0] +
							'": unknown test "' + testType + '".');
				}

				// create filter term object
				filterTerm = {
					type: 'singlePropTest',
					propPath: propNode.propPath,
					valueFuncName: valueFuncName,
					valueFuncParams: valueFuncParams,
					testType: canonicalTestType,
					invert: invert,
					testParams: testParams
				};

			} else { // collection test

				// may not have value function
				if (predParts[3] !== undefined)
					throw new common.X2UsageError(
						'Invalid filter predicate "' + testSpec[0] +
							'": collection test may not have value functions.');

				// parse and validate the test
				const testType = (
					predParts[4] !== undefined ?
						predParts[4].toLowerCase() : '!empty'
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
					throw new common.X2UsageError(
						'Invalid filter predicate "' + testSpec[0] +
							'": collection test may only be "empty" or' +
							' "!empty".');
				}

				// validate test arguments
				const colFilterSpec = testSpec[1];
				if ((testSpec.length > 2) || (
					(colFilterSpec !== undefined) &&
						!Array.isArray(colFilterSpec)))
					throw new common.X2UsageError(
						'Collection filter test may only have none or a single' +
							' array argument.');

				// create the test filter test object
				filterTerm = {
					type: 'collectionTest',
					propPath: propNode.propPath,
					invert: invert,
					subquerySpec: new QuerySpec(
						this._recordTypes,
						propNode.desc,
						new ValueExpressionContext(
							propNode.propPath, // TODO: clarify
							this._valueExprCtx.concat(propNode.childrenContainer)
						), {
							props: [],
							filter: colFilterSpec
						})
				};
			}
		}

		// return the filter term object
		return filterTerm;
	}

	/**
	 * Parse raw query order specification and, if any, add it to the specified
	 * top tree node.
	 *
	 * @private
	 * @param {Object} topRecordsNode Selected properties tree node for the
	 * subtree top collection property (the top "records" node for the top
	 * filter).
	 * @param {string[]} [orderSpecs] Raw order specification.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_addOrder(topRecordsNode, orderSpecs) {

		// check if no ordering
		if (orderSpecs === undefined)
			return;

		// validate order specification array
		if (!Array.isArray(orderSpecs))
			throw new common.X2UsageError(
				'Order specification must be an array.');

		// add order specification to the node
		topRecordsNode.order = orderSpecs.map(orderSpec => {

			// validate the specification element
			if ((typeof orderSpec) !== 'string')
				throw new common.X2UsageError(
					'An order specification element is not a string.');

			// parse the specification element
			const orderSpecParts = orderSpec.match(
					/^(.+?)(?:\s*\|\s*(asc|desc))?$/i);
			if (orderSpecParts === null)
				throw new common.X2UsageError(
					'Invalid order specification element "' + orderSpec + '".');

			// parse the value expression
			const valueExpr = new ValueExpression(
				this._valueExprCtx, orderSpecParts[1]);

			// add used properties to the tree
			valueExpr.usedPropertyPaths.forEach(propPath => {
				if (!this._parseInclusionPattern(
					topRecordsNode, propPath, 'inOrderBy').desc.isScalar())
					throw new common.X2UsageError(
						'Invalid order specification element "' + orderSpec +
							'": cannot use non-scalar property for ordering.');
			});

			// determine order direction
			const reverse = (
				orderSpecParts[2] && (orderSpecParts[2].toUpperCase() === 'DESC')
			);

			// return order specification element object
			return {
				valueExpr: valueExpr,
				reverse: reverse
			};
		});
	}

	/**
	 * Parse property inclusion pattern and add corresponding nodes to the
	 * selected properties tree as necessary.
	 *
	 * @private
	 * @param {Object} topRecordsNode Top selected properties tree node
	 * corresponding to the "records" super property.
	 * @param {string} propPattern Property inclusion pattern. May be a whildcard
	 * pattern if the clause is "inSelect". Also, the pattern may not hop over
	 * collection properties unless the clause is "inSelect".
	 * @param {string} clause The query clause where the included property is
	 * used. May be "inSelect", "inWhere" or "inOrderBy".
	 * @param {string[]} wcPatterns Array, to which to add extra patterns
	 * resulting in the wildcard pattern expansion. Only used if the clause is
	 * "inSelect".
	 * @returns {Object} The leaf node representing the property.
	 */
	_parseInclusionPattern(topRecordsNode, propPattern, clause, wcPatterns) {

		// check if adding to the SELECT clause
		const inSelect = (clause === 'inSelect');

		// split the pattern into parts
		const propPatternParts = propPattern.split('.');
		const numParts = propPatternParts.length;

		// process the property nested path and create corresponding nodes
		let parentNode = topRecordsNode;
		let patternPrefix = topRecordsNode.propPath;
		for (let i = 0; i < numParts - 1; i++) {
			const propPatternPart = propPatternParts[i];
			const parentPropDesc = parentNode.desc;

			// try to get existing intermediate node
			let node = parentNode.children.get(propPatternPart);

			// create new node if necessary
			if (!node) {

				// check if subtype node
				if (parentPropDesc.isPolymorph()) {

					// create subtype node
					node = {
						propPath: patternPrefix + propPatternPart,
						isSubtypeNode: true,
						desc: parentPropDesc,
						children: new Map()
					};

					// determine the property children container
					if (parentPropDesc.scalarValueType === 'object') {
						node.childrenContainer =
							parentPropDesc.nestedProperties[propPatternPart];
					} else { // polymorph reference
						const refTargetName = parentPropDesc.refTargets.find(
							n => (n === propPatternPart));
						if (refTargetName) {
							node.childrenContainer =
								this._recordTypes.getRecordTypeDesc(
									refTargetName);
						}
					}
					if (!node.childrenContainer)
						throw new common.X2UsageError(
							'Invalid property path "' + propPattern +
								'": polymorphic property ' +
								parentPropDesc.container.nestedPath +
								parentPropDesc.name + ' of record type ' +
								parentPropDesc.container.recordTypeName +
								' does not have subtype ' + propPatternPart +
								'.');

				} else { // property node

					// check that the parent container has the property
					const container = parentNode.childrenContainer;
					if (!container.hasProperty(propPatternPart))
						throw new common.X2UsageError(
							'Invalid property path "' + propPattern +
								'": record type ' + container.recordTypeName +
								' does not have property ' +
								container.nestedPath + propPatternPart + '.');

					// create node for the property
					node = {
						propPath: patternPrefix + propPatternPart,
						desc: container.getPropertyDesc(propPatternPart),
						children: new Map()
					};

					// determine the property children container
					switch (node.desc.scalarValueType) {
					case 'object':
						if (!node.desc.isPolymorph()) {
							node.childrenContainer = node.desc.nestedProperties;
						}
						break;
					case 'ref':
						if (!node.desc.isPolymorph()) {
							node.childrenContainer =
								this._recordTypes.getRecordTypeDesc(
									node.desc.refTarget);
						}
						break;
					default:
						throw new common.X2UsageError(
							'Invalid property path "' + propPattern +
								'": property ' +
								container.nestedPath + node.desc.name +
								' of record type ' + container.recordTypeName +
								' is neither a nested object nor a reference' +
								' and cannot be used in a nested property' +
								' path.');
					}

					// add nested collection ordering and filtering if any
					if (inSelect && !node.desc.isScalar()) {
						// TODO: add collection order and filter if any
					}
				}

				// add new node to the tree
				parentNode.children.set(propPatternPart, node);
			}

			// make sure intermediate node is not a collection unless SELECT
			if (!inSelect && !node.desc.isScalar())
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": non-scalar intermediate properties are not' +
						' allowed.');

			// set node usage flag
			node[clause] = true;

			// add part to the reconstructed pattern prefix
			patternPrefix += propPatternPart + '.';

			// advance down the tree
			parentNode = node;
		}

		// process the terminal pattern part
		const termPatternPart = propPatternParts[numParts - 1];
		let includeAllChildren = false;
		if (termPatternPart === '*') {
			if (parentNode.isSubtypeNode)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": cannot have wild card pattern following' +
						' polymorphic property subtype.');
			includeAllChildren = inSelect;
		} else {
			if (parentNode.desc.isPolymorph() && !parentNode.isSubtypeNode)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": path may not end with a polymorphic' +
						' property subtype name.');
			const container = parentNode.childrenContainer;
			if (!container.hasProperty(termPatternPart))
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": record type ' + container.recordTypeName +
						' does not have property ' +
						container.nestedPath + termPatternPart + '.');
			let node = parentNode.children.get(termPatternPart);
			if (!node) {
				node = {
					propPath: patternPrefix + termPatternPart,
					desc: container.getPropertyDesc(termPatternPart)
				};
				if (inSelect && !node.desc.isScalar()) {
					// TODO: add collection order and filter if any
				}
				parentNode.children.set(termPatternPart, node);
			}
			node[clause] = true;
			patternPrefix += termPatternPart + '.';
			if (node.desc.scalarValueType === 'object') {
				includeAllChildren = inSelect;
				if (!node.desc.isPolymorph())
					node.childrenContainer = node.desc.nestedProperties;
				node.children = new Map();
			}
			parentNode = node;
		}

		// include all children if requested
		function expandWildcardPattern(container, patternPrefix) {
			container.allPropertyNames.forEach(propName => {
				const propDesc = container.getPropertyDesc(propName);
				const propDef = propDesc.definition;
				if (propDef.fetchByDefault ||
					(
						(propDef.fetchByDefault === undefined) &&
							(
								(propDesc.scalarValueType === 'object') ||
									propDef.column ||
									(
										(propDesc.scalarValueType === 'ref') &&
											propDef.reverseRefProperty
									)
							)
					)) wcPatterns.push(patternPrefix + propName);
			});
		}
		if (includeAllChildren) {
			if (!parentNode.desc.isPolymorph()) {
				expandWildcardPattern(
					parentNode.childrenContainer,
					patternPrefix
				);
			} else if (parentNode.desc.scalarValueType === 'object') {
				for (let subtype in parentNode.desc.nestedProperties)
					expandWildcardPattern(
						parentNode.desc.nestedProperties[subtype],
						patternPrefix + subtype + '.'
					);
			} else { // polymorph ref
				parentNode.desc.refTargets.forEach(subtype => {
					expandWildcardPattern(
						this._recordTypes.getRecordTypeDesc(subtype)
							.nestedProperties,
						patternPrefix + subtype + '.'
					);
				});
			}
		}

		// return leaf node
		return parentNode;
	}

	/**
	 * Parse raw query range specification.
	 *
	 * @private
	 * @param {number[]} [rangeSpec] Two-element number array of the raw range
	 * specification.
	 * @returns {Object.<string,number>} Range specification object with two
	 * properties: <code>offset</code> and <code>limit</code>, or
	 * <code>undefined</code> if none.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_parseRange(rangeSpec) {

		// check if no range
		if (rangeSpec === undefined)
			return undefined;

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
		return {
			offset: offset,
			limit: limit
		};
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
	 * nested properties.
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
