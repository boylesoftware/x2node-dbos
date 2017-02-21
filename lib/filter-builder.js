'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const placeholders = require('./placeholders.js');


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
		this._needsParen = false;
	}

	get type() { return this._type; }

	needsParen() { return this._needsParen; }

	translate(dbDriver, propsResolver, funcResolvers, paramsHandler) {
		throw new Error('Not implemented.');
	}
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

		this._needsParen = (
			this._needsParen || (!this._invert && (this._elements.length > 1)));
	}

	isInverted() { return this._invert; }

	isEmpty() {

		return (
			(this._elements === undefined) || (this._elements.length === 0));
	}

	get elements() { return this._elements; }

	translate(dbDriver, propsResolver, funcResolvers, paramsHandler) {

		if (this.isEmpty())
			return '';

		const juncSql = this._elements.map(element => {
			const elementSql = element.translate(
				dbDriver, propsResolver, funcResolvers, paramsHandler);
			return (element.needsParen() ? '(' + elementSql + ')' : elementSql);
		}).join(' ' + this._juncType + ' ');

		return (this._invert ? 'NOT (' + juncSql + ')' : juncSql);
	}
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

	translate(dbDriver, propsResolver, funcResolvers, paramsHandler) {

		const valSql = this._valueExpr.translate(propsResolver, funcResolvers);

		function testParamSql(param, litValueFunc, exprValueFunc) {

			if (param instanceof ValueExpression) {
				const exprSql = param.translate(propsResolver, funcResolvers);
				return (exprValueFunc ? exprValueFunc(exprSql) : exprSql);
			}

			if (placeholders.isParam(param))
				return paramsHandler.addParam(param.name, litValueFunc);

			return paramsHandler.paramValueToSql(dbDriver, param, litValueFunc);
		}

		switch (this._testType) {
		case 'eq':
			return valSql + ' = ' + testParamSql(this._testParams[0]);
		case 'ne':
			return valSql + ' <> ' + testParamSql(this._testParams[0]);
		case 'ge':
			return valSql + ' >= ' + testParamSql(this._testParams[0]);
		case 'le':
			return valSql + ' <= ' + testParamSql(this._testParams[0]);
		case 'gt':
			return valSql + ' > ' + testParamSql(this._testParams[0]);
		case 'lt':
			return valSql + ' < ' + testParamSql(this._testParams[0]);
		case 'in':
			return valSql + (this._invert ? ' NOT' : '') +
				' IN (' + testParams.map(p => testParamSql(p)).join(', ') + ')';
		case 'between':
			return valSql + (this._invert ? ' NOT' : '') +
				' BETWEEN ' + testParamSql(this._testParams[0]) +
				' AND ' + testParamSql(this._testParams[1]);
		case 'contains':
		case 'containsi':
			return dbDriver.patternMatch(
				valSql, testParamSql(
					this._testParams[0],
					lit => '%' + dbDriver.safeLikePatternFromString(lit) + '%',
					expr => dbDriver.nullableConcat(
						dbDriver.stringLiteral('%'),
						dbDriver.safeLikePatternFromExpr(expr),
						dbDriver.stringLiteral('%')
					)
				),
				this._invert, !this._testType.endsWith('i'));
		case 'starts':
		case 'startsi':
			return dbDriver.patternMatch(
				valSql, testParamSql(
					this._testParams[0],
					lit => dbDriver.safeLikePatternFromString(lit) + '%',
					expr => dbDriver.nullableConcat(
						dbDriver.safeLikePatternFromExpr(expr),
						dbDriver.stringLiteral('%')
					)
				),
				this._invert, !this._testType.endsWith('i'));
		case 'matches':
		case 'matchesi':
			return dbDriver.regexpMatch(
				valSql, testParamSql(this._testParams[0]),
				this._invert, !this._testType.endsWith('i'));
		case 'empty':
			return valSql + ' IS' + (this._invert ? ' NOT' : '') + ' NULL';
		}
	}
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

	/**
	 * Create new test specification.
	 *
	 * @param {string} propPath Collection property path in the main query
	 * context.
	 * @param {boolean} invert <code>true</code> to test for collection
	 * emptiness as opposed to non-emptiness.
	 * @param {module:x2node-records~PropertyDescriptor} Collection property
	 * descriptor.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
	 * expression context for expressions in the collection filter.
	 * @param {Array[]} [filterSpec] Raw collection filter specification, if any.
	 */
	constructor(propPath, invert, colPropDesc, valueExprCtx, filterSpec) {
		super('collectionTest');

		this._propPath = propPath;
		this._invert = invert;
		this._colPropDesc = colPropDesc;
		this._valueExprCtx = valueExprCtx;
		this._filterSpec = filterSpec;
	}

	get propPath() { return this._propPath; }

	isInverted() { return this._invert; }

	get colPropDesc() { return this._colPropDesc; }

	get valueExprCtx() { return this._valueExprCtx; }

	get filterSpec() { return this._filterSpec; }

	translate(dbDriver, propsResolver, funcResolvers, paramsHandler) {

		//...
	}
}

/**
 * Parse raw query filter specification.
 *
 * @private
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top collection property being filtered.
 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
 * expression context to use.
 * @param {Array} testSpec Single raw filter test specification.
 * @param {external:Set.<string>} usedPropPaths Set, to which to add paths of all
 * properties referred in all value expressions.
 * @returns {module:x2node-queries~QueryFilterSpec} Parsed filter test
 * specification. May return <code>undefined</code>.
 * @throws {module:x2node-common.X2UsageError} If the specification is invalid.
 */
function parseFilterSpec(
	recordTypes, topPropDesc, valueExprCtx, testSpec, usedPropPaths) {

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
			'filter test specification must be an array and must not be empty.');

	// parse the predicate
	const predParts = testSpec[0].match(
			/^\s*(?:(?::(!?\w+)\s*)|([^:=\s].*?)\s*(?:=>\s*(!?\w+)\s*)?)$/i);
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
			const nestedTest = parseFilterSpec(
				recordTypes, topPropDesc, valueExprCtx, nestedTestSpec,
				usedPropPaths);
			if (nestedTest)
				junc.addElement(nestedTest);
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
				childrenContainer = recordTypes.getRecordTypeDesc(
					desc.refTarget);
				break;
			default:
				throw error(
					'intermediate property in property path "' +
						singlePropPath + '" is not an object nor a reference.');
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
		}, topPropDesc);
	}

	// check if collection test
	if (singlePropDesc && !singlePropDesc.isScalar()) {

		// check that no functions
		if (predParts[2] !== singlePropPath)
			throw error(
				'collection test "' + testSpec[0] + ' may not have functions.');

		// parse and validate the test
		const testType = (
			predParts[3] !== undefined ? predParts[3].toLowerCase() : '!empty');
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

		// add parent to used properties
		const dotInd = singlePropPath.lastIndexOf('.');
		if (dotInd >= 0)
			usedPropPaths.add(singlePropPath.substring(0, dotInd));

		// create and return collection test specification
		return new QueryFilterCollectionTestSpec(
			singlePropPath,
			invert,
			singlePropDesc,
			new ValueExpressionContext(singlePropPath, singlePropContainerChain),
			colFilterSpec
		);
	}

	// single value test:

	// collect used properties from the value expression
	valueExpr.usedPropertyPaths.forEach(p => { usedPropPaths.add(p); });

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
			if (Array.isArray(testSpec[1]) && (testSpec[1].length === 2)) {
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
			if ((v === null) || (v === undefined))
				throw error(
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
				testParams[i].usedPropertyPaths.forEach(p => {
					usedPropPaths.add(p);
				});
			}
		}
	}

	// create and return the resulting test specification
	return new QueryFilterValueTestSpec(valueExpr, testType, invert, testParams);
}

// export the parser function
exports.parseFilterSpec = parseFilterSpec;
