'use strict';


const PARAM_MARKER = Symbol('PARAM');
const EXPR_MARKER = Symbol('EXPR');
const TAG = Symbol('X2NODE_QUERIES');

/**
 * Get parameter placeholder object that can be used in query specifications.
 *
 * @memberof module:x2node-queries
 * @function param
 * @param {string} paramName Parameter name.
 * @returns {Object} Placeholder object.
 */
exports.param = function(paramName) {

	return {
		[PARAM_MARKER]: true,
		name: paramName,
		toString() { return '?{' + this.name + '}'; }
	};
};

/**
 * Tell if the provided argument is a parameter placeholder.
 *
 * @memberof module:x2node-queries
 * @function isParam
 * @param {*} v Argument to test. Safe to pass <code>null</code>,
 * <code>undefined</code>, etc.
 * @returns {boolean} <code>true</code> if the provided argument is a parameter
 * placeholder.
 */
exports.isParam = function(v) {

	return ((v !== null) && ((typeof v) === 'object') && v[PARAM_MARKER]);
};

/**
 * Get value expression placeholder object that can be used in query
 * specifications.
 *
 * @memberof module:x2node-queries
 * @function expr
 * @param {string} valueExpr Value expression text.
 * @returns {Object} Placeholder object.
 */
exports.expr = function(valueExpr) {

	return {
		[EXPR_MARKER]: true,
		expr: valueExpr,
		toString() { return '{{' + this.valueExpr + '}}'; }
	};
};

/**
 * Tell if the provided argument is a value expression placeholder.
 *
 * @memberof module:x2node-queries
 * @function isExpr
 * @param {*} v Argument to test. Safe to pass <code>null</code>,
 * <code>undefined</code>, etc.
 * @returns {boolean} <code>true</code> if the provided argument is a value
 * expression placeholder.
 */
exports.isExpr = function(v) {

	return ((v !== null) && ((typeof v) === 'object') && v[EXPR_MARKER]);
};

/**
 * Tag object.
 *
 * @private
 * @param {*} o Object to tag.
 */
exports.tag = function(o) {

	o[TAG] = true;
};

/**
 * Tell if the specified object is tagged.
 *
 * @private
 * @param {*} o Object to test.
 * @returns {boolean} <code>true</code> if tagged.
 */
exports.isTagged = function(o) {

	return o[TAG];
};
