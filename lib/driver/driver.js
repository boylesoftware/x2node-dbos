/**
 * Interface for database drivers that can be registered by the application to be
 * used by the query factory using {@link registerDriver} function.
 *
 * @interface DBDriver
 * @memberof module:x2node-queries
 */
/**
 * Get SQL for the specified ES value.
 *
 * @function module:x2node-queries.DBDriver#sql
 * @param {*} val The ES value. If object, <code>toString()</code> is called on
 * it and the result is returned as is.
 * @returns {string} String representing the value in SQL, or <code>null</code>
 * if the value cannot be represented in SQL (e.g. <code>undefined</code>,
 * <code>NaN</code>, <code>Infinity</code> or an array).
 */
/**
 * Get Boolean SQL literal.
 *
 * @function module:x2node-queries.DBDriver#booleanLiteral
 * @param {*} val The ES value.
 * @returns {string} String representing Boolean true or false in SQL.
 */
/**
 * Get string SQL literal.
 *
 * @function module:x2node-queries.DBDriver#stringLiteral
 * @param {string} val The ES string.
 * @returns {string} String representing the string in SQL.
 */
// TODO: interface methods documentation
