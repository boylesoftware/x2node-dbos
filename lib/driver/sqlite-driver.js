class SqliteDriver extends BasicDBDriver{

    constructor(options) {
        super(options);
    }

    supportsRowLocksWithAggregates() { return false; }

    safeLikePatternFromExpr(expr) {
        return `REPLACE(REPLACE(REPLACE(${expr}, '\\\\', '\\\\\\\\'),` +
            ` '%', '\\%'), '_', '\\_')`;
    }

    stringSubstring(expr, from, len) {

        return 'SUBSTRING(' + expr +
            ', ' + (
                (typeof from) === 'number' ?
                    String(from + 1) : '(' + String(from) + ') + 1'
            ) +
            (len !== undefined ? ', ' + String(len) : '') + ')';
    }

    nullableConcat() {

        return 'CONCAT(' + Array.from(arguments).join(', ') + ')';
    }

}