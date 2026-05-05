// WHY: Making the loading strategy an explicit, named type prevents
// the silent performance regression of accidentally using joined loading
// on a 1:N relationship, which would produce a Cartesian product.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationLoadingStrategy {
    /// Emits a LEFT JOIN. Correct only for 1:1 and N:1 (to-one) relations.
    /// Using this on a collection relation will produce duplicate parent rows.
    JoinedForToOneRelations,

    /// Executes a second SELECT ... WHERE parent_id IN (...).
    /// Correct for 1:N and M:N (to-many) relations. Avoids Cartesian explosion.
    SelectInForToManyRelations,
}
