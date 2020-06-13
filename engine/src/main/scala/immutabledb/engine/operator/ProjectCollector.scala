package immutabledb.operator

import immutabledb.Row

class ProjectCollector(ops: Seq[ProjectionOperator]) extends ProjectionOperator {

    def iterator = new ProjectCollectorIterator

    class ProjectCollectorIterator extends Iterator[Row] {
        def next: Row = ???

        def hasNext: Boolean = ???
    }
}
