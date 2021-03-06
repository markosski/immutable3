# ImmuTable3

## CLI commands

```
java -jar engine/target/scala-2.12/immutabledb-assembly-0.0.1-SNAPSHOT.jar -q "select id, age from test_100 where (age > 18 and age < 30) limit 10" -d "/Users/marcin/immutable3"
```

```
java -jar loader/target/scala-2.12/loader-assembly-0.0.1-SNAPSHOT.jar -t test_100m --cols id:DENSE_INT,state:DENSE_STRING:size=2,age:DENSE_TINYINT -d /Users/marcin/immutable3 -i data_100m.csv --block-size 1024 --segment-size 1000
```

## Native Image

```
native-image --verbose --no-fallback --allow-incomplete-classpath -H:IncludeResources='.*' -H:-UseServiceLoaderFeature -jar engine/target/scala-2.12/immutabledb-assembly-0.0.1-SNAPSHOT.jar immutabledb-sql
```

# Concepts
- Table
    Stores information about what columns are in the table.
    - path to directory with columns/segments
    - returns columns belonging to table

- Column
    Representation of a column.
    - path to directory with Segments

- DataType
    Supported data type and operations

- Codec
    Different implementations of data encoding/compression.
    - integer compression for sorted values
    - snappy compression for most other data
    - no compression (dense array)

- SegmentReader/Writer
    Segment is a physical file of column data consisting of fixed number
    of blocks.
    - needs to store information about location of each block. Blocks are
     compressed so size of each block will be different.

- BlockReader/Writer
    Block is the smallest unit of column data. Each
    block is compressed individually.
    - return vector of decoded values

- Vector
    Vector is the unit of data in processing pipeline.

- SegmentManager
    Loads segments into memory

- Operators
    - Scanner
        Reads blocks from segments

    - RangeFilter
        For numeric columns.
        Work on vector of data marking columns that match range of values

    - MatchFilter
        Match exact value.

    - Project
        Returns tuples of values.

- Loaders
    Load data from other formats into table.
    - CsvLoader

- IO
    Collection of tools to read/write data


# Data projection
- row iterator interface
- raster map tile interface

Loading and encoding data is a task that may require maintaining some
data in memory while loading is in progress.
It would be good to load data in batches of records for data locality.

