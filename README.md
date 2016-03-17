[![Build Status](https://api.travis-ci.org/datanerds-io/lack.svg?branch=develop)](https://travis-ci.org/datanerds-io/lack)

# About

LACK is a consensus library based on the Cassandra database system. See [here](http://www.datastax.com/dev/blog/consensus-on-cassandra) for details on its implementation.

# Build

The test will start an embedded cassandra server. If the used ports are already in use the tests will fail.

A `gradle build` will build the project.
