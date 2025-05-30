#!/bin/bash

[ -d handin ] || mkdir handin


cp -f  src/sql/execution/execute.rs handin/
cp -f  src/sql/execution/transform.rs handin/
cp -f src/sql/engine/local.rs handin/
cp -f  src/sql/execution/join.rs handin/
cp -f  src/sql/execution/write.rs handin/
cp -f  src/sql/execution/aggregate.rs handin/
cp -f  src/sql/execution/source.rs handin/
