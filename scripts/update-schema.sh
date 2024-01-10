#!/usr/bin/env bash

diesel \
  --database-url 'postgres://postgres:postgres@localhost:5432/db' \
  print-schema \
  --custom-type-derives "diesel::query_builder::QueryId" \
  > jobs-postgres/src/schema.rs
