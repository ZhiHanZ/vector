---
title: Log events
weight: 1
tags: ["logs", "events", "schema"]
---

{{< svg "img/data-model-log.svg" >}}

A **log event** in Vector is a structured representation of a point-in-time event. It contains an
arbitrary set of fields that describe the event.

A key tenet of Vector is **schema neutrality**. This ensures that Vector can work with any schema,
supporting legacy and future schemas as your needs evolve. Vector doesn't require *any* specific
fields and each [component][components] documents the fields it provides.

Here's an example representation of a log event (as JSON):

```json
{
  "log": {
    "custom": "field",
    "host": "my.host.com",
    "message": "Hello world",
    "timestamp": "2020-11-01T21:15:47+00:00"
  }
}
```

## Schema

{{< config/log-schema >}}

## How it works

### Schemas

Vector is schema-neutral and doesn't require any specific schema. This ensures that Vector can work
with a variety of schemas, supporting legacy schemas as well as future schemas.

We *do* plan to implement a common information model for Vector that would recognize popular
schemas, allowing you to seamlessly transform between schemas. You can track progress on this work
in [issue 3910][3910].

### Types

#### Strings

Strings are UTF-8 compatible and are only bounded by the available system memory.

#### Integers

Integers are signed integers up to 64 bits.

#### Floats

Floats are 64-bit [IEEE 754][ieee_754] floats.

#### Booleans

Booleans represent binary true/false values.

#### Timestamps

Timestamps are represented as [`DateTime` Rust structs][date_time] stored as UTC.

##### Timestamp Coercion

There are cases where Vector interacts with formats that don't have a formal timestamp definition,
such as JSON. In these cases, Vector ingests the timestamp in its primitive form (string or
integer). You can then coerce the field into a timestamp using the coercer transform. If you're
parsing this data out of a string, all Vector parser transforms include a `types` option, allowing
you to extract and coerce in one step.

#### Time zones

If Vector receives a timestamp that doesn't contain timezone information, it assumes that the
timestamp is in local time and converts the timestamp to UTC from the local time.

#### Null values

For compatibility with JSON log events, Vector also supports `null` values.

#### Maps

Maps are associative arrays mapping string fields to values of any type.

#### Arrays

Array fields are sequences of values of any type.

[3910]: https://github.com/vectordotdev/vector/issues/3910
[components]: /components
[date_time]: https://docs.rs/chrono/latest/chrono/struct.DateTime.html
[ieee_754]: https://en.wikipedia.org/wiki/IEEE_754
