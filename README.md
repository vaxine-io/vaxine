![Erlang CI](https://github.com/vaxine-io/vaxine/workflows/CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/vaxine-io/vaxine/badge.svg?branch=main)](https://coveralls.io/github/vaxine-io/vaxine?branch=main)
[![License - MIT](https://img.shields.io/badge/license-MIT-green)](./blob/main/LICENSE.md)
![Status - Alpha](https://img.shields.io/badge/status-alpha-red)

<h2>
  <picture>
    <source media="(prefers-color-scheme: dark)"
        srcset="https://vaxine.io/id/vaxine-logo-dark.png"
    />
    <source media="(prefers-color-scheme: light)"
        srcset="https://vaxine.io/id/vaxine-logo-light.png"
    />
    <img alt="Vaxine logo" src="https://vaxine.io/id/vaxine-logo-light.png" />
  </picture>
</h2>

Welcome to the Vaxine source code repository. Vaxine is a rich-CRDT database system based on AntidoteDB.

## About Vaxine

Vaxine is a rich-CRDT database system that extends Antidote with a relational data-model, invariant safety, query support and real-time subscriptions. Applications built on top of Vaxine offer:

* low-latency, active-active geo-distribution
* transactional causal+ consistency
* relational data model
* constraints and referential integrity
* real-time subscriptions

More information:

- [Vaxine website](https://vaxine.io)
- [Documentation](https://vaxine.io/docs)
- [Example apps](https://vaxine.io/demos) ([source code](https://github.com/vaxine-io/examples))

## About Antidote

AntidoteDB is a planet scale, highly available, transactional database. Antidote implements the [Cure protocol](https://ieeexplore.ieee.org/document/7536539/) of transactional causal+ consistency based on [CRDTs](https://crdt.tech).

More information:

- [Antidote website](https://www.antidotedb.eu)
- [Documentation](https://antidotedb.gitbook.io/documentation)

Antidote is the reference platform of the [SyncFree](https://syncfree.lip6.fr/) and the [LightKone](https://www.lightkone.eu/) european projects.

## Community Guidelines
This repo contains guidelines for participating in the Vaxine community:

* [Code of Conduct](./CODE_OF_CONDUCT.md)
* [Guide to Contributing](./CONTRIBUTING.md)
* [Contributor License Agreement](./CLA.md)

If you have any questions or concerns, please raise them on the [Vaxine community](https://vaxine.io/project/community) channels.
