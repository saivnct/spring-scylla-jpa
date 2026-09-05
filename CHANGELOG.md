# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- N/A

### Changed
- N/A

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A

## [0.2.2] - 2026-09-05

### Fixed
- `PagingScyllaRepository.findSliceWithPageAndOffset` (both overloads) returned the **last** page when
  asked for one past the end of the result set, labelled with the requested page number. The DataStax
  `OffsetPager` clamps by design — its contract is "the requested page, *or the last page if the
  requested page was past the end*" — and reports what it actually served in `Page.getPageNumber()`,
  which was being discarded. A caller jumping past the end silently received real rows for a page that
  does not exist, and a "fetch the next page until one is empty" loop never terminated, because the
  last page repeated forever. The served page number is now compared against the requested one and a
  clamped result is returned as an empty `Slice` with `hasNext = false`.

  **Behaviour change for every consumer.** An over-deep page that used to return the last page's rows
  now returns nothing. Callers that relied on the clamp — deliberately or not — see empty pages where
  they previously saw data.

## [0.2.1] - 2026-09-05

### Added
- `findSliceWithPageAndOffset(BoundStatement, Pageable, Function<Row, R>)` — the offset-paging read over
  a caller-supplied row mapper, for materialized views that select a subset of columns rather than
  `.all()` and so cannot be mapped by the entity's own row mapper.
- `com.giangbb.scylla.data.CursorPage` — a keyset page: the items plus an opaque `nextCursor`, where a
  null cursor means the walk is finished and a short page carrying one means it stopped early.

### Changed
- The two-argument `findSliceWithPageAndOffset` now delegates to the new generic form.

## [0.2.0] - 2026-02-24

### Added
- Improved project documentation for developer onboarding:
  - technical README structure and navigation,
  - architecture and capability overview,
  - version compatibility matrix,
  - Testcontainers integration testing template.
- Added a standardized `CHANGELOG.md` following Keep a Changelog.

### Changed
- Bumped project version to `0.2.0`.
- Refined GitHub-facing documentation with repository-linked badges and release links.

## [0.1.2] - 2025-07-15

### Added
- Initial public documentation and packaging for `spring-scylla-jpa`.
- Spring-oriented Scylla integration including:
  - mapping context and conversion layer,
  - template execution abstraction,
  - repository-style CRUD and async APIs,
  - schema lifecycle actions for tables, indexes, and UDTs.

### Notes
- Baseline release line for future compatibility and stabilization work.

[Unreleased]: https://github.com/saivnct/spring-scylla-jpa/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/saivnct/spring-scylla-jpa/releases/tag/v0.2.0
[0.1.2]: https://github.com/saivnct/spring-scylla-jpa/releases/tag/v0.1.2
