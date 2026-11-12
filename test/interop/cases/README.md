# Case denominators (Aeron 1.54.0)

`case-denominators.tsv` is the per-case list produced by X0-6 against Aeron
**1.54.0** (`454da1e4e6`). The generator and its audit notes live outside this
repo in `analysis/cpp/records/x0-6/` (`extract-cases.py`, `README.md`).

Columns: `area, kind, class_or_binary, suite, case, main_class, line, command, ours`.

`ours` names the gtest that carries the reference's case in this tree, or
`n/a:<why>` when the reference's shape cannot exist on this side. It is
filled by the same generator (`extract-cases.py --annotate`), card by card:
a blank cell means the case has not been claimed yet.

- `archive-c` rows are runtime gtest cases from `--gtest_list_tests` on the
  reference build (`build/reference/binaries`); `main_class` is the Java main the
  fixture starts (`io.aeron.archive.ArchivingMediaDriver`, `io.aeron.MediaDriver`
  or none for unit tests).
- `cluster-java` rows are one per `@Test`/`@ParameterizedTest` (system tests from
  `aeron-system-tests/src/test/java/io/aeron/cluster`, unit tests from
  `aeron-cluster/src/test/java`).

Totals at 1.54.0: **319** C runtime cases, **179** cluster system, **404**
cluster unit. The 1.53.2 baseline was 306 / 178 / 396.

Do not hand-edit: regenerate with `extract-cases.py` (and re-annotate with it)
and review the diff. A5b
(XA-1) and C6c (XC-2) consume this table as the denominator.
