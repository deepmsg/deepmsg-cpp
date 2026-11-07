# Long2LongHashMap iteration golden

Produced by the **reference implementation** (Agrona 2.7.0 -- the version Aeron
1.54.0 pins in `gradle/libs.versions.toml`), never by our code.

| Item | Value |
|---|---|
| generator | `/home/gavin/trader/analysis/cpp/records/x0-3/Long2LongGoldenGen.java` (to be moved to the fork `deepmsg/aeron`, branch `deepmsg-tests`, `deepmsg-tests/generators/`) |
| Agrona jar | `org.agrona:agrona:2.7.0`, sha256 `37059b565b7a735929397bf056a7270664cae708f7ded4df7c18fddf49e1f91f` |
| command | `javac -cp agrona-2.7.0.jar -d /tmp/gen Long2LongGoldenGen.java && java -cp agrona-2.7.0.jar:/tmp/gen Long2LongGoldenGen ops.tsv > expected.txt` |

`ops.tsv` is the input; each `check` emits the keys in Agrona's iteration order
(comma separated, empty line for an empty map) and each `hash` emits
`Hashing.hash(long)`. `test/unit/core/long2long_hash_map_test.cpp` replays the
same ops through the C++ port and compares line by line.

What the sequence covers: power-of-two growth from capacity 8 (resize at
`6 > int(8 * 0.65) = 5`), collisions, removals that exercise `compactChain`,
negative and `Long.MAX_VALUE` keys, and `clear`.

Regenerate only by re-running the reference generator; never edit `expected.txt`
by hand.
