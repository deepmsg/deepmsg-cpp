# deepmsg-cpp task entry points. CI calls scripts/check.sh directly (same command).
default: check

check preset="debug":
	scripts/check.sh {{preset}}

build preset="debug":
	cmake --preset {{preset}}
	cmake --build build/{{preset}} --parallel $(nproc)

test preset="debug":
	ctest --test-dir build/{{preset}} --output-on-failure

format:
	clang-format -i $(find src apps tools test bench -name '*.cpp' -o -name '*.h')

format-check:
	clang-format --dry-run --Werror $(find src apps tools test bench -name '*.cpp' -o -name '*.h')

hotpath preset="debug":
	tools/repo/hotpath-audit.sh build/{{preset}}

deps:
	tools/repo/check-deps.sh

sbe:
	scripts/fetch-sbe.sh
