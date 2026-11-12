#!/usr/bin/env bash
set -eo pipefail

RUST_TEST_THREADS="${RUST_TEST_THREADS:-2}" cargo test --workspace "$@" 2>&1 | awk '
/test result:/ {
    for (i = 1; i <= NF; i++) {
        if ($i == "passed;") passed += $(i - 1);
        if ($i == "failed;") failed += $(i - 1);
        if ($i == "ignored;") ignored += $(i - 1);
    }
}
{ print }
END {
    print "\n==========================================";
    printf "TOTAL: %d passed; %d failed; %d ignored\n", passed, failed, ignored;
    print "==========================================";
    if (failed > 0) exit 1;
}
'
