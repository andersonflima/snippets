#!/bin/sh
set -eu
# contract: mermaid_contract
# candidate: mermaid_contract
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SOURCE_FILE="$SCRIPT_DIR/../../mermaid/platform.mmd"
test -f "$SOURCE_FILE"
grep -Eq '^(graph|flowchart|sequenceDiagram|stateDiagram|stateDiagram-v2|gantt)\b' "$SOURCE_FILE"
grep -Eq '((-->|->>|==>)|[.]-.*[.]->)' "$SOURCE_FILE"
