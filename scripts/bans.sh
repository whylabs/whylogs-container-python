#!/bin/bash

# Directories to search
directories=("whylogs_container" "whylogs_container_types")

# Patterns to search for
patterns=('# type: ignore' 'print(')

# exclude line pattern
exclude_pattern="# ban: allow"

# Function to search for patterns in a given directory
search_patterns() {
  local dir=$1
  local pattern=$2
  # first find the matches,
  grep -r "$pattern" --include='*.py' "$dir" | grep -v "$exclude_pattern"
}

# Flag to indicate if any matches were found
found_any=false

# Loop through directories and patterns
for dir in "${directories[@]}"; do
  for pattern in "${patterns[@]}"; do
    found_files=$(search_patterns "$dir" "$pattern")
    if [ -n "$found_files" ]; then
      echo "Files in $dir with pattern '$pattern':"
      echo
      echo "$found_files"
      echo
      found_any=true
    fi
  done
done

# Exit with an appropriate status
if [ "$found_any" = true ]; then
  exit 1
else
  echo "No specified patterns found."
  exit 0
fi


