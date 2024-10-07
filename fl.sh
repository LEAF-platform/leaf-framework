#!/bin/bash

# Usage: ./replace_string.sh /path/to/directory "find_string" "replace_string"

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 /path/to/directory 'find_string' 'replace_string'"
    exit 1
fi

DIR=$1
FIND_STRING=$2
REPLACE_STRING=$3

# Find all files in the directory and its subdirectories
find "$DIR" -type f | while read -r file; do
    # Use sed to replace the string in each file
    if grep -q "$FIND_STRING" "$file"; then
        sed -i "s/$FIND_STRING/$REPLACE_STRING/g" "$file"
        echo "Replaced in: $file"
    fi
done

echo "String replacement complete."

