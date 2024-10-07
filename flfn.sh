#!/bin/bash

# Usage: ./flfn.sh /path/to/directory "*.adapter.py" "find_string" "replace_string"

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 /path/to/directory '*.pattern' 'find_string' 'replace_string'"
    exit 1
fi

DIR=$1
PATTERN=$2
FIND_STRING=$3
REPLACE_STRING=$4

# Find all files matching the pattern in the directory and subdirectories
find "$DIR" -type f -name "$PATTERN" | while read -r file; do
    # Get the directory name
    DIRNAME=$(dirname "$file")
    # Get the base name of the file (without the directory path)
    BASENAME=$(basename "$file")
    
    # Check if the filename contains the find_string
    if [[ "$BASENAME" == *"$FIND_STRING"* ]]; then
        # Replace the string in the file name
        NEW_BASENAME=$(echo "$BASENAME" | sed "s/$FIND_STRING/$REPLACE_STRING/g")
        
        # Move the file to the new name if there's a change
        if [ "$BASENAME" != "$NEW_BASENAME" ]; then
            mv "$file" "$DIRNAME/$NEW_BASENAME"
            echo "Renamed: $file -> $DIRNAME/$NEW_BASENAME"
        fi
    fi
done

echo "Filename replacement complete."
