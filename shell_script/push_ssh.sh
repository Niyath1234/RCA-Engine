#!/bin/bash

# Simple SSH-based Git Push Script
# Your remote is already configured for SSH: git@github.com:Niyath1234/RCA-Engine.git

set -e

echo "=== Pushing via SSH ==="
echo "Current branch: $(git branch --show-current)"
echo ""

# Stage and commit if there are changes
if ! git diff-index --quiet HEAD --; then
    echo "Staging changes..."
    git add .
    read -p "Enter commit message: " commit_msg
    git commit -m "$commit_msg"
fi

# Push via SSH
echo ""
echo "Pushing to GitHub via SSH..."
git push -u origin $(git branch --show-current)

echo ""
echo "✓ Push completed!"

