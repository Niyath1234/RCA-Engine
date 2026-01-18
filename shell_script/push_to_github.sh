#!/bin/bash

# GitHub Push Script using GitHub CLI (gh)
# This script uses the GitHub CLI for authentication and pushing

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
REPO_DIR="/Users/niyathnair/Desktop/Task/RCA-ENGINE"
DEFAULT_BRANCH="main"

# Change to repo directory
cd "$REPO_DIR" || exit 1

echo -e "${GREEN}=== GitHub Push Script (using GitHub CLI) ===${NC}\n"

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}GitHub CLI (gh) is not installed!${NC}"
    echo -e "${YELLOW}Install it with: brew install gh${NC}"
    exit 1
fi

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo -e "${YELLOW}Not authenticated with GitHub CLI${NC}"
    echo -e "${YELLOW}Authenticating...${NC}"
    gh auth login
fi

echo -e "${GREEN}✓ Authenticated with GitHub CLI${NC}\n"

# Get current status
echo -e "${YELLOW}Current Git Status:${NC}"
git status --short

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo -e "\n${YELLOW}Current branch: ${CURRENT_BRANCH}${NC}"

# Push to GitHub
echo -e "\n${YELLOW}Pushing to GitHub...${NC}"
if git push -u origin "$CURRENT_BRANCH"; then
    echo -e "${GREEN}✓ Successfully pushed to GitHub!${NC}"
    echo -e "${GREEN}Repository: https://github.com/Niyath1234/RCA-Engine${NC}"
else
    echo -e "${RED}✗ Push failed!${NC}"
    exit 1
fi


