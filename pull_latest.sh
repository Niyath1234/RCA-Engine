#!/bin/bash

# Pull latest from GitHub
# This script pulls the latest changes from the remote repository

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

echo -e "${GREEN}=== Pull Latest from GitHub ===${NC}\n"

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo -e "${YELLOW}Current branch: ${CURRENT_BRANCH}${NC}"

# Check if there are uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}Warning: You have uncommitted changes${NC}"
    echo -e "${YELLOW}Stashing changes before pull...${NC}"
    git stash
    STASHED=true
else
    STASHED=false
fi

# Fetch latest changes
echo -e "\n${YELLOW}Fetching latest changes...${NC}"
if git fetch origin "$CURRENT_BRANCH"; then
    echo -e "${GREEN}✓ Fetched successfully${NC}"
else
    echo -e "${RED}✗ Fetch failed!${NC}"
    if [ "$STASHED" = true ]; then
        git stash pop
    fi
    exit 1
fi

# Pull changes
echo -e "\n${YELLOW}Pulling changes...${NC}"
if git pull origin "$CURRENT_BRANCH"; then
    echo -e "${GREEN}✓ Pulled successfully${NC}"
    
    # Restore stashed changes if any
    if [ "$STASHED" = true ]; then
        echo -e "\n${YELLOW}Restoring stashed changes...${NC}"
        git stash pop
    fi
    
    echo -e "\n${GREEN}✓ Repository is up to date!${NC}"
else
    echo -e "${RED}✗ Pull failed!${NC}"
    if [ "$STASHED" = true ]; then
        git stash pop
    fi
    exit 1
fi

