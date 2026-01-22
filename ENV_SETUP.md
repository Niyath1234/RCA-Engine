# Environment Variables Setup Guide

This guide explains how to set up environment variables for the RCA Engine.

## Quick Start

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` file and fill in your actual credentials:**
   ```bash
   # Edit with your preferred editor
   nano .env
   # or
   vim .env
   ```

3. **The `.env` file is already in `.gitignore`**, so your credentials won't be committed to git.

## Required Configuration

### Confluence (Required)

```bash
CONFLUENCE_URL=https://slicepay.atlassian.net/wiki
CONFLUENCE_USERNAME=your-email@example.com
CONFLUENCE_API_TOKEN=your-confluence-api-token
CONFLUENCE_SPACE_KEY=HOR
```

**How to get Confluence API Token:**
1. Go to https://id.atlassian.com/manage-profile/security/api-tokens
2. Click "Create API token"
3. Copy the token and paste it in `.env`

### Slack (Optional - for notifications)

```bash
SLACK_BOT_TOKEN=xoxb-your-slack-bot-token
SLACK_DEFAULT_CHANNEL=#general
```

**How to get Slack Bot Token:**
1. Go to https://api.slack.com/apps
2. Create a new app or select existing app
3. Go to "OAuth & Permissions"
4. Install app to workspace
5. Copy "Bot User OAuth Token" (starts with `xoxb-`)

### Jira (Optional - if using Jira integration)

```bash
JIRA_URL=https://slicepay.atlassian.net
JIRA_USERNAME=your-email@example.com
JIRA_API_TOKEN=your-jira-api-token
```

**Note:** Jira and Confluence can use the same API token if they're on the same Atlassian instance.

## Using Configuration in Code

### Import Config Module

```python
from src.config import Config

# Get Confluence credentials
url = Config.get_confluence_url()
username = Config.get_confluence_username()
api_token = Config.get_confluence_api_token()

# Get Slack credentials
slack_token = Config.get_slack_bot_token()
channel = Config.get_slack_default_channel()
```

### Validate Configuration

```python
from src.config import Config

# Validate Confluence config
is_valid, error = Config.validate_confluence_config()
if not is_valid:
    print(f"Configuration error: {error}")
    sys.exit(1)

# Validate Slack config
is_valid, error = Config.validate_slack_config()
if not is_valid:
    print(f"Slack not configured: {error}")
```

## Environment Variables Reference

### Confluence Configuration
- `CONFLUENCE_URL` - Confluence base URL (default: https://slicepay.atlassian.net/wiki)
- `CONFLUENCE_USERNAME` - Your Atlassian email/username
- `CONFLUENCE_API_TOKEN` - Confluence API token (required)
- `CONFLUENCE_SPACE_KEY` - Default space key (default: HOR)

### Jira Configuration
- `JIRA_URL` - Jira base URL (default: https://slicepay.atlassian.net)
- `JIRA_USERNAME` - Your Atlassian email/username
- `JIRA_API_TOKEN` - Jira API token

### Slack Configuration
- `SLACK_BOT_TOKEN` - Slack bot token (starts with `xoxb-`)
- `SLACK_DEFAULT_CHANNEL` - Default channel for notifications (default: #general)
- `SLACK_WORKSPACE_NAME` - Workspace name (optional)
- `SLACK_MCP_XOXC_TOKEN` - XOXC token for MCP (optional)
- `SLACK_MCP_XOXD_TOKEN` - XOXD token for MCP (optional)

### Database Configuration
- `DATABASE_URL` - PostgreSQL connection string (optional)

### Vector Database Configuration
- `VECTOR_DB_PATH` - Path to vector database (default: ./data/vector_db)
- `VECTOR_DB_INDEX_PATH` - Path to vector index (default: ./data/vector_index)

### Knowledge Base Paths
- `KNOWLEDGE_BASE_PATH` - Path to knowledge base JSON (default: metadata/knowledge_base.json)
- `KNOWLEDGE_REGISTER_PATH` - Path to knowledge register JSON (default: metadata/knowledge_register.json)
- `PRODUCT_INDEX_PATH` - Path to product index JSON (default: metadata/product_index.json)

### LLM Configuration
- `OPENAI_API_KEY` - OpenAI API key (optional)
- `ANTHROPIC_API_KEY` - Anthropic API key (optional)

### Other Configuration
- `LOG_LEVEL` - Logging level (default: INFO)
- `DEBUG` - Enable debug mode (default: false)

## Security Best Practices

1. **Never commit `.env` file** - It's already in `.gitignore`
2. **Use different tokens for different environments** (dev, staging, production)
3. **Rotate tokens periodically**
4. **Use environment-specific `.env` files** (e.g., `.env.dev`, `.env.prod`)
5. **Store production secrets in a secrets manager** (AWS Secrets Manager, HashiCorp Vault, etc.)

## Troubleshooting

### "CONFLUENCE_USERNAME is not set" error

Make sure you've:
1. Created `.env` file from `.env.example`
2. Filled in `CONFLUENCE_USERNAME` with your actual email
3. The `.env` file is in the project root directory

### "CONFLUENCE_API_TOKEN is not set" error

Make sure you've:
1. Created an API token at https://id.atlassian.com/manage-profile/security/api-tokens
2. Copied the token to `.env` file
3. The token doesn't have extra spaces or quotes

### Environment variables not loading

Make sure:
1. `python-dotenv` is installed: `pip install python-dotenv`
2. You're importing `Config` from `src.config` (which auto-loads `.env`)
3. The `.env` file is in the project root

## Example .env File

```bash
# Confluence Configuration
CONFLUENCE_URL=https://slicepay.atlassian.net/wiki
CONFLUENCE_USERNAME=your-email@example.com
CONFLUENCE_API_TOKEN=your-confluence-api-token
CONFLUENCE_SPACE_KEY=HOR

# Slack Configuration
SLACK_BOT_TOKEN=xoxb-your-slack-bot-token
SLACK_DEFAULT_CHANNEL=#rca-engine

# Other configuration
LOG_LEVEL=INFO
DEBUG=false
```

## Testing Your Configuration

After setting up `.env`, test your configuration:

```bash
# Test Confluence connection
python test_confluence_connection.py

# Test Jira connection
python test_jira_connection.py

# Test knowledge extraction
python test_page_integration.py
```

All tests will automatically use credentials from your `.env` file.

