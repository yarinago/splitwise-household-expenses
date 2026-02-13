# splitwise-household-expenses

This project connects with a Splitwise group and exports expense data to an Excel file with dashboard-like reporting.

## Quick Setup

### 1. GitHub Secrets (Sensitive Credentials)

These must be stored as **GitHub Secrets** (never commit to repository):

1. Go to **Settings → Secrets and variables → Actions**
2. Click **"New repository secret"** and create each:

| Secret Name | Value | How to Get |
|-------------|-------|-----------|
| `SPLITWISE_CLIENT_ID` | Your OAuth2 client ID | [Splitwise Settings → OAuth](https://www.splitwise.com/oauth_clients) |
| `SPLITWISE_CLIENT_SECRET` | Your OAuth2 client secret | [Splitwise Settings → OAuth](https://www.splitwise.com/oauth_clients) |
| `SPLITWISE_ACCESS_TOKEN_JSON` | OAuth token JSON | Generate via OAuth flow or use existing token |
| `EMAIL_FROM` | Your Gmail address (sender) | Your Gmail account |
| `EMAIL_PASSWORD` | Gmail app password | [Create app password](https://myaccount.google.com/apppasswords) |

**Token JSON format:**
```json
{"access_token":"your_token","token_type":"bearer","refresh_token":"your_refresh_token"}
```

### 2. GitHub Variables (Public Configuration)

These can be shared/version-controlled. Set them as **GitHub Variables**:

1. Go to **Settings → Secrets and variables → Actions**
2. Click **"New repository variable"** and create each:

| Variable | Value | Required | Example |
|----------|-------|----------|---------|
| `SPLITWISE_GROUP_ID` | Your group ID | ✅ Yes | `12345678` |
| `SPLITWISE_MEMBERS` | JSON user mapping | ✅ Yes | `{"98765432": "Ally", "12312312": "Bob"}` |
| `SPLITWISE_FIRST_MONTH` | Start month | ❌ No | `2008-01` (default) |
| `SPLITWISE_EXCLUDE_MONTHS` | Months to exclude | ❌ No | `2026-01,2026-02` |
| `SPLITWISE_EXCLUDE_DESCRIPTIONS` | Description patterns | ❌ No | `Paris,trip` |
| `SEND_TO_EMAIL` | Recipients for email export | ❌ No | `user@gmail.com` or `user1@gmail.com,user2@gmail.com` |

**How to find your Group ID:**
- Open your Splitwise group
- URL: `https://splitwise.com/groups/12345678` → group ID is `12345678`

**How to create the MEMBERS JSON:**
- Find user IDs in your group settings or by inspecting Splitwise URLs
- Format: `{"user_id_1": "Display Name 1", "user_id_2": "Display Name 2"}`

### 3. Local Development

For testing locally without committing secrets:

1. Create a `.env` file (Git-ignored):
```bash
# .env (DO NOT COMMIT - already in .gitignore)
SPLITWISE_CLIENT_ID=your_client_id
SPLITWISE_CLIENT_SECRET=your_client_secret
SPLITWISE_ACCESS_TOKEN_JSON={"access_token":"...","token_type":"bearer","refresh_token":"..."}
SPLITWISE_GROUP_ID=12345678
SPLITWISE_MEMBERS={"98765432": "Ally", "12312312": "Bob"}
SPLITWISE_FIRST_MONTH=2008-01
SPLITWISE_EXCLUDE_MONTHS=2026-01,2026-02
SPLITWISE_EXCLUDE_DESCRIPTIONS=Paris
EMAIL_FROM=your-email@gmail.com
EMAIL_PASSWORD=your_gmail_app_password
```

2. Install and run:
```bash
pip install -r requirements.txt
python splitwise_to_excel.py
```

## Usage

### Run locally:
```bash
python splitwise_to_excel.py
```

### With custom arguments:
```bash
python splitwise_to_excel.py \
  --group-id 12345678 \
  --start 2008-01 \
  --end 2026-02 \
  --out my_expenses.xlsx
```

### Run via GitHub Actions:
Push to the repository. The workflow at `.github/workflows/splitwise-export.yml` automatically:
- Runs every 2 weeks (1st and 15th of the month at 2 AM UTC)
- Reads secrets from GitHub (injected at runtime)
- Reads variables from GitHub
- Generates Excel export
- Sends export via email (if `SEND_TO_EMAIL` variable is configured)
- Uploads as artifact

To run manually: Go to **Actions → Splitwise Export → Run workflow**

### Email Configuration (Optional)

To receive the export file via email (Gmail):

1. Set these as **GitHub Secrets**:
   - `EMAIL_FROM` - Your Gmail address
   - `EMAIL_PASSWORD` - [Create an app password](https://myaccount.google.com/apppasswords) in your Google Account

2. Set this as a **GitHub Variable**:
   - `SEND_TO_EMAIL` - Recipients (comma-separated, e.g., `user1@gmail.com,user2@gmail.com`)

The workflow uses Gmail's SMTP server automatically (smtp.gmail.com:587).

## Configuration Reference

All settings are environment variables (see [.env.example](.env.example)):

### Required (Secrets)
- `SPLITWISE_CLIENT_ID` - OAuth2 client ID
- `SPLITWISE_CLIENT_SECRET` - OAuth2 client secret
- `SPLITWISE_ACCESS_TOKEN_JSON` - OAuth2 token JSON

### Optional (Secrets - for Email)
- `EMAIL_FROM` - Your Gmail address (required for email export)
- `EMAIL_PASSWORD` - Gmail app password (required for email export)

### Required (Variables)
- `SPLITWISE_GROUP_ID` - Splitwise group ID
- `SPLITWISE_MEMBERS` - JSON map `{"user_id": "name", ...}`

### Optional (Variables)
- `SPLITWISE_FIRST_MONTH` - Start month (default: `2008-01`, format: `YYYY-MM`)
- `SPLITWISE_EXCLUDE_MONTHS` - Months to exclude (CSV, e.g., `2026-01,2026-02`)
- `SPLITWISE_EXCLUDE_DESCRIPTIONS` - Description patterns to exclude (CSV, case-insensitive, e.g., `Paris,trip`)
- `SEND_TO_EMAIL` - Email recipients for export (CSV, e.g., `user1@gmail.com,user2@gmail.com`)

## Output

Generates an Excel file with:
- **Raw_Expenses** - All expenses with per-person amount columns
- **Raw_Shares** - Long-form expense-person relationships
- **Monthly_By_Category** - Monthly category totals
- **PerPerson_Month** - Per-person monthly totals
- **Charts** - Interactive dashboard with tiles and charts

## Security Notes

✅ **DO:**
- Store credentials as GitHub Secrets
- Store configuration as GitHub Variables
- Keep `.env` files Git-ignored for local dev
- Regenerate tokens if accidentally leaked

✅ **DON'T:**
- Commit `.env` files or credentials to Git
- Share secrets in issues or PRs
- Hardcode credentials in Python files
- Use the same token across machines
