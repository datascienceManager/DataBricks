**Databricks**, tracking whether someone **clicked, opened, or downloaded your notebook** depends on your workspace 
settings and audit logging configuration.

There are 3 realistic ways to check this:

---

# ✅ 1️⃣ Check Notebook Revision History (Basic Visibility)

If someone **edited** your notebook:

1. Open the notebook
2. Click **File → Revision History**

You’ll see:

* Who modified it
* Timestamp
* What changes were made

⚠ This only shows edits — not views or downloads.

---

# ✅ 2️⃣ Check Workspace Audit Logs (Best Method)

To track:

* Notebook opened
* Exported / downloaded
* Shared
* Permission changes

You need **Audit Logs enabled** (usually admin-only feature).

In Databricks:

**Admin Console → Audit Logs**

Or if logs are stored in cloud storage (AWS S3 / Azure Blob), you query them.

---

## Example: Query Audit Logs in Databricks (Spark SQL)

If audit logs are stored as a table (e.g. `audit_logs`):

```sql
SELECT 
    userIdentity.email,
    actionName,
    requestParams.notebookId,
    timestamp
FROM audit_logs
WHERE actionName IN ('exportNotebook', 'getNotebook', 'workspaceExport')
ORDER BY timestamp DESC
```

Key actions to look for:

| Action            | Meaning    |
| ----------------- | ---------- |
| getNotebook       | Opened     |
| exportNotebook    | Downloaded |
| workspaceExport   | Exported   |
| permissionsChange | Shared     |

---

# ✅ 3️⃣ If You Are on Azure Databricks

In **Azure Databricks**

Audit logs go to:

* Azure Log Analytics
* Azure Storage
* Event Hub

Query in Log Analytics:

```kusto
DatabricksAuditLogs
| where actionName in ("exportNotebook", "getNotebook")
| project TimeGenerated, userIdentity, actionName, requestParams
```

---

# ❗ Important Limitation

If:

* You are NOT workspace admin
* Audit logging is NOT enabled

👉 You CANNOT see who viewed or downloaded your notebook.

Databricks does NOT show simple "view count" like Google Docs.

---

# 🔐 Alternative Control Method

If you want to monitor future downloads:

### Option A – Restrict Permissions

Set:

* Can View
* Can Run
* Can Edit

Only give export rights to specific users.

---

### Option B – Move to Repo + Git

If notebook is in Git repo:

* You can track clone activity
* Pull requests
* Commit logs

---

# 🎯 What You Should Do Now

1. Check if Audit Logs are enabled
2. Ask Workspace Admin if needed
3. Query for:

   * exportNotebook
   * workspaceExport
   * getNotebook

---

If you tell me:

* Are you on AWS or Azure?
* Are you workspace admin?

I can give you exact production-ready query for your setup.
