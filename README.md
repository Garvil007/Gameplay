# US Sports Facility Finder

Streamlit app. Finds sports facilities (soccer, baseball, basketball, tennis, volleyball, football) in any US city using free OpenStreetMap data (Overpass API + Nominatim geocoding). Exports formatted Excel.

Main file: `sports_app_2.py`

---

## 1. Prerequisites

| Tool   | Version  | Why                          |
|--------|----------|------------------------------|
| Python | 3.10+    | Runtime                      |
| pip    | 23+      | Package install              |
| git    | any      | Clone (optional)             |
| Docker | optional | Self-hosted Overpass server  |

Check Python version:

```bash
python --version
```

If older than 3.10 → install from [python.org](https://www.python.org/downloads/).

**Windows note:** during install, tick **"Add Python to PATH"**.
**macOS note:** prefer `python3` (Apple ships Python 2 sometimes).
**Linux note:** `sudo apt install python3 python3-venv python3-pip` (Debian/Ubuntu) or distro equivalent.

---

## 2. Get the code

```bash
git clone https://github.com/Garvil007/Gameplay.git
cd Gameplay/app
```

Or download ZIP from GitHub and extract.

---

## 3. Create virtual environment

Keeps dependencies isolated. One-time setup per machine.

### Windows (PowerShell)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

If PowerShell blocks the script:

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

### Windows (cmd.exe)

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Prompt should now start with `(.venv)`.

---

## 4. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Installs:
- `streamlit>=1.32.0` — web UI
- `requests>=2.31.0` — HTTP client
- `openpyxl>=3.1.2` — Excel output

Takes ~30 seconds.

---

## 5. Create `.env` file (recommended)

The app reads configuration from a `.env` file in the `app/` directory. Create it once — no need to set system environment variables every session.

### Create the file

Inside `app/`, create a file named `.env`:

```
app/
└── .env   ← create this
```

### Contents

```env
# Required for Nominatim geocoding (use your real email)
CONTACT_EMAIL=your.email@example.com
```

**That's the only variable needed for local use.** Full list:

| Variable | Required | Purpose |
|---|---|---|
| `CONTACT_EMAIL` | Recommended | Nominatim User-Agent (avoids HTTP 403) |

### Why it matters

OpenStreetMap Nominatim requires a real contact email in the `User-Agent` header. Without it you may get HTTP 403 on public mirrors, especially on shared hosting.

### Example `.env`

```env
CONTACT_EMAIL=john.doe@gmail.com
```

> **Never commit `.env` to git.** Add it to `.gitignore`:
> ```
> .env
> ```

### Streamlit Cloud deploy (no `.env` file)

Add via app Settings → Secrets instead:

```toml
CONTACT_EMAIL = "your.email@example.com"
```

---

## 6. Run the app

```bash
streamlit run sports_app_2.py
```

Output:

```
You can now view your Streamlit app in your browser.
Local URL:  http://localhost:8501
Network URL: http://192.168.x.x:8501
```

Browser opens automatically. If not → open the Local URL manually.

Stop the server with `Ctrl+C`.

---

## 7. Use the app

1. Sidebar → pick sport (Soccer, Baseball, Basketball, Tennis, Volleyball).
2. Enter **City** (e.g., `Daly City`).
3. Enter **County** (e.g., `San Mateo County`).
4. Enter **State** (e.g., `California`).
5. Click **Find Facilities**.
6. Wait 10–60 seconds (longer on first run; cached after).
7. Click **Download Excel File**.

---

## 8. Optional: self-hosted Overpass server

Public Overpass mirrors rate-limit aggressively. For heavy use → run local Overpass via Docker.

Hardware requirements:
- 16+ GB RAM
- 150+ GB disk
- 3–6 hour first-time data load

```bash
docker compose up -d overpass
```

Then in the sidebar → **Performance / Advanced** → **Custom URL** → `http://localhost:8080/api/interpreter`.

Local Overpass auto-uses 6 parallel workers (vs 2 for public).

---

## 9. Cache

App writes `facility_cache.db` (SQLite) in the working directory. TTL = 7 days. Same city searched twice → near-instant second run.

Clear cache: sidebar → **Performance / Advanced** → **Clear cache**.

Or delete the file:

```bash
# Linux/macOS
rm facility_cache.db

# Windows PowerShell
Remove-Item facility_cache.db
```

---

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `streamlit: command not found` | venv not activated | Re-run activate step (§3) |
| `ModuleNotFoundError: streamlit` | deps not installed | `pip install -r requirements.txt` |
| HTTP 403 from Nominatim | Missing `CONTACT_EMAIL` | Add to `.env` file (§5) |
| HTTP 429 from Overpass | Public mirror rate-limited | Pick different mirror in sidebar, or self-host (§8) |
| "All Nominatim mirrors failed" | Network / firewall block | Check VPN, proxy, corporate firewall |
| App slow on first search | First-time API call, no cache | Normal — second search same city is fast |
| "City not found" | Spelling / unknown OSM entry | Try `City of <name>` or nearby major city |
| PowerShell blocks venv activate | Execution policy | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` |
| Port 8501 already in use | Another Streamlit running | `streamlit run sports_app_2.py --server.port 8502` |
| SSL certificate error (corporate network) | MITM proxy | Set `REQUESTS_CA_BUNDLE=path/to/cert.pem` |

---

## 11. Update

```bash
git pull
pip install -r requirements.txt --upgrade
```

---

## 12. Deactivate venv

When done:

```bash
deactivate
```

---

## File layout

```
app/
├── sports_app_2.py           # Main Streamlit app
├── requirements.txt          # Pip dependencies
├── .env                      # Your local config (not committed to git)
├── config.toml               # Streamlit server config
├── docker-compose.yml        # Optional local Overpass
├── facility_cache.db         # SQLite cache (auto-created)
└── README.md                 # This file
```

---

## License

Proprietary. See LICENSE if included, otherwise contact the repository owner.
