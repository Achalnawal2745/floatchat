# How to Download & Ingest More ARGO Floats

## Quick Start (3 Steps)

### Step 1: Edit the Float List

Open `download_and_ingest.py` and find this section (around line 18):

```python
# ============================================================
# EDIT THIS LIST - Add float IDs you want to download
# ============================================================
FLOAT_IDS = [
    '1902669',  # INCOIS float
    '1902670',  # INCOIS float
    '2902676',  # INCOIS float
    # Add more float IDs here...
]
# ============================================================
```

**Add your float IDs** to this list. For example:

```python
FLOAT_IDS = [
    '1902669',
    '1902670', 
    '2902676',
    '3902630',  # Add this
    '7902249',  # Add this
]
```

### Step 2: Run the Script

```powershell
python download_and_ingest.py
```

The script will:
1. Show you the list of floats
2. Ask you to press ENTER to confirm
3. Download each float's merged profile file (`*_prof.nc`)
4. Automatically run the ingestion script
5. Load all data into your database

### Step 3: Verify

```powershell
python verify_ingestion.py
```

---

## Finding Float IDs

### Method 1: Browse FTP Server
```
ftp://ftp.ifremer.fr/ifremer/argo/dac/incois/
```
Each folder name is a float ID (e.g., `1900121`, `2902676`)

### Method 2: ARGO Float Tracker
Visit: https://www.ocean-ops.org/board
- Search by region (e.g., "Indian Ocean")
- Filter by country (e.g., "India")
- Copy the WMO number (this is the float ID)

### Method 3: Use Existing Floats
Your database already has these floats:
- 1900121 ✅ (already ingested)
- 2902296 ✅ (already in DB)
- 2903893 ✅ (already in DB)
- 3902630 ✅ (already in DB)
- 7902249 ✅ (already in DB)
- 7902251 ✅ (already in DB)

---

## Example: Adding 3 New Floats

1. **Edit** `download_and_ingest.py`:
```python
FLOAT_IDS = [
    '1902671',  # New float
    '1902672',  # New float
    '1902673',  # New float
]
```

2. **Run**:
```powershell
python download_and_ingest.py
```

3. **Output**:
```
Downloading Float: 1902671
  Downloaded: netcdf_data\1902671_prof.nc (450,000 bytes)

Downloading Float: 1902672
  Downloaded: netcdf_data\1902672_prof.nc (380,000 bytes)

Running Ingestion Script
  Inserted 45 measurements for cycle 1
  Inserted 44 measurements for cycle 2
  ...
  Successfully ingested!
```

---

## Troubleshooting

**Problem:** "File not found"
- **Solution:** The float might not have a `*_prof.nc` file. Try a different float ID.

**Problem:** FTP timeout
- **Solution:** Try again later or use a different FTP mirror (USGODAE).

**Problem:** "Already exists"
- **Solution:** The script skips already-downloaded files. Delete from `netcdf_data/` to re-download.

---

## What Gets Downloaded

For each float, the script downloads the **merged profile file**:
- Filename: `{FLOAT_ID}_prof.nc`
- Contains: ALL cycles/profiles for that float
- Size: Usually 200KB - 2MB
- Includes: Complete metadata, all measurements

**NOT downloaded** (single profiles):
- `D*.nc` - Single descending profile
- `R*.nc` - Single real-time profile

These have limited data. Always use `*_prof.nc` files!
