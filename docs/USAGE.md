# Usage Instructions

## How to Run the Clinical Trials Metadata Management System

### 1. Set Up Your Environment

If you haven’t already:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

### 2. (Optional) Configure the Database

- By default, the app uses `clinical_trials.db` in your project root.
- If you want to change the database location or settings, edit `config/database.yml`.

---

### 3. (Optional) Extract and Load Data

- **If `clinical_trials.db` already exists and you want to use the existing data, skip this step.**
- **If you want to fetch fresh data or the database is missing/corrupted:**
  ```bash
  python data_ingestion/extract_trials.py
  python data_cleaning/transform_trials.py
  ```

---

### 4. Start the Dashboard

```bash
streamlit run dashboard/app.py
```

- Open the provided local URL (e.g., http://localhost:8503) in your browser.

---

### 5. (Optional) Run Tests

```bash
pytest tests/
```

---

## Summary Table

| Step                        | Command/Action                                      |
|-----------------------------|-----------------------------------------------------|
| Set up environment          | `python -m venv venv`<br>`source venv/bin/activate`<br>`pip install -r requirements.txt` |
| (Optional) Configure DB     | Edit `config/database.yml`                          |
| (Optional) Run ETL pipeline | `python data_ingestion/extract_trials.py`<br>`python data_cleaning/transform_trials.py` |
| Start dashboard             | `streamlit run dashboard/app.py`                    |
| (Optional) Run tests        | `pytest tests/`                                     |

---

**Notes:**
- If `clinical_trials.db` is present, the dashboard will show the data already loaded.
- If you want to refresh the data, re-run the ETL pipeline as shown above.
- For troubleshooting or advanced configuration, see the README or other docs in the `docs/` directory. 