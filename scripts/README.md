# Setting Up the Daily Scraping Service on Windows

This document guides you step by step to set up the provided scripts as a service on a Windows machine to scrape daily at 6 AM UTC. It assumes no prior Python experience.

---

## 1. Install Necessary Tools

### 1.1 Install Python
1. Download Python 3.11 from [python.org](https://www.python.org/downloads/).
2. During installation:
   - Check **"Add Python to PATH"**.
   - Select **"Customize Installation"** and check all options.
   - Choose **"Install for all users"** and note the installation path (e.g., `C:\Python311`).

### 1.2 Install Visual Studio Code (VS Code)
1. Download VS Code from [code.visualstudio.com](https://code.visualstudio.com/).
2. Install VS Code and ensure you check:
   - Add to PATH.
   - Install recommended extensions.

### 1.3 Install Git
1. Download Git from [git-scm.com](https://git-scm.com/).
2. During installation:
   - Select **Use Git from the Command Prompt**.
   - Choose your preferred editor for Git (e.g., VS Code).
   - Accept the default options unless you have specific preferences.

### 1.4 Clone the Repository
1. Open Command Prompt as Administrator:
   - Press **Win + S**, type `cmd`, right-click Command Prompt, and choose **Run as Administrator**.
2. Navigate to the directory where you want to save the scripts using the `cd` command.
3. Run the following command to clone the repository:
   ```sh
   git clone https://github.com/JoshuaVanStraaten/retailer-scrapers.git
   ```
4. Change to the cloned repository's directory:
   ```sh
   cd retailer-scrapers/scripts
   ```

Ensure all scripts are located in the `scripts` directory of the repository.---

## 2. Install Dependencies

### 2.1 Open Command Prompt
1. Open Command Prompt as Administrator:
   - Press **Win + R**, type `cmd`, and press Enter.
   - Right-click Command Prompt and choose **Run as Administrator**.
2. Navigate to the cloned repository's directory using the `cd` command.

### 2.2 Install Required Python Packages
Run the following commands:
```sh
pip install pywin32 selenium pandas supabase pytz requests svglib reportlab psutil
```

**Notes:**
- If you encounter errors with `pip install`:
  - Ensure Python is added to PATH.
  - Run Command Prompt as Administrator.
  - Update `pip` using `pip install --upgrade pip`.

---

## 3. Set Up ChromeDriver

### 3.1 Download ChromeDriver
1. Check your Google Chrome version:
   - Open Chrome.
   - Go to **Settings > About Chrome**.
2. Download the corresponding ChromeDriver version from [chromedriver.chromium.org](https://chromedriver.chromium.org/downloads).

### 3.2 Set Up ChromeDriver Path
1. Extract the downloaded file.
2. Move it to a permanent location (e.g., `C:\WebDrivers\chromedriver.exe`).
3. Update the `driver_path` variable in `scrape_checkers.py`:
   ```python
   driver_path = r"C:\WebDrivers\chromedriver.exe"
   ```

**Common Issues:**
- If ChromeDriver doesn't work:
  - Ensure your Chrome version matches the ChromeDriver version.
  - Add `C:\WebDrivers` to your system PATH.
  - Check for missing dependencies in the error log.

---

## 4. Configure Supabase

### 4.1 Update Supabase Credentials
Open `scrape_checkers.py` and `scrape_pnp.py`, then replace the placeholders:
```python
SUPABASE_URL = "https://sfnavipqilqgzmtedfuh.supabase.co"
SUPABASE_KEY = "<your_supabase_key>"
```
Replace `<your_supabase_key>` with:
```plaintext
# YOUR SUPABASE KEY
```

**Common Issues:**
- Verify the Supabase URL and key are correct.
- Ensure your network allows access to the Supabase endpoint.
- Check for typos in the URL or key.

---

## 5. Install the Service

### 5.1 Configure the Service Script
1. Ensure the `scrape_service` script is in the same folder as the scraping scripts.
2. Update the paths in `scrape_service` if necessary:
   - `daily_scrape.py` should be in the same directory.

### 5.2 Install the Service
1. Open Command Prompt as Administrator:
   - Press **Win + S**, type `cmd`, right-click Command Prompt, and choose **Run as Administrator**.
2. Navigate to the script folder.
3. Run:
   ```sh
   python scrape_service.py install
   python scrape_service.py start
   ```

**Common Issues:**
- If the service fails to install:
  - Ensure the script paths are correct.
  - Check for typos in the service name.
  - Verify you have Administrator privileges.

---

## 6. Schedule the Service

### 6.1 Configure the Task Scheduler
1. Open Task Scheduler (Press **Win + S**, search for **Task Scheduler**, and open it).
2. Create a new task:
   - **Name:** Daily Scrape Service.
   - **Trigger:** Daily at 6:00 AM.
   - **Action:** Start a program.
     - Program/script: `sc`.
     - Add arguments: `start DailyScrapeService`.
3. Save the task.

**Common Issues:**
- If the task doesn’t run:
  - Ensure the Task Scheduler service is running.
  - Check the "History" tab for errors.
  - Verify the task is set to run with highest privileges.

---

## 7. Verify Setup

### 7.1 Test the Service
1. Run the service manually:
   ```sh
   python ScrapeService.py debug
   ```
2. Check the logs for errors.

**Common Issues:**
- If the service doesn’t start:
  - Verify the script files are error-free.
  - Check for missing dependencies.
  - Look for detailed error messages in the console.

### 7.2 Verify Outputs
- Ensure backups appear in the `backup` folder.
- Check for a `products.csv` file with data.

**Common Issues:**
- If no output appears:
  - Check for runtime errors in the scraping scripts.
  - Verify the Supabase credentials and database structure.

---

## 8. Additional Notes

- Ensure your system time is set to UTC or adjust the schedule accordingly.
- To stop the service, use:
  ```sh
  python ScrapeService.py stop
  ```
- For troubleshooting, check the Windows Event Viewer.
- Update Python dependencies periodically with:
  ```sh
  pip install --upgrade selenium pandas supabase
  ```

---

You have successfully set up the daily scraping service!
