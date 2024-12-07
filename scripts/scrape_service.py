import win32serviceutil
import win32service
import win32event
import subprocess
import os

class ScrapeService(win32serviceutil.ServiceFramework):
    _svc_name_ = "DailyScrapeService"
    _svc_display_name_ = "Daily Scrape Service"
    _svc_description_ = "Runs scrape_checkers and scrape_pnp daily at 6 AM with backups."

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        self.process = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)
        if self.process:
            self.process.terminate()
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        # Path to the daily_scrape.py script
        script_path = os.path.abspath("daily_scrape.py")
        while True:
            # Run the script
            self.process = subprocess.Popen(["python", script_path], shell=False)
            self.process.wait()

if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(ScrapeService)
