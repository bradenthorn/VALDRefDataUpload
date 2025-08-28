# VALD Reference Data Upload

A Python automation tool for processing and uploading reference data to VALD (Vienna Atomic Line Database) systems. Designed to run automatically every night to keep your database updated.

## Overview

This project automatically processes various types of reference data and uploads it to VALD systems:
- CMJ (Composite Score) data
- HJ (High Jump) data  
- IMTP (Isometric Mid-Thigh Pull) data
- PPU (Push-Up) data
- New Composite Score calculations

## Features

- **Automated processing** - Runs nightly to keep data current
- **Cloud integration** - Uploads to Google Cloud Platform
- **Error handling** - Comprehensive logging and error recovery
- **Modular design** - Easy to add new data processors

## Quick Setup

1. **Clone and setup:**
```bash
git clone <your-repo-url>
cd VALDRefDataUpload
python -m venv MyVenv
MyVenv\Scripts\activate  # Windows
# source MyVenv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

2. **Configure credentials:**
   - Copy `env.example` to `.env` and fill in your values
   - Place `gcp_credentials.json` in the project root
   - Update `VALDAPI.txt` with your API details

3. **Test the setup:**
```bash
python Scripts/enhanced_cmj_processor.py
```

## Automation Setup

### Option 1: Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger to daily at your preferred time
4. Action: Start a program
5. Program: `C:\path\to\MyVenv\Scripts\python.exe`
6. Arguments: `C:\path\to\VALDRefDataUpload\Scripts\run_all_processors.py`

### Option 2: Cron (Linux/macOS)
```bash
# Edit crontab
crontab -e

# Add this line to run at 2 AM daily
0 2 * * * cd /path/to/VALDRefDataUpload && /path/to/MyVenv/bin/python Scripts/run_all_processors.py
```

### Option 3: Systemd Timer (Linux)
Create `/etc/systemd/system/vald-upload.timer`:
```ini
[Unit]
Description=VALD Data Upload Timer

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Create `/etc/systemd/system/vald-upload.service`:
```ini
[Unit]
Description=VALD Data Upload Service

[Service]
Type=oneshot
User=your-username
WorkingDirectory=/path/to/VALDRefDataUpload
ExecStart=/path/to/MyVenv/bin/python Scripts/run_all_processors.py
Environment=PATH=/path/to/MyVenv/bin
```

## Manual Usage

Run individual processors:
```bash
python Scripts/enhanced_cmj_processor.py
python Scripts/process_hj.py
python Scripts/process_imtp.py
python Scripts/process_ppu.py
python Scripts/newcompositescore.py
```

## Configuration

### Environment Variables (`.env`)
```env
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-bucket-name
VALD_API_ENDPOINT=https://api.vald.com/v1
VALD_API_KEY=your-api-key
LOG_LEVEL=INFO
```

### GCP Setup
1. Create a service account in Google Cloud Console
2. Download the JSON credentials file
3. Place it as `gcp_credentials.json` in the project root
4. Grant necessary permissions (BigQuery, Storage)

## Project Structure
```
VALDRefDataUpload/
├── Scripts/                    # Processing scripts
├── MyVenv/                    # Virtual environment
├── requirements.txt           # Dependencies
├── .env                      # Environment variables
├── gcp_credentials.json      # GCP authentication
└── VALDAPI.txt              # API configuration
```

## Troubleshooting

### Common Issues
1. **GCP Authentication Error**: Check `gcp_credentials.json` permissions
2. **API Connection Failed**: Verify `VALDAPI.txt` configuration
3. **Missing Dependencies**: Run `pip install -r requirements.txt`

### Logs
Check the console output for detailed error messages. For production, consider redirecting output to log files in your automation setup.

## Maintenance

- **Update dependencies**: `pip install --upgrade -r requirements.txt`
- **Check GCP quotas**: Monitor usage in Google Cloud Console
- **Review logs**: Check for any failed uploads or processing errors

---

**Note**: This is a personal automation tool. Keep your credentials secure and never commit sensitive files to version control.
