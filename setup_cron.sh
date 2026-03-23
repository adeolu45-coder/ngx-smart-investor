#!/bin/bash
# ============================================
# NGX Daily Ingestion - Cron Setup Script
# ============================================
# Run this script once to set up the daily cron job
# The cron job will run at 5:00 PM WAT (4:00 PM UTC) every day

# Add cron job
CRON_CMD="0 16 * * * cd /app/backend && /usr/bin/python3 daily_ingestion_cron.py >> /var/log/ngx_ingestion.log 2>&1"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "daily_ingestion_cron.py"; then
    echo "Cron job already exists"
else
    # Add new cron job
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    echo "Cron job added successfully"
fi

# Display current crontab
echo ""
echo "Current crontab:"
crontab -l

# Create log file with proper permissions
touch /var/log/ngx_ingestion.log
chmod 666 /var/log/ngx_ingestion.log

echo ""
echo "Setup complete!"
echo "NGX data ingestion will run daily at 5:00 PM WAT (4:00 PM UTC)"
echo "Log file: /var/log/ngx_ingestion.log"
