from playwright.async_api import async_playwright
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import re
from typing import List, Dict, Optional
import csv
import logging
import os

class VillaScraper:
    def __init__(self, interval_minutes: int = 0, interval_seconds: int = 1, output_dir: str = "data"):
        self.browser = None
        self.context = None
        self.page = None
        self.base_url = "https://www.pattayapartypoolvilla.com/v/"
        self.calendar_url = "https://www.pattayapartypoolvilla.com/cld.php"
        self.price_url = "https://www.pattayapartypoolvilla.com/nsp.php"
        self.interval = interval_minutes * 60 + interval_seconds
        self.should_stop = False
        self.playwright = None
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
    def stop(self):
        """Signal the scraper to stop"""
        self.should_stop = True
        self.update_status("Stopping scraper...", level="warning")
        # Force close browser and context
        asyncio.create_task(self.force_close())
        
    async def force_close(self):
        """Force close browser and context"""
        try:
            if self.context:
                await self.context.close()
                self.context = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None
            self.update_status("Browser closed successfully", level="success")
        except Exception as e:
            logging.error(f"Error during force close: {str(e)}")
            # Try one more time with a delay
            try:
                await asyncio.sleep(1)
                if self.context:
                    await self.context.close()
                if self.browser:
                    await self.browser.close()
                if self.playwright:
                    await self.playwright.stop()
            except Exception as e2:
                logging.error(f"Error during second force close attempt: {str(e2)}")
        
    def update_status(self, message: str, level: str = "info"):
        """Update current status and log it"""
        if level == "info":
            logging.info(message)
        elif level == "success":
            logging.info(f"SUCCESS: {message}")
        elif level == "error":
            logging.error(message)
        elif level == "warning":
            logging.warning(message)

    async def initialize(self):
        """Initialize Playwright browser and context"""
        try:
            self.update_status("Initializing browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=True)
            # Create context with default settings
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()
            # Set page-level timeout
            self.page.set_default_timeout(5000)  # 5 seconds
            # Set navigation timeout
            self.page.set_default_navigation_timeout(5000)  # 5 seconds
            self.update_status("Browser initialized successfully", level="success")
        except Exception as e:
            error_msg = f"Failed to initialize browser: {str(e)}"
            logging.error(error_msg)
            raise
        
    async def close(self):
        """Close browser and context"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logging.error(f"Error during close: {str(e)}")

    def extract_villa_id(self, d_id):
        """Extract numeric ID from D ID format (e.g., DV-2242 -> 2242)"""
        return d_id.split('-')[1] if '-' in d_id else d_id

    async def get_calendar_data(self, villa_id: str, year: int, month: int) -> Dict:
        """Get calendar data for a specific villa and month"""
        if self.should_stop:
            return None
            
        try:
            # Format month to two digits
            month_str = f"{month:02d}"
            
            # Extract numeric ID from D ID
            numeric_id = self.extract_villa_id(villa_id)
            
            # Construct URL
            url = f"{self.calendar_url}?ym={year}-{month_str}&hId={numeric_id}"
            self.update_status(f"Scraping calendar for villa {villa_id} - {year}-{month_str}")
            
            # Navigate to calendar page
            await self.page.goto(url)
            
            if self.should_stop:
                return None
                
            # Wait for calendar to load
            await self.page.wait_for_selector("table")
            
            if self.should_stop:
                return None
                
            # Extract calendar data
            calendar_data = {
                "villa_id": villa_id,
                "year": year,
                "month": month,
                "dates": []
            }
            
            # Get all date cells
            date_cells = await self.page.query_selector_all("td")
            
            for cell in date_cells:
                if self.should_stop:
                    return None
                    
                try:
                    # Get date number
                    date_text = await cell.evaluate("el => el.textContent.trim()")
                    style = (await cell.get_attribute("style")) or ""

                    # Only process if the cell contains a digit (the day number)
                    if not date_text.isdigit():
                        continue

                    # Detect special price (fire emoji)
                    if "pro.gif" in style.lower():
                        special_price = "special price"
                    else:
                        special_price = ""
                    
                    # Determine booking status based on background color
                    status = "available"  # default status
                    if style:
                        if "background-color: red" in style:
                            status = "booked"  # ติดจองแล้ว
                        elif "background-color: #orange" in style:
                            status = "maintenance"  # ปิดปรับปรุง-ซ่อม
                        elif "background-color: yellow" in style:
                            status = "holiday"  # วันหยุดยาว-นักขัตฤกษ์
                        elif "background-color: green" in style:
                            status = "pending"  # มีจองแล้ว ยังไม่โอน
                    
                    # Add date data
                    calendar_data["dates"].append({
                        "villa_id": villa_id,
                        "date": f"{year}-{month_str}-{date_text}",
                        "status": status,
                        "special_price": special_price
                    })
                    
                except Exception as e:
                    self.update_status(f"Error processing date cell: {str(e)}", level="warning")
                    continue
            
            self.update_status(f"Successfully scraped calendar data for villa {villa_id}", level="success")
            return calendar_data
            
        except Exception as e:
            error_msg = f"Error scraping calendar for villa {villa_id}: {str(e)}"
            logging.error(error_msg)
            self.update_status(error_msg, level="error")
            return None

    async def scrape_calendar_range(self, d_id: str, p_id: str, start_date: str, end_date: str, output_file: str):
        """Scrape calendar data for a range of dates"""
        try:
            # Convert string dates to datetime objects
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Calculate total days for progress tracking
            total_days = (end - start).days + 1
            current_day = 0
            
            # Create a dictionary to store all data
            all_data = {}
            
            # Read existing data if file exists
            if os.path.exists(output_file):
                try:
                    df = pd.read_csv(output_file)
                    for _, row in df.iterrows():
                        key = f"{row['villa_id']}_{row['date']}"
                        all_data[key] = row.to_dict()
                except Exception as e:
                    error_msg = f"Error reading existing data: {str(e)}"
                    self.update_status(error_msg, level="warning")
            
            # Iterate through each day
            current_date = start
            while current_date <= end:
                if self.should_stop:
                    break
                    
                try:
                    current_day += 1
                    date_str = current_date.strftime("%Y-%m-%d")
                    
                    # Update progress
                    progress = current_day / total_days
                    self.update_status(f"Processing {date_str} ({current_day}/{total_days})")
                    
                    # Get calendar data for current date
                    calendar_data = await self.get_calendar_data(
                        str(d_id),  # Use D ID for scraping
                        current_date.year,
                        current_date.month
                    )
                    
                    if calendar_data:
                        # Find the specific date in the calendar data
                        for date_data in calendar_data["dates"]:
                            if date_data["date"] == date_str:
                                # Get price data for this date
                                price_data = await self.get_price_for_date(str(d_id), date_str)  # Use D ID for scraping
                                
                                # Create unique key for this villa and date
                                key = f"{p_id}_{date_str}"  # Use P ID for storage
                                
                                # Combine calendar and price data
                                all_data[key] = {
                                    'villa_id': str(p_id),  # Use P ID for CSV output
                                    'date': date_str,
                                    'status': date_data["status"],
                                    'special_price': date_data.get("special_price", ""),
                                    'price': price_data["price"] if price_data else None,
                                    'capacity': price_data["capacity"] if price_data else None,
                                    'scraped_at': datetime.now().isoformat()
                                }
                                break
                    
                    # Move to next day
                    current_date += timedelta(days=1)
                except Exception as e:
                    error_msg = f"Error processing date {date_str}: {str(e)}"
                    self.update_status(error_msg, level="error")
                    continue
            
            if not self.should_stop:
                # Convert dictionary to list for CSV writing
                data_list = list(all_data.values())
                
                # Sort data by date and villa_id
                data_list.sort(key=lambda x: (x['date'], str(x['villa_id'])))
                
                # Write all data to CSV
                try:
                    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['villa_id', 'date', 'status', 'special_price', 'price', 'capacity', 'scraped_at']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(data_list)
                    self.update_status(f"Successfully saved data to {output_file}", level="success")
                except Exception as e:
                    error_msg = f"Error writing to CSV file: {str(e)}"
                    logging.error(error_msg)
                    raise
            
        except Exception as e:
            error_msg = f"Error in calendar scraping process for villa {d_id}: {str(e)}"
            logging.error(error_msg)
            raise

    async def get_price_for_date(self, villa_id: str, date: str) -> Dict:
        """Get price information for a specific villa and date"""
        try:
            # Extract numeric ID from D ID
            numeric_id = self.extract_villa_id(villa_id)
            
            url = f"{self.price_url}?bk_day={date}&bk_hid={numeric_id}"
            await self.page.goto(url)
            
            # Wait for the modal content to load
            await self.page.wait_for_selector(".modal-body")
            
            # Extract date
            date_element = await self.page.query_selector(".modal-title")
            date_text = await date_element.evaluate("el => el.textContent.trim()")
            
            # Extract price and capacity
            price_element = await self.page.query_selector(".modal-body p")
            price_text = await price_element.evaluate("el => el.textContent.trim()")
            
            # Parse price and capacity
            price_match = re.search(r'(\d+,?\d*)\s*/\s*(\d+)\s*ท่าน', price_text)
            if price_match:
                price = price_match.group(1).replace(',', '')
                capacity = price_match.group(2)
            else:
                price = None
                capacity = None
            
            return {
                "villa_id": villa_id,
                "date": date,
                "price": price,
                "capacity": capacity,
                "scraped_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            error_msg = f"Error getting price for villa {villa_id} on {date}: {str(e)}"
            logging.warning(error_msg)
            return None

    async def scrape_prices_for_period(self, d_id: str, p_id: str, start_date: str, end_date: str, output_file: str):
        """Scrape prices for a villa over a date range and save to CSV"""
        try:
            self.update_status(f"Starting price scraping for villa {d_id}")
            
            # Convert string dates to datetime objects
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Calculate total days for progress tracking
            total_days = (end - start).days + 1
            current_day = 0
            
            # Create a dictionary to store all data
            all_data = {}
            
            # Read existing data if file exists
            if os.path.exists(output_file):
                try:
                    df = pd.read_csv(output_file)
                    for _, row in df.iterrows():
                        key = f"{row['villa_id']}_{row['date']}"
                        all_data[key] = row.to_dict()
                except Exception as e:
                    error_msg = f"Error reading existing data: {str(e)}"
                    self.update_status(error_msg, level="warning")
            
            # Iterate through each day
            current_date = start
            while current_date <= end:
                if self.should_stop:
                    break
                    
                try:
                    current_day += 1
                    date_str = current_date.strftime("%Y-%m-%d")
                    
                    # Update progress
                    progress = current_day / total_days
                    self.update_status(f"Processing {date_str} ({current_day}/{total_days})")
                    
                    # Get calendar data for current date
                    calendar_data = await self.get_calendar_data(
                        str(d_id),  # Use D ID for scraping
                        current_date.year,
                        current_date.month
                    )
                    
                    # Get price for current date
                    price_data = await self.get_price_for_date(d_id, date_str)  # Use D ID for scraping
                    
                    # Default special_price
                    special_price = ""
                    # Find status and special_price from calendar data
                    status = "available"  # default status
                    if calendar_data:
                        for date_data in calendar_data["dates"]:
                            if date_data["date"] == date_str:
                                status = date_data["status"]
                                special_price = date_data.get("special_price", "")
                                break
                    
                    if price_data:
                        key = f"{p_id}_{date_str}"  # Use P ID for storage
                        # Add status and special_price to price data and use P ID
                        price_data["status"] = status
                        price_data["special_price"] = special_price
                        price_data["villa_id"] = str(p_id)  # Use P ID for CSV output
                        all_data[key] = price_data
                    
                    # Move to next day
                    current_date += timedelta(days=1)
                except Exception as e:
                    error_msg = f"Error processing date {date_str}: {str(e)}"
                    self.update_status(error_msg, level="error")
                    continue
            
            if not self.should_stop:
                # Convert dictionary to list for CSV writing
                data_list = list(all_data.values())
                
                # Sort data by date and villa_id
                data_list.sort(key=lambda x: (x['date'], str(x['villa_id'])))
                
                # Write all data to CSV
                try:
                    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                        fieldnames = ['villa_id', 'date', 'status', 'special_price', 'price', 'capacity', 'scraped_at']
                        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(data_list)
                    self.update_status(f"Successfully saved data to {output_file}", level="success")
                except Exception as e:
                    error_msg = f"Error writing to CSV file: {str(e)}"
                    logging.error(error_msg)
                    raise
            
        except Exception as e:
            error_msg = f"Error in price scraping process for villa {d_id}: {str(e)}"
            logging.error(error_msg)
            self.update_status(error_msg, level="error")
            raise

    async def start_scraping(self, villa_df: pd.DataFrame, scrape_type: str, start_date: str, end_date: str, output_filename: str = None):
        """Start the scraping process for all villas"""
        try:
            self.update_status("Starting scraping process...")
            await self.initialize()
            
            # Use provided output filename or create default
            if not output_filename:
                output_filename = "villa_data.csv"
            
            # Ensure output filename has .csv extension
            if not output_filename.endswith('.csv'):
                output_filename += '.csv'
            
            # Create full output path
            output_file = os.path.join(self.output_dir, output_filename)
            
            # Process all villas
            total_villas = len(villa_df)
            for i, (_, row) in enumerate(villa_df.iterrows(), 1):
                if self.should_stop:
                    self.update_status("Scraping stopped by user", level="warning")
                    break
                    
                try:
                    # Get both IDs
                    d_id = row['d_id']  # Use for scraping
                    p_id = row['p_id']  # Use for CSV output
                    villa_name = row['villa_name']
                    
                    self.update_status(f"Processing villa {i}/{total_villas}: {villa_name} (D ID: {d_id}, P ID: {p_id})")
                    
                    if scrape_type in ["both", "calendar"]:
                        await self.scrape_calendar_range(d_id, p_id, start_date, end_date, output_file)
                    
                    if scrape_type in ["both", "price"]:
                        await self.scrape_prices_for_period(d_id, p_id, start_date, end_date, output_file)
                    
                    if self.should_stop:
                        break
                        
                except Exception as e:
                    error_msg = f"Error processing villa {villa_name}: {str(e)}"
                    self.update_status(error_msg, level="error")
                    logging.error(error_msg)
                    continue
            
            if not self.should_stop:
                self.update_status("Scraping completed successfully", level="success")
            
        except Exception as e:
            error_msg = f"Error in scraping process: {str(e)}"
            logging.error(error_msg)
            self.update_status(error_msg, level="error")
        finally:
            # Only close if we're not in continuous mode
            if not hasattr(self, 'is_continuous') or not self.is_continuous:
                await self.close() 