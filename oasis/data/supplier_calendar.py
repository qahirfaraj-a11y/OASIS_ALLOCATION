import pandas as pd
import re
from typing import Dict, Set, Union, List
from datetime import datetime

class SupplierCalendar:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.schedule: Dict[str, Union[Set[int], str]] = {}
        self.loaded = False

    def load(self):
        """Loads and parses the Excel calendar."""
        if self.loaded:
            return

        print(f"Loading Supplier Calendar from: {self.filepath}")
        try:
            # 1. Parse Daily Suppliers (Fresh)
            self._parse_daily_sheet()
            
            # 2. Parse Master Schedule (Dry)
            self._parse_master_schedule()
            
            self.loaded = True
            print(f"Calendar loaded. Mapped {len(self.schedule)} suppliers.")
            
        except Exception as e:
            print(f"ERROR loading calendar: {e}")

    def _normalize_name(self, raw_name: str) -> str:
        """
        Normalizes supplier name for matching.
        Removes ID prefix (e.g., 'Sa0024 - ') and standardizes text.
        """
        if not isinstance(raw_name, str):
            return ""
        
        # Remove ID prefix pattern (e.g., "Sa0024 - " or "Sa0024 ")
        name = re.sub(r'^[A-Za-z0-9]{6}\s*-\s*', '', raw_name)
        name = re.sub(r'^[A-Za-z0-9]{6}\s+', '', name)
        
        # Remove common business suffixes for fuzzy matching
        name = name.lower().replace('.', '').replace(',', '')
        name = name.replace(' limited', '').replace(' ltd', '')
        name = name.replace(' kenya', '').replace(' east africa', '')
        
        return name.strip()

    def _parse_daily_sheet(self):
        """Parses 'Daily Suppliers & Info' sheet."""
        try:
            df = pd.read_excel(self.filepath, sheet_name="Daily Suppliers & Info")
            # Find the column with names. It often has 'Supplier Name' in header or first row.
            target_col = None
            for col in df.columns:
                if df[col].astype(str).str.contains("Brookside", case=False).any():
                    target_col = col
                    break
            
            if not target_col:
                # Fallback to first column
                target_col = df.columns[0]

            count = 0
            for raw_name in df[target_col].dropna():
                raw_name = str(raw_name)
                if "Supplier Name" in raw_name:
                    continue
                    
                norm_name = self._normalize_name(raw_name)
                if norm_name:
                    self.schedule[norm_name] = 'DAILY'
                    count += 1
            
            print(f"  - Found {count} Daily Suppliers.")

        except Exception as e:
            print(f"  - Warning: Could not parse Daily sheet: {e}")

    def _parse_master_schedule(self):
        """Parses '2026 Order Schedule' sheet."""
        try:
            # Load sheet (assume it's the first one if name mismatch, but we know the name)
            df = pd.read_excel(self.filepath, sheet_name=0) 
            
            # Identify columns
            # We look for a column containing dates and a column containing long supplier strings
            date_col = None
            supp_col = None
            
            for col in df.columns:
                # Check for date-like values
                first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(first_valid, datetime) or (isinstance(first_valid, str) and '2026' in first_valid):
                    date_col = col
                
                # Check for long strings (Suppliers)
                if df[col].dtype == 'object' and df[col].str.len().mean() > 20:
                    supp_col = col
            
            if not date_col or not supp_col:
                print("  - Warning: Could not identify Date or Supplier columns in Master Schedule.")
                return

            count = 0
            # Iterate rows
            for idx, row in df.iterrows():
                date_val = row[date_col]
                suppliers_str = row[supp_col]
                
                if pd.isna(date_val) or pd.isna(suppliers_str):
                    continue
                
                # Parse Date
                try:
                    current_date = pd.to_datetime(date_val)
                    day_of_year = current_date.dayofyear
                except:
                    continue # Skip invalid dates
                
                # Parse Suppliers
                # Format: "ID - Name, ID - Name"
                suppliers = [s.strip() for s in str(suppliers_str).split(',')]
                
                for s in suppliers:
                    norm = self._normalize_name(s)
                    if norm:
                        if norm not in self.schedule:
                            self.schedule[norm] = set()
                        
                        # Add day to set (unless it's already marked DAILY from previous step)
                        if self.schedule[norm] != 'DAILY':
                            self.schedule[norm].add(day_of_year)
                            count += 1
            
            print(f"  - Parsed Master Schedule entries.")

        except Exception as e:
            print(f"  - Warning: Could not parse Master Schedule: {e}")

    def get_schedule(self, supplier_name: str) -> Union[str, Set[int], None]:
        """Returns 'DAILY', Set[DayOfYear], or None."""
        norm_query = self._normalize_name(supplier_name)
        
        # Exact Normalized Match
        if norm_query in self.schedule:
            return self.schedule[norm_query]
        
        # Fuzzy Match (Contains)
        # Check keys that contain the query or vice-versa
        for key, val in self.schedule.items():
            if norm_query in key or key in norm_query:
                return val
        
        return None
