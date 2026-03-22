import os
import time
import datetime
import warnings
import winsound
import pandas as pd
from filelock import FileLock
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

warnings.filterwarnings('ignore')

SAVE_DIR = r'C:\Users\vbgai\Downloads'
dt = datetime.datetime.now().date()
excel_name = os.path.join(SAVE_DIR, 'nifty_option_{}_{}.xlsx'.format(dt.day, dt.month))

TABLE_SELECTORS = [
    '#optionChainTable-indices',
    '#optionChainTable',
    'table[id*="optionChain"]',
    'table[class*="option"]',
]

IV_COLS    = ['CallimpliedVolatility', 'PutimpliedVolatility']
FLOAT_COLS = ['CalllastPrice', 'Callchange', 'Callbidprice', 'CallaskPrice',
              'PutlastPrice',  'Putchange',  'Putbidprice',  'PutaskPrice']
INT_COLS   = ['CallopenInterest', 'CallchangeinOpenInterest', 'CalltotalTradedVolume',
              'CallbidQty', 'CallaskQty', 'CallstrikePrice', 'PutstrikePrice',
              'PutbidQty',  'PutaskQty',  'PuttotalTradedVolume',
              'PutchangeinOpenInterest', 'PutopenInterest']

# Single JS call to extract all rows — much faster than Selenium element iteration
JS_EXTRACT = """
var selectors = arguments[0];
var table = null;
for (var i = 0; i < selectors.length; i++) {
    table = document.querySelector(selectors[i]);
    if (table) { console.log('Table found: ' + selectors[i]); break; }
}
if (!table) return null;
var rows = table.querySelectorAll('tbody tr');
var result = [];
rows.forEach(function(row) {
    var cells = row.querySelectorAll('td');
    if (cells.length < 22) return;
    var rowData = [];
    cells.forEach(function(cell) { rowData.push(cell.textContent.trim()); });
    result.push(rowData);
});
return result;
"""


def init_driver():
    options = uc.ChromeOptions()
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--window-position=-10000,0')   # off-screen, invisible to user
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    return uc.Chrome(options=options, version_main=145)


def apply_dtypes(df):
    """Apply correct dtypes: Int64 for integer cols, float for IV and float cols."""
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
    for col in IV_COLS + FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype(float)
    return df


def scrape_option_chain(driver):
    driver.get('https://www.nseindia.com/option-chain')
    wait = WebDriverWait(driver, 40)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    time.sleep(3)  # allow JS to render

    # Extract all rows in one JS call (avoids slow per-element Selenium calls)
    rows_data = driver.execute_script(JS_EXTRACT, TABLE_SELECTORS)

    if not rows_data:
        print(f"[DEBUG] Page title: {driver.title}")
        print(f"[DEBUG] Page source snippet:\n{driver.page_source[:800]}\n")
        raise ValueError("Option chain table not found on page")

    now = datetime.datetime.now()
    excel_date = now.date()                        # datetime.date object
    excel_time = now.replace(microsecond=0).time() # datetime.time object

    def clean(val):
        v = val.replace(',', '').strip()
        return '0' if v in ('-', '') else v

    # NSE table column indices (23 cols):
    # 0=empty | 1=C_OI | 2=C_ChngOI | 3=C_Vol | 4=C_IV | 5=C_LTP | 6=C_Chng
    # 7=C_BidQty | 8=C_Bid | 9=C_Ask | 10=C_AskQty | 11=Strike
    # 12=P_BidQty | 13=P_Bid | 14=P_Ask | 15=P_AskQty | 16=P_Chng | 17=P_LTP
    # 18=P_IV | 19=P_Vol | 20=P_ChngOI | 21=P_OI | 22=empty
    records = []
    for t in rows_data:
        records.append({
            'Date':                      excel_date,
            'Time':                      excel_time,
            # ── CALL columns (analysis_nifty.py naming) ──────────────────────
            'CallopenInterest':          clean(t[1]),
            'CallchangeinOpenInterest':  clean(t[2]),
            'CalltotalTradedVolume':     clean(t[3]),
            'CallimpliedVolatility':     clean(t[4]),
            'CalllastPrice':             clean(t[5]),
            'Callchange':                clean(t[6]),
            'CallbidQty':                clean(t[7]),
            'Callbidprice':              clean(t[8]),
            'CallaskPrice':              clean(t[9]),
            'CallaskQty':                clean(t[10]),
            # ── Strike (shared, duplicated for Call and Put) ──────────────────
            'CallstrikePrice':           clean(t[11]),
            'PutstrikePrice':            clean(t[11]),
            # ── PUT columns (analysis_nifty.py naming) ───────────────────────
            'PutbidQty':                 clean(t[12]),
            'Putbidprice':               clean(t[13]),
            'PutaskPrice':               clean(t[14]),
            'PutaskQty':                 clean(t[15]),
            'Putchange':                 clean(t[16]),
            'PutlastPrice':              clean(t[17]),
            'PutimpliedVolatility':      clean(t[18]),
            'PuttotalTradedVolume':      clean(t[19]),
            'PutchangeinOpenInterest':   clean(t[20]),
            'PutopenInterest':           clean(t[21]),
            # ── Columns not in Selenium table → set to 0 ─────────────────────
            'CallpchangeinOpenInterest': 0,
            'PutpchangeinOpenInterest':  0,
            'CallpChange':               0,
            'PutpChange':                0,
            'CalltotalBuyQuantity':      0,
            'PuttotalBuyQuantity':       0,
            'CalltotalSellQuantity':     0,
            'PuttotalSellQuantity':      0,
            'CallunderlyingValue':       0,
            'PutunderlyingValue':        0,
        })

    if not records:
        raise ValueError("Rows found but no data extracted")

    df = pd.DataFrame(records)
    df = apply_dtypes(df)

    # Try to get NSE timestamp from page
    refresh_time = excel_time.strftime("%H:%M:%S")
    try:
        ts_el = driver.find_element(By.XPATH,
            "//*[contains(@id,'time') or contains(@class,'time-stamp') or contains(@class,'timestamp')]")
        txt = ts_el.text.strip()
        if txt:
            refresh_time = txt
    except Exception:
        pass

    print(f"[INFO] Scraped {len(df)} rows at {refresh_time}")
    return df, refresh_time


def restore_date_time(df):
    """Restore Date and Time columns to proper Python date/time objects after reading from Excel."""
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
    if 'Time' in df.columns:
        # Excel stores time as datetime or timedelta; extract time part
        def to_time(v):
            if isinstance(v, datetime.time):
                return v
            if isinstance(v, datetime.datetime):
                return v.time()
            try:
                return pd.to_datetime(str(v)).time()
            except Exception:
                return v
        df['Time'] = df['Time'].apply(to_time)
    return df


def update_excel_file(excel_name, excel_df):
    lock = FileLock(f"{excel_name}.lock")
    with lock:
        if os.path.isfile(excel_name):
            book = pd.read_excel(excel_name, engine='openpyxl')
            book = restore_date_time(book)
            book = apply_dtypes(book)
            updated = pd.concat([book, excel_df], ignore_index=True)
        else:
            updated = excel_df
        updated.to_excel(excel_name, index=False, engine='openpyxl')
        return True


def beep_sound(num_beeps=3, frequency=7040, duration=300, pause=0.1):
    for _ in range(num_beeps):
        winsound.Beep(frequency, duration)
        time.sleep(pause)


def run(excel_name, max_retries=5, max_runtime_minutes=75):
    driver = init_driver()
    last_time = None
    retry_count = 0
    start_time = time.time()

    try:
        while retry_count < max_retries:
            now = datetime.datetime.now()

            if now.hour == 15 and now.minute >= 30:
                print("Past 3:30 PM. Exiting.")
                beep_sound()
                break

            if (time.time() - start_time) / 60 > max_runtime_minutes:
                print("Max runtime exceeded. Exiting.")
                break

            try:
                excel_df, refresh_time = scrape_option_chain(driver)

                if last_time != refresh_time:
                    update_flag = update_excel_file(excel_name, excel_df)
                    print(f"[{now.strftime('%H:%M:%S')}] Saved {len(excel_df)} rows | NSE time: {refresh_time}")
                    if update_flag:
                        time.sleep(30)

                last_time = refresh_time
                retry_count = 0

            except Exception as e:
                retry_count += 1
                print(f"Error: {e} | Retry {retry_count}/{max_retries} — reloading...")
                time.sleep(10)

    finally:
        driver.quit()
        print("Done.")


if __name__ == "__main__":
    run(excel_name)
