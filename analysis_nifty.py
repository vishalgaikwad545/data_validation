import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import datetime
import os
from glob import glob
import time
import warnings
warnings.filterwarnings("ignore")

# Set up page layout
st.set_page_config(layout="wide")

# Add custom CSS to reduce spacing
st.markdown("""
<style>
    /* Adjust sidebar width */
    [data-testid="stSidebar"] {
        min-width: 320px;
        max-width: 350px;
    }
    
    /* Reduce padding in sidebar */
    [data-testid="stSidebar"] > div:first-child {
        padding-top: 1rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    
    /* Reduce spacing between sidebar elements */
    .stSelectbox, .stNumberInput, .stMultiSelect, .stDateInput, .stCheckbox {
        margin-bottom: 0.5rem !important;
    }
    
    /* Reduce chart margins */
    .js-plotly-plot {
        margin-bottom: 0.5rem !important;
    }
    
    /* Reduce spacing between charts */
    [data-testid="stVerticalBlock"] > div {
        gap: 0.5rem;
    }
    
    /* Compact sidebar labels */
    .stSelectbox label, .stNumberInput label, .stMultiSelect label {
        font-size: 13px !important;
        margin-bottom: 0.2rem !important;
    }
    
    /* Reduce main content padding */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 100%;
    }
    
    /* Make plot selectbox labels inline */
    .plot-inline {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 0.3rem;
    }
    
    .plot-inline label {
        min-width: 50px;
        font-size: 13px !important;
        font-weight: 600;
        margin: 0 !important;
    }
    
    .plot-inline .stSelectbox {
        flex: 1;
        margin: 0 !important;
    }
    
    .plot-inline .stSelectbox > div {
        margin: 0 !important;
    }
    
    .plot-inline .stSelectbox label {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

# File path configuration
file_path = "C:\\Users\\vbgai\\Downloads\\"

# Function to load the Excel file (with caching)
@st.cache_data
def load_excel(file_path, retries=3, delay=5):
    for attempt in range(retries):
        try:
            # Try reading the Excel file
            return pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Unable to load the file.")
                raise

# Function to get the list of available Excel files (with caching)
@st.cache_data
def get_file_list():
    return glob(f"{file_path}nifty_option_*.xlsx")

# Function to check if file was modified in the last N seconds
def check_file_modified(file_path, seconds=20):
    try:
        if os.path.exists(file_path):
            file_mod_time = os.path.getmtime(file_path)
            current_time = time.time()
            time_diff = current_time - file_mod_time
            return time_diff <= seconds
        return False
    except Exception as e:
        print(f"Error checking file modification: {e}")
        return False

# Create a reusable function for calculating derived fields
def process_dataframe(df, window=3):
    # Calculate all the derived fields
    df['PuttotalBuyQuantity_diff'] = df.groupby(['Time', 'PutstrikePrice'])['PuttotalBuyQuantity'].diff()
    df['CalltotalBuyQuantity_diff'] = df.groupby(['Time', 'CallstrikePrice'])['CalltotalBuyQuantity'].diff()
    df['Callprice'] = df['CallaskQty'] * df['CallaskPrice']
    df['Putprice'] = df['PutaskQty'] * df['PutaskPrice']
    df['Change_put_call'] = df['PutpchangeinOpenInterest'] - df['CallpchangeinOpenInterest']
    df['actual_change'] = df['PutopenInterest'] - df['CallopenInterest']
    df['call_quantity_change'] = df['CalltotalSellQuantity'] - df['CalltotalBuyQuantity'] 
    df['put_quantity_change'] = df['PuttotalSellQuantity'] - df['PuttotalBuyQuantity']
    df['put_diff_quantity'] = df['put_quantity_change'] * df['PutimpliedVolatility'] 
    df['call_diff_quantity'] = df['call_quantity_change'] * df['CallimpliedVolatility']
    df['act_CallunderlyingValue'] = df['CallunderlyingValue'] - df['CallstrikePrice']
    df['act_PutunderlyingValue'] = df['PutunderlyingValue'] - df['PutstrikePrice']
    df['put_call_ratio'] = df['PutopenInterest'] / df['CallopenInterest']
    df['t_put_call_ratio'] = df['PutchangeinOpenInterest'] / df['CallchangeinOpenInterest']
    df['iv_sell_put'] = df['PuttotalSellQuantity'] * (1 + df['PutimpliedVolatility']/100)
    df['iv_sell_call'] = df['CalltotalSellQuantity'] * (1+ df['CallimpliedVolatility']/100)
    
    # Rolling calculations
    df['put_quantity_change_avg_increase'] = df['put_quantity_change'].rolling(window=window).apply(lambda x: round((x.iloc[-1] - x.iloc[0]) / window))
    df['call_quantity_change_avg_increase'] = df['call_quantity_change'].rolling(window=window).apply(lambda x: round((x.iloc[-1] - x.iloc[0]) / window))
    df['qty_avg_change_zero_line'] = df['call_quantity_change_avg_increase'] - df['put_quantity_change_avg_increase']
    
    return df

# Function to create a plotly figure
def create_figure(filtered_df, x_column, y_call_column, y_put_column, option_title, strike_price):
    fig = go.Figure()

    # Add traces for selected Call and Put columns
    fig.add_trace(go.Scatter(
        x=filtered_df[x_column], y=filtered_df[y_call_column],
        mode='lines', name=f'{y_call_column}'[:10], line=dict(color='green')
    ))
    fig.add_trace(go.Scatter(
        x=filtered_df[x_column], y=filtered_df[y_put_column],
        mode='lines', name=f'{y_put_column}'[:10], line=dict(color='red')
    ))

    # Update layout
    fig.update_layout(
        xaxis={
            'title': 'Time',
            'rangeslider': {'visible': True},
            'type': 'category',
            'tickangle': -45,
        },
        yaxis={
            'title': f'{option_title} for Call and Put',
            'fixedrange': False,
        },
        title=f'{option_title} for Strike Price {strike_price}',
        height=400,
    )

    # Set dynamic y-axis scaling
    fig.update_yaxes(autorange=True)

    return fig

# Dictionary mapping combined options for both Call and Put
options = {
    'Quantity_change':['put_quantity_change','call_quantity_change'],
    'Change in Open Interest': ['PutchangeinOpenInterest', 'CallchangeinOpenInterest'],
    'Open Interest': ['PutopenInterest', 'CallopenInterest'],
    'Total Buy Quantity': ['CalltotalBuyQuantity', 'PuttotalBuyQuantity'],
    'Total Sell Quantity': ['PuttotalSellQuantity', 'CalltotalSellQuantity'],
    'Last Price': ['CalllastPrice', 'PutlastPrice'],
    'Implied Volatility': ['CallimpliedVolatility', 'PutimpliedVolatility'],
    'Ask Quantity': ['CallaskQty', 'PutaskQty'],
    'Put Call Ratio':['put_call_ratio','put_call_ratio'],
    'Net Quantiry Diff':['put_diff_quantity','call_diff_quantity'],
    'Net Buy Quantity': ['CalltotalBuyQuantity_diff','PuttotalBuyQuantity_diff'],
    'Today Put Call Ratio':['t_put_call_ratio','t_put_call_ratio'],
    'Percentage Change in Open Interest': ['PutpchangeinOpenInterest', 'CallpchangeinOpenInterest'],
    'Percent_change':['Change_put_call','Change_put_call'],
    'actual_change':['actual_change','actual_change'],
    'Total Traded Volume': ['CalltotalTradedVolume', 'PuttotalTradedVolume'],
    'IV Sell quantity':['iv_sell_put','iv_sell_call'],
    'Change': ['Callchange', 'Putchange'],
    'Percentage Change': ['CallpChange', 'PutpChange'],
    'Bid Quantity': ['CallbidQty', 'PutbidQty'],
    'Bid Price': ['Callbidprice', 'Putbidprice'],
    'Ask Price': ['CallaskPrice', 'PutaskPrice'],
    'Underlying Value': ['act_CallunderlyingValue', 'act_PutunderlyingValue'],
    "Underlying Total Value":["Callprice","Putprice"],
    'avg_quantity_change':['put_quantity_change_avg_increase','call_quantity_change_avg_increase'],
    'qty_avg_change_zero_line':['qty_avg_change_zero_line','qty_avg_change_zero_line']
}

# Initialize session state for tracking file modification time
if 'last_mod_time' not in st.session_state:
    st.session_state.last_mod_time = None

# Initialize session state for plot selections
if 'plot_selections' not in st.session_state:
    st.session_state.plot_selections = {
        'plot1': 'Implied Volatility',
        'plot2': 'IV Sell quantity', 
        'plot3': 'Total Sell Quantity',
        'plot4': 'Quantity_change',
        'plot5': 'Change in Open Interest'
    }

# Sidebar date input
selected_date = st.sidebar.date_input("Select Date", datetime.date.today())

# Auto-reload toggle
auto_reload = st.sidebar.checkbox("Enable Auto-Reload (20s)", value=True)

# Reload button on the sidebar
if st.sidebar.button("Reload Excel"):
    # Clear the cache for all cached functions
    st.cache_data.clear()
    st.rerun()

# Construct the file path based on the selected date
file_pattern = f"{file_path}nifty_option_{selected_date.day}_{selected_date.month}.xlsx"
file_list = get_file_list()

# Check if file exists, otherwise get the latest one
if not os.path.exists(file_pattern):
    file_list.sort(reverse=True, key=os.path.getmtime)
    if file_list:
        file_path_final = file_list[0]
    else:
        st.error("No Excel files found.")
        st.stop()
else:
    file_path_final = file_pattern

# Check for file modification and auto-reload
if auto_reload:
    current_mod_time = os.path.getmtime(file_path_final)
    
    # Initialize last_mod_time on first run
    if st.session_state.last_mod_time is None:
        st.session_state.last_mod_time = current_mod_time
    
    # Check if file was modified
    if current_mod_time > st.session_state.last_mod_time:
        # Check if modification was in last 20 seconds
        if check_file_modified(file_path_final, 20):
            st.sidebar.success("File updated! Reloading...")
            st.cache_data.clear()
            st.session_state.last_mod_time = current_mod_time
            time.sleep(0.5)  # Small delay to ensure file write is complete
            st.rerun()
    
    # Display last update time
    last_update = datetime.datetime.fromtimestamp(current_mod_time)
    st.sidebar.info(f"Last updated: {last_update.strftime('%H:%M:%S')}")

# Load Excel file
df_full = load_excel(file_path_final)

# Optimize by getting unique values count before limiting the dataframe
unique_values = df_full['PutstrikePrice'].nunique()

# Sidebar inputs
value_range = st.sidebar.number_input("Values to Display", value=60, step=10)
callstrike_input = st.sidebar.number_input("Call Strike Price", value=25500, step=50)

# Limit data first before processing - big performance improvement
df = df_full.tail(value_range * unique_values)

# Process the dataframe (apply all calculations)
df = process_dataframe(df)

# Filter data based on the CallstrikePrice input - do filtering after processing
filtered_df = df[df['CallstrikePrice'] == callstrike_input]

# Sidebar selects for options with inline labels

# Plot 1
cols = st.sidebar.columns([1, 4])
cols[0].markdown("**P1:**")
with cols[1]:
    selected_option = st.selectbox("p1", list(options.keys()), 
                                  index=list(options.keys()).index(st.session_state.plot_selections['plot1']), 
                                  label_visibility="collapsed", key="plot1")
    st.session_state.plot_selections['plot1'] = selected_option

# Plot 2
cols = st.sidebar.columns([1, 4])
cols[0].markdown("**P2:**")
with cols[1]:
    selected_sec_option = st.selectbox("p2", list(options.keys()), 
                                      index=list(options.keys()).index(st.session_state.plot_selections['plot2']), 
                                      label_visibility="collapsed", key="plot2")
    st.session_state.plot_selections['plot2'] = selected_sec_option

# Plot 3
cols = st.sidebar.columns([1, 4])
cols[0].markdown("**P3:**")
with cols[1]:
    selected_thd_option = st.selectbox("p3", list(options.keys()), 
                                      index=list(options.keys()).index(st.session_state.plot_selections['plot3']), 
                                      label_visibility="collapsed", key="plot3")
    st.session_state.plot_selections['plot3'] = selected_thd_option

# Plot 4
cols = st.sidebar.columns([1, 4])
cols[0].markdown("**P4:**")
with cols[1]:
    selected_4th_option = st.selectbox("p4", list(options.keys()), 
                                      index=list(options.keys()).index(st.session_state.plot_selections['plot4']), 
                                      label_visibility="collapsed", key="plot4")
    st.session_state.plot_selections['plot4'] = selected_4th_option

# Plot 5
cols = st.sidebar.columns([1, 4])
cols[0].markdown("**P5:**")
with cols[1]:
    selected_5th_option = st.selectbox("p5", list(options.keys()), 
                                      index=list(options.keys()).index(st.session_state.plot_selections['plot5']), 
                                      label_visibility="collapsed", key="plot5")
    st.session_state.plot_selections['plot5'] = selected_5th_option

# Multi-select for signal generation - Now defaults to P1-P5 selections
st.sidebar.markdown("### 📊 Signal Generation")

# Get current plot selections as default
default_signal_options = [
    selected_option,
    selected_sec_option,
    selected_thd_option,
    selected_4th_option,
    selected_5th_option
]

signal_options = st.sidebar.multiselect(
    "Options for Signal",
    list(options.keys()),
    default=default_signal_options
)

# Parameterized threshold for Implied Volatility
iv_threshold = st.sidebar.number_input(
    "IV Threshold (%)",
    min_value=0.0,
    max_value=100.0,
    value=5.0,
    step=0.5,
    help="Percentage difference required for Implied Volatility signal"
)

# Parameterized lookback period for strength calculation
lookback_period = st.sidebar.number_input(
    "Lookback Period",
    min_value=1,
    max_value=50,
    value=5,
    step=1,
    help="Number of previous data points to compare for trend strength"
)

# Function to calculate trend strength
def calculate_strength(filtered_df, selected_signal_options, lookback_period=5):
    """
    Calculate the trend strength by comparing current values with previous values
    Returns: strength_text and strength_details
    """
    if not selected_signal_options or filtered_df.empty or len(filtered_df) < lookback_period + 1:
        return "INSUFFICIENT DATA", []
    
    strength_scores = []
    strength_details = []
    
    for option_name in selected_signal_options:
        call_col, put_col = options[option_name]
        
        # Get current and previous values
        current_call = filtered_df[call_col].iloc[-1]
        current_put = filtered_df[put_col].iloc[-1]
        previous_call = filtered_df[call_col].iloc[-lookback_period-1]
        previous_put = filtered_df[put_col].iloc[-lookback_period-1]
        
        # Calculate current difference
        current_diff = current_call - current_put
        previous_diff = previous_call - previous_put
        
        # Determine strength based on trend
        if current_diff > previous_diff:
            if current_diff > 0:
                strength = "INCREASING"
                score = 1
            else:
                strength = "WEAKENING SELL"
                score = 0
        elif current_diff < previous_diff:
            if current_diff < 0:
                strength = "INCREASING"
                score = 1
            else:
                strength = "WEAKENING BUY"
                score = 0
        else:
            strength = "NEUTRAL"
            score = 0
        
        strength_scores.append(score)
        strength_details.append({
            'option': option_name,
            'strength': strength,
            'current_diff': current_diff,
            'previous_diff': previous_diff,
            'change': current_diff - previous_diff
        })
    
    # Overall strength
    if all(s == 1 for s in strength_scores):
        overall_strength = "INCREASING"
    elif all(s == 0 for s in strength_scores) and any("WEAKENING" in d['strength'] for d in strength_details):
        overall_strength = "DECREASING"
    else:
        overall_strength = "NEUTRAL"
    
    return overall_strength, strength_details

# Function to check signal
def check_signal(filtered_df, selected_signal_options, iv_threshold=5.0):
    """
    Check if all green (Call) lines are above red (Put) lines for BUY signal
    or all red (Put) lines are above green (Call) lines for SELL signal
    Returns individual signals for each option and overall signal
    """
    if not selected_signal_options or filtered_df.empty:
        return None, []
    
    analysis_details = []
    
    for option_name in selected_signal_options:
        call_col, put_col = options[option_name]
        
        # Get the latest values
        latest_call = filtered_df[call_col].iloc[-1]
        latest_put = filtered_df[put_col].iloc[-1]
        
        # Calculate percentage change based on the nature of the metric
        if latest_call == latest_put:
            percent_diff = 0
        elif latest_put != 0:
            percent_diff = ((latest_call - latest_put) / abs(latest_put)) * 100
        else:
            percent_diff = 100 if latest_call > 0 else -100 if latest_call < 0 else 0
        
        # Determine individual signal for this option
        if option_name == 'Implied Volatility':
            # Special handling for IV with threshold
            if percent_diff > iv_threshold:
                individual_signal = "BUY"
            elif percent_diff < -iv_threshold:
                individual_signal = "SELL"
            else:
                individual_signal = "NEUTRAL"
        else:
            # For other options, use simple comparison
            if latest_call > latest_put:
                individual_signal = "BUY"
            elif latest_put > latest_call:
                individual_signal = "SELL"
            else:
                individual_signal = "NEUTRAL"
        
        analysis_details.append({
            'option': option_name,
            'call_value': latest_call,
            'put_value': latest_put,
            'percent_diff': percent_diff,
            'signal': individual_signal
        })
    
    # Count signals for overall determination
    buy_count = sum(1 for d in analysis_details if d['signal'] == "BUY")
    sell_count = sum(1 for d in analysis_details if d['signal'] == "SELL")
    total_count = len(analysis_details)
    
    # Determine overall signal based on majority
    if buy_count > total_count / 2:
        overall_signal = "BUY"
    elif sell_count > total_count / 2:
        overall_signal = "SELL"
    else:
        overall_signal = "NEUTRAL"
    
    return overall_signal, analysis_details

# Add CSS for fixed bottom signal
st.markdown("""
<style>
.fixed-signal {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    z-index: 9999;
    padding: 15px;
    text-align: center;
    font-weight: bold;
    font-size: 18px;
    box-shadow: 0 -2px 10px rgba(0,0,0,0.1);
}
.signal-buy {
    background-color: #D1FAE5;
    color: #065F46;
    border-top: 3px solid #10B981;
}
.signal-sell {
    background-color: #FEE2E2;
    color: #991B1B;
    border-top: 3px solid #EF4444;
}
.signal-neutral {
    background-color: #FEF3C7;
    color: #92400E;
    border-top: 3px solid #F59E0B;
}
.signal-details {
    font-size: 14px;
    margin-top: 8px;
    font-weight: normal;
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
    gap: 15px;
}
.signal-item {
    display: inline-flex;
    align-items: center;
    gap: 5px;
}
.signal-icon-buy {
    color: #10B981;
}
.signal-icon-sell {
    color: #EF4444;
}
.signal-icon-neutral {
    color: #F59E0B;
}
</style>
""", unsafe_allow_html=True)

# Get the corresponding Call and Put columns
selected_call_column, selected_put_column = options[selected_option]
selected_sec_call_column, selected_sec_put_column = options[selected_sec_option]
selected_thd_call_column, selected_thd_put_column = options[selected_thd_option]
selected_4th_call_column, selected_4th_put_column = options[selected_4th_option]
selected_5th_call_column, selected_5th_put_column = options[selected_5th_option]

if filtered_df.empty:
    st.write("No data available for the selected CallstrikePrice.")
else:
    # Generate signal based on selected options
    signal, analysis_details = check_signal(filtered_df, signal_options, iv_threshold)
    
    # Calculate trend strength
    strength, strength_details = calculate_strength(filtered_df, signal_options, lookback_period)
    
    # Plot 1
    fig = create_figure(
        filtered_df, 'Time', selected_call_column, selected_put_column,
        selected_option, callstrike_input
    )
    st.plotly_chart(fig, use_container_width=True)

    # Plot 2
    fig1 = create_figure(
        filtered_df, 'Time', selected_sec_call_column, selected_sec_put_column,
        selected_sec_option, callstrike_input
    )
    st.plotly_chart(fig1, use_container_width=True)

    # Plot 3
    fig3 = create_figure(
        filtered_df, 'Time', selected_thd_call_column, selected_thd_put_column,
        selected_thd_option, callstrike_input
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Plot 4
    fig4 = create_figure(
        filtered_df, 'Time', selected_4th_call_column, selected_4th_put_column,
        selected_4th_option, callstrike_input
    )
    st.plotly_chart(fig4, use_container_width=True)

    # Plot 5
    fig5 = create_figure(
        filtered_df, 'Time', selected_5th_call_column, selected_5th_put_column,
        selected_5th_option, callstrike_input
    )
    st.plotly_chart(fig5, use_container_width=True)
    
    st.write(f"Loaded file: {os.path.basename(file_path_final)}")
    
    # Display fixed signal at bottom of page
    if signal and signal_options:
        signal_class = f"signal-{signal.lower()}"
        signal_icon = "🟢" if signal == "BUY" else "🔴" if signal == "SELL" else "⚪"
        
        # Strength icon
        strength_icon = "📈" if strength == "INCREASING" else "📉" if strength == "DECREASING" else "➡️"
        
        # Create individual signal indicators with icons
        signal_items = []
        for d in analysis_details:
            # Determine icon for individual signal
            if d['signal'] == "BUY":
                icon = "✅"
            elif d['signal'] == "SELL":
                icon = "❌"
            else:
                icon = "➖"
            
            # Format the percentage with proper sign
            pct_str = f"{d['percent_diff']:+.1f}%"
            
            # Create clean formatted item
            signal_items.append(f"{d['option'][:20]}: {pct_str} {icon}")
        
        # Join all items with separator
        details_text = " | ".join(signal_items)
        
        st.markdown(f"""
        <div class="fixed-signal {signal_class}">
            <div style="font-size: 20px; margin-bottom: 5px;">
                {signal_icon} <strong>{signal} SIGNAL</strong> &nbsp;&nbsp;&nbsp; 
                {strength_icon} <strong>STRENGTH: {strength}</strong>
            </div>
            <div style="font-size: 14px; font-weight: normal;">
                {details_text}
            </div>
        </div>
        """, unsafe_allow_html=True)

# Auto-refresh mechanism - rerun every 5 seconds if auto-reload is enabled
if auto_reload:
    time.sleep(5)
    st.rerun()