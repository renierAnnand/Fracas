#!/usr/bin/env python3
"""
FRACAS - Advanced Military Vehicle Maintenance System
With Anomaly Detection and Recurrence Analysis

Features:
- Automatic column detection and mapping
- Failure reporting and analysis
- Root Cause Analysis tools (5-Whys, Ishikawa)
- CAPA tracking with effectiveness monitoring
- Reliability KPIs (MTBF, MTTR, Availability)
- Advanced anomaly detection
- Cost and performance analysis
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
from datetime import datetime, timedelta
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="FRACAS - Advanced Military Vehicle Maintenance",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
<style>
    .anomaly-high { background-color: #ffcccc !important; }
    .anomaly-medium { background-color: #ffe5cc !important; }
    .anomaly-low { background-color: #ffffcc !important; }
    .metric-card { 
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
</style>
""", unsafe_allow_html=True)

# ==================== SESSION STATE INITIALIZATION ====================

def init_session_state():
    """Initialize all session state variables"""
    defaults = {
        'df': None,
        'df_cleaned': None,
        'column_mapping': {},
        'new_failures': pd.DataFrame(),
        'rca_data': pd.DataFrame(columns=['WorkOrderID', 'Type', 'Content', 'Timestamp', 'User']),
        'capa_register': pd.DataFrame(columns=[
            'ActionID', 'WorkOrderID', 'Description', 'Owner', 'DueDate', 
            'Status', 'EffectivenessCheck', 'ClosureDate'
        ]),
        'anomalies': pd.DataFrame(),
        'user_role': 'Engineer',
        'anomaly_thresholds': {
            'repeat_days': 30,
            'quick_return_days': 10,
            'cost_multiplier': 2.0,
            'downtime_multiplier': 2.0,
            'frequency_std': 2.0
        }
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# ==================== DATA LOADING AND CLEANING ====================

@st.cache_data
def load_data(file_path=None, uploaded_file=None):
    """Load Excel file with automatic path detection"""
    try:
        if uploaded_file is not None:
            df = pd.read_excel(uploaded_file)
        else:
            # Try multiple default paths
            paths = [
                '/mnt/user-data/uploads/Latest_WO.xlsx',
                '/mnt/data/Latest WO.xlsx',
                '/mnt/data/Latest_WO.xlsx',
                'Latest_WO.xlsx'
            ]
            df = None
            for path in paths:
                try:
                    df = pd.read_excel(path)
                    st.success(f"Loaded data from: {path}")
                    break
                except:
                    continue
            
            if df is None:
                st.error("No data file found. Please upload a file.")
                return None
        
        # Clean column names
        df.columns = df.columns.str.strip()
        return df
        
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

def auto_detect_columns(df):
    """Automatically detect and map column names"""
    if df is None:
        return {}
    
    column_mapping = {}
    df_cols_lower = {col: col.lower() for col in df.columns}
    
    # Comprehensive mapping patterns
    patterns = {
        'WorkOrderID': ['id', 'work order', 'wo number', 'order number', 'customer work order'],
        'VehicleID': ['vehicle', 'vin', 'vehicle id', 'customer vehicle', 'unit'],
        'EquipmentType': ['equipment', 'type', 'make', 'model', 'vehicle make'],
        'OpenDate': ['date', 'open', 'created', 'start', 'opened'],
        'CloseDate': ['close', 'completion', 'end', 'completed', 'completetion'],
        'Status': ['status', 'state', 'work order status'],
        'Location': ['location', 'workshop', 'site', 'facility'],
        'FailureDesc': ['failure', 'description', 'malfunction', 'issue', 'problem'],
        'FailureMode': ['mode', 'type', 'category', 'classification'],
        'Subsystem': ['subsystem', 'component', 'system', 'part'],
        'LaborCost': ['labor', 'labour', 'work cost'],
        'PartCost': ['part', 'material', 'component cost'],
        'DowntimeHours': ['downtime', 'down hours', 'unavailable'],
        'RepairHours': ['repair', 'maintenance hours', 'work hours'],
        'Owner': ['owner', 'assigned', 'responsible', 'technician'],
        'Sector': ['sector', 'region', 'area'],
        'SpareParts': ['spare', 'parts required', 'parts needed']
    }
    
    # Try to match columns
    for target, keywords in patterns.items():
        for df_col, df_col_lower in df_cols_lower.items():
            for keyword in keywords:
                if keyword in df_col_lower:
                    column_mapping[target] = df_col
                    break
            if target in column_mapping:
                break
    
    return column_mapping

def clean_and_enrich_data(df, mapping):
    """Clean data and add computed fields"""
    if df is None:
        return None
    
    df_clean = df.copy()
    
    # Apply column mapping
    reverse_mapping = {v: k for k, v in mapping.items()}
    df_clean = df_clean.rename(columns=reverse_mapping)
    
    # Parse dates
    date_columns = ['OpenDate', 'CloseDate']
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Standardize Status
    if 'Status' in df_clean.columns:
        status_map = {
            'completed': 'Closed',
            'closed': 'Closed',
            'waiting': 'In Progress',
            'waiting spare parts': 'Waiting Parts',
            'under maintenance': 'In Progress',
            'process initiated': 'Open',
            'open': 'Open'
        }
        df_clean['Status'] = df_clean['Status'].astype(str).str.lower().str.strip()
        for pattern, standard in status_map.items():
            df_clean.loc[df_clean['Status'].str.contains(pattern, na=False), 'Status'] = standard
    
    # Calculate RepairDuration
    if 'OpenDate' in df_clean.columns and 'CloseDate' in df_clean.columns:
        df_clean['RepairDuration'] = (df_clean['CloseDate'] - df_clean['OpenDate']).dt.total_seconds() / 3600
        df_clean['RepairDuration'] = df_clean['RepairDuration'].clip(lower=0)
    
    # Calculate TotalCost
    if 'LaborCost' in df_clean.columns and 'PartCost' in df_clean.columns:
        df_clean['TotalCost'] = df_clean['LaborCost'].fillna(0) + df_clean['PartCost'].fillna(0)
    elif 'LaborCost' in df_clean.columns:
        df_clean['TotalCost'] = df_clean['LaborCost'].fillna(0)
    elif 'PartCost' in df_clean.columns:
        df_clean['TotalCost'] = df_clean['PartCost'].fillna(0)
    
    # Calculate DaysSinceLastFailure for each vehicle
    if 'VehicleID' in df_clean.columns and 'OpenDate' in df_clean.columns:
        df_clean = df_clean.sort_values(['VehicleID', 'OpenDate'])
        df_clean['DaysSinceLastFailure'] = df_clean.groupby('VehicleID')['OpenDate'].diff().dt.days
    
    # Initial anomaly flags (will be refined later)
    df_clean['RepeatFailureFlag'] = False
    df_clean['QuickReturnFlag'] = False
    df_clean['HighCostFlag'] = False
    df_clean['HighDowntimeFlag'] = False
    
    return df_clean

# ==================== ANOMALY DETECTION ====================

def detect_anomalies(df):
    """Comprehensive anomaly detection"""
    if df is None or df.empty:
        return df, pd.DataFrame()
    
    df_anomaly = df.copy()
    anomalies = []
    thresholds = st.session_state.anomaly_thresholds
    
    # 1. Repeat Failure Detection
    if 'VehicleID' in df_anomaly.columns and 'FailureMode' in df_anomaly.columns and 'OpenDate' in df_anomaly.columns:
        df_anomaly = df_anomaly.sort_values(['VehicleID', 'FailureMode', 'OpenDate'])
        
        for idx, row in df_anomaly.iterrows():
            # Check for repeat failures
            mask = (
                (df_anomaly['VehicleID'] == row['VehicleID']) &
                (df_anomaly['FailureMode'] == row['FailureMode']) &
                (df_anomaly['OpenDate'] < row['OpenDate']) &
                ((row['OpenDate'] - df_anomaly['OpenDate']).dt.days <= thresholds['repeat_days'])
            )
            
            if mask.any():
                df_anomaly.at[idx, 'RepeatFailureFlag'] = True
                anomalies.append({
                    'Type': 'Repeat Failure',
                    'VehicleID': row['VehicleID'],
                    'Description': f"Failure mode '{row.get('FailureMode', 'Unknown')}' repeated within {thresholds['repeat_days']} days",
                    'Severity': 'High',
                    'Date': row['OpenDate']
                })
    
    # 2. Quick Return Detection
    if 'VehicleID' in df_anomaly.columns and 'DaysSinceLastFailure' in df_anomaly.columns:
        quick_returns = df_anomaly[
            (df_anomaly['DaysSinceLastFailure'] <= thresholds['quick_return_days']) &
            (df_anomaly['DaysSinceLastFailure'] > 0)
        ]
        
        for idx in quick_returns.index:
            df_anomaly.at[idx, 'QuickReturnFlag'] = True
            anomalies.append({
                'Type': 'Quick Return',
                'VehicleID': quick_returns.at[idx, 'VehicleID'],
                'Description': f"Returned after only {quick_returns.at[idx, 'DaysSinceLastFailure']:.0f} days",
                'Severity': 'High',
                'Date': quick_returns.at[idx, 'OpenDate']
            })
    
    # 3. High Frequency Failures
    if 'VehicleID' in df_anomaly.columns and 'OpenDate' in df_anomaly.columns:
        # Calculate failure frequency per vehicle
        vehicle_counts = df_anomaly.groupby('VehicleID').size()
        
        # Calculate time span for each vehicle
        vehicle_spans = df_anomaly.groupby('VehicleID')['OpenDate'].agg(['min', 'max'])
        vehicle_spans['months'] = (vehicle_spans['max'] - vehicle_spans['min']).dt.days / 30
        vehicle_spans['months'] = vehicle_spans['months'].clip(lower=1)
        
        # Failures per month
        vehicle_frequency = vehicle_counts / vehicle_spans['months']
        
        # Identify high-frequency vehicles
        mean_freq = vehicle_frequency.mean()
        std_freq = vehicle_frequency.std()
        high_freq_threshold = mean_freq + (thresholds['frequency_std'] * std_freq)
        
        high_freq_vehicles = vehicle_frequency[vehicle_frequency > high_freq_threshold]
        
        for vehicle_id, freq in high_freq_vehicles.items():
            df_anomaly.loc[df_anomaly['VehicleID'] == vehicle_id, 'HighFrequencyFlag'] = True
            anomalies.append({
                'Type': 'High Frequency',
                'VehicleID': vehicle_id,
                'Description': f"Failure rate {freq:.2f}/month (threshold: {high_freq_threshold:.2f})",
                'Severity': 'Medium',
                'Date': datetime.now()
            })
    
    # 4. Abnormal Cost Detection
    if 'TotalCost' in df_anomaly.columns and 'FailureMode' in df_anomaly.columns:
        # Calculate median cost per failure mode
        cost_stats = df_anomaly.groupby('FailureMode')['TotalCost'].agg(['median', 'mean'])
        
        for idx, row in df_anomaly.iterrows():
            if pd.notna(row.get('FailureMode')) and pd.notna(row.get('TotalCost')):
                failure_mode = row['FailureMode']
                if failure_mode in cost_stats.index:
                    median_cost = cost_stats.loc[failure_mode, 'median']
                    if median_cost > 0 and row['TotalCost'] > (median_cost * thresholds['cost_multiplier']):
                        df_anomaly.at[idx, 'HighCostFlag'] = True
                        anomalies.append({
                            'Type': 'High Cost',
                            'VehicleID': row.get('VehicleID', 'Unknown'),
                            'Description': f"Cost ${row['TotalCost']:.2f} is {row['TotalCost']/median_cost:.1f}x median",
                            'Severity': 'Medium',
                            'Date': row.get('OpenDate', datetime.now())
                        })
    
    # 5. Abnormal Downtime Detection
    if 'DowntimeHours' in df_anomaly.columns and 'FailureMode' in df_anomaly.columns:
        # Calculate median downtime per failure mode
        downtime_stats = df_anomaly.groupby('FailureMode')['DowntimeHours'].agg(['median', 'mean'])
        
        for idx, row in df_anomaly.iterrows():
            if pd.notna(row.get('FailureMode')) and pd.notna(row.get('DowntimeHours')):
                failure_mode = row['FailureMode']
                if failure_mode in downtime_stats.index:
                    median_downtime = downtime_stats.loc[failure_mode, 'median']
                    if median_downtime > 0 and row['DowntimeHours'] > (median_downtime * thresholds['downtime_multiplier']):
                        df_anomaly.at[idx, 'HighDowntimeFlag'] = True
                        anomalies.append({
                            'Type': 'High Downtime',
                            'VehicleID': row.get('VehicleID', 'Unknown'),
                            'Description': f"Downtime {row['DowntimeHours']:.1f}hrs is {row['DowntimeHours']/median_downtime:.1f}x median",
                            'Severity': 'Medium',
                            'Date': row.get('OpenDate', datetime.now())
                        })
    
    # Create anomalies DataFrame
    anomalies_df = pd.DataFrame(anomalies) if anomalies else pd.DataFrame()
    
    return df_anomaly, anomalies_df

# ==================== RELIABILITY CALCULATIONS ====================

def calculate_mtbf(df, group_by=None, base='days'):
    """Calculate Mean Time Between Failures"""
    if df is None or df.empty or 'OpenDate' not in df.columns:
        return pd.DataFrame({'MTBF': [0], 'FailureCount': [0]})
    
    results = []
    
    if group_by and group_by in df.columns:
        for name, group in df.groupby(group_by):
            if len(group) > 1:
                group_sorted = group.sort_values('OpenDate')
                
                if base == 'days':
                    time_diffs = group_sorted['OpenDate'].diff().dt.days.dropna()
                    mtbf = time_diffs.mean() if len(time_diffs) > 0 else 0
                elif base == 'hours':
                    time_diffs = group_sorted['OpenDate'].diff().dt.total_seconds() / 3600
                    time_diffs = time_diffs.dropna()
                    mtbf = time_diffs.mean() if len(time_diffs) > 0 else 0
                else:  # operating hours or km if available
                    if 'OperatingHours' in group.columns:
                        total_hours = group['OperatingHours'].sum()
                        mtbf = total_hours / len(group) if len(group) > 0 else 0
                    else:
                        mtbf = 0
                
                results.append({
                    'Group': name,
                    'MTBF': mtbf,
                    'FailureCount': len(group),
                    'Base': base
                })
    else:
        # Overall MTBF
        if len(df) > 1:
            df_sorted = df.sort_values('OpenDate')
            
            if base == 'days':
                total_days = (df_sorted['OpenDate'].max() - df_sorted['OpenDate'].min()).days
                mtbf = total_days / (len(df) - 1) if len(df) > 1 else 0
            else:
                mtbf = 0
            
            results.append({
                'Group': 'Overall',
                'MTBF': mtbf,
                'FailureCount': len(df),
                'Base': base
            })
    
    return pd.DataFrame(results) if results else pd.DataFrame({'MTBF': [0], 'FailureCount': [0]})

def calculate_mttr(df, group_by=None):
    """Calculate Mean Time To Repair"""
    if df is None or df.empty or 'RepairDuration' not in df.columns:
        return pd.DataFrame({'MTTR': [0], 'RepairCount': [0]})
    
    results = []
    
    if group_by and group_by in df.columns:
        for name, group in df.groupby(group_by):
            valid_repairs = group['RepairDuration'].dropna()
            if len(valid_repairs) > 0:
                results.append({
                    'Group': name,
                    'MTTR': valid_repairs.mean(),
                    'RepairCount': len(valid_repairs)
                })
    else:
        valid_repairs = df['RepairDuration'].dropna()
        if len(valid_repairs) > 0:
            results.append({
                'Group': 'Overall',
                'MTTR': valid_repairs.mean(),
                'RepairCount': len(valid_repairs)
            })
    
    return pd.DataFrame(results) if results else pd.DataFrame({'MTTR': [0], 'RepairCount': [0]})

def calculate_availability(df, window_days=30):
    """Calculate equipment availability"""
    if df is None or df.empty:
        return 0
    
    # If we have downtime data
    if 'DowntimeHours' in df.columns:
        total_downtime = df['DowntimeHours'].sum()
        
        # Estimate total possible hours
        if 'VehicleID' in df.columns:
            fleet_size = df['VehicleID'].nunique()
        else:
            fleet_size = 1
        
        total_possible_hours = window_days * 24 * fleet_size
        availability = ((total_possible_hours - total_downtime) / total_possible_hours * 100) if total_possible_hours > 0 else 0
        return max(0, min(100, availability))
    
    # Alternative: use status
    if 'Status' in df.columns:
        operational = len(df[df['Status'] == 'Closed'])
        total = len(df)
        return (operational / total * 100) if total > 0 else 0
    
    return 0

# ==================== USER INTERFACE COMPONENTS ====================

def render_sidebar():
    """Render sidebar with controls and info"""
    st.sidebar.title("🚗 FRACAS Control Panel")
    
    # User role
    st.sidebar.selectbox(
        "User Role",
        ["Technician", "Engineer", "Supervisor", "Admin"],
        key="user_role"
    )
    
    st.sidebar.markdown("---")
    
    # Anomaly thresholds
    st.sidebar.subheader("⚠️ Anomaly Thresholds")
    
    st.session_state.anomaly_thresholds['repeat_days'] = st.sidebar.slider(
        "Repeat Failure Window (days)",
        7, 90, 30
    )
    
    st.session_state.anomaly_thresholds['quick_return_days'] = st.sidebar.slider(
        "Quick Return Threshold (days)",
        3, 30, 10
    )
    
    st.session_state.anomaly_thresholds['cost_multiplier'] = st.sidebar.slider(
        "High Cost Multiplier",
        1.5, 5.0, 2.0
    )
    
    st.session_state.anomaly_thresholds['downtime_multiplier'] = st.sidebar.slider(
        "High Downtime Multiplier",
        1.5, 5.0, 2.0
    )
    
    st.sidebar.markdown("---")
    
    # System info
    if st.session_state.df_cleaned is not None:
        df = st.session_state.df_cleaned
        st.sidebar.metric("Total Records", len(df))
        
        if 'Status' in df.columns:
            open_count = len(df[df['Status'].isin(['Open', 'In Progress'])])
            st.sidebar.metric("Active Work Orders", open_count)
        
        # Anomaly summary
        if st.session_state.anomalies is not None and not st.session_state.anomalies.empty:
            st.sidebar.metric("Active Anomalies", len(st.session_state.anomalies))
            
            # Anomaly breakdown
            anomaly_counts = st.session_state.anomalies['Type'].value_counts()
            for atype, count in anomaly_counts.items():
                st.sidebar.write(f"• {atype}: {count}")

def render_data_tab():
    """Data import and cleaning interface"""
    st.header("📂 Data Import & Cleaning")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # File upload
        uploaded_file = st.file_uploader(
            "Upload Excel File",
            type=['xlsx', 'xls'],
            help="Upload work order data"
        )
        
        # Load data button
        if st.button("Load Data", type="primary") or uploaded_file:
            with st.spinner("Loading and processing data..."):
                df = load_data(uploaded_file=uploaded_file)
                
                if df is not None:
                    st.session_state.df = df
                    st.session_state.column_mapping = auto_detect_columns(df)
                    st.success(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")
    
    with col2:
        if st.session_state.df is not None:
            st.subheader("Column Mapping")
            
            # Display detected mappings
            mapping = st.session_state.column_mapping
            st.info(f"Auto-detected {len(mapping)} column mappings")
            
            # Show mapping summary
            if mapping:
                mapping_df = pd.DataFrame(
                    [(k, v) for k, v in mapping.items()],
                    columns=['Target Field', 'Detected Column']
                )
                st.dataframe(mapping_df, use_container_width=True)
            
            # Apply cleaning
            if st.button("Apply Cleaning & Enrichment", type="primary"):
                with st.spinner("Cleaning data and detecting anomalies..."):
                    # Clean data
                    df_cleaned = clean_and_enrich_data(st.session_state.df, mapping)
                    
                    # Detect anomalies
                    df_cleaned, anomalies_df = detect_anomalies(df_cleaned)
                    
                    st.session_state.df_cleaned = df_cleaned
                    st.session_state.anomalies = anomalies_df
                    
                    st.success(f"✅ Data cleaned and {len(anomalies_df)} anomalies detected!")
    
    # Preview cleaned data
    if st.session_state.df_cleaned is not None:
        st.subheader("Cleaned Data Preview")
        
        df = st.session_state.df_cleaned
        
        # Metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Records", len(df))
        
        with col2:
            if 'Status' in df.columns:
                closed = len(df[df['Status'] == 'Closed'])
                st.metric("Closed", closed)
        
        with col3:
            repeat_count = df['RepeatFailureFlag'].sum() if 'RepeatFailureFlag' in df.columns else 0
            st.metric("Repeat Failures", repeat_count)
        
        with col4:
            quick_return = df['QuickReturnFlag'].sum() if 'QuickReturnFlag' in df.columns else 0
            st.metric("Quick Returns", quick_return)
        
        with col5:
            high_cost = df['HighCostFlag'].sum() if 'HighCostFlag' in df.columns else 0
            st.metric("High Cost", high_cost)
        
        # Show data with anomaly highlighting
        def highlight_anomalies(row):
            if row.get('RepeatFailureFlag') or row.get('QuickReturnFlag'):
                return ['background-color: #ffcccc'] * len(row)
            elif row.get('HighCostFlag') or row.get('HighDowntimeFlag'):
                return ['background-color: #ffe5cc'] * len(row)
            return [''] * len(row)
        
        styled_df = df.head(100).style.apply(highlight_anomalies, axis=1)
        st.dataframe(styled_df, use_container_width=True)
        
        # Export buttons
        col1, col2 = st.columns(2)
        
        with col1:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Cleaned CSV",
                csv,
                "cleaned_data.csv",
                "text/csv"
            )
        
        with col2:
            # Safe Excel export
            try:
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Cleaned_Data')
                
                st.download_button(
                    "📥 Download Cleaned Excel",
                    buffer.getvalue(),
                    "cleaned_data.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except:
                st.warning("Excel export unavailable, use CSV instead")

def render_failures_tab():
    """Failure reporting and management"""
    st.header("📝 Failure Reporting")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Filters
    st.subheader("Filter Work Orders")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'OpenDate' in df.columns:
            date_range = st.date_input(
                "Date Range",
                value=(df['OpenDate'].min(), df['OpenDate'].max()),
                key="failure_date"
            )
    
    with col2:
        if 'VehicleID' in df.columns:
            vehicles = st.multiselect(
                "Vehicle ID",
                options=df['VehicleID'].unique(),
                key="failure_vehicle"
            )
    
    with col3:
        if 'Status' in df.columns:
            status_filter = st.multiselect(
                "Status",
                options=df['Status'].unique(),
                key="failure_status"
            )
    
    with col4:
        show_anomalies = st.checkbox("Show Anomalies Only", key="show_anomalies")
    
    # Apply filters
    filtered_df = df.copy()
    
    if vehicles:
        filtered_df = filtered_df[filtered_df['VehicleID'].isin(vehicles)]
    
    if status_filter:
        filtered_df = filtered_df[filtered_df['Status'].isin(status_filter)]
    
    if show_anomalies:
        anomaly_mask = (
            filtered_df['RepeatFailureFlag'] | 
            filtered_df['QuickReturnFlag'] |
            filtered_df['HighCostFlag'] |
            filtered_df['HighDowntimeFlag']
        )
        filtered_df = filtered_df[anomaly_mask]
    
    # Display filtered data
    st.subheader(f"Work Orders ({len(filtered_df)} records)")
    
    # Highlight anomalies
    def style_anomalies(row):
        if row.get('RepeatFailureFlag') or row.get('QuickReturnFlag'):
            return ['background-color: #ffcccc'] * len(row)
        elif row.get('HighCostFlag') or row.get('HighDowntimeFlag'):
            return ['background-color: #ffe5cc'] * len(row)
        return [''] * len(row)
    
    styled = filtered_df.style.apply(style_anomalies, axis=1)
    st.dataframe(styled, use_container_width=True, height=400)
    
    # Add new failure
    st.subheader("Log New Failure")
    
    with st.form("new_failure_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            new_vehicle = st.text_input("Vehicle ID")
            new_date = st.date_input("Failure Date")
            new_status = st.selectbox("Status", ["Open", "In Progress", "Waiting Parts"])
        
        with col2:
            new_failure_desc = st.text_area("Failure Description")
            new_failure_mode = st.text_input("Failure Mode")
        
        with col3:
            new_location = st.text_input("Location/Workshop")
            new_cost = st.number_input("Estimated Cost", min_value=0.0)
        
        if st.form_submit_button("Submit Failure Report"):
            new_failure = pd.DataFrame({
                'WorkOrderID': [f"WO-{datetime.now().strftime('%Y%m%d%H%M%S')}"],
                'VehicleID': [new_vehicle],
                'OpenDate': [new_date],
                'Status': [new_status],
                'FailureDesc': [new_failure_desc],
                'FailureMode': [new_failure_mode],
                'Location': [new_location],
                'TotalCost': [new_cost]
            })
            
            st.session_state.new_failures = pd.concat(
                [st.session_state.new_failures, new_failure],
                ignore_index=True
            )
            
            st.success("✅ Failure report submitted!")

def render_analysis_tab():
    """Failure analysis and RCA tools"""
    st.header("📊 Failure Analysis & Root Cause")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Analysis tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Top Failures", "Trends", "Root Cause Analysis", "Component Analysis"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        # Top failure modes
        with col1:
            if 'FailureMode' in df.columns:
                failure_counts = df['FailureMode'].value_counts().head(10)
                
                fig = px.bar(
                    x=failure_counts.values,
                    y=failure_counts.index,
                    orientation='h',
                    title="Top 10 Failure Modes",
                    labels={'x': 'Count', 'y': 'Failure Mode'},
                    color=failure_counts.values,
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Top vehicles
        with col2:
            if 'VehicleID' in df.columns:
                vehicle_counts = df['VehicleID'].value_counts().head(10)
                
                fig = px.bar(
                    x=vehicle_counts.values,
                    y=vehicle_counts.index,
                    orientation='h',
                    title="Top 10 Vehicles by Failures",
                    labels={'x': 'Count', 'y': 'Vehicle ID'},
                    color=vehicle_counts.values,
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Location analysis
        if 'Location' in df.columns:
            location_counts = df['Location'].value_counts()
            
            fig = px.pie(
                values=location_counts.values,
                names=location_counts.index,
                title="Failures by Location/Workshop"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        # Monthly trend
        if 'OpenDate' in df.columns:
            df['Month'] = pd.to_datetime(df['OpenDate']).dt.to_period('M')
            monthly_counts = df.groupby('Month').size()
            
            fig = px.line(
                x=monthly_counts.index.astype(str),
                y=monthly_counts.values,
                title="Monthly Failure Trend",
                labels={'x': 'Month', 'y': 'Failure Count'},
                markers=True
            )
            
            # Add average line
            avg_failures = monthly_counts.mean()
            fig.add_hline(
                y=avg_failures,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Average: {avg_failures:.0f}"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Severity trend if available
            if 'Status' in df.columns:
                status_trend = df.groupby(['Month', 'Status']).size().unstack(fill_value=0)
                
                fig = px.area(
                    status_trend.T,
                    title="Status Distribution Over Time",
                    labels={'value': 'Count', 'index': 'Status'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Root Cause Analysis Tools")
        
        # Select work order
        if 'WorkOrderID' in df.columns:
            selected_wo = st.selectbox(
                "Select Work Order for RCA",
                df['WorkOrderID'].unique()
            )
            
            wo_details = df[df['WorkOrderID'] == selected_wo].iloc[0]
            
            # Show WO details
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.write("**Work Order Details:**")
                st.write(f"Vehicle: {wo_details.get('VehicleID', 'N/A')}")
                st.write(f"Date: {wo_details.get('OpenDate', 'N/A')}")
                st.write(f"Status: {wo_details.get('Status', 'N/A')}")
                
                if wo_details.get('RepeatFailureFlag'):
                    st.error("⚠️ This is a REPEAT FAILURE")
            
            with col2:
                # 5-Whys Analysis
                st.write("**5-Whys Analysis**")
                
                with st.form(f"five_whys_{selected_wo}"):
                    why1 = st.text_input("Why 1: Why did this failure occur?")
                    why2 = st.text_input("Why 2: Why did that happen?")
                    why3 = st.text_input("Why 3: Why?")
                    why4 = st.text_input("Why 4: Why?")
                    why5 = st.text_input("Why 5: Root Cause")
                    
                    if st.form_submit_button("Save 5-Whys"):
                        rca_entry = pd.DataFrame({
                            'WorkOrderID': [selected_wo],
                            'Type': ['5-Whys'],
                            'Content': [f"1:{why1}\n2:{why2}\n3:{why3}\n4:{why4}\n5:{why5}"],
                            'Timestamp': [datetime.now()],
                            'User': [st.session_state.user_role]
                        })
                        
                        st.session_state.rca_data = pd.concat(
                            [st.session_state.rca_data, rca_entry],
                            ignore_index=True
                        )
                        st.success("✅ 5-Whys analysis saved!")
            
            # Ishikawa Diagram
            st.write("**Ishikawa (Fishbone) Analysis**")
            
            with st.form(f"ishikawa_{selected_wo}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    man = st.text_area("Man (People)", help="Skills, training, fatigue")
                    machine = st.text_area("Machine", help="Equipment, tools, maintenance")
                
                with col2:
                    method = st.text_area("Method", help="Procedures, standards")
                    material = st.text_area("Material", help="Quality, availability")
                
                with col3:
                    measurement = st.text_area("Measurement", help="Accuracy, calibration")
                    environment = st.text_area("Environment", help="Temperature, humidity, location")
                
                if st.form_submit_button("Save Ishikawa Analysis"):
                    ishikawa_content = f"""
                    Man: {man}
                    Machine: {machine}
                    Method: {method}
                    Material: {material}
                    Measurement: {measurement}
                    Environment: {environment}
                    """
                    
                    rca_entry = pd.DataFrame({
                        'WorkOrderID': [selected_wo],
                        'Type': ['Ishikawa'],
                        'Content': [ishikawa_content],
                        'Timestamp': [datetime.now()],
                        'User': [st.session_state.user_role]
                    })
                    
                    st.session_state.rca_data = pd.concat(
                        [st.session_state.rca_data, rca_entry],
                        ignore_index=True
                    )
                    st.success("✅ Ishikawa analysis saved!")
    
    with tab4:
        # Component/Subsystem analysis
        st.subheader("Component Recurrence Analysis")
        
        if 'Subsystem' in df.columns or 'FailureMode' in df.columns:
            # Use FailureMode as proxy for component if Subsystem not available
            component_col = 'Subsystem' if 'Subsystem' in df.columns else 'FailureMode'
            
            # Calculate recurrence
            component_stats = df.groupby(component_col).agg({
                'WorkOrderID': 'count',
                'RepeatFailureFlag': 'sum' if 'RepeatFailureFlag' in df.columns else 'count'
            }).rename(columns={'WorkOrderID': 'TotalFailures', 'RepeatFailureFlag': 'RepeatFailures'})
            
            if 'RepeatFailureFlag' in df.columns:
                component_stats['RepeatRate'] = (component_stats['RepeatFailures'] / component_stats['TotalFailures'] * 100)
                component_stats = component_stats.sort_values('RepeatRate', ascending=False).head(15)
                
                # Heatmap of repeat failures
                fig = px.bar(
                    component_stats,
                    y=component_stats.index,
                    x='RepeatRate',
                    orientation='h',
                    title="Component Repeat Failure Rate (%)",
                    color='RepeatRate',
                    color_continuous_scale='Reds',
                    labels={'RepeatRate': 'Repeat Rate (%)'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Show detailed table
                st.dataframe(component_stats, use_container_width=True)

def render_capa_tab():
    """CAPA management interface"""
    st.header("📋 CAPA - Corrective and Preventive Actions")
    
    # CAPA entry form
    with st.form("capa_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            action_id = f"CAPA-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            st.text_input("Action ID", value=action_id, disabled=True)
            
            # Link to work order if available
            wo_link = st.text_input("Linked Work Order ID")
            description = st.text_area("Action Description")
        
        with col2:
            owner = st.text_input("Owner")
            due_date = st.date_input("Due Date")
            action_type = st.selectbox("Type", ["Corrective", "Preventive"])
        
        with col3:
            priority = st.selectbox("Priority", ["Critical", "High", "Medium", "Low"])
            status = st.selectbox("Status", ["Open", "In Progress", "Pending Review", "Closed"])
        
        if st.form_submit_button("Create CAPA"):
            new_capa = pd.DataFrame({
                'ActionID': [action_id],
                'WorkOrderID': [wo_link],
                'Description': [description],
                'Owner': [owner],
                'DueDate': [due_date],
                'Status': [status],
                'EffectivenessCheck': ['Pending'],
                'ClosureDate': [None]
            })
            
            st.session_state.capa_register = pd.concat(
                [st.session_state.capa_register, new_capa],
                ignore_index=True
            )
            st.success(f"✅ CAPA {action_id} created!")
    
    # CAPA register display
    st.subheader("CAPA Register")
    
    if not st.session_state.capa_register.empty:
        # Metrics
        col1, col2, col3, col4 = st.columns(4)
        
        capa_df = st.session_state.capa_register
        
        with col1:
            st.metric("Total CAPAs", len(capa_df))
        
        with col2:
            open_capas = len(capa_df[capa_df['Status'].isin(['Open', 'In Progress'])])
            st.metric("Open Actions", open_capas)
        
        with col3:
            # Check for overdue
            capa_df['DueDate'] = pd.to_datetime(capa_df['DueDate'])
            overdue = len(capa_df[
                (capa_df['DueDate'] < datetime.now()) & 
                (capa_df['Status'] != 'Closed')
            ])
            st.metric("Overdue", overdue, delta_color="inverse")
        
        with col4:
            # CAPA effectiveness
            if st.session_state.df_cleaned is not None:
                # Check for failures after CAPA closure
                ineffective_capas = 0
                
                for idx, capa in capa_df[capa_df['Status'] == 'Closed'].iterrows():
                    if pd.notna(capa.get('WorkOrderID')):
                        # Check if similar failures occurred after CAPA closure
                        wo_data = st.session_state.df_cleaned[
                            st.session_state.df_cleaned['WorkOrderID'] == capa['WorkOrderID']
                        ]
                        
                        if not wo_data.empty:
                            vehicle = wo_data.iloc[0].get('VehicleID')
                            failure_mode = wo_data.iloc[0].get('FailureMode')
                            
                            # Check for recurrence
                            if vehicle and failure_mode and capa.get('ClosureDate'):
                                recurring = st.session_state.df_cleaned[
                                    (st.session_state.df_cleaned['VehicleID'] == vehicle) &
                                    (st.session_state.df_cleaned['FailureMode'] == failure_mode) &
                                    (st.session_state.df_cleaned['OpenDate'] > capa['ClosureDate'])
                                ]
                                
                                if not recurring.empty:
                                    ineffective_capas += 1
                                    capa_df.at[idx, 'EffectivenessCheck'] = 'Failed'
                
                effectiveness_rate = (1 - ineffective_capas / len(capa_df[capa_df['Status'] == 'Closed'])) * 100 if len(capa_df[capa_df['Status'] == 'Closed']) > 0 else 100
                st.metric("CAPA Effectiveness", f"{effectiveness_rate:.1f}%")
        
        # Highlight overdue items
        def highlight_overdue(row):
            if row['Status'] != 'Closed' and pd.to_datetime(row['DueDate']) < datetime.now():
                return ['background-color: #ffcccc'] * len(row)
            elif row.get('EffectivenessCheck') == 'Failed':
                return ['background-color: #ffe5cc'] * len(row)
            return [''] * len(row)
        
        styled_capa = capa_df.style.apply(highlight_overdue, axis=1)
        st.dataframe(styled_capa, use_container_width=True)
        
        # Export CAPA register
        csv = capa_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Download CAPA Register",
            csv,
            "capa_register.csv",
            "text/csv"
        )

def render_reliability_tab():
    """Reliability KPIs dashboard"""
    st.header("📈 Reliability Metrics")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # KPI calculation settings
    col1, col2 = st.columns([3, 1])
    
    with col2:
        mtbf_base = st.selectbox(
            "MTBF Base",
            ["days", "hours"],
            help="Base unit for MTBF calculation"
        )
        
        window_days = st.number_input(
            "Analysis Window (days)",
            min_value=7,
            max_value=365,
            value=30
        )
    
    # Calculate KPIs
    mtbf_overall = calculate_mtbf(df, base=mtbf_base)
    mttr_overall = calculate_mttr(df)
    availability = calculate_availability(df, window_days)
    
    # Display KPI cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        mtbf_value = mtbf_overall['MTBF'].mean() if not mtbf_overall.empty else 0
        st.metric(
            f"MTBF ({mtbf_base})",
            f"{mtbf_value:.1f}",
            help="Mean Time Between Failures"
        )
    
    with col2:
        mttr_value = mttr_overall['MTTR'].mean() if not mttr_overall.empty else 0
        st.metric(
            "MTTR (hours)",
            f"{mttr_value:.1f}",
            help="Mean Time To Repair"
        )
    
    with col3:
        if mtbf_value > 0:
            failure_rate = 1 / mtbf_value
            st.metric(
                "Failure Rate",
                f"{failure_rate:.4f}",
                help=f"Failures per {mtbf_base}"
            )
        else:
            st.metric("Failure Rate", "N/A")
    
    with col4:
        st.metric(
            "Availability (%)",
            f"{availability:.1f}",
            help=f"Over {window_days} days"
        )
    
    # Detailed analysis
    tab1, tab2, tab3 = st.tabs(["MTBF Analysis", "MTTR Analysis", "Reliability Trends"])
    
    with tab1:
        if 'VehicleID' in df.columns:
            vehicle_mtbf = calculate_mtbf(df, group_by='VehicleID', base=mtbf_base)
            
            if not vehicle_mtbf.empty:
                # Best and worst performers
                col1, col2 = st.columns(2)
                
                with col1:
                    top_vehicles = vehicle_mtbf.nlargest(10, 'MTBF')
                    
                    fig = px.bar(
                        top_vehicles,
                        x='MTBF',
                        y='Group',
                        orientation='h',
                        title=f"Top 10 Vehicles - MTBF ({mtbf_base})",
                        color='MTBF',
                        color_continuous_scale='Greens'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    bottom_vehicles = vehicle_mtbf.nsmallest(10, 'MTBF')
                    
                    fig = px.bar(
                        bottom_vehicles,
                        x='MTBF',
                        y='Group',
                        orientation='h',
                        title=f"Bottom 10 Vehicles - MTBF ({mtbf_base})",
                        color='MTBF',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        if 'VehicleID' in df.columns:
            vehicle_mttr = calculate_mttr(df, group_by='VehicleID')
            
            if not vehicle_mttr.empty:
                # MTTR distribution
                fig = px.histogram(
                    vehicle_mttr,
                    x='MTTR',
                    nbins=30,
                    title="MTTR Distribution (hours)",
                    labels={'MTTR': 'MTTR (hours)', 'count': 'Number of Vehicles'}
                )
                
                # Add mean line
                mean_mttr = vehicle_mttr['MTTR'].mean()
                fig.add_vline(
                    x=mean_mttr,
                    line_dash="dash",
                    line_color="red",
                    annotation_text=f"Mean: {mean_mttr:.1f}"
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Longest repair times
                longest_repairs = vehicle_mttr.nlargest(10, 'MTTR')
                
                fig = px.bar(
                    longest_repairs,
                    x='MTTR',
                    y='Group',
                    orientation='h',
                    title="Vehicles with Longest Repair Times",
                    color='MTTR',
                    color_continuous_scale='Oranges'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        if 'OpenDate' in df.columns:
            # Calculate monthly reliability metrics
            df['Month'] = pd.to_datetime(df['OpenDate']).dt.to_period('M')
            
            monthly_metrics = []
            for month in df['Month'].unique():
                month_df = df[df['Month'] == month]
                
                month_mtbf = calculate_mtbf(month_df, base=mtbf_base)
                month_mttr = calculate_mttr(month_df)
                
                monthly_metrics.append({
                    'Month': str(month),
                    'MTBF': month_mtbf['MTBF'].mean() if not month_mtbf.empty else 0,
                    'MTTR': month_mttr['MTTR'].mean() if not month_mttr.empty else 0,
                    'FailureCount': len(month_df)
                })
            
            metrics_df = pd.DataFrame(monthly_metrics)
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('MTBF Trend', 'MTTR Trend', 'Failure Count', 'Combined View')
            )
            
            # MTBF trend
            fig.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTBF'], mode='lines+markers', name='MTBF'),
                row=1, col=1
            )
            
            # MTTR trend
            fig.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTTR'], mode='lines+markers', name='MTTR'),
                row=1, col=2
            )
            
            # Failure count
            fig.add_trace(
                go.Bar(x=metrics_df['Month'], y=metrics_df['FailureCount'], name='Failures'),
                row=2, col=1
            )
            
            # Combined view
            fig2 = make_subplots(specs=[[{"secondary_y": True}]])
            fig2.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTBF'], mode='lines', name='MTBF'),
                secondary_y=False
            )
            fig2.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTTR'], mode='lines', name='MTTR'),
                secondary_y=True
            )
            
            fig.update_layout(height=600, title_text="Reliability Metrics Over Time")
            st.plotly_chart(fig, use_container_width=True)

def render_anomalies_tab():
    """Anomaly detection and monitoring"""
    st.header("⚠️ Anomaly Detection & Monitoring")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    anomalies_df = st.session_state.anomalies
    
    # Anomaly summary cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        repeat_count = df['RepeatFailureFlag'].sum() if 'RepeatFailureFlag' in df.columns else 0
        st.metric("Repeat Failures", repeat_count, delta_color="inverse")
    
    with col2:
        quick_return = df['QuickReturnFlag'].sum() if 'QuickReturnFlag' in df.columns else 0
        st.metric("Quick Returns", quick_return, delta_color="inverse")
    
    with col3:
        high_cost = df['HighCostFlag'].sum() if 'HighCostFlag' in df.columns else 0
        st.metric("High Cost", high_cost, delta_color="inverse")
    
    with col4:
        high_downtime = df['HighDowntimeFlag'].sum() if 'HighDowntimeFlag' in df.columns else 0
        st.metric("High Downtime", high_downtime, delta_color="inverse")
    
    with col5:
        total_anomalies = len(anomalies_df) if anomalies_df is not None else 0
        st.metric("Total Anomalies", total_anomalies, delta_color="inverse")
    
    # Anomaly details tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Active Anomalies", "Repeat Failures", "Quick Returns", "Component Recurrence"])
    
    with tab1:
        st.subheader("All Active Anomalies")
        
        if anomalies_df is not None and not anomalies_df.empty:
            # Group by type
            anomaly_summary = anomalies_df.groupby(['Type', 'Severity']).size().reset_index(name='Count')
            
            # Pie chart of anomaly types
            fig = px.pie(
                anomaly_summary,
                values='Count',
                names='Type',
                title="Anomaly Distribution by Type",
                color_discrete_map={
                    'Repeat Failure': '#ff4444',
                    'Quick Return': '#ff8844',
                    'High Cost': '#ffaa44',
                    'High Downtime': '#ffcc44',
                    'High Frequency': '#ff6644'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Detailed anomaly table
            st.subheader("Anomaly Details")
            
            # Color code by severity
            def style_severity(row):
                if row['Severity'] == 'High':
                    return ['background-color: #ffcccc'] * len(row)
                elif row['Severity'] == 'Medium':
                    return ['background-color: #ffe5cc'] * len(row)
                return [''] * len(row)
            
            styled_anomalies = anomalies_df.style.apply(style_severity, axis=1)
            st.dataframe(styled_anomalies, use_container_width=True)
            
            # Export anomalies
            csv = anomalies_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Anomaly Report",
                csv,
                "anomaly_report.csv",
                "text/csv"
            )
        else:
            st.info("No anomalies detected")
    
    with tab2:
        st.subheader("Repeat Failure Analysis")
        
        repeat_failures = df[df['RepeatFailureFlag'] == True] if 'RepeatFailureFlag' in df.columns else pd.DataFrame()
        
        if not repeat_failures.empty:
            # Repeat failure patterns
            if 'VehicleID' in repeat_failures.columns and 'FailureMode' in repeat_failures.columns:
                repeat_patterns = repeat_failures.groupby(['VehicleID', 'FailureMode']).size().reset_index(name='Count')
                repeat_patterns = repeat_patterns.sort_values('Count', ascending=False).head(20)
                
                # Heatmap of repeat failures
                if len(repeat_patterns) > 0:
                    pivot_table = repeat_patterns.pivot(index='VehicleID', columns='FailureMode', values='Count').fillna(0)
                    
                    fig = px.imshow(
                        pivot_table,
                        title="Repeat Failure Heatmap",
                        labels={'x': 'Failure Mode', 'y': 'Vehicle ID', 'color': 'Repeat Count'},
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Table of repeat patterns
                st.dataframe(repeat_patterns, use_container_width=True)
        else:
            st.info("No repeat failures detected")
    
    with tab3:
        st.subheader("Quick Return Analysis")
        
        quick_returns = df[df['QuickReturnFlag'] == True] if 'QuickReturnFlag' in df.columns else pd.DataFrame()
        
        if not quick_returns.empty:
            # Days between failures distribution
            if 'DaysSinceLastFailure' in quick_returns.columns:
                fig = px.histogram(
                    quick_returns,
                    x='DaysSinceLastFailure',
                    nbins=20,
                    title="Days Between Failures for Quick Returns",
                    labels={'DaysSinceLastFailure': 'Days', 'count': 'Number of Occurrences'}
                )
                
                # Add threshold line
                fig.add_vline(
                    x=st.session_state.anomaly_thresholds['quick_return_days'],
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Threshold"
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Vehicles with most quick returns
            if 'VehicleID' in quick_returns.columns:
                vehicle_quick_returns = quick_returns['VehicleID'].value_counts().head(10)
                
                fig = px.bar(
                    x=vehicle_quick_returns.values,
                    y=vehicle_quick_returns.index,
                    orientation='h',
                    title="Vehicles with Most Quick Returns",
                    labels={'x': 'Count', 'y': 'Vehicle ID'},
                    color=vehicle_quick_returns.values,
                    color_continuous_scale='Oranges'
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No quick returns detected")
    
    with tab4:
        st.subheader("Component Recurrence Tracking")
        
        if 'FailureMode' in df.columns:
            # Calculate recurrence metrics
            component_metrics = df.groupby('FailureMode').agg({
                'WorkOrderID': 'count',
                'RepeatFailureFlag': lambda x: x.sum() if 'RepeatFailureFlag' in df.columns else 0,
                'TotalCost': lambda x: x.sum() if 'TotalCost' in df.columns else 0
            }).rename(columns={
                'WorkOrderID': 'TotalOccurrences',
                'RepeatFailureFlag': 'RepeatCount',
                'TotalCost': 'TotalCost'
            })
            
            component_metrics['RecurrenceRate'] = (
                component_metrics['RepeatCount'] / component_metrics['TotalOccurrences'] * 100
            )
            
            # Sort by recurrence rate
            component_metrics = component_metrics.sort_values('RecurrenceRate', ascending=False).head(15)
            
            # Bar chart of recurrence rates
            fig = px.bar(
                component_metrics,
                y=component_metrics.index,
                x='RecurrenceRate',
                orientation='h',
                title="Component Recurrence Rates (%)",
                color='RecurrenceRate',
                color_continuous_scale='Reds',
                labels={'RecurrenceRate': 'Recurrence Rate (%)'}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Detailed table
            st.dataframe(component_metrics, use_container_width=True)

def render_costs_tab():
    """Cost and performance analysis"""
    st.header("💰 Cost & Performance Analysis")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Check if cost data is available
    has_cost = 'TotalCost' in df.columns or 'LaborCost' in df.columns or 'PartCost' in df.columns
    
    if not has_cost:
        st.warning("Cost data not available in dataset")
        
        # Option to generate sample cost data
        if st.checkbox("Generate sample cost data for demonstration"):
            df['TotalCost'] = np.random.uniform(100, 5000, len(df))
            df['DowntimeHours'] = np.random.uniform(1, 48, len(df))
            st.session_state.df_cleaned = df
            st.rerun()
        return
    
    # Cost metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_cost = df['TotalCost'].sum() if 'TotalCost' in df.columns else 0
        st.metric("Total Cost", f"${total_cost:,.2f}")
    
    with col2:
        avg_cost = df['TotalCost'].mean() if 'TotalCost' in df.columns else 0
        st.metric("Average Cost", f"${avg_cost:,.2f}")
    
    with col3:
        if 'DowntimeHours' in df.columns:
            total_downtime = df['DowntimeHours'].sum()
            st.metric("Total Downtime", f"{total_downtime:,.1f} hrs")
    
    with col4:
        if 'TotalCost' in df.columns and 'DowntimeHours' in df.columns:
            cost_per_hour = total_cost / total_downtime if total_downtime > 0 else 0
            st.metric("Cost per Downtime Hour", f"${cost_per_hour:,.2f}")
    
    # Cost analysis tabs
    tab1, tab2, tab3 = st.tabs(["Pareto Analysis", "Cost vs Downtime", "High Cost Failures"])
    
    with tab1:
        st.subheader("Pareto Analysis (80/20 Rule)")
        
        # Choose grouping
        group_by = st.selectbox(
            "Group by",
            ['VehicleID', 'FailureMode', 'Location', 'Subsystem']
        )
        
        if group_by in df.columns and 'TotalCost' in df.columns:
            # Calculate Pareto
            cost_by_group = df.groupby(group_by)['TotalCost'].sum().sort_values(ascending=False)
            
            # Calculate cumulative percentage
            cumulative_sum = cost_by_group.cumsum()
            cumulative_percent = (cumulative_sum / cost_by_group.sum() * 100)
            
            # Find 80% threshold
            threshold_idx = (cumulative_percent <= 80).sum()
            
            st.info(f"**Pareto Principle:** Top {threshold_idx} {group_by}s ({threshold_idx/len(cost_by_group)*100:.1f}%) account for 80% of costs")
            
            # Create Pareto chart
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Bar chart
            fig.add_trace(
                go.Bar(
                    x=list(range(len(cost_by_group.head(20)))),
                    y=cost_by_group.head(20).values,
                    name='Cost',
                    marker_color='lightblue',
                    text=[f"${v:,.0f}" for v in cost_by_group.head(20).values],
                    textposition='outside'
                ),
                secondary_y=False
            )
            
            # Cumulative line
            fig.add_trace(
                go.Scatter(
                    x=list(range(len(cumulative_percent.head(20)))),
                    y=cumulative_percent.head(20).values,
                    name='Cumulative %',
                    line=dict(color='red', width=2),
                    mode='lines+markers'
                ),
                secondary_y=True
            )
            
            # Add 80% line
            fig.add_hline(
                y=80,
                line_dash="dash",
                line_color="green",
                secondary_y=True,
                annotation_text="80%"
            )
            
            fig.update_xaxes(title_text=f"{group_by} Rank")
            fig.update_yaxes(title_text="Cost ($)", secondary_y=False)
            fig.update_yaxes(title_text="Cumulative %", secondary_y=True)
            fig.update_layout(title=f"Pareto Analysis - Cost by {group_by}", height=500)
            
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Cost vs Downtime Analysis")
        
        if 'TotalCost' in df.columns and 'DowntimeHours' in df.columns:
            # Scatter plot
            fig = px.scatter(
                df,
                x='DowntimeHours',
                y='TotalCost',
                color='Status' if 'Status' in df.columns else None,
                size='TotalCost',
                hover_data=['VehicleID'] if 'VehicleID' in df.columns else None,
                title="Cost vs Downtime Correlation",
                labels={'DowntimeHours': 'Downtime (hours)', 'TotalCost': 'Total Cost ($)'},
                trendline="ols"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Calculate correlation
            correlation = df[['TotalCost', 'DowntimeHours']].corr().iloc[0, 1]
            st.info(f"**Correlation Coefficient:** {correlation:.3f}")
            
            # Cost efficiency analysis
            df['CostPerHour'] = df['TotalCost'] / df['DowntimeHours']
            df['CostPerHour'] = df['CostPerHour'].replace([np.inf, -np.inf], np.nan)
            
            if 'VehicleID' in df.columns:
                efficiency = df.groupby('VehicleID')['CostPerHour'].mean().sort_values().head(10)
                
                fig = px.bar(
                    x=efficiency.values,
                    y=efficiency.index,
                    orientation='h',
                    title="Most Cost-Efficient Vehicles ($/hour)",
                    labels={'x': 'Cost per Hour ($)', 'y': 'Vehicle ID'},
                    color=efficiency.values,
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("High Cost Failure Analysis")
        
        if 'TotalCost' in df.columns:
            # Identify high-cost failures
            cost_threshold = df['TotalCost'].quantile(0.9)  # Top 10%
            high_cost_failures = df[df['TotalCost'] > cost_threshold]
            
            st.info(f"Showing failures with cost > ${cost_threshold:,.2f} (top 10%)")
            
            # High cost by failure mode
            if 'FailureMode' in high_cost_failures.columns:
                high_cost_modes = high_cost_failures.groupby('FailureMode').agg({
                    'TotalCost': ['sum', 'mean', 'count']
                }).round(2)
                
                high_cost_modes.columns = ['Total Cost', 'Avg Cost', 'Count']
                high_cost_modes = high_cost_modes.sort_values('Total Cost', ascending=False)
                
                fig = px.bar(
                    high_cost_modes,
                    x='Total Cost',
                    y=high_cost_modes.index,
                    orientation='h',
                    title="High-Cost Failure Modes",
                    color='Total Cost',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Table
                st.dataframe(high_cost_modes, use_container_width=True)

# ==================== MAIN APPLICATION ====================

def main():
    init_session_state()
    
    st.title("🚗 FRACAS - Advanced Military Vehicle Maintenance System")
    st.markdown("**Failure Reporting, Analysis, and Corrective Action System**")
    
    # Check for anomalies on startup
    if st.session_state.df_cleaned is not None and not st.session_state.anomalies.empty:
        st.warning(f"⚠️ {len(st.session_state.anomalies)} active anomalies detected! Check the Anomalies tab for details.")
    
    # Sidebar
    render_sidebar()
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📂 Data",
        "📝 Failures",
        "📊 Analysis",
        "📋 CAPA",
        "📈 Reliability",
        "⚠️ Anomalies",
        "💰 Costs"
    ])
    
    with tab1:
        render_data_tab()
    
    with tab2:
        render_failures_tab()
    
    with tab3:
        render_analysis_tab()
    
    with tab4:
        render_capa_tab()
    
    with tab5:
        render_reliability_tab()
    
    with tab6:
        render_anomalies_tab()
    
    with tab7:
        render_costs_tab()

if __name__ == "__main__":
    main()
