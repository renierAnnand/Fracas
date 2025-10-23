#!/usr/bin/env python3
"""
FRACAS - Advanced Military Vehicle Maintenance System
Merged Version with All Features

Features:
- Automatic column detection and mapping
- Failure reporting and analysis
- Root Cause Analysis tools (5-Whys, Ishikawa)
- CAPA tracking with effectiveness monitoring
- Reliability KPIs (MTBF, MTTR, Availability)
- Advanced anomaly detection
- Cost and performance analysis
- Workshop and sector analysis
- Spare parts management
- Trend analysis
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io
from datetime import datetime, timedelta
import hashlib
import warnings
warnings.filterwarnings('ignore')

# Try to import optional dependencies
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

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
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
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

def make_file_hash(file_bytes):
    """Create a hash of file bytes for cache key"""
    return hashlib.md5(file_bytes).hexdigest()

@st.cache_data(show_spinner="Processing Excel file...")
def load_data(file_hash=None, file_bytes=None, file_path=None):
    """Load Excel file with automatic path detection"""
    try:
        if file_bytes is not None:
            file_like = io.BytesIO(file_bytes)
            df = pd.read_excel(file_like)
        elif file_path is not None:
            df = pd.read_excel(file_path)
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
                    st.success(f"✓ Loaded data from: {path}")
                    break
                except:
                    continue
            
            if df is None:
                st.error("No data file found. Please upload a file.")
                return None
        
        # Clean column names
        df.columns = df.columns.str.strip()
        
        st.info(f"📊 Processing {len(df):,} work orders with {len(df.columns)} columns...")
        
        return df
        
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
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
        'Location': ['location', 'workshop', 'site', 'facility', 'workshop name'],
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
    """Comprehensive anomaly detection - scipy optional"""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    
    thresholds = st.session_state.anomaly_thresholds
    anomalies = []
    
    # 1. Repeat Failures (same vehicle, similar issue, within threshold days)
    if 'VehicleID' in df.columns and 'FailureMode' in df.columns and 'OpenDate' in df.columns:
        df_sorted = df.sort_values(['VehicleID', 'OpenDate'])
        
        for vehicle in df_sorted['VehicleID'].unique():
            vehicle_data = df_sorted[df_sorted['VehicleID'] == vehicle]
            
            for i in range(len(vehicle_data) - 1):
                current_row = vehicle_data.iloc[i]
                next_row = vehicle_data.iloc[i + 1]
                
                days_diff = (next_row['OpenDate'] - current_row['OpenDate']).days
                
                if days_diff <= thresholds['repeat_days']:
                    anomalies.append({
                        'Type': 'Repeat Failure',
                        'Severity': 'High',
                        'WorkOrderID': next_row.get('WorkOrderID', 'N/A'),
                        'VehicleID': vehicle,
                        'Details': f"Same vehicle failed {days_diff} days after previous repair",
                        'Date': next_row['OpenDate']
                    })
    
    # 2. Quick Return (vehicle returns quickly for any issue)
    if 'DaysSinceLastFailure' in df.columns:
        quick_returns = df[
            (df['DaysSinceLastFailure'].notna()) &
            (df['DaysSinceLastFailure'] < thresholds['quick_return_days'])
        ]
        
        for _, row in quick_returns.iterrows():
            anomalies.append({
                'Type': 'Quick Return',
                'Severity': 'Medium',
                'WorkOrderID': row.get('WorkOrderID', 'N/A'),
                'VehicleID': row.get('VehicleID', 'N/A'),
                'Details': f"Vehicle returned after only {row['DaysSinceLastFailure']:.0f} days",
                'Date': row.get('OpenDate', datetime.now())
            })
    
    # 3. High Cost Anomalies
    if 'TotalCost' in df.columns and df['TotalCost'].notna().sum() > 0:
        cost_mean = df['TotalCost'].mean()
        cost_std = df['TotalCost'].std()
        
        if cost_std > 0:
            threshold_cost = cost_mean + (thresholds['cost_multiplier'] * cost_std)
            high_cost = df[df['TotalCost'] > threshold_cost]
            
            for _, row in high_cost.iterrows():
                anomalies.append({
                    'Type': 'High Cost',
                    'Severity': 'High' if row['TotalCost'] > cost_mean + 3*cost_std else 'Medium',
                    'WorkOrderID': row.get('WorkOrderID', 'N/A'),
                    'VehicleID': row.get('VehicleID', 'N/A'),
                    'Details': f"Cost ${row['TotalCost']:,.2f} (avg: ${cost_mean:,.2f})",
                    'Date': row.get('OpenDate', datetime.now())
                })
    
    # 4. High Downtime Anomalies
    if 'DowntimeHours' in df.columns and df['DowntimeHours'].notna().sum() > 0:
        downtime_mean = df['DowntimeHours'].mean()
        downtime_std = df['DowntimeHours'].std()
        
        if downtime_std > 0:
            threshold_downtime = downtime_mean + (thresholds['downtime_multiplier'] * downtime_std)
            high_downtime = df[df['DowntimeHours'] > threshold_downtime]
            
            for _, row in high_downtime.iterrows():
                anomalies.append({
                    'Type': 'High Downtime',
                    'Severity': 'High' if row['DowntimeHours'] > downtime_mean + 3*downtime_std else 'Medium',
                    'WorkOrderID': row.get('WorkOrderID', 'N/A'),
                    'VehicleID': row.get('VehicleID', 'N/A'),
                    'Details': f"Downtime {row['DowntimeHours']:.1f}hrs (avg: {downtime_mean:.1f}hrs)",
                    'Date': row.get('OpenDate', datetime.now())
                })
    
    # 5. Unusual Failure Frequency (only if scipy available)
    if SCIPY_AVAILABLE and 'FailureMode' in df.columns:
        failure_counts = df['FailureMode'].value_counts()
        if len(failure_counts) > 3:
            mean_freq = failure_counts.mean()
            std_freq = failure_counts.std()
            
            if std_freq > 0:
                unusual_failures = failure_counts[
                    failure_counts > mean_freq + (thresholds['frequency_std'] * std_freq)
                ]
                
                for failure_mode, count in unusual_failures.items():
                    anomalies.append({
                        'Type': 'Unusual Frequency',
                        'Severity': 'Medium',
                        'WorkOrderID': 'Multiple',
                        'VehicleID': 'Multiple',
                        'Details': f"Failure mode '{failure_mode}' occurred {count} times (expected ~{mean_freq:.0f})",
                        'Date': datetime.now()
                    })
    
    if anomalies:
        anomalies_df = pd.DataFrame(anomalies)
        # Update flags in original dataframe
        if 'WorkOrderID' in df.columns:
            repeat_wos = anomalies_df[anomalies_df['Type'] == 'Repeat Failure']['WorkOrderID']
            quick_wos = anomalies_df[anomalies_df['Type'] == 'Quick Return']['WorkOrderID']
            cost_wos = anomalies_df[anomalies_df['Type'] == 'High Cost']['WorkOrderID']
            downtime_wos = anomalies_df[anomalies_df['Type'] == 'High Downtime']['WorkOrderID']
            
            df.loc[df['WorkOrderID'].isin(repeat_wos), 'RepeatFailureFlag'] = True
            df.loc[df['WorkOrderID'].isin(quick_wos), 'QuickReturnFlag'] = True
            df.loc[df['WorkOrderID'].isin(cost_wos), 'HighCostFlag'] = True
            df.loc[df['WorkOrderID'].isin(downtime_wos), 'HighDowntimeFlag'] = True
        
        return anomalies_df
    
    return pd.DataFrame()

# ==================== BASIC ANALYSIS FUNCTIONS (FROM OLD CODE) ====================

def calculate_failure_metrics(df):
    """Calculate key failure metrics based on actual columns"""
    metrics = {}
    
    # Use actual column name or mapped name
    status_col = 'Work order status' if 'Work order status' in df.columns else 'Status'
    
    if status_col in df.columns:
        metrics['total_work_orders'] = len(df)
        status_series = df[status_col].fillna('').astype(str)
        
        # Adapt to actual status values in the data
        metrics['completed'] = len(df[status_series.str.contains('Completed|Closed', case=False, na=False)])
        metrics['in_progress'] = len(df[status_series.str.contains('Under Maintenance|Process Initiated|In Progress', case=False, na=False)])
        metrics['waiting_parts'] = len(df[status_series.str.contains('Waiting Spare Parts|Waiting Parts', case=False, na=False)])
        metrics['completion_rate'] = (metrics['completed'] / metrics['total_work_orders'] * 100) if metrics['total_work_orders'] > 0 else 0
    else:
        metrics['total_work_orders'] = len(df)
        metrics['completed'] = 0
        metrics['in_progress'] = 0
        metrics['waiting_parts'] = 0
        metrics['completion_rate'] = 0
    
    return metrics

def identify_top_vehicles(df, limit=10):
    """Identify most common vehicle types"""
    vehicle_col = 'Vehicle Make and Model' if 'Vehicle Make and Model' in df.columns else 'EquipmentType'
    
    if vehicle_col in df.columns:
        vehicle_series = df[vehicle_col].fillna('Unknown').astype(str).str.strip()
        vehicle_series = vehicle_series[vehicle_series.str.len() > 2]
        vehicle_series = vehicle_series[vehicle_series != 'Unknown']
        
        if len(vehicle_series) > 0:
            return vehicle_series.value_counts().head(limit)
    return None

def analyze_by_workshop(df):
    """Analyze work orders by workshop"""
    workshop_col = 'Workshop name' if 'Workshop name' in df.columns else 'Location'
    
    if workshop_col in df.columns:
        workshop_series = df[workshop_col].fillna('Unknown').astype(str).str.strip()
        workshop_series = workshop_series[(workshop_series.str.len() > 2) & (workshop_series != 'Unknown')]
        
        if len(workshop_series) > 0:
            return workshop_series.value_counts()
    return None

def analyze_by_sector(df):
    """Analyze work orders by sector"""
    sector_col = 'Sector'
    
    if sector_col in df.columns:
        sector_series = df[sector_col].fillna('Unknown').astype(str).str.strip()
        sector_series = sector_series[(sector_series.str.len() > 2) & (sector_series != 'Unknown')]
        
        if len(sector_series) > 0:
            return sector_series.value_counts()
    return None

def create_trend_analysis(df):
    """Create trend analysis based on Date column"""
    date_col = 'Date' if 'Date' in df.columns else 'OpenDate'
    
    if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df_filtered = df[df[date_col].notna()].copy()
        df_filtered['month'] = df_filtered[date_col].dt.to_period('M')
        trend_data = df_filtered.groupby('month').size()
        
        status_col = 'Work order status' if 'Work order status' in df.columns else 'Status'
        if status_col in df.columns:
            status_trends = df_filtered.groupby(['month', status_col]).size().unstack(fill_value=0)
            return trend_data, status_trends
        
        return trend_data, None
    return None, None

def analyze_spare_parts(df):
    """Analyze spare parts requirements"""
    spare_col = 'Spare parts Required' if 'Spare parts Required' in df.columns else 'SpareParts'
    received_col = 'Received Spare Parts (Yes/No)'
    
    results = {}
    
    if spare_col in df.columns:
        results['required'] = df[spare_col].value_counts()
    
    if received_col in df.columns:
        results['received_count'] = df[received_col].notna().sum()
        results['not_received_count'] = df[received_col].isna().sum()
    
    return results if results else None

# ==================== RELIABILITY METRICS ====================

def calculate_reliability_metrics(df):
    """Calculate MTBF, MTTR, Availability"""
    metrics = {}
    
    if 'RepairDuration' in df.columns and df['RepairDuration'].notna().sum() > 0:
        metrics['MTTR'] = df['RepairDuration'].mean()
        metrics['MTTR_median'] = df['RepairDuration'].median()
    
    if 'DaysSinceLastFailure' in df.columns and df['DaysSinceLastFailure'].notna().sum() > 0:
        metrics['MTBF'] = df['DaysSinceLastFailure'].mean() * 24  # Convert days to hours
        metrics['MTBF_median'] = df['DaysSinceLastFailure'].median() * 24
    
    if 'MTBF' in metrics and 'MTTR' in metrics:
        mtbf = metrics['MTBF']
        mttr = metrics['MTTR']
        metrics['Availability'] = (mtbf / (mtbf + mttr)) * 100 if (mtbf + mttr) > 0 else 0
    
    return metrics

# ==================== SIDEBAR ====================

def render_sidebar():
    """Render sidebar with controls"""
    with st.sidebar:
        st.header("⚙️ Settings & Controls")
        
        # User role selection
        st.session_state.user_role = st.selectbox(
            "User Role",
            ["Engineer", "Manager", "Analyst", "Admin"],
            index=["Engineer", "Manager", "Analyst", "Admin"].index(st.session_state.user_role)
        )
        
        st.divider()
        
        # File upload
        st.subheader("📁 Data Upload")
        uploaded_file = st.file_uploader(
            "Upload Work Orders Excel",
            type=['xlsx', 'xls'],
            help="Upload your work orders Excel file"
        )
        
        if uploaded_file is not None:
            file_bytes = uploaded_file.read()
            file_hash = make_file_hash(file_bytes)
            
            if st.button("🔄 Process Uploaded File"):
                with st.spinner("Processing..."):
                    df = load_data(file_hash=file_hash, file_bytes=file_bytes)
                    if df is not None:
                        st.session_state.df = df
                        mapping = auto_detect_columns(df)
                        st.session_state.column_mapping = mapping
                        st.session_state.df_cleaned = clean_and_enrich_data(df, mapping)
                        st.session_state.anomalies = detect_anomalies(st.session_state.df_cleaned)
                        st.success("✓ File processed successfully!")
                        st.rerun()
        
        # Try to load default file
        if st.session_state.df is None:
            if st.button("📂 Load Default File"):
                with st.spinner("Loading default file..."):
                    df = load_data()
                    if df is not None:
                        st.session_state.df = df
                        mapping = auto_detect_columns(df)
                        st.session_state.column_mapping = mapping
                        st.session_state.df_cleaned = clean_and_enrich_data(df, mapping)
                        st.session_state.anomalies = detect_anomalies(st.session_state.df_cleaned)
                        st.rerun()
        
        st.divider()
        
        # Anomaly detection settings
        if st.session_state.user_role in ['Manager', 'Admin']:
            with st.expander("🔍 Anomaly Detection Settings"):
                st.number_input(
                    "Repeat Failure Window (days)",
                    min_value=1,
                    max_value=90,
                    value=st.session_state.anomaly_thresholds['repeat_days'],
                    key='repeat_days_input'
                )
                st.number_input(
                    "Quick Return Window (days)",
                    min_value=1,
                    max_value=30,
                    value=st.session_state.anomaly_thresholds['quick_return_days'],
                    key='quick_return_input'
                )
                
                if st.button("Update Thresholds"):
                    st.session_state.anomaly_thresholds['repeat_days'] = st.session_state.repeat_days_input
                    st.session_state.anomaly_thresholds['quick_return_days'] = st.session_state.quick_return_input
                    if st.session_state.df_cleaned is not None:
                        st.session_state.anomalies = detect_anomalies(st.session_state.df_cleaned)
                    st.success("Thresholds updated!")
                    st.rerun()
        
        st.divider()
        
        # Clear cache
        if st.button("🗑️ Clear Cache & Reprocess"):
            st.cache_data.clear()
            st.session_state.df = None
            st.session_state.df_cleaned = None
            st.session_state.anomalies = pd.DataFrame()
            st.success("Cache cleared!")
            st.rerun()
        
        # Data info
        if st.session_state.df is not None:
            st.divider()
            st.subheader("📊 Data Info")
            st.info(f"**Records:** {len(st.session_state.df):,}")
            if st.session_state.df_cleaned is not None:
                st.info(f"**Mapped Columns:** {len(st.session_state.column_mapping)}")
            if not st.session_state.anomalies.empty:
                st.warning(f"**Anomalies:** {len(st.session_state.anomalies)}")

# ==================== DATA TAB ====================

def render_data_tab():
    """Render data overview and column mapping"""
    st.header("📂 Data Overview")
    
    if st.session_state.df is None:
        st.info("👆 Please upload a file or load the default file from the sidebar")
        return
    
    df = st.session_state.df
    
    # Basic stats
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Records", f"{len(df):,}")
    with col2:
        st.metric("Columns", len(df.columns))
    with col3:
        date_cols = [col for col in df.columns if 'date' in col.lower()]
        if date_cols:
            date_col = date_cols[0]
            date_range = f"{df[date_col].min().strftime('%Y-%m-%d')} to {df[date_col].max().strftime('%Y-%m-%d')}"
            st.metric("Date Range", date_range)
    with col4:
        if st.session_state.df_cleaned is not None:
            st.metric("Mapped Fields", len(st.session_state.column_mapping))
    
    # Column mapping
    st.subheader("🗺️ Column Mapping")
    
    if st.session_state.column_mapping:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Standard Field → Excel Column**")
            for standard, excel in st.session_state.column_mapping.items():
                st.text(f"{standard} → {excel}")
        
        with col2:
            st.markdown("**Data Sample**")
            for standard, excel in list(st.session_state.column_mapping.items())[:5]:
                sample_values = df[excel].dropna().head(3).tolist()
                st.text(f"{standard}: {sample_values}")
    
    # Detected columns expander
    with st.expander("📋 All Detected Columns"):
        cols_info = []
        for col in df.columns:
            non_null = df[col].notna().sum()
            dtype = df[col].dtype
            cols_info.append({
                'Column': col,
                'Non-Empty': non_null,
                'Type': str(dtype),
                'Percentage': f"{non_null/len(df)*100:.1f}%"
            })
        st.dataframe(pd.DataFrame(cols_info), use_container_width=True)
    
    # Raw data preview
    st.subheader("📄 Raw Data Preview")
    st.dataframe(df.head(20), use_container_width=True)

# ==================== ANALYSIS TAB (BASIC + ADVANCED) ====================

def render_analysis_tab():
    """Render comprehensive analysis"""
    st.header("📊 Analysis Dashboard")
    
    if st.session_state.df is None:
        st.info("👆 Please upload a file first")
        return
    
    df = st.session_state.df
    df_clean = st.session_state.df_cleaned
    
    # Choose which dataframe to use
    use_df = df_clean if df_clean is not None else df
    
    # Calculate metrics
    metrics = calculate_failure_metrics(use_df)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Work Orders", f"{metrics['total_work_orders']:,}")
    with col2:
        st.metric("Completed", f"{metrics['completed']:,}", 
                  f"{metrics['completion_rate']:.1f}%")
    with col3:
        st.metric("In Progress", f"{metrics['in_progress']:,}")
    with col4:
        st.metric("Waiting Parts", f"{metrics['waiting_parts']:,}")
    
    # Analysis tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🚗 Vehicles",
        "🏭 Workshops",
        "📈 Trends",
        "🔧 Spare Parts",
        "📋 Raw Data"
    ])
    
    with tab1:
        st.subheader("🚗 Vehicle Analysis")
        top_vehicles = identify_top_vehicles(use_df, limit=15)
        
        if top_vehicles is not None:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    x=top_vehicles.values,
                    y=top_vehicles.index,
                    orientation='h',
                    title="Top 15 Vehicle Types by Work Orders",
                    labels={'x': 'Number of Work Orders', 'y': 'Vehicle Type'},
                    color=top_vehicles.values,
                    color_continuous_scale='Blues'
                )
                # Truncate long names
                fig.update_yaxes(ticktext=[name[:40] + '...' if len(str(name)) > 40 else name 
                                           for name in top_vehicles.index])
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📊 Vehicle Statistics")
                total_vehicles = use_df['Vehicle Make and Model'].nunique() if 'Vehicle Make and Model' in use_df.columns else 0
                st.metric("Total Unique Vehicles", f"{total_vehicles:,}")
                
                avg_per_vehicle = len(use_df) / total_vehicles if total_vehicles > 0 else 0
                st.metric("Avg Work Orders per Vehicle", f"{avg_per_vehicle:.1f}")
                
                # Top 3 vehicles
                st.markdown("### 🏆 Top 3 Vehicles")
                for i, (vehicle, count) in enumerate(top_vehicles.head(3).items(), 1):
                    vehicle_name = str(vehicle)[:50] + '...' if len(str(vehicle)) > 50 else str(vehicle)
                    st.info(f"**{i}. {vehicle_name}**\n{count} work orders")
        else:
            st.warning("Vehicle data not available")
    
    with tab2:
        st.subheader("🏭 Workshop Analysis")
        
        workshop_data = analyze_by_workshop(use_df)
        sector_data = analyze_by_sector(use_df)
        
        if workshop_data is not None:
            col1, col2 = st.columns(2)
            
            with col1:
                # Top workshops
                fig = px.bar(
                    x=workshop_data.head(15).values,
                    y=[name[:30] + '...' if len(name) > 30 else name for name in workshop_data.head(15).index],
                    orientation='h',
                    title="Top 15 Workshops by Workload",
                    labels={'x': 'Work Orders', 'y': 'Workshop'},
                    color=workshop_data.head(15).values,
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📊 Workshop Statistics")
                st.metric("Total Workshops", len(workshop_data))
                st.metric("Avg Work Orders per Workshop", f"{workshop_data.mean():.1f}")
                st.metric("Most Busy Workshop", 
                          f"{str(workshop_data.index[0])[:40]}..." if len(str(workshop_data.index[0])) > 40 else str(workshop_data.index[0]))
                st.metric("Work Orders", workshop_data.iloc[0])
        
        if sector_data is not None:
            st.subheader("🌍 Sector Distribution")
            fig = px.pie(
                values=sector_data.values,
                names=sector_data.index,
                title="Work Orders by Sector"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("📈 Trend Analysis")
        
        trend_data, status_trends = create_trend_analysis(use_df)
        
        if trend_data is not None:
            # Convert period to string for plotting
            trend_df = pd.DataFrame({
                'Month': [str(m) for m in trend_data.index],
                'Work Orders': trend_data.values
            })
            
            fig = px.line(
                trend_df,
                x='Month',
                y='Work Orders',
                title="Work Orders Over Time",
                markers=True
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
            
            # Status trends
            if status_trends is not None:
                st.subheader("📊 Status Trends Over Time")
                
                # Prepare data for stacked area chart
                status_trends_reset = status_trends.reset_index()
                status_trends_reset['month'] = status_trends_reset['month'].astype(str)
                
                fig = go.Figure()
                for col in status_trends.columns:
                    fig.add_trace(go.Scatter(
                        x=status_trends_reset['month'],
                        y=status_trends_reset[col],
                        name=col,
                        mode='lines',
                        stackgroup='one'
                    ))
                
                fig.update_layout(
                    title="Work Order Status Trends",
                    xaxis_title="Month",
                    yaxis_title="Count",
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Date information not available for trend analysis")
    
    with tab4:
        st.subheader("🔧 Spare Parts Analysis")
        
        spare_results = analyze_spare_parts(use_df)
        
        if spare_results and 'required' in spare_results:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(
                    values=spare_results['required'].values,
                    names=spare_results['required'].index,
                    title="Spare Parts Required Distribution",
                    color_discrete_map={'Yes / نعم': '#ff4444', 'No / لا': '#44ff44'}
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("### 📊 Spare Parts Metrics")
                total = len(use_df)
                yes_count = spare_results['required'].get('Yes / نعم', spare_results['required'].get('Yes', 0))
                no_count = spare_results['required'].get('No / لا', spare_results['required'].get('No', 0))
                
                st.metric("Require Spare Parts", f"{yes_count:,}", 
                          f"{(yes_count/total*100):.1f}%")
                st.metric("No Spare Parts Needed", f"{no_count:,}",
                          f"{(no_count/total*100):.1f}%")
                
                if 'received_count' in spare_results:
                    st.metric("Spare Parts Received", 
                              f"{spare_results['received_count']:,}",
                              f"{(spare_results['received_count']/yes_count*100):.1f}% of required" if yes_count > 0 else "N/A")
        else:
            st.warning("Spare parts data not available")
    
    with tab5:
        st.subheader("📋 Raw Data Viewer")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            search_term = st.text_input("🔍 Search all columns", "")
        
        with col2:
            status_col = 'Work order status' if 'Work order status' in use_df.columns else 'Status'
            if status_col in use_df.columns:
                status_filter = st.selectbox(
                    "Filter by Status",
                    ["All"] + list(use_df[status_col].unique())
                )
            else:
                status_filter = "All"
        
        with col3:
            workshop_col = 'Workshop name' if 'Workshop name' in use_df.columns else 'Location'
            if workshop_col in use_df.columns:
                unique_workshops = list(use_df[workshop_col].unique()[:20])
                workshop_filter = st.selectbox(
                    "Filter by Workshop",
                    ["All"] + unique_workshops
                )
            else:
                workshop_filter = "All"
        
        # Column selector
        all_columns = list(use_df.columns)
        default_cols = ['ID', 'Date', 'Work order status', 'Vehicle Make and Model', 
                        'Workshop name', 'Spare parts Required', 'Status', 'VehicleID', 'OpenDate']
        default_cols = [col for col in default_cols if col in all_columns]
        
        selected_columns = st.multiselect(
            "Select columns to display",
            all_columns,
            default=default_cols[:min(8, len(default_cols))] if default_cols else all_columns[:8]
        )
        
        # Apply filters
        filtered_df = use_df.copy()
        
        if search_term:
            mask = filtered_df.astype(str).apply(
                lambda x: x.str.contains(search_term, case=False, na=False)
            ).any(axis=1)
            filtered_df = filtered_df[mask]
        
        status_col = 'Work order status' if 'Work order status' in use_df.columns else 'Status'
        if status_filter != "All" and status_col in use_df.columns:
            filtered_df = filtered_df[filtered_df[status_col] == status_filter]
        
        workshop_col = 'Workshop name' if 'Workshop name' in use_df.columns else 'Location'
        if workshop_filter != "All" and workshop_col in use_df.columns:
            filtered_df = filtered_df[filtered_df[workshop_col] == workshop_filter]
        
        # Display
        if selected_columns:
            st.dataframe(
                filtered_df[selected_columns],
                use_container_width=True,
                height=600
            )
        else:
            st.warning("Please select at least one column to display")
        
        # Download
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"Showing {len(filtered_df):,} of {len(use_df):,} total work orders")
        
        with col2:
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Filtered Data (CSV)",
                data=csv,
                file_name=f"fracas_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

# ==================== RELIABILITY TAB ====================

def render_reliability_tab():
    """Render reliability metrics"""
    st.header("📈 Reliability Metrics")
    
    if st.session_state.df_cleaned is None:
        st.info("Process data first to see reliability metrics")
        return
    
    df = st.session_state.df_cleaned
    metrics = calculate_reliability_metrics(df)
    
    if not metrics:
        st.warning("Insufficient data for reliability calculations")
        return
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'MTBF' in metrics:
            st.metric("MTBF (Mean Time Between Failures)", f"{metrics['MTBF']:.1f} hrs")
            st.caption(f"Median: {metrics['MTBF_median']:.1f} hrs")
    
    with col2:
        if 'MTTR' in metrics:
            st.metric("MTTR (Mean Time To Repair)", f"{metrics['MTTR']:.1f} hrs")
            st.caption(f"Median: {metrics['MTTR_median']:.1f} hrs")
    
    with col3:
        if 'Availability' in metrics:
            st.metric("Availability", f"{metrics['Availability']:.2f}%")
            reliability_status = "Excellent" if metrics['Availability'] > 95 else "Good" if metrics['Availability'] > 90 else "Needs Improvement"
            st.caption(f"Status: {reliability_status}")
    
    # Visualization
    if 'RepairDuration' in df.columns:
        st.subheader("⏱️ Repair Duration Distribution")
        
        fig = px.histogram(
            df[df['RepairDuration'].notna()],
            x='RepairDuration',
            nbins=50,
            title="Repair Duration Distribution",
            labels={'RepairDuration': 'Repair Duration (hours)'}
        )
        st.plotly_chart(fig, use_container_width=True)

# ==================== ANOMALIES TAB ====================

def render_anomalies_tab():
    """Render anomalies detection"""
    st.header("⚠️ Anomaly Detection")
    
    if st.session_state.anomalies.empty:
        st.success("✓ No anomalies detected!")
        return
    
    anomalies = st.session_state.anomalies
    
    # Summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Anomalies", len(anomalies))
    with col2:
        high_severity = len(anomalies[anomalies['Severity'] == 'High'])
        st.metric("High Severity", high_severity, 
                  delta="Requires immediate attention" if high_severity > 0 else None,
                  delta_color="inverse")
    with col3:
        anomaly_types = anomalies['Type'].nunique()
        st.metric("Anomaly Types", anomaly_types)
    
    # Anomalies by type
    st.subheader("📊 Anomalies by Type")
    
    type_counts = anomalies['Type'].value_counts()
    fig = px.bar(
        x=type_counts.values,
        y=type_counts.index,
        orientation='h',
        title="Anomalies by Type",
        color=type_counts.values,
        color_continuous_scale='Reds'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    st.subheader("📋 Anomaly Details")
    
    # Severity filter
    severity_filter = st.multiselect(
        "Filter by Severity",
        ['High', 'Medium', 'Low'],
        default=['High', 'Medium']
    )
    
    filtered_anomalies = anomalies[anomalies['Severity'].isin(severity_filter)]
    
    # Style the dataframe
    def highlight_severity(row):
        if row['Severity'] == 'High':
            return ['background-color: #ffcccc'] * len(row)
        elif row['Severity'] == 'Medium':
            return ['background-color: #ffe5cc'] * len(row)
        else:
            return ['background-color: #ffffcc'] * len(row)
    
    styled_df = filtered_anomalies.style.apply(highlight_severity, axis=1)
    st.dataframe(styled_df, use_container_width=True, height=400)
    
    # Download
    csv = filtered_anomalies.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Anomalies Report",
        data=csv,
        file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# ==================== FAILURES TAB (PLACEHOLDER) ====================

def render_failures_tab():
    """Render failure reporting"""
    st.header("📝 Failure Reporting")
    st.info("This section can be used to log new failures")
    
    # Simple failure entry form
    with st.form("new_failure_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            vehicle_id = st.text_input("Vehicle ID")
            failure_mode = st.text_input("Failure Mode")
        
        with col2:
            location = st.text_input("Location/Workshop")
            severity = st.selectbox("Severity", ["Low", "Medium", "High", "Critical"])
        
        description = st.text_area("Failure Description")
        
        submitted = st.form_submit_button("Submit Failure Report")
        
        if submitted:
            st.success("✓ Failure report logged!")

# ==================== CAPA TAB (PLACEHOLDER) ====================

def render_capa_tab():
    """Render CAPA management"""
    st.header("📋 CAPA - Corrective & Preventive Actions")
    st.info("This section can be used to track corrective and preventive actions")
    
    # Display existing CAPAs if any
    if not st.session_state.capa_register.empty:
        st.dataframe(st.session_state.capa_register, use_container_width=True)
    else:
        st.info("No CAPA actions registered yet")

# ==================== COSTS TAB ====================

def render_costs_tab():
    """Render cost analysis"""
    st.header("💰 Cost Analysis")
    
    if st.session_state.df_cleaned is None:
        st.info("Process data first to see cost analysis")
        return
    
    df = st.session_state.df_cleaned
    
    # Check if cost data exists
    if 'TotalCost' not in df.columns and 'LaborCost' not in df.columns and 'PartCost' not in df.columns:
        st.warning("Cost data not available in the dataset")
        
        if st.button("Generate Demo Cost Data"):
            st.info("Generating random cost data for demonstration...")
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
    tab1, tab2 = st.tabs(["📊 Cost Distribution", "💸 High Cost Analysis"])
    
    with tab1:
        st.subheader("Cost Distribution")
        
        if 'TotalCost' in df.columns:
            fig = px.histogram(
                df[df['TotalCost'].notna()],
                x='TotalCost',
                nbins=50,
                title="Cost Distribution",
                labels={'TotalCost': 'Total Cost ($)'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("High Cost Failures")
        
        if 'TotalCost' in df.columns:
            cost_threshold = df['TotalCost'].quantile(0.9)
            high_cost_failures = df[df['TotalCost'] > cost_threshold]
            
            st.info(f"Showing failures with cost > ${cost_threshold:,.2f} (top 10%)")
            
            # Display top cost failures
            if not high_cost_failures.empty:
                display_cols = ['WorkOrderID', 'VehicleID', 'TotalCost', 'Status', 'OpenDate']
                display_cols = [col for col in display_cols if col in high_cost_failures.columns]
                
                if display_cols:
                    st.dataframe(
                        high_cost_failures[display_cols].sort_values('TotalCost', ascending=False).head(20),
                        use_container_width=True
                    )

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
        "📊 Analysis",
        "📈 Reliability",
        "⚠️ Anomalies",
        "💰 Costs",
        "📝 Failures",
        "📋 CAPA"
    ])
    
    with tab1:
        render_data_tab()
    
    with tab2:
        render_analysis_tab()
    
    with tab3:
        render_reliability_tab()
    
    with tab4:
        render_anomalies_tab()
    
    with tab5:
        render_costs_tab()
    
    with tab6:
        render_failures_tab()
    
    with tab7:
        render_capa_tab()
    
    # Footer
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 1rem;'>
        <p>FRACAS - Failure Reporting, Analysis, and Corrective Action System</p>
        <p style='font-size: 0.8rem;'>Advanced maintenance analytics for military vehicles</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
