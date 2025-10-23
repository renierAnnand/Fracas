#!/usr/bin/env python3
"""
FRACAS - Failure Reporting, Analysis, and Corrective Action System
Military Vehicle Maintenance Management System

QUICKSTART:
1. Install requirements: pip install streamlit pandas numpy plotly openpyxl xlsxwriter
2. Run: streamlit run fracas_system.py
3. Upload your Excel file or use default path
4. Map columns to expected schema
5. Begin analysis and tracking

Author: FRACAS System v1.0
Domain: Military Vehicle Maintenance & Reliability Engineering
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
import hashlib
from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="FRACAS - Military Vehicle Maintenance",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def init_session_state():
    """Initialize all session state variables"""
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'df_cleaned' not in st.session_state:
        st.session_state.df_cleaned = None
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = {}
    if 'df_new_failures' not in st.session_state:
        st.session_state.df_new_failures = pd.DataFrame()
    if 'rca_store' not in st.session_state:
        st.session_state.rca_store = pd.DataFrame(columns=['WorkOrderID', 'RCA_Type', 'Content', 'Timestamp', 'User'])
    if 'fmea_data' not in st.session_state:
        st.session_state.fmea_data = pd.DataFrame(columns=[
            'Function_Item', 'FailureMode', 'Effect', 'Cause', 'CurrentControls',
            'Severity', 'Occurrence', 'Detection', 'RPN', 'RecommendedAction',
            'Owner', 'DueDate', 'Status'
        ])
    if 'capa_register' not in st.session_state:
        st.session_state.capa_register = pd.DataFrame(columns=[
            'ActionID', 'LinkedWorkOrderID', 'ProblemSummary', 'RootCauseRef',
            'ActionType', 'Owner', 'DueDate', 'Priority', 'Status',
            'EffectivenessCheck', 'VerifiedBy', 'VerifiedDate'
        ])
    if 'audit_trail' not in st.session_state:
        st.session_state.audit_trail = pd.DataFrame(columns=[
            'Timestamp', 'User', 'Action', 'Module', 'Details'
        ])
    if 'user_role' not in st.session_state:
        st.session_state.user_role = 'Engineer'
    if 'reference_data' not in st.session_state:
        st.session_state.reference_data = {
            'failure_modes': ['Mechanical Failure', 'Electrical Fault', 'Hydraulic Leak', 
                            'Software Error', 'Wear & Tear', 'Corrosion', 'Overheating',
                            'Contamination', 'Fatigue Failure', 'Impact Damage'],
            'subsystems': ['Engine', 'Transmission', 'Suspension', 'Electrical', 
                         'Hydraulic', 'Braking', 'Steering', 'Cooling', 'Fuel', 'Exhaust'],
            'severity_levels': ['Critical', 'Major', 'Minor'],
            'locations': ['Base Alpha', 'Base Bravo', 'Base Charlie', 'Field Operations', 'Depot']
        }

# Canonical schema definition
CANONICAL_SCHEMA = {
    'WorkOrderID': 'Work order identifier',
    'VehicleID': 'Vehicle identification number',
    'EquipmentType': 'Type of vehicle/equipment',
    'OpenDate': 'Date work order opened',
    'CloseDate': 'Date work order closed',
    'ReportedBy': 'Person who reported the issue',
    'Location': 'Location of vehicle/maintenance',
    'Unit': 'Military unit',
    'OdometerKM': 'Odometer reading in kilometers',
    'OperatingHours': 'Equipment operating hours',
    'FailureDesc': 'Description of failure',
    'FailureMode': 'Type/mode of failure',
    'Subsystem': 'Affected subsystem',
    'RootCause': 'Root cause of failure',
    'Severity': 'Severity level (Critical/Major/Minor)',
    'Status': 'Work order status',
    'PartNumber': 'Part number used',
    'PartName': 'Part name/description',
    'PartCost': 'Cost of parts',
    'LaborHours': 'Labor hours spent',
    'LaborCost': 'Labor cost',
    'DowntimeHours': 'Equipment downtime in hours',
    'Notes': 'Additional notes'
}

def add_audit_entry(action: str, module: str, details: str):
    """Add entry to audit trail"""
    new_entry = pd.DataFrame({
        'Timestamp': [datetime.now()],
        'User': [st.session_state.user_role],
        'Action': [action],
        'Module': [module],
        'Details': [details]
    })
    st.session_state.audit_trail = pd.concat([st.session_state.audit_trail, new_entry], ignore_index=True)

@st.cache_data
def load_excel_file(file_path: str = None, uploaded_file=None) -> pd.DataFrame:
    """Load Excel file with error handling"""
    try:
        if uploaded_file is not None:
            # Reset file pointer to beginning
            if hasattr(uploaded_file, 'seek'):
                uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        elif file_path:
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            # Try default paths
            try:
                df = pd.read_excel('/mnt/user-data/uploads/Latest_WO.xlsx', engine='openpyxl')
            except:
                try:
                    df = pd.read_excel('/mnt/data/Latest WO.xlsx', engine='openpyxl')
                except:
                    st.error("No file found at default locations. Please upload a file.")
                    return None
        
        # Basic data type cleanup
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    # Try to convert to datetime if it looks like dates
                    if df[col].astype(str).str.contains(r'\d{4}-\d{2}-\d{2}', na=False).any():
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass
        
        return df
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        st.info("Please ensure the file is a valid Excel file (.xlsx or .xls)")
        return None

def auto_map_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Automatically map DataFrame columns to canonical schema"""
    mapping = {}
    df_cols_lower = {col: col.lower() for col in df.columns}
    
    # Mapping rules
    mapping_rules = {
        'WorkOrderID': ['id', 'work order', 'wo number', 'order id', 'customer work order number'],
        'VehicleID': ['vin', 'vehicle id', 'vehicle identification', 'customer vehicle id'],
        'EquipmentType': ['vehicle make', 'equipment type', 'vehicle type', 'make model', 'vehicle make and model'],
        'OpenDate': ['date', 'open date', 'created', 'start date'],
        'CloseDate': ['close date', 'completion date', 'end date', 'work orer completetion date'],
        'ReportedBy': ['reported by', 'created by', 'reporter'],
        'Location': ['location', 'workshop', 'workshop name', 'facility'],
        'Unit': ['unit', 'sector', 'brigade', 'unit vehicle belongs to'],
        'FailureDesc': ['failure desc', 'description', 'malfunction reason', 'vehicle malfunction reason'],
        'FailureMode': ['failure mode', 'failure type', 'malfunction type'],
        'Status': ['status', 'work order status', 'wo status'],
        'Notes': ['notes', 'comments', 'remarks']
    }
    
    for canonical_col, patterns in mapping_rules.items():
        for df_col, df_col_lower in df_cols_lower.items():
            for pattern in patterns:
                if pattern in df_col_lower:
                    mapping[canonical_col] = df_col
                    break
            if canonical_col in mapping:
                break
    
    return mapping

def clean_data(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """Clean and standardize the data"""
    df_clean = df.copy()
    
    # Apply column mapping
    reverse_mapping = {v: k for k, v in mapping.items()}
    df_clean = df_clean.rename(columns=reverse_mapping)
    
    # Date parsing
    date_columns = ['OpenDate', 'CloseDate']
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Numeric columns
    numeric_columns = ['OdometerKM', 'OperatingHours', 'PartCost', 'LaborHours', 'LaborCost', 'DowntimeHours']
    for col in numeric_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    # Standardize severity
    if 'Severity' in df_clean.columns:
        severity_mapping = {
            'critical': 'Critical', 'high': 'Critical', '1': 'Critical',
            'major': 'Major', 'medium': 'Major', '2': 'Major',
            'minor': 'Minor', 'low': 'Minor', '3': 'Minor'
        }
        df_clean['Severity'] = df_clean['Severity'].astype(str).str.lower().map(severity_mapping).fillna('Major')
    else:
        df_clean['Severity'] = 'Major'  # Default
    
    # Standardize status
    if 'Status' in df_clean.columns:
        status_mapping = {
            'completed': 'Closed', 'closed': 'Closed', 'done': 'Closed',
            'waiting': 'In Progress', 'under maintenance': 'In Progress', 'process initiated': 'Open',
            'open': 'Open', 'new': 'Open'
        }
        df_clean['Status'] = df_clean['Status'].astype(str).str.lower()
        for pattern, standard_status in status_mapping.items():
            df_clean.loc[df_clean['Status'].str.contains(pattern, na=False), 'Status'] = standard_status
    
    # Calculate derived fields
    if 'OpenDate' in df_clean.columns and 'CloseDate' in df_clean.columns:
        df_clean['RepairDurationHours'] = (
            (df_clean['CloseDate'] - df_clean['OpenDate']).dt.total_seconds() / 3600
        )
    
    if 'PartCost' in df_clean.columns and 'LaborCost' in df_clean.columns:
        df_clean['TotalCost'] = df_clean['PartCost'].fillna(0) + df_clean['LaborCost'].fillna(0)
    
    # Identify repeat failures
    if 'VehicleID' in df_clean.columns and 'FailureMode' in df_clean.columns:
        df_clean = df_clean.sort_values('OpenDate', na_position='last')
        df_clean['IsRepeatFailure'] = False
        
        for idx, row in df_clean.iterrows():
            if pd.notna(row.get('VehicleID')) and pd.notna(row.get('FailureMode')):
                mask = (
                    (df_clean['VehicleID'] == row['VehicleID']) &
                    (df_clean['FailureMode'] == row['FailureMode']) &
                    (df_clean.index < idx)
                )
                if 'OpenDate' in df_clean.columns:
                    mask = mask & (
                        (row['OpenDate'] - df_clean['OpenDate']).dt.days <= 180
                    )
                if mask.any():
                    df_clean.at[idx, 'IsRepeatFailure'] = True
    
    return df_clean

def suggest_failure_mode(description: str) -> str:
    """Suggest failure mode based on description keywords"""
    if not description:
        return "Unknown"
    
    description_lower = description.lower()
    
    keyword_mapping = {
        'Mechanical Failure': ['broken', 'crack', 'fracture', 'bent', 'worn', 'damage'],
        'Electrical Fault': ['electrical', 'wiring', 'short', 'battery', 'alternator'],
        'Hydraulic Leak': ['hydraulic', 'leak', 'fluid', 'pressure loss'],
        'Software Error': ['software', 'system error', 'fault code', 'diagnostic'],
        'Wear & Tear': ['wear', 'worn out', 'aged', 'deteriorated'],
        'Corrosion': ['rust', 'corrosion', 'oxidation'],
        'Overheating': ['overheat', 'temperature', 'cooling', 'thermal'],
        'Contamination': ['contaminated', 'dirty', 'clogged', 'blocked'],
    }
    
    for mode, keywords in keyword_mapping.items():
        if any(keyword in description_lower for keyword in keywords):
            return mode
    
    return "General Failure"

def calculate_mtbf(df: pd.DataFrame, base: str = 'days', group_by: str = None) -> pd.DataFrame:
    """Calculate Mean Time Between Failures"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    df_failures = df[df['Status'] == 'Closed'].copy()
    
    if group_by and group_by in df_failures.columns:
        groups = df_failures.groupby(group_by)
    else:
        groups = [(None, df_failures)]
    
    results = []
    for name, group in groups:
        if len(group) <= 1:
            continue
            
        if base == 'hours' and 'OperatingHours' in group.columns:
            total_hours = group['OperatingHours'].sum()
            failure_count = len(group)
            mtbf = total_hours / failure_count if failure_count > 0 else np.nan
        elif base == 'km' and 'OdometerKM' in group.columns:
            total_km = group['OdometerKM'].diff().sum()
            failure_count = len(group) - 1
            mtbf = total_km / failure_count if failure_count > 0 else np.nan
        else:  # Calendar days
            if 'OpenDate' in group.columns:
                date_range = (group['OpenDate'].max() - group['OpenDate'].min()).days
                failure_count = len(group)
                mtbf = date_range / failure_count if failure_count > 0 else np.nan
            else:
                mtbf = np.nan
        
        results.append({
            'Group': name if name else 'Overall',
            'MTBF': mtbf,
            'FailureCount': len(group),
            'Base': base
        })
    
    return pd.DataFrame(results)

def calculate_mttr(df: pd.DataFrame, group_by: str = None) -> pd.DataFrame:
    """Calculate Mean Time To Repair"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    if 'RepairDurationHours' not in df.columns:
        return pd.DataFrame()
    
    df_repairs = df[df['RepairDurationHours'].notna()].copy()
    
    if group_by and group_by in df_repairs.columns:
        result = df_repairs.groupby(group_by)['RepairDurationHours'].agg(['mean', 'count'])
        result.columns = ['MTTR_Hours', 'RepairCount']
    else:
        result = pd.DataFrame({
            'MTTR_Hours': [df_repairs['RepairDurationHours'].mean()],
            'RepairCount': [len(df_repairs)]
        })
    
    return result

def calculate_availability(df: pd.DataFrame, window_days: int = 30) -> float:
    """Calculate equipment availability"""
    if df is None or df.empty or 'DowntimeHours' not in df.columns:
        return 0.0
    
    total_downtime = df['DowntimeHours'].sum()
    total_possible_hours = window_days * 24 * df['VehicleID'].nunique() if 'VehicleID' in df.columns else window_days * 24
    
    availability = 1 - (total_downtime / total_possible_hours) if total_possible_hours > 0 else 0
    return max(0, min(1, availability)) * 100  # Percentage

# UI Components
def render_sidebar():
    """Render sidebar with role selection and info"""
    st.sidebar.title("🔧 FRACAS System")
    
    # User role selector
    st.sidebar.selectbox(
        "User Role",
        ["Technician", "Engineer", "Supervisor", "Admin"],
        key="user_role"
    )
    
    st.sidebar.markdown("---")
    
    # System info
    st.sidebar.info("""
    **FRACAS v1.0**
    Military Vehicle Maintenance
    
    Features:
    - Failure Reporting
    - Root Cause Analysis
    - FMEA Management
    - CAPA Tracking
    - Reliability KPIs
    - Cost Analysis
    """)
    
    # Data quality indicator
    if st.session_state.df_cleaned is not None:
        df = st.session_state.df_cleaned
        quality_score = (df.notna().sum().sum() / (len(df) * len(df.columns))) * 100
        st.sidebar.metric("Data Quality", f"{quality_score:.1f}%")

def render_data_import():
    """Render data import and mapping interface"""
    st.header("📂 Data Import & Column Mapping")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # File upload
        uploaded_file = st.file_uploader(
            "Upload Excel File",
            type=['xlsx', 'xls'],
            help="Upload work order data"
        )
        
        # Load data
        if st.button("Load Default File") or uploaded_file:
            df = load_excel_file(uploaded_file=uploaded_file)
            if df is not None:
                st.session_state.df = df
                st.session_state.column_mapping = auto_map_columns(df)
                add_audit_entry("Data Loaded", "Import", f"Loaded {len(df)} records")
                st.success(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")
    
    with col2:
        if st.session_state.df is not None:
            st.subheader("Column Mapping")
            
            # Display mapping interface
            mapping = {}
            df_columns = list(st.session_state.df.columns)
            
            with st.expander("Configure Column Mappings", expanded=True):
                for canonical_col, description in CANONICAL_SCHEMA.items():
                    col1_map, col2_map = st.columns([1, 2])
                    
                    with col1_map:
                        st.text(canonical_col)
                    
                    with col2_map:
                        default_val = st.session_state.column_mapping.get(canonical_col, "Not Mapped")
                        if default_val not in df_columns:
                            default_val = "Not Mapped"
                        
                        selected_col = st.selectbox(
                            description,
                            ["Not Mapped"] + df_columns,
                            index=0 if default_val == "Not Mapped" else df_columns.index(default_val) + 1,
                            key=f"map_{canonical_col}"
                        )
                        
                        if selected_col != "Not Mapped":
                            mapping[canonical_col] = selected_col
            
            # Apply mapping and clean data
            if st.button("Apply Mapping & Clean Data", type="primary"):
                st.session_state.column_mapping = mapping
                st.session_state.df_cleaned = clean_data(st.session_state.df, mapping)
                add_audit_entry("Data Cleaned", "Import", f"Applied mapping and cleaning")
                st.success("✅ Data cleaned and mapped successfully!")
    
    # Show data preview and export
    if st.session_state.df_cleaned is not None:
        st.subheader("Cleaned Data Preview")
        
        # Data quality metrics
        col1, col2, col3, col4 = st.columns(4)
        df = st.session_state.df_cleaned
        
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Columns Mapped", len(st.session_state.column_mapping))
        with col3:
            if 'Status' in df.columns:
                st.metric("Open Work Orders", len(df[df['Status'] == 'Open']))
        with col4:
            if 'RepairDurationHours' in df.columns:
                st.metric("Avg Repair Time", f"{df['RepairDurationHours'].mean():.1f} hrs")
        
        # Data preview
        st.dataframe(df.head(100), use_container_width=True)
        
        # Export buttons
        col1, col2 = st.columns(2)
        with col1:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Cleaned CSV",
                csv,
                "cleaned_work_orders.csv",
                "text/csv"
            )
        
        with col2:
            try:
                # Prepare DataFrame for Excel export
                df_excel = df.copy()
                
                # Convert datetime columns to string to avoid timezone issues
                for col in df_excel.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_excel[col]):
                        df_excel[col] = df_excel[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                    # Convert any object columns that might have issues
                    elif df_excel[col].dtype == 'object':
                        df_excel[col] = df_excel[col].astype(str).fillna('')
                    # Handle any numeric columns with inf or very large values
                    elif pd.api.types.is_numeric_dtype(df_excel[col]):
                        df_excel[col] = df_excel[col].replace([np.inf, -np.inf], np.nan).fillna(0)
                
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_excel.to_excel(writer, index=False, sheet_name='Cleaned_Data')
                
                st.download_button(
                    "📥 Download Cleaned Excel",
                    buffer.getvalue(),
                    "cleaned_work_orders.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Error exporting to Excel: {str(e)}")
                st.info("Try downloading as CSV instead")

def render_failure_reporting():
    """Render failure reporting interface"""
    st.header("📝 Failure Reporting")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Filters
    st.subheader("Filter Work Orders")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="failure_date_range"
        )
    
    with col2:
        vehicle_filter = st.multiselect(
            "Vehicle ID",
            df['VehicleID'].unique() if 'VehicleID' in df.columns else [],
            key="failure_vehicle"
        )
    
    with col3:
        if 'Severity' in df.columns:
            severity_filter = st.multiselect(
                "Severity",
                df['Severity'].unique(),
                key="failure_severity"
            )
        else:
            severity_filter = []
    
    with col4:
        if 'Status' in df.columns:
            status_filter = st.multiselect(
                "Status",
                df['Status'].unique(),
                key="failure_status"
            )
        else:
            status_filter = []
    
    # Apply filters
    filtered_df = df.copy()
    
    if vehicle_filter:
        filtered_df = filtered_df[filtered_df['VehicleID'].isin(vehicle_filter)]
    if severity_filter:
        filtered_df = filtered_df[filtered_df['Severity'].isin(severity_filter)]
    if status_filter:
        filtered_df = filtered_df[filtered_df['Status'].isin(status_filter)]
    
    # Display filtered work orders
    st.subheader(f"Work Orders ({len(filtered_df)} records)")
    
    # Highlight repeat failures
    if 'IsRepeatFailure' in filtered_df.columns:
        def highlight_repeats(row):
            if row.get('IsRepeatFailure', False):
                return ['background-color: #ffcccc'] * len(row)
            return [''] * len(row)
        
        st.dataframe(
            filtered_df.style.apply(highlight_repeats, axis=1),
            use_container_width=True
        )
    else:
        st.dataframe(filtered_df, use_container_width=True)
    
    # Log new failure form
    if st.session_state.user_role in ['Technician', 'Engineer', 'Supervisor', 'Admin']:
        st.subheader("Log New Failure")
        
        with st.form("new_failure_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_vehicle_id = st.text_input("Vehicle ID*")
                new_equipment_type = st.selectbox(
                    "Equipment Type*",
                    df['EquipmentType'].unique() if 'EquipmentType' in df.columns else ['Unknown']
                )
                new_failure_desc = st.text_area("Failure Description*")
                suggested_mode = suggest_failure_mode(new_failure_desc)
                new_failure_mode = st.selectbox(
                    "Failure Mode",
                    st.session_state.reference_data['failure_modes'],
                    index=st.session_state.reference_data['failure_modes'].index(suggested_mode) 
                    if suggested_mode in st.session_state.reference_data['failure_modes'] else 0
                )
            
            with col2:
                new_severity = st.selectbox("Severity*", ['Critical', 'Major', 'Minor'])
                new_location = st.selectbox(
                    "Location*",
                    st.session_state.reference_data['locations']
                )
                new_subsystem = st.selectbox(
                    "Subsystem",
                    st.session_state.reference_data['subsystems']
                )
                new_notes = st.text_area("Notes")
            
            submitted = st.form_submit_button("Submit Failure Report", type="primary")
            
            if submitted:
                if new_vehicle_id and new_failure_desc:
                    new_failure = pd.DataFrame({
                        'WorkOrderID': [f"WO-{datetime.now().strftime('%Y%m%d%H%M%S')}"],
                        'VehicleID': [new_vehicle_id],
                        'EquipmentType': [new_equipment_type],
                        'OpenDate': [datetime.now()],
                        'FailureDesc': [new_failure_desc],
                        'FailureMode': [new_failure_mode],
                        'Severity': [new_severity],
                        'Location': [new_location],
                        'Subsystem': [new_subsystem],
                        'Status': ['Open'],
                        'Notes': [new_notes],
                        'ReportedBy': [st.session_state.user_role]
                    })
                    
                    st.session_state.df_new_failures = pd.concat(
                        [st.session_state.df_new_failures, new_failure],
                        ignore_index=True
                    )
                    
                    add_audit_entry("New Failure Logged", "Failure Reporting", f"WO for {new_vehicle_id}")
                    st.success("✅ Failure report submitted successfully!")
                else:
                    st.error("Please fill in required fields")

def render_analysis_rca():
    """Render analysis and RCA interface"""
    st.header("📊 Analysis & Root Cause Analysis")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Top failure analysis
    tab1, tab2, tab3, tab4 = st.tabs(["Top Failures", "Trends", "Root Cause Analysis", "Repeat Failures"])
    
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
                    labels={'x': 'Count', 'y': 'Failure Mode'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(failure_counts.reset_index(), use_container_width=True)
        
        # Top vehicles with failures
        with col2:
            if 'VehicleID' in df.columns:
                vehicle_counts = df['VehicleID'].value_counts().head(10)
                fig = px.bar(
                    x=vehicle_counts.values,
                    y=vehicle_counts.index,
                    orientation='h',
                    title="Top 10 Vehicles by Failure Count",
                    labels={'x': 'Count', 'y': 'Vehicle ID'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(vehicle_counts.reset_index(), use_container_width=True)
        
        # Top subsystems
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Subsystem' in df.columns:
                subsystem_counts = df['Subsystem'].value_counts().head(10)
                fig = px.pie(
                    values=subsystem_counts.values,
                    names=subsystem_counts.index,
                    title="Failure Distribution by Subsystem"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            if 'Location' in df.columns:
                location_counts = df['Location'].value_counts().head(10)
                fig = px.pie(
                    values=location_counts.values,
                    names=location_counts.index,
                    title="Failures by Location"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        # Trend analysis
        if 'OpenDate' in df.columns:
            df['Month'] = pd.to_datetime(df['OpenDate']).dt.to_period('M')
            monthly_counts = df.groupby('Month').size()
            
            fig = px.line(
                x=monthly_counts.index.astype(str),
                y=monthly_counts.values,
                title="Monthly Failure Trend",
                labels={'x': 'Month', 'y': 'Failure Count'}
            )
            fig.add_hline(
                y=monthly_counts.mean(),
                line_dash="dash",
                line_color="red",
                annotation_text=f"Average: {monthly_counts.mean():.0f}"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Severity trend
            if 'Severity' in df.columns:
                severity_trend = df.groupby(['Month', 'Severity']).size().unstack(fill_value=0)
                
                fig = go.Figure()
                for severity in severity_trend.columns:
                    fig.add_trace(go.Scatter(
                        x=severity_trend.index.astype(str),
                        y=severity_trend[severity],
                        mode='lines+markers',
                        name=severity,
                        stackgroup='one'
                    ))
                
                fig.update_layout(
                    title="Failure Severity Trend",
                    xaxis_title="Month",
                    yaxis_title="Count",
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Root Cause Analysis Tools
        st.subheader("Root Cause Analysis Tools")
        
        # Select work order for RCA
        if 'WorkOrderID' in df.columns:
            selected_wo = st.selectbox(
                "Select Work Order for RCA",
                df['WorkOrderID'].unique()
            )
            
            wo_details = df[df['WorkOrderID'] == selected_wo].iloc[0]
            
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.write("**Work Order Details:**")
                st.write(f"Vehicle: {wo_details.get('VehicleID', 'N/A')}")
                st.write(f"Failure: {wo_details.get('FailureMode', 'N/A')}")
                st.write(f"Severity: {wo_details.get('Severity', 'N/A')}")
            
            with col2:
                # 5-Whys Analysis
                st.write("**5-Whys Analysis:**")
                
                with st.form(f"five_whys_{selected_wo}"):
                    why1 = st.text_input("Why 1: Why did the failure occur?")
                    why2 = st.text_input("Why 2: Why did that happen?")
                    why3 = st.text_input("Why 3: Why did that happen?")
                    why4 = st.text_input("Why 4: Why did that happen?")
                    why5 = st.text_input("Why 5: Root Cause")
                    
                    if st.form_submit_button("Save 5-Whys Analysis"):
                        rca_content = f"Why1: {why1}\nWhy2: {why2}\nWhy3: {why3}\nWhy4: {why4}\nWhy5: {why5}"
                        new_rca = pd.DataFrame({
                            'WorkOrderID': [selected_wo],
                            'RCA_Type': ['5-Whys'],
                            'Content': [rca_content],
                            'Timestamp': [datetime.now()],
                            'User': [st.session_state.user_role]
                        })
                        st.session_state.rca_store = pd.concat(
                            [st.session_state.rca_store, new_rca],
                            ignore_index=True
                        )
                        add_audit_entry("RCA Completed", "Analysis", f"5-Whys for {selected_wo}")
                        st.success("✅ 5-Whys analysis saved!")
            
            # Ishikawa Diagram
            st.write("**Ishikawa (Fishbone) Analysis:**")
            
            with st.form(f"ishikawa_{selected_wo}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    methods = st.text_area("Methods", help="Process, procedures, policies")
                    man = st.text_area("Man (People)", help="Skills, training, experience")
                
                with col2:
                    machine = st.text_area("Machine", help="Equipment, tools, technology")
                    materials = st.text_area("Materials", help="Raw materials, consumables")
                
                with col3:
                    measurement = st.text_area("Measurement", help="Inspection, testing, metrics")
                    environment = st.text_area("Environment", help="Location, weather, conditions")
                
                if st.form_submit_button("Save Ishikawa Analysis"):
                    ishikawa_content = f"""
                    Methods: {methods}
                    Man: {man}
                    Machine: {machine}
                    Materials: {materials}
                    Measurement: {measurement}
                    Environment: {environment}
                    """
                    new_rca = pd.DataFrame({
                        'WorkOrderID': [selected_wo],
                        'RCA_Type': ['Ishikawa'],
                        'Content': [ishikawa_content],
                        'Timestamp': [datetime.now()],
                        'User': [st.session_state.user_role]
                    })
                    st.session_state.rca_store = pd.concat(
                        [st.session_state.rca_store, new_rca],
                        ignore_index=True
                    )
                    add_audit_entry("RCA Completed", "Analysis", f"Ishikawa for {selected_wo}")
                    st.success("✅ Ishikawa analysis saved!")
    
    with tab4:
        # Repeat failures analysis
        if 'IsRepeatFailure' in df.columns:
            repeat_df = df[df['IsRepeatFailure'] == True]
            
            st.metric("Total Repeat Failures", len(repeat_df))
            
            if not repeat_df.empty:
                # Repeat failure patterns
                if 'VehicleID' in repeat_df.columns and 'FailureMode' in repeat_df.columns:
                    repeat_patterns = repeat_df.groupby(['VehicleID', 'FailureMode']).size().reset_index(name='Count')
                    repeat_patterns = repeat_patterns.sort_values('Count', ascending=False).head(20)
                    
                    fig = px.bar(
                        repeat_patterns,
                        x='Count',
                        y='VehicleID',
                        color='FailureMode',
                        orientation='h',
                        title="Top Repeat Failure Patterns",
                        labels={'Count': 'Repeat Count'}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.dataframe(repeat_patterns, use_container_width=True)

def render_fmea():
    """Render FMEA module"""
    st.header("⚠️ FMEA - Failure Mode and Effects Analysis")
    
    # FMEA input form
    with st.form("fmea_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            function_item = st.text_input("Function/Item*")
            failure_mode = st.selectbox("Failure Mode*", st.session_state.reference_data['failure_modes'])
            effect = st.text_area("Effect of Failure*")
        
        with col2:
            cause = st.text_area("Potential Cause*")
            current_controls = st.text_area("Current Controls")
            severity = st.slider("Severity (1-10)", 1, 10, 5)
        
        with col3:
            occurrence = st.slider("Occurrence (1-10)", 1, 10, 5)
            detection = st.slider("Detection (1-10)", 1, 10, 5)
            recommended_action = st.text_area("Recommended Action")
        
        col1, col2 = st.columns(2)
        
        with col1:
            owner = st.text_input("Action Owner")
        
        with col2:
            due_date = st.date_input("Due Date")
            status = st.selectbox("Status", ["Open", "In Progress", "Closed"])
        
        submitted = st.form_submit_button("Add FMEA Entry", type="primary")
        
        if submitted and function_item and failure_mode and effect and cause:
            rpn = severity * occurrence * detection
            
            new_fmea = pd.DataFrame({
                'Function_Item': [function_item],
                'FailureMode': [failure_mode],
                'Effect': [effect],
                'Cause': [cause],
                'CurrentControls': [current_controls],
                'Severity': [severity],
                'Occurrence': [occurrence],
                'Detection': [detection],
                'RPN': [rpn],
                'RecommendedAction': [recommended_action],
                'Owner': [owner],
                'DueDate': [due_date],
                'Status': [status]
            })
            
            st.session_state.fmea_data = pd.concat(
                [st.session_state.fmea_data, new_fmea],
                ignore_index=True
            )
            
            add_audit_entry("FMEA Entry Added", "FMEA", f"RPN: {rpn}")
            st.success(f"✅ FMEA entry added with RPN: {rpn}")
    
    # Display FMEA table
    if not st.session_state.fmea_data.empty:
        st.subheader("FMEA Register")
        
        # Sort by RPN
        fmea_display = st.session_state.fmea_data.sort_values('RPN', ascending=False)
        
        # Highlight high RPN values
        def highlight_rpn(row):
            if row['RPN'] >= 100:
                return ['background-color: #ff9999'] * len(row)
            elif row['RPN'] >= 50:
                return ['background-color: #ffcc99'] * len(row)
            else:
                return [''] * len(row)
        
        st.dataframe(
            fmea_display.style.apply(highlight_rpn, axis=1),
            use_container_width=True
        )
        
        # RPN distribution
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.histogram(
                fmea_display,
                x='RPN',
                nbins=20,
                title="RPN Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            high_risk = len(fmea_display[fmea_display['RPN'] >= 100])
            medium_risk = len(fmea_display[(fmea_display['RPN'] >= 50) & (fmea_display['RPN'] < 100)])
            low_risk = len(fmea_display[fmea_display['RPN'] < 50])
            
            fig = px.pie(
                values=[high_risk, medium_risk, low_risk],
                names=['High Risk (RPN≥100)', 'Medium Risk (50≤RPN<100)', 'Low Risk (RPN<50)'],
                title="Risk Distribution",
                color_discrete_map={
                    'High Risk (RPN≥100)': '#ff0000',
                    'Medium Risk (50≤RPN<100)': '#ffa500',
                    'Low Risk (RPN<50)': '#00ff00'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Export FMEA
        csv = fmea_display.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Export FMEA Register",
            csv,
            "fmea_register.csv",
            "text/csv"
        )

def render_capa():
    """Render CAPA tracker"""
    st.header("📋 CAPA - Corrective and Preventive Actions")
    
    # CAPA input form
    with st.form("capa_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            action_id = f"CAPA-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            st.text_input("Action ID", value=action_id, disabled=True)
            
            linked_wo = st.text_input("Linked Work Order ID")
            problem_summary = st.text_area("Problem Summary*")
            root_cause_ref = st.text_input("Root Cause Reference")
        
        with col2:
            action_type = st.selectbox("Action Type*", ["Corrective", "Preventive"])
            owner = st.text_input("Owner*")
            due_date = st.date_input("Due Date*")
            priority = st.selectbox("Priority*", ["Critical", "High", "Medium", "Low"])
            status = st.selectbox("Status", ["Open", "In Progress", "Pending Verification", "Closed"])
        
        submitted = st.form_submit_button("Create CAPA", type="primary")
        
        if submitted and problem_summary and owner:
            new_capa = pd.DataFrame({
                'ActionID': [action_id],
                'LinkedWorkOrderID': [linked_wo],
                'ProblemSummary': [problem_summary],
                'RootCauseRef': [root_cause_ref],
                'ActionType': [action_type],
                'Owner': [owner],
                'DueDate': [due_date],
                'Priority': [priority],
                'Status': [status],
                'EffectivenessCheck': ['Pending'],
                'VerifiedBy': [None],
                'VerifiedDate': [None]
            })
            
            st.session_state.capa_register = pd.concat(
                [st.session_state.capa_register, new_capa],
                ignore_index=True
            )
            
            add_audit_entry("CAPA Created", "CAPA", f"{action_id}")
            st.success(f"✅ CAPA {action_id} created successfully!")
    
    # Display CAPA register
    if not st.session_state.capa_register.empty:
        st.subheader("CAPA Register")
        
        # Filter options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                st.session_state.capa_register['Status'].unique()
            )
        
        with col2:
            priority_filter = st.multiselect(
                "Filter by Priority",
                st.session_state.capa_register['Priority'].unique()
            )
        
        with col3:
            show_overdue = st.checkbox("Show Overdue Only")
        
        # Apply filters
        capa_display = st.session_state.capa_register.copy()
        
        if status_filter:
            capa_display = capa_display[capa_display['Status'].isin(status_filter)]
        
        if priority_filter:
            capa_display = capa_display[capa_display['Priority'].isin(priority_filter)]
        
        if show_overdue:
            capa_display['DueDate'] = pd.to_datetime(capa_display['DueDate'])
            capa_display = capa_display[
                (capa_display['DueDate'] < datetime.now()) &
                (capa_display['Status'] != 'Closed')
            ]
        
        # Highlight overdue items
        def highlight_overdue(row):
            if pd.to_datetime(row['DueDate']) < datetime.now() and row['Status'] != 'Closed':
                return ['background-color: #ffcccc'] * len(row)
            return [''] * len(row)
        
        st.dataframe(
            capa_display.style.apply(highlight_overdue, axis=1),
            use_container_width=True
        )
        
        # CAPA metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_capa = len(st.session_state.capa_register)
            st.metric("Total CAPA", total_capa)
        
        with col2:
            open_capa = len(st.session_state.capa_register[
                st.session_state.capa_register['Status'].isin(['Open', 'In Progress'])
            ])
            st.metric("Open Actions", open_capa)
        
        with col3:
            st.session_state.capa_register['DueDate'] = pd.to_datetime(
                st.session_state.capa_register['DueDate']
            )
            overdue_capa = len(st.session_state.capa_register[
                (st.session_state.capa_register['DueDate'] < datetime.now()) &
                (st.session_state.capa_register['Status'] != 'Closed')
            ])
            st.metric("Overdue Actions", overdue_capa, delta_color="inverse")
        
        with col4:
            closure_rate = (
                len(st.session_state.capa_register[st.session_state.capa_register['Status'] == 'Closed']) /
                total_capa * 100
            ) if total_capa > 0 else 0
            st.metric("Closure Rate", f"{closure_rate:.1f}%")
        
        # Effectiveness verification
        st.subheader("Effectiveness Verification")
        
        pending_verification = st.session_state.capa_register[
            st.session_state.capa_register['Status'] == 'Pending Verification'
        ]
        
        if not pending_verification.empty:
            selected_capa = st.selectbox(
                "Select CAPA for Verification",
                pending_verification['ActionID'].values
            )
            
            with st.form("verification_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    effectiveness = st.selectbox("Effectiveness Check", ["Pass", "Fail", "Partial"])
                    verified_by = st.text_input("Verified By")
                
                with col2:
                    verification_notes = st.text_area("Verification Notes")
                
                if st.form_submit_button("Submit Verification"):
                    idx = st.session_state.capa_register[
                        st.session_state.capa_register['ActionID'] == selected_capa
                    ].index[0]
                    
                    st.session_state.capa_register.at[idx, 'EffectivenessCheck'] = effectiveness
                    st.session_state.capa_register.at[idx, 'VerifiedBy'] = verified_by
                    st.session_state.capa_register.at[idx, 'VerifiedDate'] = datetime.now()
                    
                    if effectiveness == "Pass":
                        st.session_state.capa_register.at[idx, 'Status'] = "Closed"
                    
                    add_audit_entry("CAPA Verified", "CAPA", f"{selected_capa}: {effectiveness}")
                    st.success(f"✅ Verification completed for {selected_capa}")
        
        # Export CAPA register
        csv = capa_display.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Export CAPA Register",
            csv,
            "capa_register.csv",
            "text/csv"
        )

def render_reliability_kpis():
    """Render reliability KPIs dashboard"""
    st.header("📈 Reliability KPIs")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # KPI calculation base selector
    col1, col2 = st.columns([3, 1])
    
    with col2:
        kpi_base = st.selectbox(
            "MTBF Calculation Base",
            ["Calendar Days", "Operating Hours", "Kilometers"],
            help="Select the base unit for MTBF calculation"
        )
        
        window_days = st.number_input(
            "Analysis Window (days)",
            min_value=7,
            max_value=365,
            value=30
        )
    
    # Map base selection
    base_map = {
        "Calendar Days": "days",
        "Operating Hours": "hours",
        "Kilometers": "km"
    }
    selected_base = base_map[kpi_base]
    
    # Calculate KPIs
    mtbf_data = calculate_mtbf(df, base=selected_base)
    mttr_data = calculate_mttr(df)
    availability = calculate_availability(df, window_days)
    
    # Display KPI cards
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if not mtbf_data.empty:
            overall_mtbf = mtbf_data[mtbf_data['Group'] == 'Overall']['MTBF'].values[0] if len(mtbf_data) > 0 else 0
            st.metric(
                f"MTBF ({selected_base})",
                f"{overall_mtbf:.1f}",
                help=f"Mean Time Between Failures in {selected_base}"
            )
        else:
            st.metric("MTBF", "N/A", help="Insufficient data")
    
    with col2:
        if not mttr_data.empty:
            overall_mttr = mttr_data['MTTR_Hours'].mean()
            st.metric(
                "MTTR (hours)",
                f"{overall_mttr:.1f}",
                help="Mean Time To Repair in hours"
            )
        else:
            st.metric("MTTR", "N/A", help="Insufficient data")
    
    with col3:
        if not mtbf_data.empty and overall_mtbf > 0:
            failure_rate = 1 / overall_mtbf
            st.metric(
                f"Failure Rate (λ)",
                f"{failure_rate:.4f}",
                help=f"Failures per {selected_base}"
            )
        else:
            st.metric("Failure Rate", "N/A", help="Insufficient data")
    
    with col4:
        st.metric(
            "Availability (%)",
            f"{availability:.1f}",
            help=f"Equipment availability over {window_days} days"
        )
    
    # Detailed KPI analysis
    tab1, tab2, tab3, tab4 = st.tabs(["MTBF Analysis", "MTTR Analysis", "Availability", "Reliability Trends"])
    
    with tab1:
        st.subheader(f"MTBF Analysis - {kpi_base}")
        
        if 'VehicleID' in df.columns:
            vehicle_mtbf = calculate_mtbf(df, base=selected_base, group_by='VehicleID')
            
            if not vehicle_mtbf.empty:
                # Top and bottom performers
                col1, col2 = st.columns(2)
                
                with col1:
                    top_vehicles = vehicle_mtbf.nlargest(10, 'MTBF')
                    fig = px.bar(
                        top_vehicles,
                        x='MTBF',
                        y='Group',
                        orientation='h',
                        title=f"Top 10 Vehicles - MTBF ({selected_base})",
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
                        title=f"Bottom 10 Vehicles - MTBF ({selected_base})",
                        color='MTBF',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # MTBF table
                st.dataframe(
                    vehicle_mtbf.sort_values('MTBF', ascending=False),
                    use_container_width=True
                )
    
    with tab2:
        st.subheader("MTTR Analysis")
        
        if 'VehicleID' in df.columns:
            vehicle_mttr = calculate_mttr(df, group_by='VehicleID')
            
            if not vehicle_mttr.empty:
                vehicle_mttr = vehicle_mttr.reset_index()
                vehicle_mttr.columns = ['VehicleID', 'MTTR_Hours', 'RepairCount']
                
                # MTTR distribution
                fig = px.histogram(
                    vehicle_mttr,
                    x='MTTR_Hours',
                    nbins=30,
                    title="MTTR Distribution",
                    labels={'MTTR_Hours': 'MTTR (hours)', 'count': 'Number of Vehicles'}
                )
                fig.add_vline(
                    x=vehicle_mttr['MTTR_Hours'].mean(),
                    line_dash="dash",
                    line_color="red",
                    annotation_text=f"Mean: {vehicle_mttr['MTTR_Hours'].mean():.1f} hours"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Top repair time vehicles
                longest_repairs = vehicle_mttr.nlargest(10, 'MTTR_Hours')
                fig = px.bar(
                    longest_repairs,
                    x='MTTR_Hours',
                    y='VehicleID',
                    orientation='h',
                    title="Vehicles with Longest Average Repair Times",
                    color='MTTR_Hours',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Availability Analysis")
        
        # Availability calculation details
        st.info(f"""
        **Availability Calculation:**
        - Analysis Window: {window_days} days
        - Total Fleet Size: {df['VehicleID'].nunique() if 'VehicleID' in df.columns else 'N/A'}
        - Total Downtime: {df['DowntimeHours'].sum():.1f} hours
        - Calculated Availability: {availability:.2f}%
        
        *Note: This is an approximation based on recorded downtime. Actual availability may vary.*
        """)
        
        if 'VehicleID' in df.columns and 'DowntimeHours' in df.columns:
            # Vehicle availability
            vehicle_downtime = df.groupby('VehicleID')['DowntimeHours'].sum().reset_index()
            vehicle_downtime['Availability'] = (
                1 - (vehicle_downtime['DowntimeHours'] / (window_days * 24))
            ) * 100
            vehicle_downtime['Availability'] = vehicle_downtime['Availability'].clip(0, 100)
            
            # Availability chart
            fig = px.scatter(
                vehicle_downtime,
                x='DowntimeHours',
                y='Availability',
                hover_data=['VehicleID'],
                title="Vehicle Availability vs Downtime",
                labels={'DowntimeHours': 'Total Downtime (hours)', 'Availability': 'Availability (%)'}
            )
            fig.add_hline(
                y=95,
                line_dash="dash",
                line_color="green",
                annotation_text="Target: 95%"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Reliability Trends")
        
        if 'OpenDate' in df.columns:
            # Monthly reliability metrics
            df['Month'] = pd.to_datetime(df['OpenDate']).dt.to_period('M')
            
            monthly_metrics = []
            for month in df['Month'].unique():
                month_df = df[df['Month'] == month]
                
                month_mtbf = calculate_mtbf(month_df, base=selected_base)
                month_mttr = calculate_mttr(month_df)
                
                monthly_metrics.append({
                    'Month': str(month),
                    'MTBF': month_mtbf['MTBF'].values[0] if not month_mtbf.empty else np.nan,
                    'MTTR': month_mttr['MTTR_Hours'].values[0] if not month_mttr.empty else np.nan,
                    'FailureCount': len(month_df)
                })
            
            metrics_df = pd.DataFrame(monthly_metrics)
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('MTBF Trend', 'MTTR Trend', 'Failure Count Trend', 'Combined Metrics'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                      [{"secondary_y": False}, {"secondary_y": True}]]
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
            
            # Failure count trend
            fig.add_trace(
                go.Bar(x=metrics_df['Month'], y=metrics_df['FailureCount'], name='Failures'),
                row=2, col=1
            )
            
            # Combined metrics
            fig.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTBF'], mode='lines', name='MTBF'),
                row=2, col=2, secondary_y=False
            )
            fig.add_trace(
                go.Scatter(x=metrics_df['Month'], y=metrics_df['MTTR'], mode='lines', name='MTTR'),
                row=2, col=2, secondary_y=True
            )
            
            fig.update_layout(height=800, showlegend=True, title_text="Reliability Metrics Trends")
            st.plotly_chart(fig, use_container_width=True)
    
    # Export KPI summary
    kpi_summary = pd.DataFrame({
        'Metric': ['MTBF', 'MTTR', 'Failure Rate', 'Availability'],
        'Value': [
            f"{overall_mtbf:.1f} {selected_base}" if 'overall_mtbf' in locals() else 'N/A',
            f"{overall_mttr:.1f} hours" if 'overall_mttr' in locals() else 'N/A',
            f"{failure_rate:.4f}" if 'failure_rate' in locals() else 'N/A',
            f"{availability:.1f}%"
        ]
    })
    
    csv = kpi_summary.to_csv(index=False).encode('utf-8')
    st.download_button(
        "📥 Export KPI Summary",
        csv,
        "kpi_summary.csv",
        "text/csv"
    )

def render_cost_performance():
    """Render cost and performance analysis"""
    st.header("💰 Cost & Performance Analysis")
    
    if st.session_state.df_cleaned is None:
        st.warning("Please import and clean data first")
        return
    
    df = st.session_state.df_cleaned
    
    # Check for cost columns
    has_cost_data = 'TotalCost' in df.columns or ('PartCost' in df.columns and 'LaborCost' in df.columns)
    
    if not has_cost_data:
        st.warning("Cost data not available in the dataset")
        # Generate sample cost data for demonstration
        if st.checkbox("Generate sample cost data for demonstration"):
            df['PartCost'] = np.random.uniform(100, 5000, len(df))
            df['LaborCost'] = np.random.uniform(50, 1000, len(df))
            df['TotalCost'] = df['PartCost'] + df['LaborCost']
            has_cost_data = True
    
    if has_cost_data:
        # Cost metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_cost = df['TotalCost'].sum() if 'TotalCost' in df.columns else 0
            st.metric("Total Cost", f"${total_cost:,.2f}")
        
        with col2:
            avg_cost = df['TotalCost'].mean() if 'TotalCost' in df.columns else 0
            st.metric("Average Cost per WO", f"${avg_cost:,.2f}")
        
        with col3:
            if 'PartCost' in df.columns:
                parts_cost = df['PartCost'].sum()
                st.metric("Total Parts Cost", f"${parts_cost:,.2f}")
        
        with col4:
            if 'LaborCost' in df.columns:
                labor_cost = df['LaborCost'].sum()
                st.metric("Total Labor Cost", f"${labor_cost:,.2f}")
        
        # Cost analysis tabs
        tab1, tab2, tab3, tab4 = st.tabs(["Cost by Vehicle", "Cost by Failure Mode", "Pareto Analysis", "Cost vs Downtime"])
        
        with tab1:
            if 'VehicleID' in df.columns:
                vehicle_costs = df.groupby('VehicleID')['TotalCost'].agg(['sum', 'mean', 'count']).reset_index()
                vehicle_costs.columns = ['VehicleID', 'TotalCost', 'AvgCost', 'WorkOrders']
                
                # Top cost vehicles
                top_cost_vehicles = vehicle_costs.nlargest(15, 'TotalCost')
                
                fig = px.bar(
                    top_cost_vehicles,
                    x='TotalCost',
                    y='VehicleID',
                    orientation='h',
                    title="Top 15 Vehicles by Total Cost",
                    color='TotalCost',
                    color_continuous_scale='Reds',
                    hover_data=['AvgCost', 'WorkOrders']
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Cost per work order
                fig = px.scatter(
                    vehicle_costs,
                    x='WorkOrders',
                    y='AvgCost',
                    size='TotalCost',
                    hover_data=['VehicleID'],
                    title="Average Cost vs Work Order Count by Vehicle"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            if 'FailureMode' in df.columns:
                failure_costs = df.groupby('FailureMode')['TotalCost'].agg(['sum', 'mean', 'count']).reset_index()
                failure_costs.columns = ['FailureMode', 'TotalCost', 'AvgCost', 'Occurrences']
                
                # Top cost failure modes
                top_failures = failure_costs.nlargest(10, 'TotalCost')
                
                fig = px.bar(
                    top_failures,
                    x='TotalCost',
                    y='FailureMode',
                    orientation='h',
                    title="Top 10 Failure Modes by Cost",
                    color='TotalCost',
                    color_continuous_scale='Oranges'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with tab3:
            # Pareto analysis
            if 'VehicleID' in df.columns:
                pareto_data = df.groupby('VehicleID')['TotalCost'].sum().sort_values(ascending=False).reset_index()
                pareto_data['CumulativeCost'] = pareto_data['TotalCost'].cumsum()
                pareto_data['CumulativePercent'] = (pareto_data['CumulativeCost'] / pareto_data['TotalCost'].sum()) * 100
                
                # Find 80% threshold
                threshold_80 = pareto_data[pareto_data['CumulativePercent'] <= 80]
                
                st.info(f"**Pareto Principle:** {len(threshold_80)} vehicles ({len(threshold_80)/len(pareto_data)*100:.1f}% of fleet) account for 80% of costs")
                
                # Pareto chart
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Bar(
                        x=list(range(len(pareto_data.head(20)))),
                        y=pareto_data.head(20)['TotalCost'],
                        name='Cost',
                        marker_color='lightblue'
                    ),
                    secondary_y=False,
                )
                
                fig.add_trace(
                    go.Scatter(
                        x=list(range(len(pareto_data.head(20)))),
                        y=pareto_data.head(20)['CumulativePercent'],
                        name='Cumulative %',
                        line=dict(color='red', width=2),
                        mode='lines+markers'
                    ),
                    secondary_y=True,
                )
                
                fig.add_hline(
                    y=80,
                    line_dash="dash",
                    line_color="green",
                    secondary_y=True,
                    annotation_text="80% threshold"
                )
                
                fig.update_xaxes(title_text="Vehicle Rank")
                fig.update_yaxes(title_text="Cost ($)", secondary_y=False)
                fig.update_yaxes(title_text="Cumulative Percentage", secondary_y=True)
                fig.update_layout(title="Pareto Analysis - Cost by Vehicle (Top 20)")
                
                st.plotly_chart(fig, use_container_width=True)
        
        with tab4:
            if 'DowntimeHours' in df.columns:
                # Cost vs Downtime correlation
                fig = px.scatter(
                    df,
                    x='DowntimeHours',
                    y='TotalCost',
                    color='Severity' if 'Severity' in df.columns else None,
                    title="Cost vs Downtime Analysis",
                    trendline="ols",
                    labels={'DowntimeHours': 'Downtime (hours)', 'TotalCost': 'Total Cost ($)'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Calculate correlation
                if df['DowntimeHours'].notna().sum() > 2 and df['TotalCost'].notna().sum() > 2:
                    correlation = df[['DowntimeHours', 'TotalCost']].corr().iloc[0, 1]
                    st.info(f"**Correlation between Cost and Downtime:** {correlation:.3f}")
                
                # Cost efficiency
                df['CostPerDowntimeHour'] = df['TotalCost'] / df['DowntimeHours']
                df['CostPerDowntimeHour'] = df['CostPerDowntimeHour'].replace([np.inf, -np.inf], np.nan)
                
                if 'VehicleID' in df.columns:
                    efficiency = df.groupby('VehicleID')['CostPerDowntimeHour'].mean().sort_values().head(10)
                    
                    fig = px.bar(
                        x=efficiency.values,
                        y=efficiency.index,
                        orientation='h',
                        title="Most Cost-Efficient Vehicles ($/Downtime Hour)",
                        labels={'x': 'Cost per Downtime Hour ($)', 'y': 'Vehicle ID'}
                    )
                    st.plotly_chart(fig, use_container_width=True)

def render_admin():
    """Render admin and reference data management"""
    st.header("⚙️ Admin & Reference Data")
    
    if st.session_state.user_role != 'Admin':
        st.warning("Admin access required")
        return
    
    tab1, tab2, tab3 = st.tabs(["Reference Data", "User Management", "Audit Trail"])
    
    with tab1:
        st.subheader("Reference Data Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Failure Modes
            st.write("**Failure Modes**")
            
            current_modes = st.session_state.reference_data['failure_modes']
            
            # Display current modes
            for i, mode in enumerate(current_modes):
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.text(mode)
                with col_b:
                    if st.button(f"Remove", key=f"remove_mode_{i}"):
                        st.session_state.reference_data['failure_modes'].remove(mode)
                        add_audit_entry("Reference Data Updated", "Admin", f"Removed failure mode: {mode}")
                        st.rerun()
            
            # Add new mode
            new_mode = st.text_input("Add New Failure Mode")
            if st.button("Add Failure Mode"):
                if new_mode and new_mode not in current_modes:
                    st.session_state.reference_data['failure_modes'].append(new_mode)
                    add_audit_entry("Reference Data Updated", "Admin", f"Added failure mode: {new_mode}")
                    st.success(f"Added: {new_mode}")
                    st.rerun()
        
        with col2:
            # Subsystems
            st.write("**Subsystems**")
            
            current_subsystems = st.session_state.reference_data['subsystems']
            
            # Display current subsystems
            for i, subsystem in enumerate(current_subsystems):
                col_a, col_b = st.columns([3, 1])
                with col_a:
                    st.text(subsystem)
                with col_b:
                    if st.button(f"Remove", key=f"remove_subsystem_{i}"):
                        st.session_state.reference_data['subsystems'].remove(subsystem)
                        add_audit_entry("Reference Data Updated", "Admin", f"Removed subsystem: {subsystem}")
                        st.rerun()
            
            # Add new subsystem
            new_subsystem = st.text_input("Add New Subsystem")
            if st.button("Add Subsystem"):
                if new_subsystem and new_subsystem not in current_subsystems:
                    st.session_state.reference_data['subsystems'].append(new_subsystem)
                    add_audit_entry("Reference Data Updated", "Admin", f"Added subsystem: {new_subsystem}")
                    st.success(f"Added: {new_subsystem}")
                    st.rerun()
    
    with tab2:
        st.subheader("User Role Management")
        
        st.info("""
        **Current Role Permissions:**
        
        - **Technician**: View data, log failures, basic reports
        - **Engineer**: All Technician permissions + RCA, FMEA
        - **Supervisor**: All Engineer permissions + CAPA management
        - **Admin**: Full system access + configuration
        """)
        
        # Simulated user management
        users_df = pd.DataFrame({
            'User': ['John Smith', 'Jane Doe', 'Mike Johnson', 'Sarah Williams'],
            'Role': ['Admin', 'Engineer', 'Technician', 'Supervisor'],
            'Last Login': [datetime.now() - timedelta(hours=i*24) for i in range(4)]
        })
        
        st.dataframe(users_df, use_container_width=True)
    
    with tab3:
        st.subheader("Audit Trail")
        
        if not st.session_state.audit_trail.empty:
            # Filter options
            col1, col2 = st.columns(2)
            
            with col1:
                module_filter = st.multiselect(
                    "Filter by Module",
                    st.session_state.audit_trail['Module'].unique()
                )
            
            with col2:
                days_back = st.slider("Days to show", 1, 30, 7)
            
            # Apply filters
            audit_display = st.session_state.audit_trail.copy()
            
            if module_filter:
                audit_display = audit_display[audit_display['Module'].isin(module_filter)]
            
            # Time filter
            cutoff_date = datetime.now() - timedelta(days=days_back)
            audit_display = audit_display[audit_display['Timestamp'] >= cutoff_date]
            
            # Display audit trail
            audit_display = audit_display.sort_values('Timestamp', ascending=False)
            st.dataframe(audit_display, use_container_width=True)
            
            # Export audit trail
            csv = audit_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Export Audit Trail",
                csv,
                "audit_trail.csv",
                "text/csv"
            )
        else:
            st.info("No audit entries yet")

# Main application
def main():
    init_session_state()
    render_sidebar()
    
    # Main navigation tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "📂 Data",
        "📝 Failures",
        "📊 Analysis",
        "⚠️ FMEA",
        "📋 CAPA",
        "📈 KPIs",
        "💰 Costs",
        "⚙️ Admin"
    ])
    
    with tab1:
        render_data_import()
    
    with tab2:
        render_failure_reporting()
    
    with tab3:
        render_analysis_rca()
    
    with tab4:
        render_fmea()
    
    with tab5:
        render_capa()
    
    with tab6:
        render_reliability_kpis()
    
    with tab7:
        render_cost_performance()
    
    with tab8:
        render_admin()

if __name__ == "__main__":
    main()
