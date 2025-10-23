import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import openpyxl
import io

# Page configuration
st.set_page_config(
    page_title="FRACAS - Failure Analysis System",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# Helper function to parse the Work Orders Excel file
def parse_work_orders(file):
    """Parse the Work Orders Excel file with schema in headers"""
    try:
        # Load workbook
        wb = openpyxl.load_workbook(file, data_only=True)
        ws = wb.active
        
        # Extract column names from the complex schema in row 1
        column_names = []
        for col_idx in range(1, ws.max_column + 1):
            cell_value = ws.cell(row=1, column=col_idx).value
            
            if cell_value and isinstance(cell_value, str):
                # Try to extract DisplayName
                if 'DisplayName=' in cell_value:
                    try:
                        start_idx = cell_value.find('DisplayName="') + 13
                        end_idx = cell_value.find('"', start_idx)
                        if start_idx > 12 and end_idx > start_idx:
                            display_name = cell_value[start_idx:end_idx]
                            if display_name and len(display_name) < 200:
                                # Clean up Arabic/English names
                                if ' / ' in display_name:
                                    display_name = display_name.split(' / ')[0].strip()
                                column_names.append(display_name)
                                continue
                    except:
                        pass
            
            # Fallback to generic column name
            column_names.append(f'Column_{col_idx}')
        
        # Extract data rows (starting from row 2)
        data = []
        for row_idx in range(2, ws.max_row + 1):
            row_data = []
            for col_idx in range(1, ws.max_column + 1):
                cell_value = ws.cell(row=row_idx, column=col_idx).value
                row_data.append(cell_value)
            data.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=column_names)
        
        # Clean up column names and identify key columns
        df.columns = df.columns.str.strip()
        
        # Try to identify standard columns based on content
        for col in df.columns:
            col_lower = col.lower()
            if 'date' in col_lower and df[col].dtype == 'object':
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass
        
        wb.close()
        return df
    
    except Exception as e:
        st.error(f"Error parsing file: {str(e)}")
        return None

# Analysis functions
def calculate_failure_metrics(df):
    """Calculate key failure metrics"""
    metrics = {}
    
    # Find status column
    status_col = None
    for col in df.columns:
        if 'status' in col.lower():
            status_col = col
            break
    
    if status_col:
        metrics['total_work_orders'] = len(df)
        metrics['completed'] = len(df[df[status_col].astype(str).str.contains('Completed', case=False, na=False)])
        metrics['in_progress'] = len(df[df[status_col].astype(str).str.contains('Maintenance|Testing', case=False, na=False)])
        metrics['waiting_parts'] = len(df[df[status_col].astype(str).str.contains('Waiting', case=False, na=False)])
        metrics['completion_rate'] = (metrics['completed'] / metrics['total_work_orders'] * 100) if metrics['total_work_orders'] > 0 else 0
    
    return metrics

def identify_top_failures(df, limit=10):
    """Identify most common failure types"""
    # Look for vehicle type or malfunction columns
    vehicle_col = None
    for col in df.columns:
        if 'vehicle' in col.lower() or 'model' in col.lower():
            vehicle_col = col
            break
    
    if vehicle_col:
        failure_counts = df[vehicle_col].value_counts().head(limit)
        return failure_counts
    
    return None

def analyze_by_workshop(df):
    """Analyze failures by workshop"""
    workshop_col = None
    for col in df.columns:
        if 'workshop' in col.lower():
            workshop_col = col
            break
    
    if workshop_col:
        workshop_analysis = df[workshop_col].value_counts()
        return workshop_analysis
    
    return None

def analyze_by_sector(df):
    """Analyze failures by sector"""
    sector_col = None
    for col in df.columns:
        if 'sector' in col.lower():
            sector_col = col
            break
    
    if sector_col:
        sector_analysis = df[sector_col].value_counts()
        return sector_analysis
    
    return None

def create_trend_analysis(df):
    """Create time-based trend analysis"""
    date_col = None
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            date_col = col
            break
    
    if date_col:
        df_copy = df.copy()
        df_copy['Year-Month'] = df_copy[date_col].dt.to_period('M').astype(str)
        trend = df_copy.groupby('Year-Month').size()
        return trend
    
    return None

# Main app
def main():
    st.markdown('<h1 class="main-header">🔧 FRACAS System</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666;">Failure Reporting, Analysis, and Corrective Action System</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("📊 System Controls")
        uploaded_file = st.file_uploader(
            "Upload Work Orders File", 
            type=['xlsx', 'xls'],
            help="Upload your Work Orders Excel file for analysis"
        )
        
        st.markdown("---")
        st.markdown("### 📋 Quick Stats")
        
    # Main content
    if uploaded_file is not None:
        # Parse the file
        with st.spinner("Processing Work Orders..."):
            df = parse_work_orders(uploaded_file)
        
        if df is not None:
            st.success(f"✅ Successfully loaded {len(df)} work orders with {len(df.columns)} fields")
            
            # Calculate metrics
            metrics = calculate_failure_metrics(df)
            
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Work Orders", metrics.get('total_work_orders', 0))
            with col2:
                st.metric("Completed", metrics.get('completed', 0), 
                         delta=f"{metrics.get('completion_rate', 0):.1f}% completion")
            with col3:
                st.metric("In Progress", metrics.get('in_progress', 0))
            with col4:
                st.metric("Waiting Parts", metrics.get('waiting_parts', 0))
            
            # Tabs for different analyses
            tabs = st.tabs(["📈 Dashboard", "🔍 Fault Analysis", "🏭 Workshop Analysis", "📊 Trends", "📋 Raw Data"])
            
            # Tab 1: Dashboard
            with tabs[0]:
                st.header("Overview Dashboard")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Status distribution
                    status_col = None
                    for col in df.columns:
                        if 'status' in col.lower():
                            status_col = col
                            break
                    
                    if status_col:
                        st.subheader("Work Order Status Distribution")
                        status_counts = df[status_col].value_counts()
                        fig = px.pie(values=status_counts.values, names=status_counts.index,
                                   title="Status Distribution", hole=0.4)
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Sector distribution
                    sector_analysis = analyze_by_sector(df)
                    if sector_analysis is not None:
                        st.subheader("Work Orders by Sector")
                        fig = px.bar(x=sector_analysis.values, y=sector_analysis.index,
                                   orientation='h', title="Sector Distribution")
                        fig.update_layout(xaxis_title="Number of Work Orders", yaxis_title="Sector")
                        st.plotly_chart(fig, use_container_width=True)
            
            # Tab 2: Fault Analysis
            with tabs[1]:
                st.header("Fault & Failure Analysis")
                
                # Top failures
                top_failures = identify_top_failures(df, limit=15)
                if top_failures is not None:
                    st.subheader("Top Vehicle/Equipment Failures")
                    
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        fig = px.bar(x=top_failures.values, y=top_failures.index,
                                   orientation='h',
                                   title="Most Common Vehicle Types with Issues",
                                   labels={'x': 'Number of Work Orders', 'y': 'Vehicle Type'})
                        fig.update_layout(height=500)
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.subheader("Failure Summary")
                        failure_df = pd.DataFrame({
                            'Vehicle Type': top_failures.index[:10],
                            'Count': top_failures.values[:10]
                        })
                        st.dataframe(failure_df, use_container_width=True, height=500)
                
                # Failure types
                st.subheader("Failure Type Analysis")
                malfunction_col = None
                for col in df.columns:
                    if 'malfunction' in col.lower() or 'description' in col.lower():
                        malfunction_col = col
                        break
                
                if malfunction_col:
                    malfunction_counts = df[malfunction_col].value_counts()
                    fig = px.pie(values=malfunction_counts.values, names=malfunction_counts.index,
                               title="Corrective vs Planned Maintenance")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Tab 3: Workshop Analysis
            with tabs[2]:
                st.header("Workshop Performance Analysis")
                
                workshop_analysis = analyze_by_workshop(df)
                if workshop_analysis is not None:
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.subheader("Work Orders by Workshop")
                        # Limit to top 15 workshops for readability
                        top_workshops = workshop_analysis.head(15)
                        fig = px.bar(x=top_workshops.values, y=top_workshops.index,
                                   orientation='h',
                                   title="Workshop Workload Distribution",
                                   color=top_workshops.values,
                                   color_continuous_scale='Blues')
                        fig.update_layout(height=600, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        st.subheader("Workshop Statistics")
                        st.metric("Total Workshops", len(workshop_analysis))
                        st.metric("Busiest Workshop", workshop_analysis.index[0])
                        st.metric("Max Work Orders", workshop_analysis.values[0])
                        
                        avg_workload = workshop_analysis.mean()
                        st.metric("Avg Work Orders/Workshop", f"{avg_workload:.1f}")
            
            # Tab 4: Trends
            with tabs[3]:
                st.header("Trend Analysis")
                
                trend_data = create_trend_analysis(df)
                if trend_data is not None:
                    st.subheader("Work Orders Over Time")
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=trend_data.index, y=trend_data.values,
                                           mode='lines+markers', name='Work Orders',
                                           line=dict(color='#1f77b4', width=3),
                                           marker=dict(size=8)))
                    fig.update_layout(title="Monthly Work Order Trends",
                                    xaxis_title="Month", yaxis_title="Number of Work Orders",
                                    hovermode='x unified', height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Additional metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Peak Month", trend_data.idxmax())
                    with col2:
                        st.metric("Peak Work Orders", trend_data.max())
                    with col3:
                        avg_per_month = trend_data.mean()
                        st.metric("Avg per Month", f"{avg_per_month:.1f}")
                
                # Spare parts analysis
                st.subheader("Spare Parts Requirements")
                spare_parts_col = None
                for col in df.columns:
                    if 'spare' in col.lower() and 'parts' in col.lower():
                        spare_parts_col = col
                        break
                
                if spare_parts_col:
                    spare_counts = df[spare_parts_col].value_counts()
                    col1, col2 = st.columns(2)
                    with col1:
                        fig = px.pie(values=spare_counts.values, names=spare_counts.index,
                                   title="Spare Parts Required vs Not Required")
                        st.plotly_chart(fig, use_container_width=True)
                    with col2:
                        total = len(df)
                        if 'Yes' in spare_counts.index or 'Yes / نعم' in spare_counts.index:
                            yes_count = spare_counts.get('Yes', 0) + spare_counts.get('Yes / نعم', 0)
                            spare_rate = (yes_count / total * 100) if total > 0 else 0
                            st.metric("Spare Parts Required Rate", f"{spare_rate:.1f}%")
            
            # Tab 5: Raw Data
            with tabs[4]:
                st.header("Raw Data Viewer")
                
                # Filter options
                col1, col2 = st.columns(2)
                with col1:
                    search_term = st.text_input("🔍 Search across all columns", "")
                with col2:
                    # Column selector
                    all_columns = list(df.columns)
                    selected_columns = st.multiselect("Select columns to display", 
                                                     all_columns,
                                                     default=all_columns[:10])
                
                # Apply search filter
                if search_term:
                    mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
                    filtered_df = df[mask]
                else:
                    filtered_df = df
                
                # Display selected columns
                if selected_columns:
                    st.dataframe(filtered_df[selected_columns], use_container_width=True, height=600)
                else:
                    st.dataframe(filtered_df, use_container_width=True, height=600)
                
                # Download option
                csv = filtered_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Filtered Data as CSV",
                    data=csv,
                    file_name="fracas_data.csv",
                    mime="text/csv"
                )
                
                st.info(f"Showing {len(filtered_df)} of {len(df)} total work orders")
    
    else:
        # Welcome screen
        st.info("👆 Please upload a Work Orders Excel file to begin analysis")
        
        st.markdown("### 📖 About FRACAS")
        st.markdown("""
        This **FRACAS (Failure Reporting, Analysis, and Corrective Action System)** helps you:
        
        - 📊 **Analyze failure patterns** across your military equipment maintenance operations
        - 🔍 **Identify recurring faults** and high-failure equipment
        - 🏭 **Monitor workshop performance** and workload distribution
        - 📈 **Track trends** over time to improve preventive maintenance
        - 🎯 **Make data-driven decisions** for resource allocation
        
        ---
        
        ### 🚀 Getting Started
        1. Upload your Work Orders Excel file using the sidebar
        2. Explore the different analysis tabs
        3. Download reports and insights for your team
        """)
        
        with st.expander("ℹ️ System Requirements"):
            st.markdown("""
            **Supported File Formats:**
            - Excel files (.xlsx, .xls)
            - Work Orders with standard schema
            
            **Expected Data Fields:**
            - Work Order Number
            - Vehicle/Equipment Type
            - Workshop
            - Status
            - Date
            - VIN Number
            - Malfunction Type
            - Spare Parts Required
            - Sector
            """)

if __name__ == "__main__":
    main()
