import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import io
import hashlib

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

# Helper function to create file hash
def make_file_hash(file_bytes):
    """Create a hash of file bytes for cache key"""
    return hashlib.md5(file_bytes).hexdigest()

# Simplified parser for actual Excel format
@st.cache_data(show_spinner="Processing Excel file...")
def parse_work_orders(file_hash, file_bytes):
    """Parse the Work Orders Excel file - SIMPLIFIED VERSION for actual data format"""
    try:
        # Read Excel file directly with pandas
        file_like = io.BytesIO(file_bytes)
        df = pd.read_excel(file_like)
        
        st.info(f"📊 Processing {len(df):,} work orders with {len(df.columns)} columns...")
        
        # Clean column names (remove extra spaces)
        df.columns = df.columns.str.strip()
        
        # Convert date columns
        date_columns = []
        for col in df.columns:
            if 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    date_columns.append(col)
                except:
                    pass
        
        # Show column mapping for debugging
        with st.expander("📋 Detected Columns"):
            cols_info = []
            for col in df.columns:
                non_null = df[col].notna().sum()
                cols_info.append(f"• **{col}**: {non_null:,} non-empty values")
            st.markdown("\n".join(cols_info))
        
        st.success(f"✓ Loaded {len(df):,} work orders successfully!")
        
        return df
    
    except Exception as e:
        st.error(f"Error parsing file: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None

# Analysis functions adapted for actual column names
def calculate_failure_metrics(df):
    """Calculate key failure metrics based on actual columns"""
    metrics = {}
    
    # Use actual column name
    status_col = 'Work order status'
    
    if status_col in df.columns:
        metrics['total_work_orders'] = len(df)
        status_series = df[status_col].fillna('').astype(str)
        
        # Adapt to actual status values in the data
        metrics['completed'] = len(df[status_series.str.contains('Completed', case=False, na=False)])
        metrics['in_progress'] = len(df[status_series.str.contains('Under Maintenance|Process Initiated', case=False, na=False)])
        metrics['waiting_parts'] = len(df[status_series.str.contains('Waiting Spare Parts', case=False, na=False)])
        metrics['completion_rate'] = (metrics['completed'] / metrics['total_work_orders'] * 100) if metrics['total_work_orders'] > 0 else 0
    else:
        metrics['total_work_orders'] = len(df)
        metrics['completed'] = 0
        metrics['in_progress'] = 0
        metrics['waiting_parts'] = 0
        metrics['completion_rate'] = 0
    
    return metrics

def identify_top_vehicles(df, limit=10):
    """Identify most common vehicle types using actual column name"""
    vehicle_col = 'Vehicle Make and Model'
    
    if vehicle_col in df.columns:
        # Clean the data
        vehicle_series = df[vehicle_col].fillna('Unknown').astype(str).str.strip()
        # Remove empty or very short values
        vehicle_series = vehicle_series[vehicle_series.str.len() > 2]
        # Remove "Unknown"
        vehicle_series = vehicle_series[vehicle_series != 'Unknown']
        
        if len(vehicle_series) > 0:
            return vehicle_series.value_counts().head(limit)
    return None

def analyze_by_workshop(df):
    """Analyze work orders by workshop using actual column name"""
    workshop_col = 'Workshop name'
    
    if workshop_col in df.columns:
        # Clean the data
        workshop_series = df[workshop_col].fillna('Unknown').astype(str).str.strip()
        # Remove empty values
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
    date_col = 'Date'
    
    if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
        # Filter out invalid dates
        df_filtered = df[df[date_col].notna()].copy()
        
        # Group by month
        df_filtered['month'] = df_filtered[date_col].dt.to_period('M')
        trend_data = df_filtered.groupby('month').size()
        
        # Also calculate status trends
        if 'Work order status' in df.columns:
            status_trends = df_filtered.groupby(['month', 'Work order status']).size().unstack(fill_value=0)
            return trend_data, status_trends
        
        return trend_data, None
    return None, None

def analyze_spare_parts(df):
    """Analyze spare parts requirements"""
    spare_col = 'Spare parts Required'
    received_col = 'Received Spare Parts (Yes/No)'
    
    results = {}
    
    if spare_col in df.columns:
        results['required'] = df[spare_col].value_counts()
    
    if received_col in df.columns:
        # This column seems to have dates, so let's count non-null as received
        results['received_count'] = df[received_col].notna().sum()
        results['total_required'] = len(df[df[spare_col].str.contains('Yes', case=False, na=False)]) if spare_col in df.columns else 0
    
    return results

# Main application
def main():
    st.markdown('<h1 class="main-header">🔧 FRACAS - Failure Analysis System</h1>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.header("📁 Data Upload")
        uploaded_file = st.file_uploader("Choose Work Orders Excel file", type=['xlsx', 'xls'])
        
        if uploaded_file is not None:
            st.success(f"✓ File uploaded: {uploaded_file.name}")
            st.info(f"Size: {uploaded_file.size / (1024*1024):.2f} MB")
        
        st.markdown("---")
        st.markdown("### 📊 System Information")
        st.info("""
        **Version:** 3.0 (Optimized for your data)
        **Status:** Production Ready
        **Performance:** Real-time analysis
        """)
        
        # Clear cache button
        if st.button("🔄 Clear Cache & Reprocess"):
            st.cache_data.clear()
            st.rerun()
    
    # Main content
    if uploaded_file is not None:
        try:
            # Read file and create hash
            file_bytes = uploaded_file.read()
            file_hash = make_file_hash(file_bytes)
            
            # Parse the Excel file
            df = parse_work_orders(file_hash, file_bytes)
            
            if df is not None and not df.empty:
                # Create tabs
                tabs = st.tabs(["📊 Overview", "🚗 Vehicle Analysis", "🏭 Workshop Analysis", "📈 Trends", "🔧 Spare Parts", "📋 Raw Data"])
                
                # Tab 1: Overview
                with tabs[0]:
                    st.header("Dashboard Overview")
                    
                    # Key metrics
                    metrics = calculate_failure_metrics(df)
                    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Total Work Orders", f"{metrics['total_work_orders']:,}")
                    with col2:
                        st.metric("Completed", f"{metrics['completed']:,}", 
                                delta=f"{metrics['completion_rate']:.1f}%")
                    with col3:
                        st.metric("Under Maintenance", f"{metrics['in_progress']:,}")
                    with col4:
                        st.metric("Waiting Spare Parts", f"{metrics['waiting_parts']:,}",
                                delta=f"{(metrics['waiting_parts']/metrics['total_work_orders']*100):.1f}%" if metrics['total_work_orders'] > 0 else "0%")
                    with col5:
                        completion_rate_color = "🟢" if metrics['completion_rate'] > 50 else "🟡" if metrics['completion_rate'] > 25 else "🔴"
                        st.metric("Completion Rate", f"{completion_rate_color} {metrics['completion_rate']:.1f}%")
                    
                    # Status distribution
                    st.subheader("📊 Work Order Status Distribution")
                    if 'Work order status' in df.columns:
                        status_counts = df['Work order status'].value_counts()
                        
                        col1, col2 = st.columns([2, 1])
                        with col1:
                            fig = px.pie(values=status_counts.values, 
                                       names=status_counts.index,
                                       title="Current Status of All Work Orders",
                                       color_discrete_sequence=px.colors.sequential.Blues_r)
                            fig.update_traces(textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with col2:
                            st.markdown("### Status Breakdown")
                            for status, count in status_counts.items():
                                percentage = (count / len(df) * 100)
                                st.metric(status, f"{count:,}", f"{percentage:.1f}%")
                    
                    # Sector analysis
                    st.subheader("🌍 Sector Distribution")
                    sector_data = analyze_by_sector(df)
                    if sector_data is not None:
                        fig = px.bar(x=sector_data.values, y=sector_data.index,
                                   orientation='h',
                                   title="Work Orders by Sector",
                                   color=sector_data.values,
                                   color_continuous_scale='Viridis')
                        fig.update_layout(height=300, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                
                # Tab 2: Vehicle Analysis
                with tabs[1]:
                    st.header("Vehicle/Equipment Analysis")
                    
                    # Vehicle statistics
                    if 'Vehicle VIN' in df.columns:
                        unique_vehicles = df['Vehicle VIN'].nunique()
                        st.metric("Unique Vehicles (by VIN)", f"{unique_vehicles:,}")
                    
                    # Top vehicles
                    st.subheader("🔝 Top Vehicles by Work Orders")
                    top_vehicles = identify_top_vehicles(df, 15)
                    
                    if top_vehicles is not None and not top_vehicles.empty:
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            fig = px.bar(x=top_vehicles.values, 
                                       y=top_vehicles.index,
                                       orientation='h',
                                       title="Most Serviced Vehicle Types",
                                       color=top_vehicles.values,
                                       color_continuous_scale='Reds',
                                       labels={'x': 'Number of Work Orders', 'y': 'Vehicle Type'})
                            fig.update_layout(height=500, showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)
                        
                        with col2:
                            st.markdown("### Vehicle Rankings")
                            for i, (vehicle, count) in enumerate(top_vehicles.items(), 1):
                                percentage = (count / len(df) * 100)
                                st.markdown(f"**{i}. {vehicle[:30]}...**")
                                st.markdown(f"   {count:,} orders ({percentage:.1f}%)")
                                st.markdown("---")
                    
                    # Vehicle by status
                    st.subheader("📊 Vehicle Status Analysis")
                    if 'Vehicle Make and Model' in df.columns and 'Work order status' in df.columns:
                        # Get top 5 vehicles for cleaner visualization
                        top_5_vehicles = top_vehicles.head(5).index if top_vehicles is not None else []
                        if len(top_5_vehicles) > 0:
                            df_top_vehicles = df[df['Vehicle Make and Model'].isin(top_5_vehicles)]
                            
                            status_by_vehicle = pd.crosstab(
                                df_top_vehicles['Vehicle Make and Model'],
                                df_top_vehicles['Work order status']
                            )
                            
                            fig = px.bar(status_by_vehicle.T,
                                       title="Status Distribution for Top 5 Vehicle Types",
                                       labels={'value': 'Count', 'index': 'Status'},
                                       color_discrete_sequence=px.colors.qualitative.Set3)
                            fig.update_layout(height=400, barmode='group')
                            st.plotly_chart(fig, use_container_width=True)
                
                # Tab 3: Workshop Analysis
                with tabs[2]:
                    st.header("Workshop Performance Analysis")
                    
                    workshop_analysis = analyze_by_workshop(df)
                    
                    if workshop_analysis is not None and not workshop_analysis.empty:
                        # Overall metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Workshops", f"{len(workshop_analysis):,}")
                        with col2:
                            st.metric("Busiest Workshop", workshop_analysis.index[0][:30])
                        with col3:
                            st.metric("Max Work Orders", f"{workshop_analysis.values[0]:,}")
                        with col4:
                            avg_workload = workshop_analysis.mean()
                            st.metric("Avg Work Orders", f"{avg_workload:.0f}")
                        
                        st.subheader("📊 Workshop Workload Distribution")
                        
                        # Show top 10 workshops
                        top_workshops = workshop_analysis.head(10)
                        
                        fig = px.bar(x=top_workshops.values,
                                   y=top_workshops.index,
                                   orientation='h',
                                   title="Top 10 Workshops by Work Order Volume",
                                   color=top_workshops.values,
                                   color_continuous_scale='Blues',
                                   labels={'x': 'Number of Work Orders', 'y': 'Workshop'})
                        
                        # Truncate long workshop names for better display
                        fig.update_yaxes(tickmode='array',
                                       tickvals=list(range(len(top_workshops))),
                                       ticktext=[name[:40] + '...' if len(name) > 40 else name 
                                               for name in top_workshops.index])
                        
                        fig.update_layout(height=500, showlegend=False)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Workshop efficiency
                        st.subheader("🎯 Workshop Completion Rates")
                        if 'Work order status' in df.columns:
                            # Calculate completion rate per workshop
                            workshop_completion = []
                            
                            for workshop in top_workshops.index[:5]:  # Top 5 for clarity
                                workshop_df = df[df['Workshop name'] == workshop]
                                completed = len(workshop_df[workshop_df['Work order status'] == 'Completed'])
                                total = len(workshop_df)
                                completion_rate = (completed / total * 100) if total > 0 else 0
                                
                                workshop_completion.append({
                                    'Workshop': workshop[:30] + '...' if len(workshop) > 30 else workshop,
                                    'Completion Rate': completion_rate,
                                    'Completed': completed,
                                    'Total': total
                                })
                            
                            if workshop_completion:
                                wc_df = pd.DataFrame(workshop_completion)
                                
                                fig = px.bar(wc_df, x='Workshop', y='Completion Rate',
                                           title="Completion Rates for Top 5 Workshops",
                                           color='Completion Rate',
                                           color_continuous_scale='RdYlGn',
                                           range_color=[0, 100],
                                           text='Completion Rate')
                                
                                fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, use_container_width=True)
                
                # Tab 4: Trends
                with tabs[3]:
                    st.header("Trend Analysis")
                    
                    trend_data, status_trends = create_trend_analysis(df)
                    
                    if trend_data is not None and not trend_data.empty:
                        # Overall trend
                        st.subheader("📈 Work Orders Over Time")
                        
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=trend_data.index.astype(str),
                            y=trend_data.values,
                            mode='lines+markers',
                            name='Total Work Orders',
                            line=dict(color='#1f77b4', width=3),
                            marker=dict(size=8),
                            hovertemplate='<b>%{x}</b><br>Work Orders: %{y}<extra></extra>'
                        ))
                        
                        # Add average line
                        avg_value = trend_data.mean()
                        fig.add_hline(y=avg_value, line_dash="dash", 
                                    line_color="gray", opacity=0.7,
                                    annotation_text=f"Average: {avg_value:.0f}")
                        
                        fig.update_layout(
                            title="Monthly Work Order Volume",
                            xaxis_title="Month",
                            yaxis_title="Number of Work Orders",
                            hovermode='x unified',
                            height=400
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Trend metrics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Peak Month", str(trend_data.idxmax()))
                        with col2:
                            st.metric("Peak Volume", f"{trend_data.max():,}")
                        with col3:
                            st.metric("Average/Month", f"{trend_data.mean():.0f}")
                        with col4:
                            # Calculate trend (increasing/decreasing)
                            if len(trend_data) > 1:
                                recent_avg = trend_data.iloc[-3:].mean()
                                older_avg = trend_data.iloc[:-3].mean()
                                trend_direction = "📈" if recent_avg > older_avg else "📉"
                                st.metric("Recent Trend", trend_direction, 
                                        f"{((recent_avg/older_avg - 1) * 100):.1f}%")
                        
                        # Status trends over time
                        if status_trends is not None and not status_trends.empty:
                            st.subheader("📊 Status Trends Over Time")
                            
                            fig = go.Figure()
                            colors = ['green', 'red', 'orange', 'blue']
                            
                            for i, status in enumerate(status_trends.columns):
                                fig.add_trace(go.Scatter(
                                    x=status_trends.index.astype(str),
                                    y=status_trends[status],
                                    mode='lines+markers',
                                    name=status,
                                    line=dict(width=2),
                                    stackgroup='one'
                                ))
                            
                            fig.update_layout(
                                title="Work Order Status Distribution Over Time",
                                xaxis_title="Month",
                                yaxis_title="Number of Work Orders",
                                hovermode='x unified',
                                height=400
                            )
                            st.plotly_chart(fig, use_container_width=True)
                
                # Tab 5: Spare Parts Analysis
                with tabs[4]:
                    st.header("Spare Parts Analysis")
                    
                    spare_results = analyze_spare_parts(df)
                    
                    if spare_results:
                        # Spare parts required
                        if 'required' in spare_results:
                            st.subheader("🔧 Spare Parts Requirements")
                            
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
                                st.markdown("### Spare Parts Metrics")
                                
                                total = len(df)
                                yes_count = spare_results['required'].get('Yes / نعم', 0)
                                no_count = spare_results['required'].get('No / لا', 0)
                                
                                st.metric("Require Spare Parts", f"{yes_count:,}", 
                                        f"{(yes_count/total*100):.1f}%")
                                st.metric("No Spare Parts Needed", f"{no_count:,}",
                                        f"{(no_count/total*100):.1f}%")
                                
                                if 'received_count' in spare_results:
                                    st.metric("Spare Parts Received", 
                                            f"{spare_results['received_count']:,}",
                                            f"{(spare_results['received_count']/yes_count*100):.1f}% of required" if yes_count > 0 else "N/A")
                        
                        # Spare parts by workshop
                        st.subheader("📊 Spare Parts Requirements by Workshop")
                        if 'Workshop name' in df.columns and 'Spare parts Required' in df.columns:
                            # Get top 5 workshops
                            top_workshops = df['Workshop name'].value_counts().head(5).index
                            df_top = df[df['Workshop name'].isin(top_workshops)]
                            
                            spare_by_workshop = pd.crosstab(
                                df_top['Workshop name'],
                                df_top['Spare parts Required']
                            )
                            
                            # Truncate workshop names
                            spare_by_workshop.index = [name[:30] + '...' if len(name) > 30 else name 
                                                      for name in spare_by_workshop.index]
                            
                            fig = px.bar(spare_by_workshop,
                                       title="Spare Parts Requirements - Top 5 Workshops",
                                       labels={'value': 'Count', 'index': 'Workshop'},
                                       color_discrete_map={'Yes / نعم': '#ff4444', 'No / لا': '#44ff44'})
                            fig.update_layout(height=400, barmode='group')
                            st.plotly_chart(fig, use_container_width=True)
                
                # Tab 6: Raw Data
                with tabs[5]:
                    st.header("Raw Data Viewer")
                    
                    # Filter options
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        search_term = st.text_input("🔍 Search all columns", "")
                    
                    with col2:
                        # Filter by status
                        status_filter = st.selectbox(
                            "Filter by Status",
                            ["All"] + list(df['Work order status'].unique()) if 'Work order status' in df.columns else ["All"]
                        )
                    
                    with col3:
                        # Filter by workshop
                        workshop_filter = st.selectbox(
                            "Filter by Workshop",
                            ["All"] + list(df['Workshop name'].unique()[:20]) if 'Workshop name' in df.columns else ["All"]
                        )
                    
                    # Column selector
                    all_columns = list(df.columns)
                    default_cols = ['ID', 'Date', 'Work order status', 'Vehicle Make and Model', 
                                  'Workshop name', 'Spare parts Required']
                    default_cols = [col for col in default_cols if col in all_columns]
                    
                    selected_columns = st.multiselect(
                        "Select columns to display",
                        all_columns,
                        default=default_cols[:min(8, len(default_cols))]
                    )
                    
                    # Apply filters
                    filtered_df = df.copy()
                    
                    if search_term:
                        mask = filtered_df.astype(str).apply(
                            lambda x: x.str.contains(search_term, case=False, na=False)
                        ).any(axis=1)
                        filtered_df = filtered_df[mask]
                    
                    if status_filter != "All" and 'Work order status' in df.columns:
                        filtered_df = filtered_df[filtered_df['Work order status'] == status_filter]
                    
                    if workshop_filter != "All" and 'Workshop name' in df.columns:
                        filtered_df = filtered_df[filtered_df['Workshop name'] == workshop_filter]
                    
                    # Display data
                    if selected_columns:
                        st.dataframe(
                            filtered_df[selected_columns],
                            use_container_width=True,
                            height=600
                        )
                    else:
                        st.warning("Please select at least one column to display")
                    
                    # Statistics and download
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.info(f"Showing {len(filtered_df):,} of {len(df):,} total work orders")
                    
                    with col2:
                        # Download button
                        csv = filtered_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Filtered Data (CSV)",
                            data=csv,
                            file_name=f"fracas_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
            
            else:
                st.error("❌ Failed to load data. Please check your Excel file format.")
                
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            st.info("💡 Try clicking 'Clear Cache & Reprocess' in the sidebar")
            
            with st.expander("🔍 Show Error Details"):
                import traceback
                st.code(traceback.format_exc())
    
    else:
        # Welcome screen
        st.info("👆 Please upload a Work Orders Excel file to begin analysis")
        
        # Show sample data structure
        st.markdown("### 📖 About FRACAS")
        st.markdown("""
        This **FRACAS (Failure Reporting, Analysis, and Corrective Action System)** provides comprehensive analysis of your maintenance data:
        
        #### 📊 Key Features:
        - **Real-time Status Tracking** - Monitor work order completion rates
        - **Vehicle Analysis** - Identify high-maintenance vehicles and equipment
        - **Workshop Performance** - Evaluate workshop efficiency and workload
        - **Trend Analysis** - Track patterns over time
        - **Spare Parts Management** - Analyze parts requirements and availability
        - **Data Export** - Download filtered data for reporting
        
        ---
        
        ### 📋 Expected Data Columns:
        Your Excel file should include these columns:
        - **Work order status** - Current status (Completed, Waiting Spare Parts, etc.)
        - **Vehicle Make and Model** - Equipment/vehicle type information
        - **Workshop name** - Assigned workshop
        - **Date** - Work order creation date
        - **Spare parts Required** - Yes/No indicator
        - **Sector** - Operational sector
        - **Vehicle VIN** - Vehicle identification
        
        ### 🚀 Getting Started:
        1. Upload your Excel file using the sidebar
        2. Wait for processing (usually under 30 seconds)
        3. Explore the analysis tabs
        4. Export filtered data as needed
        """)
        
        # Show sample metrics
        with st.expander("📈 Sample Metrics You'll See"):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Work Orders", "3,225", "Sample")
            with col2:
                st.metric("Completion Rate", "41.5%", "Sample")
            with col3:
                st.metric("Waiting Parts", "55.7%", "Sample")
            with col4:
                st.metric("Active Workshops", "31", "Sample")

if __name__ == "__main__":
    main()
