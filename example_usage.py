"""
FRACAS System - Example Usage Script
This script demonstrates how to use the FRACAS functions programmatically
"""

import pandas as pd
import sys

# Example: Load and analyze work orders programmatically
def analyze_work_orders(file_path):
    """
    Example function showing how to use FRACAS analysis functions
    outside of the Streamlit interface
    """
    
    print("="*60)
    print("FRACAS System - Programmatic Analysis Example")
    print("="*60)
    
    # Import the parse function from the main app
    from fracas_app import (
        parse_work_orders, 
        calculate_failure_metrics,
        identify_top_failures,
        analyze_by_workshop,
        analyze_by_sector
    )
    
    # Load data
    print("\n1. Loading work orders...")
    df = parse_work_orders(file_path)
    
    if df is None:
        print("❌ Failed to load work orders")
        return
    
    print(f"✅ Loaded {len(df)} work orders")
    print(f"📊 Columns: {len(df.columns)}")
    
    # Calculate metrics
    print("\n2. Calculating metrics...")
    metrics = calculate_failure_metrics(df)
    
    print(f"\n📈 KEY METRICS:")
    print(f"   • Total Work Orders: {metrics.get('total_work_orders', 0)}")
    print(f"   • Completed: {metrics.get('completed', 0)}")
    print(f"   • In Progress: {metrics.get('in_progress', 0)}")
    print(f"   • Waiting Parts: {metrics.get('waiting_parts', 0)}")
    print(f"   • Completion Rate: {metrics.get('completion_rate', 0):.1f}%")
    
    # Top failures
    print("\n3. Identifying top failures...")
    top_failures = identify_top_failures(df, limit=5)
    
    if top_failures is not None:
        print(f"\n🔴 TOP 5 FAILURES:")
        for idx, (vehicle, count) in enumerate(top_failures.items(), 1):
            print(f"   {idx}. {vehicle[:50]}... : {count} orders")
    
    # Workshop analysis
    print("\n4. Analyzing workshops...")
    workshop_data = analyze_by_workshop(df)
    
    if workshop_data is not None:
        print(f"\n🏭 WORKSHOP SUMMARY:")
        print(f"   • Total Workshops: {len(workshop_data)}")
        print(f"   • Busiest Workshop: {workshop_data.index[0][:40]}...")
        print(f"   • Max Workload: {workshop_data.values[0]} orders")
        print(f"   • Avg Workload: {workshop_data.mean():.1f} orders/workshop")
    
    # Sector analysis
    print("\n5. Analyzing sectors...")
    sector_data = analyze_by_sector(df)
    
    if sector_data is not None:
        print(f"\n🗺️  SECTOR DISTRIBUTION:")
        for sector, count in sector_data.items():
            print(f"   • {sector}: {count} orders")
    
    print("\n" + "="*60)
    print("✅ Analysis Complete!")
    print("="*60)
    
    return df, metrics


def generate_summary_report(df, metrics):
    """
    Generate a text-based summary report
    """
    report = []
    report.append("\n" + "="*70)
    report.append("FRACAS SYSTEM - EXECUTIVE SUMMARY REPORT")
    report.append("="*70)
    
    # Overview
    report.append("\n📊 OVERVIEW")
    report.append(f"   Total Work Orders: {metrics.get('total_work_orders', 0)}")
    report.append(f"   Completion Rate: {metrics.get('completion_rate', 0):.1f}%")
    report.append(f"   Status Breakdown:")
    report.append(f"      - Completed: {metrics.get('completed', 0)}")
    report.append(f"      - In Progress: {metrics.get('in_progress', 0)}")
    report.append(f"      - Waiting Parts: {metrics.get('waiting_parts', 0)}")
    
    # Date range
    date_col = None
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            date_col = col
            break
    
    if date_col:
        report.append(f"\n📅 DATE RANGE")
        report.append(f"   From: {df[date_col].min()}")
        report.append(f"   To: {df[date_col].max()}")
    
    report.append("\n" + "="*70)
    
    return "\n".join(report)


def export_to_csv(df, output_file='fracas_export.csv'):
    """
    Export analysis results to CSV
    """
    try:
        df.to_csv(output_file, index=False)
        print(f"✅ Data exported to {output_file}")
        return True
    except Exception as e:
        print(f"❌ Export failed: {str(e)}")
        return False


# Main execution
if __name__ == "__main__":
    """
    Run this script from command line:
    python example_usage.py path/to/your/Work_Orders.xlsx
    """
    
    if len(sys.argv) < 2:
        print("Usage: python example_usage.py <path_to_work_orders_file>")
        print("\nExample:")
        print("  python example_usage.py Work_Orders.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    # Perform analysis
    df, metrics = analyze_work_orders(file_path)
    
    # Generate summary report
    report = generate_summary_report(df, metrics)
    print(report)
    
    # Optional: Export to CSV
    export_choice = input("\n📤 Export data to CSV? (y/n): ")
    if export_choice.lower() == 'y':
        export_to_csv(df)
    
    print("\n✅ Done! Run 'streamlit run fracas_app.py' for interactive analysis.")
