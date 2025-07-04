"""
Clinical Trials Analytics Dashboard

Streamlit application for visualizing and analyzing clinical trials data
from the dimensional data warehouse.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from utils.database import db_manager
from utils.helpers import format_number, safe_divide
from dashboard.queries import get_sample_queries

# Page configuration
st.set_page_config(
    page_title="Clinical Trials Analytics",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .metric-value, .metric-name {
        color: #000 !important;
    }
    .sidebar-header {
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_dashboard_data():
    """Load data for dashboard."""
    try:
        # Load key metrics
        metrics_query = """
        SELECT 
            COUNT(*) as total_trials,
            COUNT(DISTINCT sponsor_key) as unique_sponsors,
            COUNT(DISTINCT condition_key) as unique_conditions,
            COUNT(DISTINCT location_key) as unique_locations,
            AVG(enrollment_count) as avg_enrollment,
            AVG(duration_days) as avg_duration
        FROM fact_trials
        """
        metrics_df = db_manager.execute_query(metrics_query)
        
        # Load sponsor data
        sponsor_query = """
        SELECT 
            s.sponsor_name,
            COUNT(*) as trial_count,
            AVG(t.enrollment_count) as avg_enrollment,
            AVG(t.duration_days) as avg_duration
        FROM fact_trials t
        JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
        GROUP BY s.sponsor_name
        ORDER BY trial_count DESC
        LIMIT 20
        """
        sponsor_df = db_manager.execute_query(sponsor_query)
        
        # Load condition data
        condition_query = """
        SELECT 
            c.condition_name,
            c.condition_category,
            COUNT(*) as trial_count,
            AVG(t.enrollment_count) as avg_enrollment,
            AVG(t.duration_days) as avg_duration
        FROM fact_trials t
        JOIN dim_condition c ON t.condition_key = c.condition_key
        GROUP BY c.condition_name, c.condition_category
        ORDER BY trial_count DESC
        LIMIT 20
        """
        condition_df = db_manager.execute_query(condition_query)
        
        # Load geographic data
        location_query = """
        SELECT 
            l.country,
            l.state,
            COUNT(*) as trial_count,
            SUM(t.enrollment_count) as total_enrollment
        FROM fact_trials t
        JOIN dim_location l ON t.location_key = l.location_key
        WHERE l.country IS NOT NULL
        GROUP BY l.country, l.state
        ORDER BY trial_count DESC
        LIMIT 20
        """
        location_df = db_manager.execute_query(location_query)
        
        # Load temporal data
        temporal_query = """
        SELECT 
            d.year,
            d.month_name,
            COUNT(*) as trial_count,
            AVG(t.enrollment_count) as avg_enrollment
        FROM fact_trials t
        JOIN dim_dates d ON t.start_date_key = d.date_key
        WHERE d.year >= 2020
        GROUP BY d.year, d.month_name, d.month_number
        ORDER BY d.year, d.month_number
        """
        temporal_df = db_manager.execute_query(temporal_query)
        
        return {
            'metrics': metrics_df,
            'sponsors': sponsor_df,
            'conditions': condition_df,
            'locations': location_df,
            'temporal': temporal_df
        }
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def display_header():
    """Display dashboard header."""
    st.markdown('<h1 class="main-header">🏥 Clinical Trials Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("---")

def display_metrics(data):
    """Display key metrics."""
    if data is None or data['metrics'].empty:
        return
    
    metrics = data['metrics'].iloc[0]
    
    def safe_metric(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return 'N/A (no valid data)'
        return format_number(val, 'comma')
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3 class="metric-name">Total Trials</h3>
            <h2 class="metric-value">{safe_metric(metrics['total_trials'])}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3 class="metric-name">Unique Sponsors</h3>
            <h2 class="metric-value">{safe_metric(metrics['unique_sponsors'])}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3 class="metric-name">Avg Enrollment</h3>
            <h2 class="metric-value">{safe_metric(metrics['avg_enrollment'])}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3 class="metric-name">Avg Duration (Days)</h3>
            <h2 class="metric-value">{safe_metric(metrics['avg_duration'])}</h2>
        </div>
        """, unsafe_allow_html=True)

def display_sponsor_analysis(data):
    """Display sponsor analysis."""
    if data is None or data['sponsors'].empty:
        return
    
    st.subheader("📊 Top Sponsors by Trial Count")
    
    # Create bar chart
    fig = px.bar(
        data['sponsors'].head(10),
        x='trial_count',
        y='sponsor_name',
        orientation='h',
        title="Top 10 Sponsors by Number of Trials",
        labels={'trial_count': 'Number of Trials', 'sponsor_name': 'Sponsor'}
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Display table
    st.dataframe(
        data['sponsors'].head(10)[['sponsor_name', 'trial_count', 'avg_enrollment', 'avg_duration']],
        use_container_width=True
    )

def display_condition_analysis(data):
    """Display condition analysis."""
    if data is None or data['conditions'].empty:
        return
    
    st.subheader("🏥 Medical Conditions Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Condition category distribution
        category_data = data['conditions'].groupby('condition_category').agg({
            'trial_count': 'sum',
            'avg_enrollment': 'mean'
        }).reset_index()
        
        fig = px.pie(
            category_data,
            values='trial_count',
            names='condition_category',
            title="Trial Distribution by Condition Category"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Top conditions
        fig = px.bar(
            data['conditions'].head(10),
            x='trial_count',
            y='condition_name',
            orientation='h',
            title="Top 10 Conditions by Trial Count",
            labels={'trial_count': 'Number of Trials', 'condition_name': 'Condition'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def display_geographic_analysis(data):
    """Display geographic analysis."""
    if data is None or data['locations'].empty:
        return
    
    st.subheader("🌍 Geographic Distribution")
    
    # Country-level analysis
    country_data = data['locations'].groupby('country').agg({
        'trial_count': 'sum',
        'total_enrollment': 'sum'
    }).reset_index().sort_values('trial_count', ascending=False)
    
    fig = px.bar(
        country_data.head(15),
        x='trial_count',
        y='country',
        orientation='h',
        title="Top 15 Countries by Trial Count",
        labels={'trial_count': 'Number of Trials', 'country': 'Country'}
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

def display_temporal_analysis(data):
    """Display temporal analysis."""
    if data is None or data['temporal'].empty:
        return
    
    st.subheader("📈 Temporal Trends")
    
    # Prepare data for time series
    temporal_data = data['temporal'].copy()
    temporal_data['date'] = pd.to_datetime(temporal_data['year'].astype(str) + '-' + 
                                         temporal_data['month_name'] + '-01')
    
    # Time series chart
    fig = px.line(
        temporal_data,
        x='date',
        y='trial_count',
        title="Trial Count Over Time",
        labels={'trial_count': 'Number of Trials', 'date': 'Date'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

def display_sample_queries():
    """Display sample analytical queries."""
    st.subheader("🔍 Sample Analytical Queries")
    
    queries = get_sample_queries()
    
    for i, query_info in enumerate(queries):
        with st.expander(f"Query {i+1}: {query_info['title']}"):
            st.code(query_info['query'], language='sql')
            
            if st.button(f"Run Query {i+1}", key=f"run_query_{i}"):
                try:
                    result = db_manager.execute_query(query_info['query'])
                    st.dataframe(result, use_container_width=True)
                except Exception as e:
                    st.error(f"Error executing query: {e}")

def main():
    """Main dashboard function."""
    display_header()
    
    # Load data
    with st.spinner("Loading dashboard data..."):
        data = load_dashboard_data()
    
    if data is None:
        st.error("Failed to load dashboard data. Please check your database connection.")
        return
    
    # Display metrics
    display_metrics(data)
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Sponsor Analysis", 
        "🏥 Condition Analysis", 
        "🌍 Geographic Analysis",
        "📈 Temporal Trends",
        "🔍 Sample Queries"
    ])
    
    with tab1:
        display_sponsor_analysis(data)
    
    with tab2:
        display_condition_analysis(data)
    
    with tab3:
        display_geographic_analysis(data)
    
    with tab4:
        display_temporal_analysis(data)
    
    with tab5:
        display_sample_queries()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "**Data Source:** ClinicalTrials.gov API | "
        "**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

if __name__ == "__main__":
    main() 