"""
Sample Analytical Queries

Collection of sample SQL queries for clinical trials analysis
that can be executed from the dashboard.
"""

def get_sample_queries():
    """Get list of sample analytical queries."""
    return [
        {
            'title': 'Top 10 Sponsors by Number of Trials (2023)',
            'query': """
            SELECT 
                s.sponsor_name,
                COUNT(*) as trial_count,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration_days
            FROM fact_trials t
            JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
            JOIN dim_dates d ON t.start_date_key = d.date_key
            WHERE d.year = 2023
            GROUP BY s.sponsor_name
            ORDER BY trial_count DESC
            LIMIT 10;
            """
        },
        {
            'title': 'Most Common Trial Conditions and Average Duration',
            'query': """
            SELECT 
                c.condition_name,
                c.condition_category,
                COUNT(*) as trial_count,
                AVG(t.duration_days) as avg_duration_days,
                AVG(t.enrollment_count) as avg_enrollment,
                MIN(t.duration_days) as min_duration,
                MAX(t.duration_days) as max_duration
            FROM fact_trials t
            JOIN dim_condition c ON t.condition_key = c.condition_key
            WHERE t.duration_days IS NOT NULL
            GROUP BY c.condition_name, c.condition_category
            ORDER BY trial_count DESC
            LIMIT 20;
            """
        },
        {
            'title': 'Geographic Distribution of Trials',
            'query': """
            SELECT 
                l.country,
                l.state,
                COUNT(*) as trial_count,
                SUM(t.enrollment_count) as total_enrollment,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration
            FROM fact_trials t
            JOIN dim_location l ON t.location_key = l.location_key
            WHERE l.country IS NOT NULL
            GROUP BY l.country, l.state
            ORDER BY trial_count DESC
            LIMIT 20;
            """
        },
        {
            'title': 'Trial Status Distribution by Phase',
            'query': """
            SELECT 
                t.phase,
                t.status,
                COUNT(*) as trial_count,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration
            FROM fact_trials t
            WHERE t.phase IS NOT NULL AND t.status IS NOT NULL
            GROUP BY t.phase, t.status
            ORDER BY t.phase, trial_count DESC;
            """
        },
        {
            'title': 'Monthly Trial Activity (Last 2 Years)',
            'query': """
            SELECT 
                d.year,
                d.month_name,
                COUNT(*) as trial_count,
                AVG(t.enrollment_count) as avg_enrollment,
                SUM(t.enrollment_count) as total_enrollment
            FROM fact_trials t
            JOIN dim_dates d ON t.start_date_key = d.date_key
            WHERE d.year >= strftime('%Y', 'now') - 2
            GROUP BY d.year, d.month_name, d.month_number
            ORDER BY d.year, d.month_number;
            """
        },
        {
            'title': 'Sponsor Performance Analysis',
            'query': """
            SELECT 
                s.sponsor_name,
                s.sponsor_type,
                COUNT(*) as total_trials,
                COUNT(CASE WHEN t.status = 'Completed' THEN 1 END) as completed_trials,
                COUNT(CASE WHEN t.status = 'Recruiting' THEN 1 END) as recruiting_trials,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration,
                ROUND(
                    COUNT(CASE WHEN t.status = 'Completed' THEN 1 END) * 100.0 / COUNT(*), 2
                ) as completion_rate
            FROM fact_trials t
            JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
            GROUP BY s.sponsor_name, s.sponsor_type
            HAVING COUNT(*) >= 5
            ORDER BY total_trials DESC
            LIMIT 15;
            """
        },
        {
            'title': 'Condition Category Analysis',
            'query': """
            SELECT 
                c.condition_category,
                COUNT(*) as trial_count,
                COUNT(DISTINCT s.sponsor_key) as unique_sponsors,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration,
                SUM(t.enrollment_count) as total_enrollment
            FROM fact_trials t
            JOIN dim_condition c ON t.condition_key = c.condition_key
            JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
            WHERE c.condition_category IS NOT NULL
            GROUP BY c.condition_category
            ORDER BY trial_count DESC;
            """
        },
        {
            'title': 'Intervention Type Distribution',
            'query': """
            SELECT 
                i.intervention_type,
                COUNT(*) as trial_count,
                AVG(t.enrollment_count) as avg_enrollment,
                AVG(t.duration_days) as avg_duration,
                COUNT(DISTINCT c.condition_key) as unique_conditions
            FROM fact_trials t
            JOIN dim_intervention i ON t.intervention_key = i.intervention_key
            JOIN dim_condition c ON t.condition_key = c.condition_key
            WHERE i.intervention_type IS NOT NULL
            GROUP BY i.intervention_type
            ORDER BY trial_count DESC;
            """
        },
        {
            'title': 'Data Quality Assessment',
            'query': """
            SELECT 
                'Overall' as metric,
                COUNT(*) as total_trials,
                COUNT(CASE WHEN t.data_completeness_score >= 90 THEN 1 END) as high_quality,
                COUNT(CASE WHEN t.data_completeness_score >= 70 AND t.data_completeness_score < 90 THEN 1 END) as medium_quality,
                COUNT(CASE WHEN t.data_completeness_score < 70 THEN 1 END) as low_quality,
                AVG(t.data_completeness_score) as avg_completeness,
                AVG(t.data_quality_score) as avg_quality
            FROM fact_trials t
            
            UNION ALL
            
            SELECT 
                'By Sponsor Type' as metric,
                COUNT(*) as total_trials,
                COUNT(CASE WHEN t.data_completeness_score >= 90 THEN 1 END) as high_quality,
                COUNT(CASE WHEN t.data_completeness_score >= 70 AND t.data_completeness_score < 90 THEN 1 END) as medium_quality,
                COUNT(CASE WHEN t.data_completeness_score < 70 THEN 1 END) as low_quality,
                AVG(t.data_completeness_score) as avg_completeness,
                AVG(t.data_quality_score) as avg_quality
            FROM fact_trials t
            JOIN dim_sponsor s ON t.sponsor_key = s.sponsor_key
            WHERE s.sponsor_type = 'Industry'
            """
        },
        {
            'title': 'Trial Duration Analysis by Condition',
            'query': """
            SELECT 
                c.condition_category,
                COUNT(*) as trial_count,
                AVG(t.duration_days) as avg_duration,
                MIN(t.duration_days) as min_duration,
                MAX(t.duration_days) as max_duration
            FROM fact_trials t
            JOIN dim_condition c ON t.condition_key = c.condition_key
            WHERE t.duration_days IS NOT NULL
              AND t.duration_days > 0
            GROUP BY c.condition_category
            ORDER BY avg_duration DESC;
            """
        },
        {
            'title': 'Enrollment Trends by Year and Phase',
            'query': """
            SELECT 
                d.year,
                t.phase,
                COUNT(*) as trial_count,
                AVG(t.enrollment_count) as avg_enrollment,
                SUM(t.enrollment_count) as total_enrollment,
                COUNT(CASE WHEN t.enrollment_count > 1000 THEN 1 END) as large_trials,
                COUNT(CASE WHEN t.enrollment_count <= 100 THEN 1 END) as small_trials
            FROM fact_trials t
            JOIN dim_dates d ON t.start_date_key = d.date_key
            WHERE t.enrollment_count IS NOT NULL
              AND t.phase IS NOT NULL
              AND d.year >= 2020
            GROUP BY d.year, t.phase
            ORDER BY d.year DESC, trial_count DESC;
            """
        }
    ] 