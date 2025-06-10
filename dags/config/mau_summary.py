{
    'table': 'mau_summary',
    'schema': 'ryanp3',
    'main_sql': """
SELECT LEFT(login_date, 7) AS month, COUNT(DISTINCT user_id) AS mau
FROM user_login_log
GROUP BY 1
ORDER BY 1;
""",
    'input_check': [
        {
            'sql': 'SELECT COUNT(1) FROM user_login_log',
            'count': 10000
        }
    ],
    'output_check': [
        {
            'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
            'count': 12
        }
    ],
}
