{
    'table': 'channel_summary',
    'schema': 'ryanp3',
    'main_sql': """
SELECT channel, COUNT(*) AS total_users
FROM user_channel_log
GROUP BY channel
ORDER BY total_users DESC;
""",
    'input_check': [
        {
            'sql': 'SELECT COUNT(DISTINCT channel) FROM user_channel_log',
            'count': 5
        }
    ],
    'output_check': [
        {
            'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
            'count': 5
        }
    ],
}
