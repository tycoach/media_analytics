-- Analysis Queries for Media Analytics Platform

-- Query to calculate unique visitors per day
SELECT 
    event_date,
    COUNT(DISTINCT user_id) AS unique_visitors
FROM 
    user_interactions
GROUP BY 
    event_date
ORDER BY 
    event_date;

----Save it in a VIEW
CREATE OR REPLACE VIEW vw_daily_visitors AS
SELECT 
    event_date,
    COUNT(DISTINCT user_id) AS unique_visitors
FROM 
    user_interactions
GROUP BY 
    event_date
ORDER BY 
    event_date;


-- Query to find most popular articles based on user interactions
SELECT 
    article_id,
    content_category,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT user_id) AS unique_viewers,
    COUNT(*) FILTER (WHERE action = 'read') AS read_count,
    COUNT(*) FILTER (WHERE action = 'share') AS share_count,
    COUNT(*) FILTER (WHERE action = 'like') AS like_count,
    COUNT(*) FILTER (WHERE action = 'comment') AS comment_count,
    CAST(AVG(COALESCE(time_spent_seconds, 0)) AS NUMERIC(10,2)) AS avg_time_spent_seconds
FROM 
    user_interactions
WHERE 
    article_id IS NOT NULL
GROUP BY 
    article_id, content_category
ORDER BY 
    total_interactions DESC
LIMIT 20;

---Save it in a view
CREATE OR REPLACE VIEW vw_popular_articles AS
SELECT 
    article_id,
    content_category,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT user_id) AS unique_viewers,
    COUNT(*) FILTER (WHERE action = 'read') AS read_count,
    COUNT(*) FILTER (WHERE action = 'share') AS share_count,
    COUNT(*) FILTER (WHERE action = 'like') AS like_count,
    COUNT(*) FILTER (WHERE action = 'comment') AS comment_count,
    CAST(AVG(COALESCE(time_spent_seconds, 0)) AS NUMERIC(10,2)) AS avg_time_spent_seconds
FROM 
    user_interactions
WHERE 
    article_id IS NOT NULL
GROUP BY 
    article_id, content_category
ORDER BY 
    total_interactions DESC;

--  Calculate engagement by content category
SELECT 
    content_category,
    COUNT(*) AS total_interactions,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) / COUNT(DISTINCT user_id)::FLOAT AS avg_interactions_per_user
FROM 
    user_interactions
GROUP BY 
    content_category
ORDER BY 
    total_interactions DESC;
