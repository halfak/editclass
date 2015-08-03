SELECT 
    page_id, 
    page_title, 
    MIN(rev_id) AS start_rev_id, 
    MAX(rev_id) AS end_rev_id,
    COUNT(*) AS revisions
FROM revision
INNER JOIN page ON
    rev_page = page_id
WHERE 
    page_namespace = 0 AND
    rev_timestamp BETWEEN "201501" AND "201507"
GROUP BY page_id, page_title;
