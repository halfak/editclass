SELECT
  MAX(IF(page_namespace = 0, page_id, NULL)) AS page_id,
  page_title,
  SUM(page_namespace = 0) AS article_edits,
  SUM(page_namespace = 0 AND bot.ug_user IS NOT NULL) AS article_bot_edits,
  COUNT(DISTINCT IF(page_namespace = 0 AND bot.ug_user IS NOT NULL, revision.rev_user, NULL)) AS article_bot_editors,
  SUM(page_namespace = 0 AND revision.rev_user > 0 AND bot.ug_user IS NULL) AS article_registered_edits,
  COUNT(DISTINCT IF(page_namespace = 0 AND revision.rev_user > 0 and bot.ug_user IS NULL, revision.rev_user, NULL)) AS article_registered_editors,
  SUM(page_namespace = 0 AND revision.rev_user = 0) AS article_anon_edits,
  COUNT(DISTINCT IF(page_namespace = 0 AND revision.rev_user = 0, revision.rev_user_text, NULL)) AS article_anon_editors,
  SUM(IF(page_namespace = 0, ABS(CAST(revision.rev_len AS INT) - CAST(parent.rev_len AS INT)), 0)) AS article_sum_bytes_changed,
  SUM(page_namespace = 0 AND revision.rev_len > parent.rev_len) AS article_insertion_edits,
  SUM(IF(page_namespace = 0 AND revision.rev_len > parent.rev_len, revision.rev_len - parent.rev_len, 0)) AS article_bytes_added,
  SUM(page_namespace = 0 AND revision.rev_len < parent.rev_len) AS article_removal_edits,
  SUM(IF(page_namespace = 0 AND revision.rev_len < parent.rev_len, parent.rev_len - revision.rev_len, 0)) AS article_bytes_removed,
  SUM(page_namespace = 1) AS talk_edits,
  SUM(page_namespace = 1 AND bot.ug_user IS NOT NULL) AS talk_bot_edits,
  COUNT(DISTINCT IF(page_namespace = 1 AND bot.ug_user IS NOT NULL, revision.rev_user, NULL)) AS talk_bot_editors,
  SUM(page_namespace = 0 AND revision.rev_user > 1 AND bot.ug_user IS NULL) AS talk_registered_edits,
  COUNT(DISTINCT IF(page_namespace = 1 AND revision.rev_user > 0 and bot.ug_user IS NULL, revision.rev_user, NULL)) AS talk_registered_editors,
  SUM(page_namespace = 1 AND revision.rev_user = 0) AS talk_anon_edits,
  COUNT(DISTINCT IF(page_namespace = 1 AND revision.rev_user = 0, revision.rev_user_text, NULL)) AS talk_anon_editors,
  SUM(IF(page_namespace = 1, ABS(CAST(revision.rev_len AS INT) - CAST(parent.rev_len AS INT)), 0)) AS talk_sum_bytes_changed,
  SUM(page_namespace = 1 AND revision.rev_len > parent.rev_len) AS talk_insertion_edits,
  SUM(IF(page_namespace = 1 AND revision.rev_len > parent.rev_len, revision.rev_len - parent.rev_len, 0)) AS talk_bytes_added,
  SUM(page_namespace = 1 AND revision.rev_len < parent.rev_len) AS talk_removal_edits,
  SUM(IF(page_namespace = 1 AND revision.rev_len < parent.rev_len, parent.rev_len - revision.rev_len, 0)) AS talk_bytes_removed
FROM revision
LEFT JOIN revision AS parent ON
  parent.rev_id = revision.rev_parent_id
INNER JOIN page ON
  revision.rev_page = page_id
LEFT JOIN user_groups bot ON
  revision.rev_user = ug_user AND
  ug_group = "bot"
WHERE
    page_namespace IN (0, 1) AND
    revision.rev_timestamp BETWEEN "201501" AND "201507"
GROUP BY page_title;
